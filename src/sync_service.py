from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from src.db import db_session
from src.models import JobRun, OrderSync, SyncEvent, Tenant
from src.susoft_client import (
    authenticate as susoft_authenticate,
    create_order as create_susoft_order,
    find_cart_by_uuid,
    find_order_by_uuid,
)
from src.tripletex_client import create_session_token, fetch_open_orders
from tripletex_invoice_payment_flow import (
    AlreadyInvoicedError,
    build_basic_headers,
    create_invoice,
    register_payment,
)


TRIPLETEX_TO_SUSOFT_PRODUCT_ID_MAP: dict[str, str] = {
    "69775686": "10002",  # Susoft M10
}


def _add_event(
    session: Session,
    *,
    tenant_id: int,
    event_type: str,
    message: str,
    level: str = "INFO",
    job_run_id: int | None = None,
    order_sync_id: int | None = None,
    details: dict[str, Any] | None = None,
) -> None:
    event = SyncEvent(
        tenant_id=tenant_id,
        job_run_id=job_run_id,
        order_sync_id=order_sync_id,
        event_type=event_type,
        level=level,
        message=message,
        details_json=json.dumps(details, ensure_ascii=False) if details is not None else None,
    )
    session.add(event)


def _upsert_order_sync(session: Session, tenant_id: int, order_payload: dict[str, Any]) -> OrderSync:
    tripletex_order_id = str(order_payload.get("id", ""))
    if not tripletex_order_id:
        raise RuntimeError("Order mangler id i Tripletex-respons.")

    existing = session.scalar(
        select(OrderSync).where(
            OrderSync.tenant_id == tenant_id,
            OrderSync.tripletex_order_id == tripletex_order_id,
        )
    )

    payload_json = json.dumps(order_payload, ensure_ascii=False)
    now = datetime.now(UTC)

    if existing is None:
        created = OrderSync(
            tenant_id=tenant_id,
            tripletex_order_id=tripletex_order_id,
            status="DISCOVERED",
            payload_json=payload_json,
            updated_at=now,
        )
        session.add(created)
        session.flush()
        return created

    existing.payload_json = payload_json
    existing.updated_at = now
    session.flush()
    return existing


def _safe_float(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value))
    except Exception:
        return 0.0


def _is_tripletex_order_open(order_payload: dict[str, Any]) -> bool:
    value = order_payload.get("isClosed")
    if isinstance(value, bool):
        return not value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes"}:
            return False
        if normalized in {"false", "0", "no"}:
            return True
    # Backward compatibility: if the field is absent, keep prior behavior.
    return True


def _is_locally_handled(order_sync: OrderSync) -> bool:
    return order_sync.status in {"PUSHED_TO_SUSOFT", "TT_PAID", "TT_INVOICE_EXISTS"} and bool(order_sync.susoft_uuid)


def _get_tenant_or_raise(session: Session, tenant_key: str) -> Tenant:
    tenant = session.scalar(select(Tenant).where(Tenant.tenant_key == tenant_key))
    if tenant is None:
        raise RuntimeError(f"Tenant finnes ikke: {tenant_key}")
    if not tenant.active:
        raise RuntimeError(f"Tenant er inaktiv: {tenant_key}")
    return tenant


def _tripletex_overrides_for_tenant(tenant: Tenant) -> dict[str, str]:
    data: dict[str, str] = {}
    if tenant.tripletex_base_url:
        data["tripletex_base_url"] = tenant.tripletex_base_url
    if tenant.tripletex_consumer_token:
        data["tripletex_consumer_token"] = tenant.tripletex_consumer_token
    if tenant.tripletex_employee_token:
        data["tripletex_employee_token"] = tenant.tripletex_employee_token
    return data


def _susoft_overrides_for_tenant(tenant: Tenant) -> dict[str, str]:
    data: dict[str, str] = {}
    if tenant.susoft_base_url:
        data["susoft_base_url"] = tenant.susoft_base_url
    if tenant.susoft_shop_url_key:
        data["susoft_shop_url_key"] = tenant.susoft_shop_url_key
    if tenant.susoft_username:
        data["susoft_username"] = tenant.susoft_username
    if tenant.susoft_password:
        data["susoft_password"] = tenant.susoft_password
    return data


def _find_order_sync_by_tripletex_order_id(session: Session, tenant_id: int, tripletex_order_id: str) -> OrderSync | None:
    return session.scalar(
        select(OrderSync).where(
            OrderSync.tenant_id == tenant_id,
            OrderSync.tripletex_order_id == tripletex_order_id,
        )
    )


def _find_order_sync_by_susoft_uuid(session: Session, tenant_id: int, susoft_uuid: str) -> OrderSync | None:
    return session.scalar(
        select(OrderSync).where(
            OrderSync.tenant_id == tenant_id,
            OrderSync.susoft_uuid == susoft_uuid,
        )
    )


def process_tripletex_order_for_tenant(
    tenant_key: str,
    order_payload: dict[str, Any],
    *,
    dry_run: bool = False,
    job_run_id: int | None = None,
) -> dict[str, Any]:
    if not _is_tripletex_order_open(order_payload):
        raise RuntimeError("Tripletex-ordre er lukket og skal ikke pushes til Susoft")

    with db_session() as session:
        tenant = _get_tenant_or_raise(session, tenant_key)
        susoft_overrides = _susoft_overrides_for_tenant(tenant)

        order_sync = _upsert_order_sync(session, tenant.id, order_payload)
        _add_event(
            session,
            tenant_id=tenant.id,
            job_run_id=job_run_id,
            order_sync_id=order_sync.id,
            event_type="ORDER_DISCOVERED",
            message="Ordre mottatt fra Tripletex webhook",
            details={"tripletex_order_id": order_sync.tripletex_order_id},
        )

        if dry_run:
            session.commit()
            return {
                "tenant_key": tenant_key,
                "tripletex_order_id": order_sync.tripletex_order_id,
                "status": order_sync.status,
                "pushed_to_susoft": False,
                "message": "dry_run",
            }

        if _is_locally_handled(order_sync):
            _add_event(
                session,
                tenant_id=tenant.id,
                job_run_id=job_run_id,
                order_sync_id=order_sync.id,
                event_type="ORDER_SKIPPED_ALREADY_HANDLED",
                message="Ordre er allerede handtert lokalt, hopper over ny push",
                details={
                    "tripletex_order_id": order_sync.tripletex_order_id,
                    "status": order_sync.status,
                    "susoft_uuid": order_sync.susoft_uuid,
                },
            )
            session.commit()
            return {
                "tenant_key": tenant_key,
                "tripletex_order_id": order_sync.tripletex_order_id,
                "status": order_sync.status,
                "pushed_to_susoft": False,
                "message": "already_handled",
            }

        ok = _push_order_to_susoft(
            session,
            tenant_id=tenant.id,
            job_run_id=job_run_id or 0,
            order_sync=order_sync,
            order_payload=order_payload,
            susoft_overrides=susoft_overrides,
        )
        session.commit()
        return {
            "tenant_key": tenant_key,
            "tripletex_order_id": order_sync.tripletex_order_id,
            "status": order_sync.status,
            "pushed_to_susoft": ok,
            "susoft_uuid": order_sync.susoft_uuid,
            "message": order_sync.last_error or "ok",
        }


def process_susoft_payment_for_tenant(
    tenant_key: str,
    susoft_uuid: str,
    *,
    payment_type_id: int,
    paid_amount: float | None = None,
    payment_date: str | None = None,
    job_run_id: int | None = None,
) -> dict[str, Any]:
    with db_session() as session:
        tenant = _get_tenant_or_raise(session, tenant_key)
        susoft_overrides = _susoft_overrides_for_tenant(tenant)
        tripletex_overrides = _tripletex_overrides_for_tenant(tenant)
        token = susoft_authenticate(overrides=susoft_overrides)
        order = find_order_by_uuid(susoft_uuid, token=token, overrides=susoft_overrides)
        cart = find_cart_by_uuid(susoft_uuid, token=token, overrides=susoft_overrides)

        row = _find_order_sync_by_susoft_uuid(session, tenant.id, susoft_uuid)
        if row is None:
            tt_candidate = None
            if isinstance(order, dict):
                alt = order.get("alternativeId")
                tt_candidate = str(alt).strip() if alt is not None else None

            if not tt_candidate and isinstance(cart, dict):
                ext = cart.get("externalRef")
                tt_candidate = str(ext).strip() if ext is not None else None

            if tt_candidate:
                row = _find_order_sync_by_tripletex_order_id(session, tenant.id, tt_candidate)
                if row is not None and not row.susoft_uuid:
                    row.susoft_uuid = susoft_uuid
                    row.updated_at = datetime.now(UTC)

        if row is None:
            raise RuntimeError(
                f"Fant ingen lokal ordre for Susoft UUID {susoft_uuid} "
                f"(alternativeId={order.get('alternativeId') if isinstance(order, dict) else None}, "
                f"externalRef={cart.get('externalRef') if isinstance(cart, dict) else None})"
            )

        if order is None:
            _add_event(
                session,
                tenant_id=tenant.id,
                job_run_id=job_run_id,
                order_sync_id=row.id,
                event_type="SUSOFT_ORDER_NOT_READY",
                level="INFO",
                message="Susoft-order finnes ikke enda, venter",
                details={"susoft_uuid": susoft_uuid, "cart_exists": cart is not None},
            )
            session.commit()
            return {
                "tenant_key": tenant_key,
                "susoft_uuid": susoft_uuid,
                "status": row.status,
                "matched": False,
                "message": "not_ready",
            }

        payments = order.get("payments") if isinstance(order.get("payments"), list) else []
        resolved_paid_amount = paid_amount
        if resolved_paid_amount is None:
            resolved_paid_amount = 0.0
            for payment in payments:
                if isinstance(payment, dict):
                    resolved_paid_amount += _safe_float(payment.get("amount"))

        if resolved_paid_amount <= 0:
            _add_event(
                session,
                tenant_id=tenant.id,
                job_run_id=job_run_id,
                order_sync_id=row.id,
                event_type="SUSOFT_ORDER_NOT_PAID",
                message="Susoft order har ingen registrert betaling enda",
                details={"susoft_uuid": susoft_uuid},
            )
            session.commit()
            return {
                "tenant_key": tenant_key,
                "susoft_uuid": susoft_uuid,
                "status": row.status,
                "matched": False,
                "message": "not_paid",
            }

        tt_order_id_raw = order.get("alternativeId")
        try:
            tt_order_id = int(str(tt_order_id_raw)) if tt_order_id_raw is not None else None
        except (TypeError, ValueError):
            tt_order_id = None
        if tt_order_id is None:
            row.status = "TT_PAYMENT_FAILED"
            row.last_error = "Susoft order mangler alternativeId mot TT order id"
            row.updated_at = datetime.now(UTC)
            _add_event(
                session,
                tenant_id=tenant.id,
                job_run_id=job_run_id,
                order_sync_id=row.id,
                event_type="SUSOFT_MISSING_ALTERNATIVE_ID",
                level="ERROR",
                message="Susoft order mangler alternativeId",
                details={"susoft_uuid": susoft_uuid},
            )
            session.commit()
            return {
                "tenant_key": tenant_key,
                "susoft_uuid": susoft_uuid,
                "status": row.status,
                "matched": False,
                "message": row.last_error,
            }

        try:
            today = payment_date or datetime.now(UTC).date().isoformat()
            tt_headers = build_basic_headers(create_session_token(overrides=tripletex_overrides))
            invoice_id = create_invoice(order_id=tt_order_id, invoice_date=today, headers=tt_headers, dry_run=False)
            register_payment(
                invoice_id=invoice_id,
                payment_date=today,
                payment_type_id=payment_type_id,
                paid_amount=resolved_paid_amount,
                headers=tt_headers,
                dry_run=False,
            )
            row.status = "TT_PAID"
            row.last_error = None
            row.updated_at = datetime.now(UTC)
            _add_event(
                session,
                tenant_id=tenant.id,
                job_run_id=job_run_id,
                order_sync_id=row.id,
                event_type="TT_PAYMENT_REGISTERED",
                message="Betaling registrert i Tripletex basert på webhook",
                details={
                    "tripletex_order_id": tt_order_id,
                    "susoft_uuid": susoft_uuid,
                    "paid_amount": resolved_paid_amount,
                    "payment_type_id": payment_type_id,
                },
            )
            session.commit()
            return {
                "tenant_key": tenant_key,
                "susoft_uuid": susoft_uuid,
                "tripletex_order_id": tt_order_id,
                "status": row.status,
                "matched": True,
                "invoice_id": invoice_id,
                "paid_amount": resolved_paid_amount,
            }
        except AlreadyInvoicedError:
            row.status = "TT_INVOICE_EXISTS"
            row.last_error = None
            row.updated_at = datetime.now(UTC)
            _add_event(
                session,
                tenant_id=tenant.id,
                job_run_id=job_run_id,
                order_sync_id=row.id,
                event_type="TT_INVOICE_EXISTS",
                level="WARNING",
                message="Tripletex-ordre er allerede fakturert",
                details={"tripletex_order_id": tt_order_id, "susoft_uuid": susoft_uuid},
            )
            session.commit()
            return {
                "tenant_key": tenant_key,
                "susoft_uuid": susoft_uuid,
                "tripletex_order_id": tt_order_id,
                "status": row.status,
                "matched": True,
                "message": "already_invoiced",
            }
        except Exception as exc:
            row.status = "TT_PAYMENT_FAILED"
            row.last_error = str(exc)
            row.updated_at = datetime.now(UTC)
            _add_event(
                session,
                tenant_id=tenant.id,
                job_run_id=job_run_id,
                order_sync_id=row.id,
                event_type="TT_PAYMENT_SYNC_FAILED",
                level="ERROR",
                message="Kunne ikke registrere betaling i Tripletex",
                details={"tripletex_order_id": tt_order_id, "error": str(exc)},
            )
            session.commit()
            return {
                "tenant_key": tenant_key,
                "susoft_uuid": susoft_uuid,
                "tripletex_order_id": tt_order_id,
                "status": row.status,
                "matched": True,
                "message": str(exc),
            }


def find_tripletex_order_by_id(order_id: int, *, tripletex_overrides: dict[str, str] | None = None) -> dict[str, Any] | None:
    token = create_session_token(overrides=tripletex_overrides)
    payload = fetch_open_orders(token, overrides=tripletex_overrides)
    values = payload.get("values") if isinstance(payload, dict) else []
    orders = values if isinstance(values, list) else []
    for order in orders:
        if isinstance(order, dict) and str(order.get("id")) == str(order_id):
            return order
    return None


def process_tripletex_order_by_id_for_tenant(
    tenant_key: str,
    order_id: int,
    *,
    dry_run: bool = False,
    job_run_id: int | None = None,
) -> dict[str, Any]:
    with db_session() as session:
        tenant = _get_tenant_or_raise(session, tenant_key)
        tripletex_overrides = _tripletex_overrides_for_tenant(tenant)
    order = find_tripletex_order_by_id(order_id, tripletex_overrides=tripletex_overrides)
    if order is None:
        raise RuntimeError(f"Fant ikke Tripletex-ordre {order_id} i åpne ordrer")
    if not _is_tripletex_order_open(order):
        raise RuntimeError(f"Tripletex-ordre {order_id} er lukket og kan ikke sendes til Susoft")
    return process_tripletex_order_for_tenant(tenant_key, order, dry_run=dry_run, job_run_id=job_run_id)


def get_sendable_orders_for_tenant(tenant_key: str, *, limit: int) -> dict[str, Any]:
    with db_session() as session:
        tenant = session.scalar(select(Tenant).where(Tenant.tenant_key == tenant_key))
        if tenant is None:
            raise RuntimeError(f"Tenant finnes ikke: {tenant_key}")
        if not tenant.active:
            raise RuntimeError(f"Tenant er inaktiv: {tenant_key}")

        tripletex_overrides = _tripletex_overrides_for_tenant(tenant)
        token = create_session_token(overrides=tripletex_overrides)
        payload = fetch_open_orders(token, overrides=tripletex_overrides)
        values = payload.get("values")
        orders = values if isinstance(values, list) else []

        sendable_orders: list[dict[str, Any]] = []
        already_handled_orders: list[dict[str, Any]] = []
        failed_orders: list[dict[str, Any]] = []

        for order in orders[:limit]:
            if not isinstance(order, dict):
                continue

            if not _is_tripletex_order_open(order):
                continue

            tripletex_order_id = str(order.get("id", "")).strip()
            if not tripletex_order_id:
                continue

            order_sync = session.scalar(
                select(OrderSync).where(
                    OrderSync.tenant_id == tenant.id,
                    OrderSync.tripletex_order_id == tripletex_order_id,
                )
            )

            item = {
                "tripletex_order_id": tripletex_order_id,
                "order_number": order.get("number"),
                "order_date": order.get("orderDate"),
                "local_status": order_sync.status if order_sync is not None else None,
                "susoft_uuid": order_sync.susoft_uuid if order_sync is not None else None,
            }

            if order_sync is None:
                sendable_orders.append(item)
            elif _is_locally_handled(order_sync):
                already_handled_orders.append(item)
            elif order_sync.status == "FAILED":
                failed_orders.append(item)
            else:
                sendable_orders.append(item)

        return {
            "tenant_key": tenant_key,
            "fetched_total": len(orders),
            "evaluated": min(len(orders), limit),
            "sendable_count": len(sendable_orders),
            "already_handled_count": len(already_handled_orders),
            "failed_count": len(failed_orders),
            "sendable_orders": sendable_orders,
            "already_handled_orders": already_handled_orders,
            "failed_orders": failed_orders,
        }


def _build_susoft_order_payload(order_payload: dict[str, Any]) -> dict[str, Any]:
    tripletex_order_id = str(order_payload.get("id", "")).strip()
    if not tripletex_order_id:
        raise RuntimeError("Tripletex ordre mangler id.")

    customer = order_payload.get("customer") if isinstance(order_payload.get("customer"), dict) else {}
    customer_name = str(customer.get("name", "Kontantkunde")).strip() or "Kontantkunde"
    customer_parts = customer_name.split(" ", 1)
    customer_first_name = customer_parts[0]
    customer_last_name = customer_parts[1] if len(customer_parts) > 1 else customer_parts[0]

    lines: list[dict[str, Any]] = []
    for raw_line in order_payload.get("orderLines", []) if isinstance(order_payload.get("orderLines"), list) else []:
        if not isinstance(raw_line, dict):
            continue

        product = raw_line.get("product") if isinstance(raw_line.get("product"), dict) else {}
        tt_product_number = str(product.get("number") or "").strip()
        tt_product_id = str(product.get("id") or "").strip()
        fallback_susoft_id = TRIPLETEX_TO_SUSOFT_PRODUCT_ID_MAP.get(tt_product_id)
        product_id = str(tt_product_number or fallback_susoft_id or tt_product_id).strip()
        if not product_id:
            raise RuntimeError("Ordrelinje mangler produktreferanse for Susoft-mapping.")

        qty = _safe_float(raw_line.get("count"))
        if qty <= 0:
            qty = 1.0

        vat_type = raw_line.get("vatType") if isinstance(raw_line.get("vatType"), dict) else {}
        vat_percent = _safe_float(vat_type.get("percentage"))
        amount_excl_vat = _safe_float(raw_line.get("amountExcludingVatCurrency"))
        amount_incl_vat = _safe_float(raw_line.get("amountIncludingVatCurrency"))
        unit_price_excl_vat = _safe_float(raw_line.get("unitPriceExcludingVatCurrency"))
        unit_price_incl_vat = _safe_float(raw_line.get("unitPriceIncludingVatCurrency"))

        # Use line totals as source of truth, then derive effective per-unit prices.
        # This prevents wrong totals when Tripletex discounts are already reflected in amount fields.
        effective_unit_excl_vat = (amount_excl_vat / qty) if amount_excl_vat > 0 and qty > 0 else unit_price_excl_vat
        effective_unit_incl_vat = (amount_incl_vat / qty) if amount_incl_vat > 0 and qty > 0 else unit_price_incl_vat

        if effective_unit_excl_vat <= 0:
            effective_unit_excl_vat = unit_price_excl_vat
        if effective_unit_incl_vat <= 0:
            effective_unit_incl_vat = unit_price_incl_vat

        line_payload = {
            "text": str(raw_line.get("description", "Tripletex line")),
            "product": {"id": product_id},
            "qty": qty,
            "qtyOrdered": qty,
            "quantity": qty,
            "salesPriceInclTax": effective_unit_incl_vat,
            "price": effective_unit_excl_vat,
            "priceInclTax": effective_unit_incl_vat,
            "lineTaxPercent": vat_percent,
        }
        lines.append(line_payload)

    if not lines:
        raise RuntimeError("Tripletex ordre mangler gyldige ordrelinjer for mapping til Susoft.")

    return {
        "externalRef": tripletex_order_id,
        "orderDateTime": f"{order_payload.get('orderDate')}T12:00:00" if order_payload.get("orderDate") else None,
        "customer": {
            "firstName": customer_first_name,
            "lastName": customer_last_name,
            "displayName": customer_name,
        },
        "lines": lines,
    }


def _push_order_to_susoft(
    session: Session,
    *,
    tenant_id: int,
    job_run_id: int | None,
    order_sync: OrderSync,
    order_payload: dict[str, Any],
    susoft_token: str | None = None,
    susoft_overrides: dict[str, str] | None = None,
) -> bool:
    try:
        if order_sync.status == "PUSHED_TO_SUSOFT" and order_sync.susoft_uuid:
            _add_event(
                session,
                tenant_id=tenant_id,
                job_run_id=job_run_id,
                order_sync_id=order_sync.id,
                event_type="SUSOFT_ALREADY_SYNCED",
                message="Ordre var allerede synket mot Susoft, hopper over push",
                details={"tripletex_order_id": order_sync.tripletex_order_id},
            )
            return True

        mapped = _build_susoft_order_payload(order_payload)
        created = create_susoft_order(mapped, token=susoft_token, overrides=susoft_overrides)

        order_sync.status = "PUSHED_TO_SUSOFT"
        order_sync.last_error = None
        order_sync.susoft_uuid = str(created.get("uuid", "")) or None
        order_sync.updated_at = datetime.now(UTC)

        _add_event(
            session,
            tenant_id=tenant_id,
            job_run_id=job_run_id,
            order_sync_id=order_sync.id,
            event_type="SUSOFT_PUSH_OK",
            message="Ordre sendt til Susoft",
            details={
                "tripletex_order_id": order_sync.tripletex_order_id,
                "susoft_uuid": order_sync.susoft_uuid,
            },
        )
        return True
    except Exception as exc:
        order_sync.status = "FAILED"
        order_sync.last_error = str(exc)
        order_sync.updated_at = datetime.now(UTC)
        _add_event(
            session,
            tenant_id=tenant_id,
            job_run_id=job_run_id,
            order_sync_id=order_sync.id,
            event_type="SUSOFT_PUSH_FAILED",
            level="ERROR",
            message="Susoft push feilet",
            details={
                "tripletex_order_id": order_sync.tripletex_order_id,
                "error": str(exc),
            },
        )
        return False


def run_manual_sync_for_tenant(tenant_key: str, *, dry_run: bool, limit: int) -> dict[str, Any]:
    with db_session() as session:
        tenant = session.scalar(select(Tenant).where(Tenant.tenant_key == tenant_key))
        if tenant is None:
            raise RuntimeError(f"Tenant finnes ikke: {tenant_key}")
        if not tenant.active:
            raise RuntimeError(f"Tenant er inaktiv: {tenant_key}")

        job = JobRun(tenant_id=tenant.id, job_name="manual_sync", status="RUNNING", started_at=datetime.now(UTC))
        session.add(job)
        session.flush()

        _add_event(
            session,
            tenant_id=tenant.id,
            job_run_id=job.id,
            event_type="SYNC_STARTED",
            message="Manuell sync startet",
            details={"dry_run": dry_run, "limit": limit},
        )

        discovered = 0
        errors = 0
        synced = 0

        try:
            tripletex_overrides = _tripletex_overrides_for_tenant(tenant)
            susoft_overrides = _susoft_overrides_for_tenant(tenant)
            susoft_token = None if dry_run else susoft_authenticate(overrides=susoft_overrides)
            token = create_session_token(overrides=tripletex_overrides)
            payload = fetch_open_orders(token, overrides=tripletex_overrides)
            values = payload.get("values")
            orders = values if isinstance(values, list) else []

            _add_event(
                session,
                tenant_id=tenant.id,
                job_run_id=job.id,
                event_type="TRIPLETEX_FETCHED",
                message="Ordrehenting fra Tripletex fullfort",
                details={"count": len(orders)},
            )

            for order in orders[:limit]:
                if not isinstance(order, dict):
                    errors += 1
                    _add_event(
                        session,
                        tenant_id=tenant.id,
                        job_run_id=job.id,
                        event_type="ORDER_PARSE_ERROR",
                        level="ERROR",
                        message="Ugyldig ordreobjekt i Tripletex-respons",
                    )
                    continue

                if not _is_tripletex_order_open(order):
                    _add_event(
                        session,
                        tenant_id=tenant.id,
                        job_run_id=job.id,
                        event_type="ORDER_SKIPPED_CLOSED",
                        level="INFO",
                        message="Ordre er lukket i Tripletex og hoppes over",
                        details={"tripletex_order_id": str(order.get("id", ""))},
                    )
                    continue

                order_sync = _upsert_order_sync(session, tenant.id, order)
                discovered += 1

                _add_event(
                    session,
                    tenant_id=tenant.id,
                    job_run_id=job.id,
                    order_sync_id=order_sync.id,
                    event_type="ORDER_DISCOVERED",
                    message="Ordre oppdatert i order_sync",
                    details={"tripletex_order_id": order_sync.tripletex_order_id},
                )

                if dry_run:
                    order_sync.updated_at = datetime.now(UTC)
                    _add_event(
                        session,
                        tenant_id=tenant.id,
                        job_run_id=job.id,
                        order_sync_id=order_sync.id,
                        event_type="SUSOFT_PUSH_SKIPPED",
                        message="Susoft push hoppet over i dry-run uten a endre eksisterende status",
                    )
                    continue

                if _is_locally_handled(order_sync):
                    _add_event(
                        session,
                        tenant_id=tenant.id,
                        job_run_id=job.id,
                        order_sync_id=order_sync.id,
                        event_type="ORDER_SKIPPED_ALREADY_HANDLED",
                        message="Ordre er allerede handtert lokalt, hopper over ny push",
                        details={
                            "tripletex_order_id": order_sync.tripletex_order_id,
                            "status": order_sync.status,
                            "susoft_uuid": order_sync.susoft_uuid,
                        },
                    )
                    continue

                ok = _push_order_to_susoft(
                    session,
                    tenant_id=tenant.id,
                    job_run_id=job.id,
                    order_sync=order_sync,
                    order_payload=order,
                    susoft_token=susoft_token,
                    susoft_overrides=susoft_overrides,
                )
                if ok:
                    synced += 1
                else:
                    errors += 1

            job.status = "SUCCESS" if errors == 0 else "PARTIAL_SUCCESS"
            job.message = f"orders={discovered}, pushed_to_susoft={synced}, errors={errors}"
        except Exception as exc:
            errors += 1
            job.status = "FAILED"
            job.message = str(exc)
            _add_event(
                session,
                tenant_id=tenant.id,
                job_run_id=job.id,
                event_type="SYNC_FAILED",
                level="ERROR",
                message="Manuell sync feilet",
                details={"error": str(exc)},
            )
        finally:
            job.finished_at = datetime.now(UTC)
            session.commit()

        return {
            "tenant_key": tenant_key,
            "job_run_id": job.id,
            "status": job.status,
            "discovered_orders": discovered,
            "pushed_to_susoft": synced,
            "errors": errors,
            "message": job.message,
        }


def retry_failed_orders_for_tenant(tenant_key: str, *, limit: int) -> dict[str, Any]:
    with db_session() as session:
        tenant = session.scalar(select(Tenant).where(Tenant.tenant_key == tenant_key))
        if tenant is None:
            raise RuntimeError(f"Tenant finnes ikke: {tenant_key}")
        if not tenant.active:
            raise RuntimeError(f"Tenant er inaktiv: {tenant_key}")

        job = JobRun(tenant_id=tenant.id, job_name="retry_failed_orders", status="RUNNING", started_at=datetime.now(UTC))
        session.add(job)
        session.flush()

        retried = 0
        succeeded = 0
        failed = 0

        _add_event(
            session,
            tenant_id=tenant.id,
            job_run_id=job.id,
            event_type="RETRY_STARTED",
            message="Retry av feilede ordrer startet",
            details={"limit": limit},
        )

        try:
            susoft_overrides = _susoft_overrides_for_tenant(tenant)
            susoft_token = susoft_authenticate(overrides=susoft_overrides)
            rows = session.scalars(
                select(OrderSync)
                .where(OrderSync.tenant_id == tenant.id, OrderSync.status == "FAILED")
                .order_by(desc(OrderSync.updated_at))
                .limit(limit)
            ).all()

            for row in rows:
                retried += 1
                payload_json = row.payload_json or "{}"
                try:
                    payload = json.loads(payload_json)
                except Exception as exc:
                    row.last_error = f"payload_json parse feil: {exc}"
                    row.updated_at = datetime.now(UTC)
                    failed += 1
                    _add_event(
                        session,
                        tenant_id=tenant.id,
                        job_run_id=job.id,
                        order_sync_id=row.id,
                        event_type="RETRY_PARSE_FAILED",
                        level="ERROR",
                        message="Kunne ikke parse payload_json for retry",
                    )
                    continue

                ok = _push_order_to_susoft(
                    session,
                    tenant_id=tenant.id,
                    job_run_id=job.id,
                    order_sync=row,
                    order_payload=payload,
                    susoft_token=susoft_token,
                    susoft_overrides=susoft_overrides,
                )
                if ok:
                    succeeded += 1
                else:
                    failed += 1

            job.status = "SUCCESS" if failed == 0 else "PARTIAL_SUCCESS"
            job.message = f"retried={retried}, succeeded={succeeded}, failed={failed}"
        except Exception as exc:
            job.status = "FAILED"
            job.message = str(exc)
            _add_event(
                session,
                tenant_id=tenant.id,
                job_run_id=job.id,
                event_type="RETRY_FAILED",
                level="ERROR",
                message="Retry-jobb feilet",
                details={"error": str(exc)},
            )
        finally:
            job.finished_at = datetime.now(UTC)
            session.commit()

        return {
            "tenant_key": tenant_key,
            "job_run_id": job.id,
            "status": job.status,
            "retried": retried,
            "succeeded": succeeded,
            "failed": failed,
            "message": job.message,
        }


def sync_paid_orders_to_tripletex_for_tenant(
    tenant_key: str,
    *,
    limit: int,
    payment_type_id: int,
) -> dict[str, Any]:
    with db_session() as session:
        tenant = session.scalar(select(Tenant).where(Tenant.tenant_key == tenant_key))
        if tenant is None:
            raise RuntimeError(f"Tenant finnes ikke: {tenant_key}")
        if not tenant.active:
            raise RuntimeError(f"Tenant er inaktiv: {tenant_key}")

        job = JobRun(tenant_id=tenant.id, job_name="sync_paid_orders", status="RUNNING", started_at=datetime.now(UTC))
        session.add(job)
        session.flush()

        checked = 0
        synced_to_tt = 0
        skipped = 0
        errors = 0

        _add_event(
            session,
            tenant_id=tenant.id,
            job_run_id=job.id,
            event_type="PAID_SYNC_STARTED",
            message="Polling av betalte Susoft-ordrer startet",
            details={"limit": limit, "payment_type_id": payment_type_id},
        )

        try:
            susoft_overrides = _susoft_overrides_for_tenant(tenant)
            tripletex_overrides = _tripletex_overrides_for_tenant(tenant)
            token = susoft_authenticate(overrides=susoft_overrides)
            tt_headers = build_basic_headers(create_session_token(overrides=tripletex_overrides))

            rows = session.scalars(
                select(OrderSync)
                .where(
                    OrderSync.tenant_id == tenant.id,
                    OrderSync.susoft_uuid.is_not(None),
                    OrderSync.status.in_(["PUSHED_TO_SUSOFT", "TT_PAYMENT_FAILED"]),
                )
                .order_by(desc(OrderSync.updated_at))
                .limit(limit)
            ).all()

            for row in rows:
                checked += 1

                uuid = row.susoft_uuid
                if not uuid:
                    skipped += 1
                    continue

                try:
                    order = find_order_by_uuid(uuid, token=token, overrides=susoft_overrides)
                    cart = find_cart_by_uuid(uuid, token=token, overrides=susoft_overrides)
                except Exception as exc:
                    errors += 1
                    row.status = "TT_PAYMENT_FAILED"
                    row.last_error = str(exc)
                    row.updated_at = datetime.now(UTC)
                    _add_event(
                        session,
                        tenant_id=tenant.id,
                        job_run_id=job.id,
                        order_sync_id=row.id,
                        event_type="SUSOFT_STATUS_LOOKUP_FAILED",
                        level="ERROR",
                        message="Feil ved oppslag av Susoft-status for uuid",
                        details={"uuid": uuid, "error": str(exc)},
                    )
                    continue

                if order is None:
                    skipped += 1
                    _add_event(
                        session,
                        tenant_id=tenant.id,
                        job_run_id=job.id,
                        order_sync_id=row.id,
                        event_type="SUSOFT_NOT_YET_ORDER",
                        message="UUID finnes ikke som order i Susoft enda, hopper over",
                        details={"uuid": uuid, "cart_exists": cart is not None},
                    )
                    continue

                payments = order.get("payments") if isinstance(order.get("payments"), list) else []
                paid_amount = 0.0
                for payment in payments:
                    if isinstance(payment, dict):
                        paid_amount += _safe_float(payment.get("amount"))

                if paid_amount <= 0:
                    skipped += 1
                    _add_event(
                        session,
                        tenant_id=tenant.id,
                        job_run_id=job.id,
                        order_sync_id=row.id,
                        event_type="SUSOFT_ORDER_NOT_PAID",
                        message="Susoft order har ingen registrert betaling enda",
                        details={"uuid": uuid},
                    )
                    continue

                tt_order_id_raw = order.get("alternativeId")
                tt_order_id = int(str(tt_order_id_raw)) if tt_order_id_raw is not None else None
                if tt_order_id is None:
                    errors += 1
                    row.status = "TT_PAYMENT_FAILED"
                    row.last_error = "Susoft order mangler alternativeId mot TT order id"
                    row.updated_at = datetime.now(UTC)
                    _add_event(
                        session,
                        tenant_id=tenant.id,
                        job_run_id=job.id,
                        order_sync_id=row.id,
                        event_type="SUSOFT_MISSING_ALTERNATIVE_ID",
                        level="ERROR",
                        message="Susoft order mangler alternativeId",
                        details={"uuid": uuid},
                    )
                    continue

                try:
                    today = datetime.now(UTC).date().isoformat()
                    invoice_id = create_invoice(order_id=tt_order_id, invoice_date=today, headers=tt_headers, dry_run=False)
                    register_payment(
                        invoice_id=invoice_id,
                        payment_date=today,
                        payment_type_id=payment_type_id,
                        paid_amount=paid_amount,
                        headers=tt_headers,
                        dry_run=False,
                    )

                    row.status = "TT_PAID"
                    row.last_error = None
                    row.updated_at = datetime.now(UTC)
                    synced_to_tt += 1
                    _add_event(
                        session,
                        tenant_id=tenant.id,
                        job_run_id=job.id,
                        order_sync_id=row.id,
                        event_type="TT_PAYMENT_REGISTERED",
                        message="Betaling registrert i Tripletex basert på Susoft payment",
                        details={
                            "tripletex_order_id": tt_order_id,
                            "susoft_uuid": uuid,
                            "paid_amount": paid_amount,
                            "payment_type_id": payment_type_id,
                        },
                    )
                except AlreadyInvoicedError as exc:
                    skipped += 1
                    row.status = "TT_INVOICE_EXISTS"
                    row.last_error = str(exc)
                    row.updated_at = datetime.now(UTC)
                    _add_event(
                        session,
                        tenant_id=tenant.id,
                        job_run_id=job.id,
                        order_sync_id=row.id,
                        event_type="TT_INVOICE_EXISTS",
                        level="WARNING",
                        message="Tripletex-ordre er allerede fakturert, hopper over automatisk betalingsregistrering",
                        details={"tripletex_order_id": tt_order_id, "susoft_uuid": uuid},
                    )
                except Exception as exc:
                    errors += 1
                    row.status = "TT_PAYMENT_FAILED"
                    row.last_error = str(exc)
                    row.updated_at = datetime.now(UTC)
                    _add_event(
                        session,
                        tenant_id=tenant.id,
                        job_run_id=job.id,
                        order_sync_id=row.id,
                        event_type="TT_PAYMENT_SYNC_FAILED",
                        level="ERROR",
                        message="Kunne ikke registrere betaling i Tripletex",
                        details={"tripletex_order_id": tt_order_id, "error": str(exc)},
                    )

            job.status = "SUCCESS" if errors == 0 else "PARTIAL_SUCCESS"
            job.message = f"checked={checked}, synced_to_tt={synced_to_tt}, skipped={skipped}, errors={errors}"
        except Exception as exc:
            job.status = "FAILED"
            job.message = str(exc)
            _add_event(
                session,
                tenant_id=tenant.id,
                job_run_id=job.id,
                event_type="PAID_SYNC_FAILED",
                level="ERROR",
                message="Paid sync-jobb feilet",
                details={"error": str(exc)},
            )
        finally:
            job.finished_at = datetime.now(UTC)
            session.commit()

        return {
            "tenant_key": tenant_key,
            "job_run_id": job.id,
            "status": job.status,
            "checked": checked,
            "synced_to_tt": synced_to_tt,
            "skipped": skipped,
            "errors": errors,
            "message": job.message,
        }
