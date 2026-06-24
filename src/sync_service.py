from __future__ import annotations

import json
from collections import defaultdict
from datetime import UTC, date, datetime, timedelta
from datetime import time as dtime
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from src.db import db_session
from src.config import get_settings
from src.models import ArticleIncomeMapping, DirectSalesSettlementRun, JobRun, OrderSync, SyncEvent, Tenant
from src.susoft_client import (
    authenticate as susoft_authenticate,
    create_product as create_susoft_product,
    create_product_category as create_susoft_product_category,
    create_order as create_susoft_order,
    find_cart_by_uuid,
    find_order_by_uuid,
    find_product_by_alternative_id,
    list_orders_by_date_range,
    list_product_category_tree,
    update_product as update_susoft_product,
)
from src.tripletex_client import create_session_token, fetch_open_orders, list_products
from src.tripletex_client import create_ledger_voucher, find_voucher_by_external_number, resolve_account_ids_by_number
from tripletex_invoice_payment_flow import (
    AlreadyInvoicedError,
    build_basic_headers,
    create_invoice,
    register_payment,
)


TRIPLETEX_TO_SUSOFT_PRODUCT_ID_MAP: dict[str, str] = {
    "69775686": "10002",  # Susoft M10
}
DEFAULT_DIRECT_SALES_SETTLEMENT_MODE = "FINANCIAL"


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


def _parse_datetime_any(value: Any) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    candidates = [normalized]
    if " " in normalized and "T" not in normalized:
        candidates.append(normalized.replace(" ", "T", 1))
    for candidate in candidates:
        try:
            dt = datetime.fromisoformat(candidate)
            if dt.tzinfo is None:
                return dt.replace(tzinfo=UTC)
            return dt.astimezone(UTC)
        except Exception:
            continue
    return None


def _is_tt_linked_susoft_order(order: dict[str, Any]) -> bool:
    for key in ("alternativeId", "externalRef"):
        value = order.get(key)
        if value is not None and str(value).strip():
            return True
    return False


def _settlement_day_paid_amount(order: dict[str, Any], settlement_date: date, *, timezone_name: str) -> float:
    payments = order.get("payments") if isinstance(order.get("payments"), list) else []
    tz = ZoneInfo(timezone_name)
    total = 0.0
    for payment in payments:
        if not isinstance(payment, dict):
            continue
        payment_dt = (
            _parse_datetime_any(payment.get("paymentDateTime"))
            or _parse_datetime_any(payment.get("paymentDate"))
            or _parse_datetime_any(payment.get("created"))
            or _parse_datetime_any(payment.get("updated"))
        )
        if payment_dt is None:
            continue
        if payment_dt.astimezone(tz).date() != settlement_date:
            continue
        total += _safe_float(payment.get("amount"))
    return total


def _payment_method_key(payment: dict[str, Any]) -> str:
    payment_type = str(payment.get("paymentType") or "").strip().upper()
    payment_provider = str(payment.get("paymentProvider") or "").strip().upper()
    payment_card_id = str(payment.get("paymentCardId") or "").strip()
    source = str(payment.get("source") or "").strip().upper()

    if payment_type and payment_provider and payment_card_id:
        return f"{payment_type}:{payment_provider}:{payment_card_id}"
    if payment_type and payment_provider:
        return f"{payment_type}:{payment_provider}"
    if payment_type:
        return payment_type
    if payment_provider:
        return payment_provider
    if source:
        return source
    return "UNKNOWN"


def _payment_method_label(payment: dict[str, Any]) -> str:
    payment_type = str(payment.get("paymentType") or "").strip()
    payment_provider = str(payment.get("paymentProvider") or "").strip()
    payment_card_id = str(payment.get("paymentCardId") or "").strip()
    text = str(payment.get("text") or "").strip()

    parts = [part for part in [payment_type, payment_provider, payment_card_id, text] if part]
    return " / ".join(parts) if parts else "UNKNOWN"


def _load_payment_rules(raw_rules: str | None) -> list[dict[str, Any]]:
    if not raw_rules:
        return []
    try:
        parsed = json.loads(raw_rules)
    except Exception:
        return []
    if not isinstance(parsed, list):
        return []
    return [item for item in parsed if isinstance(item, dict)]


def _match_payment_rule(payment_key: str, payment_label: str, payment_rules: list[dict[str, Any]]) -> dict[str, Any] | None:
    normalized_key = payment_key.upper()
    normalized_label = payment_label.upper()
    for rule in payment_rules:
        match_value = str(rule.get("match") or rule.get("pattern") or "").strip().upper()
        if not match_value:
            continue
        if match_value == normalized_key or match_value == normalized_label:
            return rule
    return None


def _summarize_payment_methods(orders: list[dict[str, Any]], payment_rules: list[dict[str, Any]]) -> list[dict[str, Any]]:
    buckets: dict[str, dict[str, Any]] = {}
    for order in orders:
        payments = order.get("payments") if isinstance(order.get("payments"), list) else []
        for payment in payments:
            if not isinstance(payment, dict):
                continue
            amount = _safe_float(payment.get("amount"))
            if amount <= 0:
                continue
            key = _payment_method_key(payment)
            label = _payment_method_label(payment)
            bucket = buckets.setdefault(
                key,
                {
                    "key": key,
                    "label": label,
                    "amount": 0.0,
                    "count": 0,
                    "example_payment_type": payment.get("paymentType"),
                    "example_payment_provider": payment.get("paymentProvider"),
                    "example_payment_card_id": payment.get("paymentCardId"),
                },
            )
            bucket["amount"] = round(float(bucket["amount"]) + amount, 2)
            bucket["count"] = int(bucket["count"]) + 1

    result: list[dict[str, Any]] = []
    for bucket in buckets.values():
        rule = _match_payment_rule(str(bucket["key"]), str(bucket["label"]), payment_rules)
        result.append(
            {
                "key": bucket["key"],
                "label": bucket["label"],
                "amount": round(_safe_float(bucket["amount"]), 2),
                "count": int(bucket["count"]),
                "mapped_account": str(rule.get("account") or rule.get("tripletex_account") or "").strip() if rule else None,
                "mapped_name": str(rule.get("name") or rule.get("label") or "").strip() if rule else None,
                "rule_match": str(rule.get("match") or rule.get("pattern") or "").strip() if rule else None,
                "example_payment_type": bucket.get("example_payment_type"),
                "example_payment_provider": bucket.get("example_payment_provider"),
                "example_payment_card_id": bucket.get("example_payment_card_id"),
            }
        )

    return sorted(result, key=lambda row: (-row["amount"], row["key"]))


def _load_category_rules(raw_rules: str | None) -> list[dict[str, Any]]:
    if not raw_rules:
        return []
    try:
        parsed = json.loads(raw_rules)
    except Exception:
        return []
    if not isinstance(parsed, list):
        return []
    return [item for item in parsed if isinstance(item, dict)]


def _normalize_text(value: Any) -> str:
    return str(value or "").strip().upper()


def _match_category_rule(
    account_number: str,
    account_name: str,
    rules: list[dict[str, Any]],
) -> dict[str, Any] | None:
    normalized_number = _normalize_text(account_number)
    normalized_name = _normalize_text(account_name)
    combined = f"{normalized_number}:{normalized_name}" if normalized_number or normalized_name else ""

    for rule in rules:
        pattern = _normalize_text(
            rule.get("match")
            or rule.get("pattern")
            or rule.get("account")
            or rule.get("account_number")
            or rule.get("account_name")
        )
        if not pattern:
            continue
        if pattern in {normalized_number, normalized_name, combined}:
            return rule
        if normalized_name and pattern in normalized_name:
            return rule
    return None


def _iter_susoft_categories(node: dict[str, Any]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []

    def walk(item: dict[str, Any]) -> None:
        category_id = str(item.get("id") or "").strip()
        category_name = str(item.get("name") or "").strip()
        if category_id and category_name:
            out.append({"id": category_id, "name": category_name})

        children = item.get("children")
        if isinstance(children, dict):
            for child in children.values():
                if isinstance(child, dict):
                    walk(child)

    walk(node)
    return out


def _safe_round_2(value: Any) -> float | None:
    if value is None:
        return None
    parsed = _safe_float(value)
    return round(parsed, 2)


def _extract_order_line_amount_incl_vat(line: dict[str, Any]) -> float:
    amount_candidates = [
        line.get("amountIncludingVat"),
        line.get("amountIncludingVatCurrency"),
        line.get("amountInclTax"),
        line.get("lineTotalIncludingVat"),
        line.get("lineTotalInclTax"),
        line.get("totalIncludingVat"),
    ]
    for candidate in amount_candidates:
        amount = _safe_float(candidate)
        if amount > 0:
            return amount

    qty = _safe_float(line.get("qty") or line.get("quantity") or line.get("count"))
    unit_candidates = [
        line.get("salesPriceInclTax"),
        line.get("unitPriceIncludingVatCurrency"),
        line.get("unitPriceIncludingVat"),
        line.get("priceInclTax"),
    ]
    for candidate in unit_candidates:
        unit = _safe_float(candidate)
        if unit > 0 and qty > 0:
            return unit * qty
    return 0.0


def _extract_order_line_product(line: dict[str, Any]) -> tuple[str, str | None]:
    product = line.get("product") if isinstance(line.get("product"), dict) else None
    if product is not None:
        product_id_raw = product.get("id") or product.get("number")
        product_name_raw = product.get("name")
    else:
        product_id_raw = line.get("productId") or line.get("articleId") or line.get("itemId")
        product_name_raw = line.get("productName") or line.get("articleName") or line.get("itemName")

    product_id = str(product_id_raw).strip() if product_id_raw is not None else ""
    product_name = str(product_name_raw).strip() if product_name_raw is not None else None
    return product_id, product_name or None


def _normalize_hhmm(raw_value: object, *, default: str = "05:00") -> str:
    value = str(raw_value or "").strip()
    if len(value) != 5 or value[2] != ":":
        return default
    hh, mm = value.split(":", 1)
    if not (hh.isdigit() and mm.isdigit()):
        return default
    hour = int(hh)
    minute = int(mm)
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        return default
    return f"{hour:02d}:{minute:02d}"


def _parse_hhmm(value: str) -> dtime:
    normalized = _normalize_hhmm(value)
    hour, minute = normalized.split(":", 1)
    return dtime(hour=int(hour), minute=int(minute))


def _resolve_sales_day_cutoff(tenant: Tenant) -> dtime:
    return _parse_hhmm(tenant.direct_sales_sales_day_cutoff or "05:00")


def _settlement_date_from_now(now_utc: datetime, *, timezone_name: str, cutoff: dtime) -> date:
    local_now = now_utc.astimezone(ZoneInfo(timezone_name))
    if local_now.time() >= cutoff:
        return local_now.date() - timedelta(days=1)
    return local_now.date() - timedelta(days=2)


def _direct_sales_window(settlement_date: date, *, timezone_name: str, cutoff: dtime) -> tuple[str, str]:
    tz = ZoneInfo(timezone_name)
    start_local = datetime.combine(settlement_date, cutoff, tzinfo=tz)
    end_local = datetime.combine(settlement_date + timedelta(days=1), cutoff, tzinfo=tz)
    return start_local.astimezone(UTC).isoformat(), end_local.astimezone(UTC).isoformat()


def _build_settlement_external_voucher_number(tenant_key: str, settlement_date: date) -> str:
    return f"DS-{tenant_key}-{settlement_date.isoformat()}"


def _post_direct_sales_settlement_to_tripletex(
    *,
    tenant: Tenant,
    settlement_date: date,
    account_lines: list[dict[str, Any]],
    offset_account: str,
    send_to_ledger: bool,
    tripletex_overrides: dict[str, str] | None = None,
) -> dict[str, Any]:
    external_number = _build_settlement_external_voucher_number(tenant.tenant_key, settlement_date)
    session_token = create_session_token(overrides=tripletex_overrides)

    existing = find_voucher_by_external_number(
        session_token,
        external_voucher_number=external_number,
        overrides=tripletex_overrides,
    )
    if existing is not None:
        return {
            "created": False,
            "voucher_id": existing.get("id"),
            "voucher_number": existing.get("number"),
            "external_voucher_number": external_number,
            "message": "Voucher finnes allerede (idempotent oppslag)",
        }

    credit_lines = [
        {
            "income_account": str(line.get("income_account") or "").strip(),
            "amount": round(_safe_float(line.get("amount")), 2),
        }
        for line in account_lines
    ]
    credit_lines = [line for line in credit_lines if line["income_account"] and line["amount"] > 0]
    if not credit_lines:
        raise RuntimeError("Ingen gyldige account_lines for posting")

    account_numbers = [line["income_account"] for line in credit_lines] + [offset_account]
    account_id_by_number = resolve_account_ids_by_number(
        session_token,
        account_numbers=account_numbers,
        overrides=tripletex_overrides,
    )

    voucher_postings: list[dict[str, Any]] = []
    total_credit = 0.0
    for line in credit_lines:
        amount = round(line["amount"], 2)
        if amount <= 0:
            continue
        total_credit += amount
        voucher_postings.append(
            {
                "account_id": account_id_by_number[line["income_account"]],
                "amount": -amount,
                "description": f"Direktesalg oppgjor {settlement_date.isoformat()} konto {line['income_account']}",
            }
        )

    voucher_postings.append(
        {
            "account_id": account_id_by_number[offset_account],
            "amount": round(total_credit, 2),
            "description": f"Direktesalg oppgjor {settlement_date.isoformat()} motkonto {offset_account}",
        }
    )

    voucher = create_ledger_voucher(
        session_token,
        voucher_date=settlement_date.isoformat(),
        description=(
            f"Direktesalg oppgjor {tenant.tenant_key} {settlement_date.isoformat()}"
            + (" - IKKE AUTO-BOKFOR" if not send_to_ledger else " - AUTO-BOKFOR")
        ),
        external_voucher_number=external_number,
        postings=voucher_postings,
        send_to_ledger=send_to_ledger,
        overrides=tripletex_overrides,
    )
    return {
        "created": True,
        "voucher_id": voucher.get("id"),
        "voucher_number": voucher.get("number"),
        "external_voucher_number": external_number,
        "message": (
            "Bilag opprettet i Tripletex med automatisk bokforing"
            if send_to_ledger
            else "Bilag opprettet i Tripletex uten automatisk bokforing"
        ),
    }


def _upsert_direct_sales_settlement_run(
    session: Session,
    *,
    tenant_id: int,
    settlement_date: date,
) -> DirectSalesSettlementRun:
    existing = session.scalar(
        select(DirectSalesSettlementRun).where(
            DirectSalesSettlementRun.tenant_id == tenant_id,
            DirectSalesSettlementRun.settlement_date == settlement_date,
        )
    )
    if existing is None:
        created = DirectSalesSettlementRun(
            tenant_id=tenant_id,
            settlement_date=settlement_date,
            status="RUNNING",
            started_at=datetime.now(UTC),
        )
        session.add(created)
        session.flush()
        return created
    existing.status = "RUNNING"
    existing.started_at = datetime.now(UTC)
    existing.finished_at = None
    session.flush()
    return existing


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


def sync_products_from_tripletex_for_tenant(
    tenant_key: str,
    *,
    execute: bool,
    limit: int = 200,
) -> dict[str, Any]:
    safe_limit = max(1, min(1000, int(limit)))

    with db_session() as session:
        tenant = session.scalar(select(Tenant).where(Tenant.tenant_key == tenant_key))
        if tenant is None:
            raise RuntimeError(f"Tenant finnes ikke: {tenant_key}")
        if not tenant.active:
            raise RuntimeError(f"Tenant er inaktiv: {tenant_key}")

        job = JobRun(tenant_id=tenant.id, job_name="product_sync_tt_to_susoft", status="RUNNING")
        session.add(job)
        session.flush()

        checked = 0
        created = 0
        updated = 0
        skipped = 0
        errors = 0
        created_categories = 0
        result_rows: list[dict[str, Any]] = []

        try:
            tt_token = create_session_token(overrides=_tripletex_overrides_for_tenant(tenant))
            ss_token = susoft_authenticate(overrides=_susoft_overrides_for_tenant(tenant))

            tt_products = list_products(
                tt_token,
                include_inactive=True,
                limit=safe_limit,
                overrides=_tripletex_overrides_for_tenant(tenant),
            )
            category_rules = _load_category_rules(tenant.product_sync_category_rules_json)

            tree = list_product_category_tree(ss_token, overrides=_susoft_overrides_for_tenant(tenant))
            categories = _iter_susoft_categories(tree) if isinstance(tree, dict) else []
            category_by_id = {str(item["id"]): item for item in categories if item.get("id")}
            category_by_name = {str(item["name"]).strip().lower(): item for item in categories if item.get("name")}

            for tt_product in tt_products:
                checked += 1
                tt_product_id = str(tt_product.get("id") or "").strip()
                if not tt_product_id:
                    skipped += 1
                    continue

                tt_name = str(tt_product.get("name") or "").strip()
                tt_number = str(tt_product.get("number") or "").strip()
                account = tt_product.get("account") if isinstance(tt_product.get("account"), dict) else {}
                account_number = str(account.get("number") or "").strip()
                account_name = str(account.get("name") or "").strip()
                vat_type = tt_product.get("vatType") if isinstance(tt_product.get("vatType"), dict) else {}
                vat_percent = _safe_round_2(vat_type.get("percentage"))
                price_ex_vat = _safe_round_2(tt_product.get("priceExcludingVatCurrency"))
                price_inc_vat = _safe_round_2(tt_product.get("priceIncludingVatCurrency"))
                is_active = not bool(tt_product.get("isInactive"))

                rule = _match_category_rule(account_number, account_name, category_rules)
                requested_category_id = str(
                    (rule or {}).get("susoft_category_id")
                    or (rule or {}).get("category_id")
                    or (rule or {}).get("id")
                    or ""
                ).strip()
                requested_category_name = str(
                    (rule or {}).get("susoft_category_name")
                    or (rule or {}).get("category_name")
                    or (rule or {}).get("name_override")
                    or ""
                ).strip()
                fallback_category_name = requested_category_name or account_name or (f"TT konto {account_number}" if account_number else "TT produkter")

                resolved_category: dict[str, Any] | None = None
                category_resolution = ""

                if requested_category_id and requested_category_id in category_by_id:
                    resolved_category = category_by_id[requested_category_id]
                    category_resolution = "matched_by_id"
                elif fallback_category_name and fallback_category_name.lower() in category_by_name:
                    resolved_category = category_by_name[fallback_category_name.lower()]
                    category_resolution = "matched_by_name"
                elif execute:
                    created_category = create_susoft_product_category(
                        name=fallback_category_name,
                        level=1,
                        vat=vat_percent,
                        alternative_vat=vat_percent,
                        token=ss_token,
                        overrides=_susoft_overrides_for_tenant(tenant),
                    )
                    new_category_id = str(created_category.get("id") or "").strip()
                    new_category_name = str(created_category.get("name") or fallback_category_name).strip()
                    if new_category_id:
                        resolved_category = {"id": new_category_id, "name": new_category_name}
                        category_by_id[new_category_id] = resolved_category
                        if new_category_name:
                            category_by_name[new_category_name.lower()] = resolved_category
                        created_categories += 1
                        category_resolution = "created"
                else:
                    category_resolution = "would_create"

                alternative_id = f"TT:{tt_product_id}"
                try:
                    existing_susoft_product = find_product_by_alternative_id(
                        alternative_id,
                        token=ss_token,
                        overrides=_susoft_overrides_for_tenant(tenant),
                    )
                except Exception as exc:
                    errors += 1
                    result_rows.append(
                        {
                            "tripletex_product_id": tt_product_id,
                            "tripletex_name": tt_name,
                            "status": "ERROR",
                            "error": str(exc),
                        }
                    )
                    continue

                susoft_payload: dict[str, Any] = {
                    "alternativeId": alternative_id,
                    "externalRefId": tt_number or None,
                    "name": tt_name,
                    "active": is_active,
                    "vatPercent": vat_percent,
                    "unitPrice": price_inc_vat,
                    "netPrice": price_ex_vat,
                    "retailPrice": price_inc_vat,
                    "category1": str((resolved_category or {}).get("id") or "").strip() or None,
                    "categoryName": str((resolved_category or {}).get("name") or fallback_category_name).strip() or None,
                }
                susoft_payload = {k: v for k, v in susoft_payload.items() if v is not None}

                if existing_susoft_product is not None:
                    susoft_product_id = str(existing_susoft_product.get("id") or "").strip() or None
                    if execute:
                        update_payload = dict(susoft_payload)
                        if susoft_product_id:
                            update_payload["id"] = susoft_product_id
                        updated_product = update_susoft_product(
                            update_payload,
                            token=ss_token,
                            overrides=_susoft_overrides_for_tenant(tenant),
                        )
                        updated += 1
                        result_rows.append(
                            {
                                "tripletex_product_id": tt_product_id,
                                "tripletex_name": tt_name,
                                "susoft_product_id": str(updated_product.get("id") or susoft_product_id or "") or None,
                                "status": "UPDATED",
                                "category_resolution": category_resolution,
                                "category_id": susoft_payload.get("category1"),
                                "category_name": susoft_payload.get("categoryName"),
                                "tripletex_account_number": account_number or None,
                                "tripletex_account_name": account_name or None,
                            }
                        )
                    else:
                        result_rows.append(
                            {
                                "tripletex_product_id": tt_product_id,
                                "tripletex_name": tt_name,
                                "susoft_product_id": susoft_product_id,
                                "status": "WOULD_UPDATE",
                                "category_resolution": category_resolution,
                                "category_id": susoft_payload.get("category1"),
                                "category_name": susoft_payload.get("categoryName"),
                                "tripletex_account_number": account_number or None,
                                "tripletex_account_name": account_name or None,
                            }
                        )
                else:
                    if execute:
                        created_product = create_susoft_product(
                            susoft_payload,
                            token=ss_token,
                            overrides=_susoft_overrides_for_tenant(tenant),
                        )
                        created += 1
                        result_rows.append(
                            {
                                "tripletex_product_id": tt_product_id,
                                "tripletex_name": tt_name,
                                "susoft_product_id": str(created_product.get("id") or "") or None,
                                "status": "CREATED",
                                "category_resolution": category_resolution,
                                "category_id": susoft_payload.get("category1"),
                                "category_name": susoft_payload.get("categoryName"),
                                "tripletex_account_number": account_number or None,
                                "tripletex_account_name": account_name or None,
                            }
                        )
                    else:
                        result_rows.append(
                            {
                                "tripletex_product_id": tt_product_id,
                                "tripletex_name": tt_name,
                                "susoft_product_id": None,
                                "status": "WOULD_CREATE",
                                "category_resolution": category_resolution,
                                "category_id": susoft_payload.get("category1"),
                                "category_name": susoft_payload.get("categoryName"),
                                "tripletex_account_number": account_number or None,
                                "tripletex_account_name": account_name or None,
                            }
                        )

            job.status = "SUCCESS" if errors == 0 else "PARTIAL_SUCCESS"
            job.message = (
                f"checked={checked}, created={created}, updated={updated}, skipped={skipped}, "
                f"created_categories={created_categories}, errors={errors}"
            )
            _add_event(
                session,
                tenant_id=tenant.id,
                job_run_id=job.id,
                event_type="TT_PRODUCT_SYNC_COMPLETED",
                level="INFO" if errors == 0 else "WARNING",
                message="Tripletex produkter synket mot Susoft",
                details={
                    "execute": execute,
                    "checked": checked,
                    "created": created,
                    "updated": updated,
                    "skipped": skipped,
                    "created_categories": created_categories,
                    "errors": errors,
                    "rows": result_rows[:50],
                },
            )
        except Exception as exc:
            errors += 1
            job.status = "FAILED"
            job.message = str(exc)
            _add_event(
                session,
                tenant_id=tenant.id,
                job_run_id=job.id,
                event_type="TT_PRODUCT_SYNC_FAILED",
                level="ERROR",
                message="Tripletex -> Susoft produktsynk feilet",
                details={"error": str(exc), "execute": execute},
            )
        finally:
            job.finished_at = datetime.now(UTC)
            session.commit()

        return {
            "tenant_key": tenant_key,
            "job_run_id": job.id,
            "status": job.status,
            "execute": execute,
            "checked": checked,
            "created": created,
            "updated": updated,
            "skipped": skipped,
            "created_categories": created_categories,
            "errors": errors,
            "message": job.message,
            "results": result_rows,
        }


def calculate_direct_sales_settlement_for_tenant(
    tenant_key: str,
    *,
    settlement_date: date | None = None,
    execute: bool = False,
) -> dict[str, Any]:
    settings = get_settings()
    tz_name = settings.tripletex_timezone

    with db_session() as session:
        tenant = session.scalar(select(Tenant).where(Tenant.tenant_key == tenant_key))
        if tenant is None:
            raise RuntimeError(f"Tenant finnes ikke: {tenant_key}")
        if not tenant.active:
            raise RuntimeError(f"Tenant er inaktiv: {tenant_key}")

        sales_day_cutoff = _resolve_sales_day_cutoff(tenant)
        target_date = settlement_date or _settlement_date_from_now(datetime.now(UTC), timezone_name=tz_name, cutoff=sales_day_cutoff)
        from_dt, to_dt = _direct_sales_window(target_date, timezone_name=tz_name, cutoff=sales_day_cutoff)

        run = _upsert_direct_sales_settlement_run(
            session,
            tenant_id=tenant.id,
            settlement_date=target_date,
        )

        direct_sales_gross = 0.0
        tt_linked_gross = 0.0
        net_transfer_gross = 0.0
        direct_paid_orders = 0
        tt_linked_paid_orders = 0
        account_amounts: dict[str, float] = defaultdict(float)
        unresolved_products: dict[str, dict[str, Any]] = {}
        direct_sales_orders_detail: list[dict[str, Any]] = []

        try:
            token = susoft_authenticate(overrides=_susoft_overrides_for_tenant(tenant))
            payment_rules = _load_payment_rules(tenant.direct_sales_payment_rules_json)
            orders = list_orders_by_date_range(
                from_date=from_dt,
                to_date=to_dt,
                mode=DEFAULT_DIRECT_SALES_SETTLEMENT_MODE,
                token=token,
                overrides=_susoft_overrides_for_tenant(tenant),
            )
            default_income_account = str(tenant.direct_sales_default_income_account or "").strip() or None
            mapping_rows = session.scalars(
                select(ArticleIncomeMapping)
                .where(
                    ArticleIncomeMapping.tenant_id == tenant.id,
                    ArticleIncomeMapping.active.is_(True),
                )
                .order_by(ArticleIncomeMapping.id.asc())
            ).all()
            mapping_by_product_id = {
                str(row.susoft_product_id).strip(): row
                for row in mapping_rows
                if str(row.susoft_product_id).strip()
            }

            for order in orders:
                if not isinstance(order, dict):
                    continue
                paid_amount = _settlement_day_paid_amount(order, target_date, timezone_name=tz_name)
                if paid_amount <= 0:
                    continue

                if _is_tt_linked_susoft_order(order):
                    tt_linked_gross += paid_amount
                    tt_linked_paid_orders += 1
                else:
                    direct_sales_gross += paid_amount
                    direct_paid_orders += 1

                    payment_summary = [
                        {
                            "key": _payment_method_key(payment),
                            "label": _payment_method_label(payment),
                            "amount": round(_safe_float(payment.get("amount")), 2),
                            "payment_type": payment.get("paymentType"),
                            "payment_provider": payment.get("paymentProvider"),
                            "payment_card_id": payment.get("paymentCardId"),
                        }
                        for payment in (order.get("payments") if isinstance(order.get("payments"), list) else [])
                        if isinstance(payment, dict) and _safe_float(payment.get("amount")) > 0
                    ]

                    line_items: list[tuple[str, str | None, float]] = []
                    raw_lines = order.get("lines") if isinstance(order.get("lines"), list) else []
                    for raw_line in raw_lines:
                        if not isinstance(raw_line, dict):
                            continue
                        product_id, product_name = _extract_order_line_product(raw_line)
                        line_amount = _extract_order_line_amount_incl_vat(raw_line)
                        if not product_id or line_amount <= 0:
                            continue
                        line_items.append((product_id, product_name, line_amount))

                    order_lines_total = sum(item[2] for item in line_items)
                    if order_lines_total <= 0:
                        unresolved = unresolved_products.setdefault(
                            "__ORDER_TOTAL_UNKNOWN__",
                            {
                                "susoft_product_id": "__ORDER_TOTAL_UNKNOWN__",
                                "susoft_product_name": "Order without line totals",
                                "amount": 0.0,
                                "count": 0,
                            },
                        )
                        unresolved["amount"] = round(float(unresolved["amount"]) + paid_amount, 2)
                        unresolved["count"] = int(unresolved["count"]) + 1
                        continue

                    for product_id, product_name, line_total in line_items:
                        allocated_amount = paid_amount * (line_total / order_lines_total)
                        mapping = mapping_by_product_id.get(product_id)
                        account = (
                            str(mapping.income_account).strip()
                            if mapping is not None and mapping.income_account is not None and str(mapping.income_account).strip()
                            else default_income_account
                        )
                        if account:
                            account_amounts[account] += allocated_amount
                        else:
                            unresolved = unresolved_products.setdefault(
                                product_id,
                                {
                                    "susoft_product_id": product_id,
                                    "susoft_product_name": product_name,
                                    "amount": 0.0,
                                    "count": 0,
                                },
                            )
                            unresolved["amount"] = round(float(unresolved["amount"]) + allocated_amount, 2)
                            unresolved["count"] = int(unresolved["count"]) + 1

                    direct_sales_orders_detail.append(
                        {
                            "susoft_uuid": order.get("uuid"),
                            "susoft_number": order.get("number"),
                            "external_ref": order.get("externalRef"),
                            "alternative_id": order.get("alternativeId"),
                            "paid_amount": round(paid_amount, 2),
                            "payment_summary": payment_summary,
                            "line_items": [
                                {
                                    "product_id": product_id,
                                    "product_name": product_name,
                                    "line_total": round(line_total, 2),
                                }
                                for product_id, product_name, line_total in line_items
                            ],
                        }
                    )

            net_transfer_gross = max(0.0, direct_sales_gross - tt_linked_gross)
            account_lines = [
                {"income_account": account, "amount": round(amount, 2)}
                for account, amount in sorted(account_amounts.items())
                if round(amount, 2) != 0
            ]
            payment_method_breakdown = _summarize_payment_methods([order for order in orders if isinstance(order, dict) and not _is_tt_linked_susoft_order(order) and _settlement_day_paid_amount(order, target_date, timezone_name=tz_name) > 0], payment_rules)
            unresolved_list = sorted(
                [
                    {
                        "susoft_product_id": str(item["susoft_product_id"]),
                        "susoft_product_name": item.get("susoft_product_name"),
                        "amount": round(_safe_float(item.get("amount")), 2),
                        "count": int(item.get("count") or 0),
                    }
                    for item in unresolved_products.values()
                ],
                key=lambda row: (row["susoft_product_id"], row["susoft_product_name"] or ""),
            )

            if not execute:
                run.status = "PREVIEW"
            elif net_transfer_gross <= 0:
                run.status = "NOOP"
            elif unresolved_list:
                run.status = "MAPPING_MISSING"
            else:
                run.status = "READY_FOR_POSTING"
            run.direct_sales_gross = round(direct_sales_gross, 2)
            run.tt_linked_gross = round(tt_linked_gross, 2)
            run.net_transfer_gross = round(net_transfer_gross, 2)
            run.lines_count = len(account_lines)
            if run.status == "NOOP":
                run.message = "Ingen netto differanse for datoen."
            elif run.status == "MAPPING_MISSING":
                run.message = "Mangler inntektskonto-mapping for en eller flere artikler."
            elif run.status == "READY_FOR_POSTING":
                run.message = "Klar for bilagsopprettelse i Tripletex."
            else:
                run.message = "Preview beregnet uten posting."

            posting_result: dict[str, Any] | None = None
            offset_account = str(tenant.direct_sales_settlement_offset_account or "1900").strip() or "1900"
            send_to_ledger = bool(tenant.direct_sales_settlement_send_to_ledger)
            if execute and run.status == "READY_FOR_POSTING":
                posting_result = _post_direct_sales_settlement_to_tripletex(
                    tenant=tenant,
                    settlement_date=target_date,
                    account_lines=account_lines,
                    offset_account=offset_account,
                    send_to_ledger=send_to_ledger,
                    tripletex_overrides=_tripletex_overrides_for_tenant(tenant),
                )
                run.status = "POSTED"
                run.posted_voucher_id = str(posting_result.get("voucher_id") or "") or None
                run.message = str(posting_result.get("message") or "Direktesalg bilag opprettet uten automatisk bokforing")

            run.details_json = json.dumps(
                {
                    "from": from_dt,
                    "to": to_dt,
                    "direct_paid_orders": direct_paid_orders,
                    "tt_linked_paid_orders": tt_linked_paid_orders,
                    "default_income_account": default_income_account,
                    "offset_account": offset_account,
                    "send_to_ledger": send_to_ledger,
                    "payment_rules_json": tenant.direct_sales_payment_rules_json or "[]",
                    "account_lines": account_lines,
                    "unresolved_products": unresolved_list,
                    "payment_method_breakdown": payment_method_breakdown,
                    "direct_sales_orders_detail": direct_sales_orders_detail,
                    "posting_result": posting_result,
                },
                ensure_ascii=False,
            )
            run.finished_at = datetime.now(UTC)

            _add_event(
                session,
                tenant_id=tenant.id,
                event_type="DIRECT_SALES_SETTLEMENT_CALCULATED",
                message="Direktesalg dagsoppgjor beregnet",
                level="INFO",
                details={
                    "settlement_date": target_date.isoformat(),
                    "sales_day_cutoff": _normalize_hhmm(tenant.direct_sales_sales_day_cutoff or "05:00"),
                    "direct_sales_gross": run.direct_sales_gross,
                    "tt_linked_gross": run.tt_linked_gross,
                    "net_transfer_gross": run.net_transfer_gross,
                    "account_lines": account_lines,
                    "unresolved_products": unresolved_list,
                    "payment_method_breakdown": payment_method_breakdown,
                    "direct_sales_orders_detail": direct_sales_orders_detail,
                    "offset_account": offset_account,
                    "send_to_ledger": send_to_ledger,
                    "posted_voucher_id": run.posted_voucher_id,
                    "execute": execute,
                },
            )
            session.commit()
        except Exception as exc:
            run.status = "FAILED"
            run.message = str(exc)
            run.finished_at = datetime.now(UTC)
            _add_event(
                session,
                tenant_id=tenant.id,
                event_type="DIRECT_SALES_SETTLEMENT_FAILED",
                message="Direktesalg dagsoppgjor feilet",
                level="ERROR",
                details={"settlement_date": target_date.isoformat(), "error": str(exc)},
            )
            session.commit()
            raise

        return {
            "tenant_key": tenant_key,
            "settlement_date": target_date.isoformat(),
            "run_id": run.id,
            "status": run.status,
            "sales_day_cutoff": _normalize_hhmm(tenant.direct_sales_sales_day_cutoff or "05:00"),
            "direct_sales_gross": run.direct_sales_gross,
            "tt_linked_gross": run.tt_linked_gross,
            "net_transfer_gross": run.net_transfer_gross,
            "direct_paid_orders": direct_paid_orders,
            "tt_linked_paid_orders": tt_linked_paid_orders,
            "account_lines": account_lines,
            "unresolved_products": unresolved_list,
            "payment_method_breakdown": payment_method_breakdown,
            "direct_sales_orders_detail": direct_sales_orders_detail,
            "offset_account": str(tenant.direct_sales_settlement_offset_account or "1900").strip() or "1900",
            "send_to_ledger": bool(tenant.direct_sales_settlement_send_to_ledger),
            "posted_voucher_id": run.posted_voucher_id,
            "message": run.message,
        }
