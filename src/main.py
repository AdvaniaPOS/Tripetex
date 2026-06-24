from __future__ import annotations

import secrets
import threading
import time
from datetime import UTC, date, datetime, timedelta
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from zoneinfo import ZoneInfo

from fastapi import Depends, FastAPI, Header, HTTPException, Query, status
from fastapi.responses import HTMLResponse
from sqlalchemy import desc, func, select

from src.auth import require_dashboard_auth
from src.config import get_settings
from src.db import db_health_check, db_session, init_db
from src.models import ArticleIncomeMapping, DirectSalesSettlementRun, JobRun, OrderSync, SyncEvent, Tenant
from src.susoft_client import add_webhook as add_susoft_webhook
from src.susoft_client import authenticate as authenticate_susoft
from src.susoft_client import list_webhooks as list_susoft_webhooks
from src.sync_service import (
    calculate_direct_sales_settlement_for_tenant,
    get_sendable_orders_for_tenant,
    process_susoft_payment_for_tenant,
    process_tripletex_order_by_id_for_tenant,
    retry_failed_orders_for_tenant,
    run_manual_sync_for_tenant,
    sync_paid_orders_to_tripletex_for_tenant,
)
from src.tripletex_client import create_event_subscription, create_session_token, list_event_subscriptions


settings = get_settings()
app = FastAPI(title=settings.app_name)
AUTO_PAID_SYNC_MIN_INTERVAL_MINUTES = 1
AUTO_PAID_SYNC_MAX_INTERVAL_MINUTES = 1440
AUTO_PAID_SYNC_TICK_SECONDS = 5
DIRECT_SALES_SETTLEMENT_TICK_SECONDS = 30


def _require_webhook_secret(x_webhook_secret: str | None = Header(default=None, alias="X-Webhook-Secret")) -> None:
    expected_secret = settings.webhook_shared_secret.strip()
    if not expected_secret:
        return
    if not x_webhook_secret or not secrets.compare_digest(x_webhook_secret, expected_secret):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid webhook secret")


def _tripletex_overrides_from_tenant(tenant: Tenant) -> dict[str, str]:
    data: dict[str, str] = {}
    if tenant.tripletex_base_url:
        data["tripletex_base_url"] = tenant.tripletex_base_url
    if tenant.tripletex_consumer_token:
        data["tripletex_consumer_token"] = tenant.tripletex_consumer_token
    if tenant.tripletex_employee_token:
        data["tripletex_employee_token"] = tenant.tripletex_employee_token
    return data


def _susoft_overrides_from_tenant(tenant: Tenant) -> dict[str, str]:
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


def _keep_or_replace_secret(current: str | None, incoming: str | None) -> str | None:
    if incoming is None:
        return current
    value = incoming.strip()
    return current if not value else value


def _to_bool(value: object, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return default


def _clamp_auto_paid_sync_interval_minutes(raw_value: object, *, default: int = 1) -> int:
    try:
        value = int(str(raw_value))
    except Exception:
        value = default
    return max(AUTO_PAID_SYNC_MIN_INTERVAL_MINUTES, min(AUTO_PAID_SYNC_MAX_INTERVAL_MINUTES, value))


def _normalize_hhmm(raw_value: object, *, default: str = "23:00") -> str:
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


def _run_auto_paid_sync_worker(stop_event: threading.Event) -> None:
    next_run_by_tenant: dict[str, float] = {}
    while not stop_event.wait(AUTO_PAID_SYNC_TICK_SECONDS):
        try:
            with db_session() as session:
                tenants = session.scalars(
                    select(Tenant)
                    .where(
                        Tenant.active.is_(True),
                        Tenant.auto_paid_sync_enabled.is_(True),
                    )
                    .order_by(Tenant.id.asc())
                ).all()
        except Exception:
            continue

        now = time.monotonic()
        active_keys = {tenant.tenant_key for tenant in tenants}
        for key in list(next_run_by_tenant.keys()):
            if key not in active_keys:
                next_run_by_tenant.pop(key, None)

        for tenant in tenants:
            if not (tenant.susoft_shop_url_key and tenant.susoft_username and tenant.susoft_password):
                continue

            interval_minutes = _clamp_auto_paid_sync_interval_minutes(tenant.auto_paid_sync_interval_minutes, default=1)
            next_due = next_run_by_tenant.get(tenant.tenant_key, 0.0)
            if now < next_due:
                continue

            next_run_by_tenant[tenant.tenant_key] = now + float(interval_minutes * 60)
            try:
                sync_paid_orders_to_tripletex_for_tenant(
                    tenant.tenant_key,
                    limit=settings.sync_default_limit,
                    payment_type_id=20756819,
                )
            except Exception:
                # Keep the scheduler resilient; operational details are captured as sync events.
                continue


def _run_daily_direct_sales_settlement_worker(stop_event: threading.Event) -> None:
    next_run_by_tenant: dict[str, float] = {}
    while not stop_event.wait(DIRECT_SALES_SETTLEMENT_TICK_SECONDS):
        try:
            with db_session() as session:
                tenants = session.scalars(
                    select(Tenant)
                    .where(
                        Tenant.active.is_(True),
                        Tenant.daily_direct_sales_sync_enabled.is_(True),
                    )
                    .order_by(Tenant.id.asc())
                ).all()
        except Exception:
            continue

        now_utc = datetime.now(UTC)
        active_keys = {tenant.tenant_key for tenant in tenants}
        for key in list(next_run_by_tenant.keys()):
            if key not in active_keys:
                next_run_by_tenant.pop(key, None)

        for tenant in tenants:
            time_value = _normalize_hhmm(tenant.daily_direct_sales_sync_time, default="23:00")
            hh, mm = time_value.split(":", 1)
            target_hour = int(hh)
            target_minute = int(mm)
            tz_name = get_settings().tripletex_timezone
            local_now = now_utc.astimezone(ZoneInfo(tz_name))
            target_local = local_now.replace(hour=target_hour, minute=target_minute, second=0, microsecond=0)
            if local_now < target_local:
                target_local = target_local - timedelta(days=1)
            settlement_date = target_local.date()

            gate_key = f"{tenant.tenant_key}:{settlement_date.isoformat()}"
            if gate_key in next_run_by_tenant and time.monotonic() < next_run_by_tenant[gate_key]:
                continue
            next_run_by_tenant[gate_key] = time.monotonic() + 3600.0

            try:
                calculate_direct_sales_settlement_for_tenant(
                    tenant.tenant_key,
                    settlement_date=settlement_date,
                    execute=True,
                )
            except Exception:
                continue


def _append_tenant_key_to_target_url(target_url: str, tenant_key: str) -> str:
    parts = urlsplit(target_url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    if not query.get("tenant_key"):
        query["tenant_key"] = tenant_key
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


def _resolve_tripletex_webhook_tenant_key(incoming_tenant_key: str | None) -> str:
    if incoming_tenant_key and incoming_tenant_key.strip():
        return incoming_tenant_key.strip()

    with db_session() as session:
        active_tenants = session.scalars(select(Tenant).where(Tenant.active.is_(True)).order_by(Tenant.id.asc())).all()

    if len(active_tenants) == 1:
        return active_tenants[0].tenant_key
    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail="tenant_key mangler. Sett tenant_key i callback-url query eller payload.",
    )


def _resolve_susoft_webhook_tenant_key(incoming_tenant_key: str | None) -> str:
    if incoming_tenant_key and incoming_tenant_key.strip():
        return incoming_tenant_key.strip()
    return _resolve_tripletex_webhook_tenant_key(incoming_tenant_key)


def _is_valid_webhook_secret(
    *,
    provided_header: str | None = None,
    provided_token: str | None = None,
    provided_auth_header: str | None = None,
    provided_alt_header: str | None = None,
) -> bool:
    expected_secret = settings.webhook_shared_secret.strip()
    if not expected_secret:
        return True
    if provided_header and secrets.compare_digest(str(provided_header), expected_secret):
        return True
    if provided_alt_header and secrets.compare_digest(str(provided_alt_header), expected_secret):
        return True
    if provided_auth_header:
        auth_value = str(provided_auth_header).strip()
        if auth_value.lower().startswith("bearer "):
            auth_value = auth_value[7:].strip()
        if auth_value and secrets.compare_digest(auth_value, expected_secret):
            return True
    if provided_token and secrets.compare_digest(str(provided_token), expected_secret):
        return True
    return False


def _extract_susoft_uuid_from_payload(payload: dict[str, object]) -> str:
    direct_uuid = payload.get("susoft_uuid") or payload.get("susoftUuid") or payload.get("uuid")
    if isinstance(direct_uuid, str) and direct_uuid.strip():
        return direct_uuid.strip()

    for container_key in ("entity", "order", "value", "data"):
        container = payload.get(container_key)
        if isinstance(container, dict):
            nested_uuid = container.get("uuid")
            if isinstance(nested_uuid, str) and nested_uuid.strip():
                return nested_uuid.strip()

    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="susoft_uuid mangler")


def _extract_tripletex_order_id_from_susoft_payload(payload: dict[str, object]) -> str | None:
    for key in ("alternativeId", "externalRef", "tripletex_order_id", "tripletexOrderId", "order_id", "orderId"):
        raw = payload.get(key)
        if raw is not None and str(raw).strip():
            return str(raw).strip()

    for container_key in ("entity", "order", "value", "data"):
        container = payload.get(container_key)
        if isinstance(container, dict):
            for key in ("alternativeId", "externalRef", "tripletex_order_id", "tripletexOrderId", "order_id", "orderId"):
                raw = container.get(key)
                if raw is not None and str(raw).strip():
                    return str(raw).strip()
    return None


@app.on_event("startup")
def on_startup() -> None:
    app.state.startup_error = None
    if settings.app_auto_create_tables:
        try:
            init_db()
        except Exception as exc:
            app.state.startup_error = f"database init failed: {exc}"

    stop_event = threading.Event()
    worker = threading.Thread(target=_run_auto_paid_sync_worker, args=(stop_event,), daemon=True, name="auto-paid-sync-worker")
    direct_sales_worker = threading.Thread(
        target=_run_daily_direct_sales_settlement_worker,
        args=(stop_event,),
        daemon=True,
        name="daily-direct-sales-settlement-worker",
    )
    app.state.auto_paid_sync_stop_event = stop_event
    app.state.auto_paid_sync_worker = worker
    app.state.daily_direct_sales_worker = direct_sales_worker
    worker.start()
    direct_sales_worker.start()


@app.on_event("shutdown")
def on_shutdown() -> None:
    stop_event = getattr(app.state, "auto_paid_sync_stop_event", None)
    worker = getattr(app.state, "auto_paid_sync_worker", None)
    direct_sales_worker = getattr(app.state, "daily_direct_sales_worker", None)
    if isinstance(stop_event, threading.Event):
        stop_event.set()
    if isinstance(worker, threading.Thread) and worker.is_alive():
        worker.join(timeout=3)
    if isinstance(direct_sales_worker, threading.Thread) and direct_sales_worker.is_alive():
        direct_sales_worker.join(timeout=3)


@app.get("/health")
def health() -> dict[str, str | bool]:
    startup_error = getattr(app.state, "startup_error", None)
    return {
        "service": settings.app_name,
        "environment": settings.app_env,
        "database_ok": db_health_check(),
        "startup_error": startup_error or "",
    }


@app.get("/api/status", dependencies=[Depends(require_dashboard_auth)])
def api_status() -> dict[str, object]:
    with db_session() as session:
        tenant_count = session.scalar(select(func.count()).select_from(Tenant)) or 0
        running_jobs = session.scalar(select(func.count()).select_from(JobRun).where(JobRun.status == "RUNNING")) or 0

        latest_run_stmt = select(JobRun).order_by(desc(JobRun.started_at)).limit(1)
        latest_run = session.scalar(latest_run_stmt)

    latest: dict[str, object] | None = None
    if latest_run is not None:
        latest = {
            "id": latest_run.id,
            "tenant_id": latest_run.tenant_id,
            "job_name": latest_run.job_name,
            "status": latest_run.status,
            "started_at": latest_run.started_at.isoformat(),
            "finished_at": latest_run.finished_at.isoformat() if latest_run.finished_at else None,
        }

    return {
        "tenant_count": tenant_count,
        "running_jobs": running_jobs,
        "latest_job_run": latest,
        "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    }


@app.post("/api/tenants/{tenant_key}/sync/manual", dependencies=[Depends(require_dashboard_auth)])
def api_manual_sync(
    tenant_key: str,
    dry_run: bool = True,
    limit: int = Query(default=settings.sync_default_limit, ge=1, le=500),
) -> dict[str, object]:
    result = run_manual_sync_for_tenant(tenant_key, dry_run=dry_run, limit=limit)
    return result


@app.post("/api/tenants/{tenant_key}/sync/retry-failed", dependencies=[Depends(require_dashboard_auth)])
def api_retry_failed_sync(
    tenant_key: str,
    limit: int = Query(default=settings.sync_default_limit, ge=1, le=500),
) -> dict[str, object]:
    result = retry_failed_orders_for_tenant(tenant_key, limit=limit)
    return result


@app.post("/api/tenants/{tenant_key}/sync/paid-from-susoft", dependencies=[Depends(require_dashboard_auth)])
def api_sync_paid_from_susoft(
    tenant_key: str,
    limit: int = Query(default=settings.sync_default_limit, ge=1, le=500),
    payment_type_id: int = Query(default=20756819, ge=1),
) -> dict[str, object]:
    result = sync_paid_orders_to_tripletex_for_tenant(
        tenant_key,
        limit=limit,
        payment_type_id=payment_type_id,
    )
    return result


@app.post("/webhooks/tripletex/order", dependencies=[Depends(_require_webhook_secret)])
def webhook_tripletex_order(payload: dict[str, object], tenant_key: str | None = Query(default=None)) -> dict[str, object]:
    tenant_key_value = _resolve_tripletex_webhook_tenant_key(
        tenant_key
        or str(payload.get("tenant_key") or payload.get("tenantKey") or "").strip()
    )

    # Tripletex webhook format uses id/event/value, while manual test payloads may use order_id.
    raw_order_id = (
        payload.get("order_id")
        or payload.get("orderId")
        or payload.get("tripletex_order_id")
        or payload.get("tripletexOrderId")
        or payload.get("id")
    )
    if raw_order_id is None and isinstance(payload.get("value"), dict):
        raw_order_id = payload["value"].get("id")
    if raw_order_id is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="order_id mangler")

    try:
        order_id = int(str(raw_order_id))
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="order_id må være et tall") from exc

    dry_run = bool(payload.get("dry_run") or payload.get("dryRun") or False)
    try:
        return process_tripletex_order_by_id_for_tenant(tenant_key_value, order_id, dry_run=dry_run)
    except RuntimeError as exc:
        detail = str(exc)
        status_code = status.HTTP_404_NOT_FOUND if "Fant ikke" in detail or "finnes ikke" in detail else status.HTTP_400_BAD_REQUEST
        raise HTTPException(status_code=status_code, detail=detail) from exc


@app.post("/webhooks/susoft/payment")
def webhook_susoft_payment(
    payload: dict[str, object],
    tenant_key: str | None = Query(default=None),
    token: str | None = Query(default=None),
    x_webhook_secret: str | None = Header(default=None, alias="X-Webhook-Secret"),
    x_webhook_token: str | None = Header(default=None, alias="X-Webhook-Token"),
    authorization: str | None = Header(default=None, alias="Authorization"),
) -> dict[str, object]:
    payload_token_value = payload.get("token") if isinstance(payload.get("token"), str) else None
    if not _is_valid_webhook_secret(
        provided_header=x_webhook_secret,
        provided_alt_header=x_webhook_token,
        provided_auth_header=authorization,
        provided_token=token or payload_token_value,
    ):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid webhook secret")

    tenant_key_value = _resolve_susoft_webhook_tenant_key(
        tenant_key
        or str(payload.get("tenant_key") or payload.get("tenantKey") or "").strip()
    )
    tripletex_order_id = _extract_tripletex_order_id_from_susoft_payload(payload)

    susoft_uuid: str | None = None
    try:
        susoft_uuid = _extract_susoft_uuid_from_payload(payload)
    except HTTPException:
        susoft_uuid = None

    if not susoft_uuid and tripletex_order_id:
        with db_session() as session:
            tenant = session.scalar(select(Tenant).where(Tenant.tenant_key == tenant_key_value))
            if tenant is not None:
                row = session.scalar(
                    select(OrderSync).where(
                        OrderSync.tenant_id == tenant.id,
                        OrderSync.tripletex_order_id == tripletex_order_id,
                    )
                )
                if row is not None and row.susoft_uuid:
                    susoft_uuid = row.susoft_uuid

    if not susoft_uuid:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="susoft_uuid mangler")

    raw_payment_type_id = payload.get("payment_type_id") or payload.get("paymentTypeId") or 20756819
    try:
        payment_type_id = int(str(raw_payment_type_id))
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="payment_type_id må være et tall") from exc

    paid_amount_value = payload.get("paid_amount") or payload.get("paidAmount")
    if paid_amount_value is None or not str(paid_amount_value).strip():
        paid_amount = None
    else:
        try:
            paid_amount = float(str(paid_amount_value))
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="paid_amount må være et tall") from exc
    payment_date = str(payload.get("payment_date") or payload.get("paymentDate") or "").strip() or None

    try:
        return process_susoft_payment_for_tenant(
            tenant_key_value,
            susoft_uuid,
            payment_type_id=payment_type_id,
            paid_amount=paid_amount,
            payment_date=payment_date,
        )
    except RuntimeError as exc:
        detail = str(exc)
        status_code = status.HTTP_404_NOT_FOUND if "Fant" in detail or "finnes ikke" in detail else status.HTTP_400_BAD_REQUEST
        raise HTTPException(status_code=status_code, detail=detail) from exc


@app.get("/api/susoft/webhooks", dependencies=[Depends(require_dashboard_auth)])
def api_susoft_webhooks(tenant_key: str, webhook_type: str = "ON_ORDER_INVOICED") -> dict[str, object]:
    with db_session() as session:
        tenant = session.scalar(select(Tenant).where(Tenant.tenant_key == tenant_key))
        if tenant is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Tenant finnes ikke")

    overrides = _susoft_overrides_from_tenant(tenant)
    token = authenticate_susoft(overrides=overrides)
    webhooks = list_susoft_webhooks(webhook_type, token=token, overrides=overrides)
    return {"webhooks": webhooks}


@app.post("/api/susoft/webhooks/order-invoiced", dependencies=[Depends(require_dashboard_auth)])
def api_susoft_create_order_invoiced_webhook(tenant_key: str, target_url: str) -> dict[str, object]:
    with db_session() as session:
        tenant = session.scalar(select(Tenant).where(Tenant.tenant_key == tenant_key))
        if tenant is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Tenant finnes ikke")

    overrides = _susoft_overrides_from_tenant(tenant)
    token = authenticate_susoft(overrides=overrides)
    target_url_with_tenant = _append_tenant_key_to_target_url(target_url, tenant_key)
    shared_secret = settings.webhook_shared_secret.strip()
    try:
        created = add_susoft_webhook(
            webhook_type="ON_ORDER_INVOICED",
            target_url=target_url_with_tenant,
            webhook_token=shared_secret,
            active=True,
            token=token,
            overrides=overrides,
        )
        return created
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@app.get("/api/tripletex/webhooks/subscriptions", dependencies=[Depends(require_dashboard_auth)])
def api_tripletex_webhook_subscriptions(tenant_key: str) -> dict[str, object]:
    with db_session() as session:
        tenant = session.scalar(select(Tenant).where(Tenant.tenant_key == tenant_key))
        if tenant is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Tenant finnes ikke")

    overrides = _tripletex_overrides_from_tenant(tenant)
    token = create_session_token(overrides=overrides)
    subscriptions = list_event_subscriptions(token, overrides=overrides)
    return {"subscriptions": subscriptions}


@app.post("/api/tripletex/webhooks/subscriptions/order-create", dependencies=[Depends(require_dashboard_auth)])
def api_tripletex_create_order_webhook(tenant_key: str, target_url: str) -> dict[str, object]:
    with db_session() as session:
        tenant = session.scalar(select(Tenant).where(Tenant.tenant_key == tenant_key))
        if tenant is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Tenant finnes ikke")

    overrides = _tripletex_overrides_from_tenant(tenant)
    token = create_session_token(overrides=overrides)
    target_url_with_tenant = _append_tenant_key_to_target_url(target_url, tenant_key)
    try:
        # Keep subscription payload minimal. Some Tripletex setups reject complex fields filters for events.
        result = create_event_subscription(
            token,
            event="order.create",
            target_url=target_url_with_tenant,
            overrides=overrides,
            auth_header_name="X-Webhook-Secret" if settings.webhook_shared_secret.strip() else None,
            auth_header_value=settings.webhook_shared_secret.strip() or None,
        )
        return result
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@app.post("/api/tenants", dependencies=[Depends(require_dashboard_auth)])
def api_upsert_tenant(payload: dict[str, object]) -> dict[str, object]:
    tenant_key = str(payload.get("tenant_key") or payload.get("tenantKey") or "").strip()
    name = str(payload.get("name") or "").strip()
    if not tenant_key:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="tenant_key mangler")
    if not name:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="name mangler")

    with db_session() as session:
        row = session.scalar(select(Tenant).where(Tenant.tenant_key == tenant_key))
        if row is None:
            row = Tenant(tenant_key=tenant_key, name=name, active=True)
            session.add(row)
            session.flush()

        row.name = name
        row.active = bool(payload.get("active", True))
        row.tripletex_base_url = str(payload.get("tripletex_base_url") or payload.get("tripletexBaseUrl") or row.tripletex_base_url or "").strip() or None
        row.tripletex_consumer_token = _keep_or_replace_secret(
            row.tripletex_consumer_token,
            payload.get("tripletex_consumer_token") if "tripletex_consumer_token" in payload else payload.get("tripletexConsumerToken"),
        )
        row.tripletex_employee_token = _keep_or_replace_secret(
            row.tripletex_employee_token,
            payload.get("tripletex_employee_token") if "tripletex_employee_token" in payload else payload.get("tripletexEmployeeToken"),
        )
        row.susoft_base_url = str(payload.get("susoft_base_url") or payload.get("susoftBaseUrl") or row.susoft_base_url or "").strip() or None
        row.susoft_shop_url_key = _keep_or_replace_secret(
            row.susoft_shop_url_key,
            payload.get("susoft_shop_url_key") if "susoft_shop_url_key" in payload else payload.get("susoftShopUrlKey"),
        )
        row.susoft_username = _keep_or_replace_secret(
            row.susoft_username,
            payload.get("susoft_username") if "susoft_username" in payload else payload.get("susoftUsername"),
        )
        row.susoft_password = _keep_or_replace_secret(
            row.susoft_password,
            payload.get("susoft_password") if "susoft_password" in payload else payload.get("susoftPassword"),
        )
        row.auto_paid_sync_enabled = _to_bool(
            payload.get("auto_paid_sync_enabled") if "auto_paid_sync_enabled" in payload else payload.get("autoPaidSyncEnabled"),
            default=row.auto_paid_sync_enabled if row.auto_paid_sync_enabled is not None else True,
        )
        row.auto_paid_sync_interval_minutes = _clamp_auto_paid_sync_interval_minutes(
            payload.get("auto_paid_sync_interval_minutes") if "auto_paid_sync_interval_minutes" in payload else payload.get("autoPaidSyncIntervalMinutes"),
            default=row.auto_paid_sync_interval_minutes if row.auto_paid_sync_interval_minutes is not None else 1,
        )
        row.daily_direct_sales_sync_enabled = _to_bool(
            payload.get("daily_direct_sales_sync_enabled") if "daily_direct_sales_sync_enabled" in payload else payload.get("dailyDirectSalesSyncEnabled"),
            default=row.daily_direct_sales_sync_enabled if row.daily_direct_sales_sync_enabled is not None else False,
        )
        row.daily_direct_sales_sync_time = _normalize_hhmm(
            payload.get("daily_direct_sales_sync_time") if "daily_direct_sales_sync_time" in payload else payload.get("dailyDirectSalesSyncTime"),
            default=row.daily_direct_sales_sync_time or "23:00",
        )
        incoming_default_account = payload.get("direct_sales_default_income_account") if "direct_sales_default_income_account" in payload else payload.get("directSalesDefaultIncomeAccount")
        row.direct_sales_default_income_account = _keep_or_replace_secret(
            row.direct_sales_default_income_account,
            str(incoming_default_account) if incoming_default_account is not None else None,
        )
        incoming_offset_account = payload.get("direct_sales_settlement_offset_account") if "direct_sales_settlement_offset_account" in payload else payload.get("directSalesSettlementOffsetAccount")
        row.direct_sales_settlement_offset_account = (
            str(incoming_offset_account).strip()
            if incoming_offset_account is not None and str(incoming_offset_account).strip()
            else (row.direct_sales_settlement_offset_account or "1900")
        )

        session.commit()
        session.refresh(row)

    return {
        "id": row.id,
        "tenant_key": row.tenant_key,
        "name": row.name,
        "active": row.active,
        "auto_paid_sync_enabled": row.auto_paid_sync_enabled,
        "auto_paid_sync_interval_minutes": row.auto_paid_sync_interval_minutes,
        "daily_direct_sales_sync_enabled": row.daily_direct_sales_sync_enabled,
        "daily_direct_sales_sync_time": row.daily_direct_sales_sync_time,
        "direct_sales_default_income_account": row.direct_sales_default_income_account,
        "direct_sales_settlement_offset_account": row.direct_sales_settlement_offset_account,
        "has_tripletex_tokens": bool(row.tripletex_consumer_token and row.tripletex_employee_token),
        "has_susoft_credentials": bool(row.susoft_shop_url_key and row.susoft_username and row.susoft_password),
    }


@app.get("/api/tenants/{tenant_key}/connections", dependencies=[Depends(require_dashboard_auth)])
def api_tenant_connections(tenant_key: str) -> dict[str, object]:
    with db_session() as session:
        row = session.scalar(select(Tenant).where(Tenant.tenant_key == tenant_key))
        if row is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Tenant finnes ikke")

    return {
        "tenant_key": row.tenant_key,
        "name": row.name,
        "active": row.active,
        "auto_paid_sync_enabled": row.auto_paid_sync_enabled,
        "auto_paid_sync_interval_minutes": row.auto_paid_sync_interval_minutes,
        "daily_direct_sales_sync_enabled": row.daily_direct_sales_sync_enabled,
        "daily_direct_sales_sync_time": row.daily_direct_sales_sync_time,
        "direct_sales_default_income_account": row.direct_sales_default_income_account,
        "direct_sales_settlement_offset_account": row.direct_sales_settlement_offset_account,
        "tripletex_base_url": row.tripletex_base_url or settings.tripletex_base_url,
        "susoft_base_url": row.susoft_base_url or settings.susoft_base_url,
        "has_tripletex_tokens": bool(row.tripletex_consumer_token and row.tripletex_employee_token),
        "has_susoft_credentials": bool(row.susoft_shop_url_key and row.susoft_username and row.susoft_password),
    }


@app.get("/api/tenants/{tenant_key}/sendable-orders", dependencies=[Depends(require_dashboard_auth)])
def api_sendable_orders(
    tenant_key: str,
    limit: int = Query(default=settings.sync_default_limit, ge=1, le=500),
) -> dict[str, object]:
    result = get_sendable_orders_for_tenant(tenant_key, limit=limit)
    return result


@app.get("/api/tenants", dependencies=[Depends(require_dashboard_auth)])
def api_tenants() -> list[dict[str, object]]:
    with db_session() as session:
        rows = session.scalars(select(Tenant).order_by(Tenant.id.asc())).all()

    return [
        {
            "id": row.id,
            "tenant_key": row.tenant_key,
            "name": row.name,
            "active": row.active,
            "auto_paid_sync_enabled": row.auto_paid_sync_enabled,
            "auto_paid_sync_interval_minutes": row.auto_paid_sync_interval_minutes,
            "daily_direct_sales_sync_enabled": row.daily_direct_sales_sync_enabled,
            "daily_direct_sales_sync_time": row.daily_direct_sales_sync_time,
            "direct_sales_default_income_account": row.direct_sales_default_income_account,
            "direct_sales_settlement_offset_account": row.direct_sales_settlement_offset_account,
            "has_tripletex_tokens": bool(row.tripletex_consumer_token and row.tripletex_employee_token),
            "has_susoft_credentials": bool(row.susoft_shop_url_key and row.susoft_username and row.susoft_password),
            "created_at": row.created_at.isoformat(),
        }
        for row in rows
    ]


@app.get("/api/order-sync", dependencies=[Depends(require_dashboard_auth)])
def api_order_sync(tenant_key: str, limit: int = Query(default=100, ge=1, le=500)) -> list[dict[str, object]]:
    with db_session() as session:
        tenant = session.scalar(select(Tenant).where(Tenant.tenant_key == tenant_key))
        if tenant is None:
            return []

        rows = session.scalars(
            select(OrderSync)
            .where(OrderSync.tenant_id == tenant.id)
            .order_by(desc(OrderSync.updated_at))
            .limit(limit)
        ).all()

    return [
        {
            "id": row.id,
            "tripletex_order_id": row.tripletex_order_id,
            "status": row.status,
            "susoft_uuid": row.susoft_uuid,
            "last_error": row.last_error,
            "updated_at": row.updated_at.isoformat(),
            "created_at": row.created_at.isoformat(),
        }
        for row in rows
    ]


@app.get("/api/events", dependencies=[Depends(require_dashboard_auth)])
def api_events(tenant_key: str, limit: int = Query(default=100, ge=1, le=500)) -> list[dict[str, object]]:
    with db_session() as session:
        tenant = session.scalar(select(Tenant).where(Tenant.tenant_key == tenant_key))
        if tenant is None:
            return []

        rows = session.scalars(
            select(SyncEvent)
            .where(SyncEvent.tenant_id == tenant.id)
            .order_by(desc(SyncEvent.created_at))
            .limit(limit)
        ).all()

    return [
        {
            "id": row.id,
            "job_run_id": row.job_run_id,
            "order_sync_id": row.order_sync_id,
            "event_type": row.event_type,
            "level": row.level,
            "message": row.message,
            "details_json": row.details_json,
            "created_at": row.created_at.isoformat(),
        }
        for row in rows
    ]


@app.get("/api/tenants/{tenant_key}/article-income-mappings", dependencies=[Depends(require_dashboard_auth)])
def api_article_income_mappings(tenant_key: str) -> list[dict[str, object]]:
    with db_session() as session:
        tenant = session.scalar(select(Tenant).where(Tenant.tenant_key == tenant_key))
        if tenant is None:
            return []

        rows = session.scalars(
            select(ArticleIncomeMapping)
            .where(ArticleIncomeMapping.tenant_id == tenant.id)
            .order_by(ArticleIncomeMapping.active.desc(), ArticleIncomeMapping.susoft_product_id.asc(), ArticleIncomeMapping.id.asc())
        ).all()

    return [
        {
            "id": row.id,
            "susoft_product_id": row.susoft_product_id,
            "susoft_product_name": row.susoft_product_name,
            "tripletex_product_id": row.tripletex_product_id,
            "income_account": row.income_account,
            "source": row.source,
            "active": row.active,
            "created_at": row.created_at.isoformat(),
            "updated_at": row.updated_at.isoformat(),
        }
        for row in rows
    ]


@app.post("/api/tenants/{tenant_key}/article-income-mappings", dependencies=[Depends(require_dashboard_auth)])
def api_upsert_article_income_mapping(tenant_key: str, payload: dict[str, object]) -> dict[str, object]:
    susoft_product_id = str(payload.get("susoft_product_id") or payload.get("susoftProductId") or "").strip()
    if not susoft_product_id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="susoft_product_id mangler")

    with db_session() as session:
        tenant = session.scalar(select(Tenant).where(Tenant.tenant_key == tenant_key))
        if tenant is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Tenant finnes ikke")

        row = session.scalar(
            select(ArticleIncomeMapping).where(
                ArticleIncomeMapping.tenant_id == tenant.id,
                ArticleIncomeMapping.susoft_product_id == susoft_product_id,
            )
        )
        if row is None:
            row = ArticleIncomeMapping(
                tenant_id=tenant.id,
                susoft_product_id=susoft_product_id,
                source="MANUAL",
                active=True,
            )
            session.add(row)
            session.flush()

        row.susoft_product_name = str(payload.get("susoft_product_name") or payload.get("susoftProductName") or row.susoft_product_name or "").strip() or None
        row.tripletex_product_id = str(payload.get("tripletex_product_id") or payload.get("tripletexProductId") or row.tripletex_product_id or "").strip() or None
        row.income_account = str(payload.get("income_account") or payload.get("incomeAccount") or row.income_account or "").strip() or None
        row.active = _to_bool(payload.get("active"), default=row.active if row.active is not None else True)
        row.source = str(payload.get("source") or row.source or "MANUAL").strip() or "MANUAL"
        row.updated_at = datetime.now(UTC)

        session.commit()
        session.refresh(row)

    return {
        "id": row.id,
        "susoft_product_id": row.susoft_product_id,
        "susoft_product_name": row.susoft_product_name,
        "tripletex_product_id": row.tripletex_product_id,
        "income_account": row.income_account,
        "source": row.source,
        "active": row.active,
        "created_at": row.created_at.isoformat(),
        "updated_at": row.updated_at.isoformat(),
    }


@app.delete("/api/tenants/{tenant_key}/article-income-mappings/{mapping_id}", dependencies=[Depends(require_dashboard_auth)])
def api_delete_article_income_mapping(tenant_key: str, mapping_id: int) -> dict[str, object]:
    with db_session() as session:
        tenant = session.scalar(select(Tenant).where(Tenant.tenant_key == tenant_key))
        if tenant is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Tenant finnes ikke")

        row = session.scalar(
            select(ArticleIncomeMapping).where(
                ArticleIncomeMapping.id == mapping_id,
                ArticleIncomeMapping.tenant_id == tenant.id,
            )
        )
        if row is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Mapping finnes ikke")

        session.delete(row)
        session.commit()

    return {"deleted": True, "id": mapping_id}


@app.post("/api/tenants/{tenant_key}/settlement/direct-sales", dependencies=[Depends(require_dashboard_auth)])
def api_direct_sales_settlement_run(
    tenant_key: str,
    settlement_date: str | None = Query(default=None),
    execute: bool = Query(default=False),
) -> dict[str, object]:
    parsed_date = date.fromisoformat(settlement_date) if settlement_date else None
    result = calculate_direct_sales_settlement_for_tenant(
        tenant_key,
        settlement_date=parsed_date,
        execute=execute,
    )
    return result


@app.get("/api/tenants/{tenant_key}/settlement/direct-sales/runs", dependencies=[Depends(require_dashboard_auth)])
def api_direct_sales_settlement_runs(tenant_key: str, limit: int = Query(default=30, ge=1, le=365)) -> list[dict[str, object]]:
    with db_session() as session:
        tenant = session.scalar(select(Tenant).where(Tenant.tenant_key == tenant_key))
        if tenant is None:
            return []

        rows = session.scalars(
            select(DirectSalesSettlementRun)
            .where(DirectSalesSettlementRun.tenant_id == tenant.id)
            .order_by(desc(DirectSalesSettlementRun.settlement_date), desc(DirectSalesSettlementRun.id))
            .limit(limit)
        ).all()

    return [
        {
            "id": row.id,
            "settlement_date": row.settlement_date.isoformat(),
            "status": row.status,
            "direct_sales_gross": row.direct_sales_gross,
            "tt_linked_gross": row.tt_linked_gross,
            "net_transfer_gross": row.net_transfer_gross,
            "lines_count": row.lines_count,
            "posted_voucher_id": row.posted_voucher_id,
            "message": row.message,
            "started_at": row.started_at.isoformat(),
            "finished_at": row.finished_at.isoformat() if row.finished_at else None,
        }
        for row in rows
    ]


@app.get("/", response_class=HTMLResponse, dependencies=[Depends(require_dashboard_auth)])
def dashboard_home() -> str:
    return """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>TT-Susoft Operations</title>
  <style>
        :root {
            --bg: #edf2f7;
            --bg-alt: #f7fafc;
            --panel: rgba(255, 255, 255, 0.88);
            --panel-solid: #ffffff;
            --ink: #102033;
            --muted: #5f6b7a;
            --brand: #0f4c81;
            --brand-2: #0b6ea8;
            --accent: #f58220;
            --accent-2: #ffb14d;
            --ok: #1b7f4d;
            --warn: #b7791f;
            --err: #b8322d;
            --line: #d6dde6;
            --line-strong: #bac5d3;
            --shadow: 0 18px 45px rgba(16, 32, 51, 0.08);
            --shadow-soft: 0 10px 24px rgba(16, 32, 51, 0.06);
        }
        * { box-sizing: border-box; }
        html { scroll-behavior: smooth; }
        body {
            margin: 0;
            font-family: "Aptos", "Segoe UI", sans-serif;
            background:
                radial-gradient(circle at 8% 4%, rgba(15, 76, 129, 0.16), transparent 24%),
                radial-gradient(circle at 92% 8%, rgba(245, 130, 32, 0.15), transparent 22%),
                linear-gradient(180deg, var(--bg-alt) 0%, var(--bg) 100%);
            color: var(--ink);
            min-height: 100vh;
        }
        .wrap { max-width: 1400px; margin: 0 auto; padding: 24px; }
        .header {
            display: flex;
            align-items: flex-start;
            justify-content: space-between;
            gap: 18px;
            margin-bottom: 20px;
            padding: 22px 24px;
            border: 1px solid rgba(15, 76, 129, 0.10);
            border-radius: 22px;
            background:
                linear-gradient(135deg, rgba(15, 76, 129, 0.98), rgba(11, 110, 168, 0.92));
            color: #fff;
            box-shadow: var(--shadow);
        }
        .brandmark {
            width: 52px;
            height: 52px;
            border-radius: 16px;
            background: linear-gradient(135deg, #fff 0%, #e8f1f8 100%);
            display: grid;
            place-items: center;
            color: var(--brand);
            font-weight: 900;
            letter-spacing: -0.04em;
            box-shadow: inset 0 0 0 1px rgba(15, 76, 129, 0.12);
            flex-shrink: 0;
        }
        .header-left {
            display: flex;
            align-items: center;
            gap: 14px;
            min-width: 0;
        }
        h1 { margin: 0; font-size: 30px; line-height: 1.05; }
        .sub { margin-top: 6px; color: rgba(255, 255, 255, 0.82); }
        .header-right {
            text-align: right;
            display: grid;
            gap: 8px;
            justify-items: end;
        }
        .header-pill {
            display: inline-flex;
            align-items: center;
            gap: 8px;
            padding: 8px 12px;
            border-radius: 999px;
            background: rgba(255, 255, 255, 0.12);
            border: 1px solid rgba(255, 255, 255, 0.16);
            color: #fff;
            font-size: 12px;
            font-weight: 700;
            letter-spacing: 0.04em;
            text-transform: uppercase;
        }
        .controls {
            background: var(--panel);
            border: 1px solid rgba(15, 76, 129, 0.10);
            border-radius: 18px;
            padding: 16px;
            display: grid;
            grid-template-columns: 1.1fr 0.6fr repeat(4, minmax(140px, 1fr));
            gap: 12px;
            margin-bottom: 16px;
            box-shadow: var(--shadow-soft);
        }
        input, select, button {
            border-radius: 12px;
            border: 1px solid var(--line);
            padding: 11px 12px;
            font-size: 14px;
            width: 100%;
            background: #fff;
            color: var(--ink);
        }
        button {
            border: none;
            background: linear-gradient(180deg, var(--brand-2) 0%, var(--brand) 100%);
            color: #fff;
            font-weight: 700;
            cursor: pointer;
            box-shadow: 0 8px 18px rgba(15, 76, 129, 0.18);
            transition: transform 0.15s ease, box-shadow 0.15s ease, filter 0.15s ease;
        }
        button.secondary { background: linear-gradient(180deg, #5d6f82 0%, #445468 100%); }
        button:hover { filter: brightness(1.04); transform: translateY(-1px); }
        button:active { transform: translateY(0); }
        .grid {
            display: grid;
            grid-template-columns: repeat(6, minmax(0, 1fr));
            gap: 12px;
            margin-bottom: 16px;
        }
        .card {
            background: var(--panel);
            border-radius: 18px;
            border: 1px solid rgba(15, 76, 129, 0.10);
            padding: 16px;
            min-height: 104px;
            box-shadow: var(--shadow-soft);
            backdrop-filter: blur(8px);
        }
        .k { color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: 0.08em; font-weight: 700; }
        .v { font-size: 28px; font-weight: 900; margin-top: 6px; letter-spacing: -0.03em; }
        .v.small { font-size: 15px; line-height: 1.35; font-weight: 700; }
        .panel {
            background: var(--panel);
            border-radius: 18px;
            border: 1px solid rgba(15, 76, 129, 0.10);
            padding: 16px;
            margin-bottom: 14px;
            overflow: auto;
            box-shadow: var(--shadow-soft);
            backdrop-filter: blur(8px);
        }
        .panel h3 {
            margin: 0 0 12px;
            font-size: 16px;
            letter-spacing: -0.02em;
        }
        .tabs {
            display: inline-flex;
            gap: 8px;
            padding: 4px;
            border-radius: 999px;
            background: #eef3f8;
            border: 1px solid #dae3ec;
            margin-bottom: 12px;
        }
        .tab-btn {
            border: none;
            background: transparent;
            color: var(--muted);
            box-shadow: none;
            padding: 8px 14px;
            border-radius: 999px;
            cursor: pointer;
            font-weight: 800;
        }
        .tab-btn:hover { transform: none; filter: none; }
        .tab-btn.active {
            background: linear-gradient(180deg, var(--brand-2) 0%, var(--brand) 100%);
            color: #fff;
        }
        .tab-panel { display: none; }
        .tab-panel.active { display: block; }
        .menu-tabs {
            display: inline-flex;
            gap: 8px;
            padding: 6px;
            border-radius: 999px;
            background: #eef3f8;
            border: 1px solid #dae3ec;
            margin: 10px 0 14px;
        }
        .menu-btn {
            border: none;
            background: transparent;
            color: var(--muted);
            box-shadow: none;
            padding: 9px 16px;
            border-radius: 999px;
            cursor: pointer;
            font-weight: 800;
            width: auto;
        }
        .menu-btn:hover { transform: none; filter: none; }
        .menu-btn.active {
            background: linear-gradient(180deg, var(--brand-2) 0%, var(--brand) 100%);
            color: #fff;
        }
        .menu-panel { display: none; }
        .menu-panel.active { display: block; }
        .clickable-row { cursor: pointer; }
        .clickable-row.selected { background: #eaf3fb; }
        .table-wrap {
            overflow: auto;
            border-radius: 14px;
            border: 1px solid #e5ebf1;
            background: rgba(255, 255, 255, 0.74);
        }
        table { width: 100%; border-collapse: collapse; font-size: 13px; }
        th, td { text-align: left; border-bottom: 1px solid #e8edf3; padding: 11px 10px; vertical-align: top; }
        th { font-size: 11px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.08em; background: #f8fbfe; }
        tbody tr:hover { background: #f6f9fc; }
        .tag { border-radius: 999px; padding: 4px 10px; font-size: 11px; color: #fff; display: inline-block; font-weight: 700; letter-spacing: 0.03em; }
        .tag.ok { background: linear-gradient(180deg, #23915b 0%, #1b7f4d 100%); }
        .tag.warn { background: linear-gradient(180deg, #d79a28 0%, #b7791f 100%); }
        .tag.err { background: linear-gradient(180deg, #d14a42 0%, #b8322d 100%); }
        .muted { color: var(--muted); }
        .section-grid { display: grid; gap: 14px; }
        .toolbar-label { font-size: 12px; color: var(--muted); font-weight: 700; text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 8px; }
        @media (max-width: 700px) {
            .header { display: block; }
            .header-right { justify-items: start; text-align: left; margin-top: 12px; }
            .header-left { align-items: flex-start; }
            .wrap { padding: 14px; }
            .controls { grid-template-columns: 1fr; }
            .grid { grid-template-columns: 1fr 1fr; }
        }
  </style>
</head>
<body>
    <div class="wrap">
        <div class="header">
            <div class="header-left">
                <div class="brandmark">A</div>
                <div>
                    <h1>TT-Susoft Operations</h1>
                    <div class="sub">Advania-drevet drift for Tripletex, Susoft og betalingsstatus</div>
                </div>
            </div>
            <div class="header-right">
                <div class="header-pill">Operations dashboard</div>
                <div class="muted" id="now">-</div>
            </div>
        </div>

        <div class="menu-tabs" role="tablist" aria-label="Hovedmeny">
            <button class="menu-btn active" id="menuAddTenant" type="button">Legg til tenant</button>
            <button class="menu-btn" id="menuAllTenants" type="button">Alle tenants</button>
            <button class="menu-btn" id="menuSupport" type="button">Support</button>
        </div>

        <div class="menu-panel" id="menuAllTenantsPanel">
            <div class="panel">
                <h3>Alle Tenants</h3>
                <div class="table-wrap">
                    <table>
                        <thead><tr><th>Key</th><th>Navn</th><th>Aktiv</th><th>Tripletex</th><th>Susoft</th><th>Handling</th></tr></thead>
                        <tbody id="tenantListRows"><tr><td colspan="6" class="muted">Ingen tenants funnet.</td></tr></tbody>
                    </table>
                </div>
            </div>
        </div>

        <div class="menu-panel active" id="menuAddTenantPanel">

            <div class="panel">
                <h3>Tenant Setup</h3>
                <div class="section-grid" style="grid-template-columns: repeat(4, minmax(0, 1fr)); align-items: end;">
                    <div style="grid-column: span 2;">
                        <div class="toolbar-label">Rediger Eksisterende Tenant</div>
                        <select id="cfgExistingTenantSelect">
                            <option value="">-- Ny tenant --</option>
                        </select>
                    </div>
                    <div>
                        <button id="cfgLoadExistingTenant" class="secondary" type="button">Load Into Form</button>
                    </div>
                    <div>
                        <button id="cfgClearExistingTenant" class="secondary" type="button">Clear Edit Mode</button>
                    </div>
                    <div>
                        <div class="toolbar-label">Tenant Key</div>
                        <input id="cfgTenantKey" type="text" placeholder="butikk-1" />
                    </div>
                    <div>
                        <div class="toolbar-label">Tenant Name</div>
                        <input id="cfgTenantName" type="text" placeholder="Butikk 1" />
                    </div>
                    <div>
                        <div class="toolbar-label">Tripletex Base URL</div>
                        <input id="cfgTripletexBaseUrl" type="text" placeholder="https://tripletex.no/v2" />
                    </div>
                    <div>
                        <div class="toolbar-label">Susoft Base URL</div>
                        <input id="cfgSusoftBaseUrl" type="text" placeholder="https://api.susoft.com:4443" />
                    </div>
                    <div>
                        <div class="toolbar-label">Tripletex Consumer Token</div>
                        <input id="cfgTripletexConsumerToken" type="text" placeholder="Lim inn token (tom = behold eksisterende)" />
                    </div>
                    <div>
                        <div class="toolbar-label">Tripletex Employee Token</div>
                        <input id="cfgTripletexEmployeeToken" type="text" placeholder="Lim inn token (tom = behold eksisterende)" />
                    </div>
                    <div>
                        <div class="toolbar-label">Susoft Shop URL Key</div>
                        <input id="cfgSusoftShopUrlKey" type="text" placeholder="Lim inn key (tom = behold eksisterende)" />
                    </div>
                    <div>
                        <div class="toolbar-label">Susoft Username</div>
                        <input id="cfgSusoftUsername" type="text" placeholder="Bruker (tom = behold eksisterende)" />
                    </div>
                    <div style="grid-column: span 2;">
                        <div class="toolbar-label">Susoft Password</div>
                        <input id="cfgSusoftPassword" type="password" placeholder="Passord (tom = behold eksisterende)" />
                    </div>
                    <div>
                        <div class="toolbar-label">Auto Betalingspolling</div>
                        <select id="cfgAutoPaidSyncEnabled">
                            <option value="true">Aktiv</option>
                            <option value="false">Inaktiv</option>
                        </select>
                    </div>
                    <div>
                        <div class="toolbar-label">Polling Intervall (min)</div>
                        <input id="cfgAutoPaidSyncIntervalMinutes" type="number" min="1" max="1440" value="1" />
                    </div>
                    <div>
                        <div class="toolbar-label">Daglig Direktesalg Sync</div>
                        <select id="cfgDailyDirectSalesSyncEnabled">
                            <option value="true">Aktiv</option>
                            <option value="false">Inaktiv</option>
                        </select>
                    </div>
                    <div>
                        <div class="toolbar-label">Direktesalg Tid (HH:MM)</div>
                        <input id="cfgDailyDirectSalesSyncTime" type="time" value="23:00" />
                    </div>
                    <div>
                        <div class="toolbar-label">Default Inntektskonto</div>
                        <input id="cfgDirectSalesDefaultIncomeAccount" type="text" placeholder="f.eks. 3000" />
                    </div>
                    <div>
                        <div class="toolbar-label">Oppgjor Motkonto</div>
                        <input id="cfgDirectSalesSettlementOffsetAccount" type="text" placeholder="f.eks. 1900" value="1900" />
                    </div>
                    <div>
                        <div class="toolbar-label">Connection Status</div>
                        <div id="cfgStatus" class="muted">Velg eller opprett tenant.</div>
                    </div>
                    <div>
                        <button id="saveTenantConfig" type="button">Save Tenant Config</button>
                    </div>
                    <div>
                        <div class="toolbar-label">Lagringsstatus</div>
                        <div id="cfgSaveMessage" class="muted">Ikke lagret ennå.</div>
                    </div>
                </div>
            </div>
        </div>

        <div class="menu-panel" id="menuSupportPanel">
            <div class="controls">
                <div>
                    <div class="toolbar-label">Tenant</div>
                <select id="tenant"></select>
                </div>
                <div>
                    <div class="toolbar-label">Limit</div>
                <input id="limit" type="number" min="1" max="500" value="50" />
                </div>
                <button id="dry">Manual Sync (Dry)</button>
                <button id="exec">Manual Sync (Execute)</button>
                <button class="secondary" id="retry">Retry Failed</button>
                <button class="secondary" id="paid">Sync Paid -> TT</button>
                <button class="secondary" id="refresh">Refresh</button>
            </div>

            <div class="panel">
                <h3>Tripletex Webhooks</h3>
                <div class="section-grid" style="grid-template-columns: 1.2fr 0.6fr auto auto; align-items: end;">
                    <div>
                        <div class="toolbar-label">Callback URL</div>
                        <input id="webhookTargetUrl" type="text" placeholder="https://your-public-host/webhooks/tripletex/order" />
                    </div>
                    <div>
                        <div class="toolbar-label">Secret</div>
                        <input id="webhookSecretPreview" type="text" readonly value="X-Webhook-Secret" />
                    </div>
                    <button class="secondary" id="refreshWebhooks" type="button">Refresh Subscriptions</button>
                    <button id="createOrderWebhook" type="button">Create order.create</button>
                </div>
                <div class="muted" style="margin-top: 10px;">
                    Tripletex må sende til en offentlig URL. Lokale localhost-adresser vil ikke nå oss fra Tripletex.
                </div>
                <div class="table-wrap" style="margin-top: 12px;">
                    <table>
                        <thead><tr><th>ID</th><th>Event</th><th>Status</th><th>Target URL</th></tr></thead>
                        <tbody id="webhookRows"><tr><td colspan="4" class="muted">Ingen subscriptions lastet ennå.</td></tr></tbody>
                    </table>
                </div>
            </div>

            <div class="panel">
                <h3>Susoft Webhooks</h3>
                <div class="section-grid" style="grid-template-columns: 1.2fr auto auto; align-items: end;">
                    <div>
                        <div class="toolbar-label">Callback URL</div>
                        <input id="susoftWebhookTargetUrl" type="text" placeholder="https://your-public-host/webhooks/susoft/payment" />
                    </div>
                    <button class="secondary" id="refreshSusoftWebhooks" type="button">Refresh Webhooks</button>
                    <button id="createSusoftWebhook" type="button">Create ON_ORDER_INVOICED</button>
                </div>
                <div class="muted" style="margin-top: 10px;">
                    Oppretter webhook for ON_ORDER_INVOICED mot valgt callback URL.
                </div>
                <div class="table-wrap" style="margin-top: 12px;">
                    <table>
                        <thead><tr><th>ID</th><th>Type</th><th>Active</th><th>URL</th><th>Last Error</th></tr></thead>
                        <tbody id="susoftWebhookRows"><tr><td colspan="5" class="muted">Ingen webhooks lastet ennå.</td></tr></tbody>
                    </table>
                </div>
            </div>

            <div class="grid">
                <div class="card"><div class="k">Tenants</div><div class="v" id="tenantCount">0</div></div>
                <div class="card"><div class="k">Running Jobs</div><div class="v" id="runningJobs">0</div></div>
                <div class="card"><div class="k">Latest Job</div><div class="v small" id="latestJob">-</div></div>
                <div class="card"><div class="k">Health</div><div class="v small" id="health">-</div></div>
                <div class="card"><div class="k">Sendable Now</div><div class="v" id="sendableCount">0</div></div>
                <div class="card"><div class="k">Already Handled</div><div class="v" id="handledCount">0</div></div>
            </div>

            <div class="panel">
                <h3>Sendable Orders Now</h3>
                <div class="table-wrap">
                    <table>
                        <thead><tr><th>TT Order</th><th>Number</th><th>Order Date</th><th>Local Status</th><th>Susoft UUID</th></tr></thead>
                        <tbody id="sendableRows"></tbody>
                    </table>
                </div>
            </div>

            <div class="panel">
                <h3>Order Sync</h3>
                <div class="table-wrap">
                    <table>
                        <thead><tr><th>ID</th><th>TT Order</th><th>Status</th><th>Susoft UUID</th><th>Error</th><th>Updated</th></tr></thead>
                        <tbody id="orderRows"></tbody>
                    </table>
                </div>
            </div>

            <div class="panel">
                <h3>Events</h3>
                <div class="tabs" role="tablist" aria-label="Events tabs">
                    <button class="tab-btn active" id="eventsShortTab" type="button">Kort</button>
                    <button class="tab-btn" id="eventsDetailTab" type="button">Detaljer</button>
                </div>
                <div class="toolbar-label">Filter</div>
                <div style="max-width: 220px; margin-bottom: 12px;">
                    <select id="eventsLevelFilter">
                        <option value="ERROR" selected>Error</option>
                        <option value="WARN">Warn</option>
                        <option value="INFO">Info</option>
                        <option value="ALL">Alle nivåer</option>
                    </select>
                </div>
                <div class="tab-panel active" id="eventsShortPanel">
                    <div class="table-wrap">
                        <table>
                            <thead><tr><th>ID</th><th>Level</th><th>Type</th><th>Created</th></tr></thead>
                            <tbody id="eventRows"></tbody>
                        </table>
                    </div>
                </div>
                <div class="tab-panel" id="eventsDetailPanel">
                    <div class="table-wrap">
                        <table>
                            <thead><tr><th>Selected Event</th><th>Value</th></tr></thead>
                            <tbody id="eventDetailRows">
                                <tr><td class="muted">Velg en event i kort-visningen for å se detaljer.</td><td>-</td></tr>
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
        </div>

        <div class="panel muted" id="log">Klar.</div>
  </div>

    <script>
        const tenantEl = document.getElementById('tenant');
        const limitEl = document.getElementById('limit');
        const cfgTenantKeyEl = document.getElementById('cfgTenantKey');
        const cfgTenantNameEl = document.getElementById('cfgTenantName');
        const cfgExistingTenantSelectEl = document.getElementById('cfgExistingTenantSelect');
        const cfgTripletexBaseUrlEl = document.getElementById('cfgTripletexBaseUrl');
        const cfgSusoftBaseUrlEl = document.getElementById('cfgSusoftBaseUrl');
        const cfgTripletexConsumerTokenEl = document.getElementById('cfgTripletexConsumerToken');
        const cfgTripletexEmployeeTokenEl = document.getElementById('cfgTripletexEmployeeToken');
        const cfgSusoftShopUrlKeyEl = document.getElementById('cfgSusoftShopUrlKey');
        const cfgSusoftUsernameEl = document.getElementById('cfgSusoftUsername');
        const cfgSusoftPasswordEl = document.getElementById('cfgSusoftPassword');
        const cfgAutoPaidSyncEnabledEl = document.getElementById('cfgAutoPaidSyncEnabled');
        const cfgAutoPaidSyncIntervalMinutesEl = document.getElementById('cfgAutoPaidSyncIntervalMinutes');
        const cfgDailyDirectSalesSyncEnabledEl = document.getElementById('cfgDailyDirectSalesSyncEnabled');
        const cfgDailyDirectSalesSyncTimeEl = document.getElementById('cfgDailyDirectSalesSyncTime');
        const cfgDirectSalesDefaultIncomeAccountEl = document.getElementById('cfgDirectSalesDefaultIncomeAccount');
        const cfgDirectSalesSettlementOffsetAccountEl = document.getElementById('cfgDirectSalesSettlementOffsetAccount');
        const cfgStatusEl = document.getElementById('cfgStatus');
        const cfgSaveMessageEl = document.getElementById('cfgSaveMessage');
        const tenantListRowsEl = document.getElementById('tenantListRows');
        const webhookTargetUrlEl = document.getElementById('webhookTargetUrl');
        const susoftWebhookTargetUrlEl = document.getElementById('susoftWebhookTargetUrl');
        const logEl = document.getElementById('log');
        const eventsLevelFilterEl = document.getElementById('eventsLevelFilter');
        const webhookRowsEl = document.getElementById('webhookRows');
        const susoftWebhookRowsEl = document.getElementById('susoftWebhookRows');
        let latestEvents = [];
        let selectedEventId = null;
        let latestWebhooks = [];
        let latestSusoftWebhooks = [];

        function stamp() {
            document.getElementById('now').textContent = new Date().toLocaleString();
        }

        function statusTag(status) {
            const s = String(status || '').toUpperCase();
            if (s.includes('FAILED') || s === 'ERROR') return '<span class="tag err">' + s + '</span>';
            if (s.includes('PENDING') || s.includes('PARTIAL')) return '<span class="tag warn">' + s + '</span>';
            return '<span class="tag ok">' + s + '</span>';
        }

        function renderEventDetail(eventItem) {
            document.getElementById('eventDetailRows').innerHTML = eventItem
                ? [
                    ['ID', eventItem.id],
                    ['Level', eventItem.level],
                    ['Type', eventItem.event_type],
                    ['Message', eventItem.message],
                    ['Job Run', eventItem.job_run_id || '-'],
                    ['Order Sync', eventItem.order_sync_id || '-'],
                    ['Created', eventItem.created_at],
                    ['Details JSON', eventItem.details_json || '-'],
                ].map(([label, value]) => '<tr><td class="muted">' + label + '</td><td>' + String(value ?? '-') + '</td></tr>').join('')
                : '<tr><td class="muted">Ingen event valgt</td><td>-</td></tr>';
        }

        function bindEventRowClicks() {
            document.querySelectorAll('#eventRows .clickable-row').forEach((row) => {
                row.addEventListener('click', () => {
                    selectedEventId = row.getAttribute('data-event-id');
                    const selectedEvent = latestEvents.find((event) => String(event.id) === String(selectedEventId)) || null;
                    document.querySelectorAll('#eventRows .clickable-row').forEach((item) => {
                        item.classList.toggle('selected', item.getAttribute('data-event-id') === String(selectedEventId));
                    });
                    renderEventDetail(selectedEvent);
                });
            });
        }

        function matchesLevelFilter(eventItem) {
            const filter = String(eventsLevelFilterEl.value || 'ERROR').toUpperCase();
            const level = String(eventItem.level || '').toUpperCase();
            if (filter === 'ALL') return true;
            if (filter === 'WARN') return level === 'WARN' || level === 'WARNING';
            if (filter === 'INFO') return level === 'INFO';
            return level === 'ERROR';
        }

        function renderWebhookRows(items) {
            webhookRowsEl.innerHTML = items.map((item) => {
                const status = String(item.status || '-');
                const tag = status.includes('DISABLED') ? '<span class="tag err">' + status + '</span>' : '<span class="tag ok">' + status + '</span>';
                return '<tr>' +
                    '<td>' + (item.id || '-') + '</td>' +
                    '<td>' + (item.event || '-') + '</td>' +
                    '<td>' + tag + '</td>' +
                    '<td>' + (item.targetUrl || '-') + '</td>' +
                '</tr>';
            }).join('') || '<tr><td colspan="4" class="muted">Ingen subscriptions funnet.</td></tr>';
        }

        function renderSusoftWebhookRows(items) {
            susoftWebhookRowsEl.innerHTML = items.map((item) => {
                const active = item.active === true ? '<span class="tag ok">ACTIVE</span>' : '<span class="tag err">INACTIVE</span>';
                return '<tr>' +
                    '<td>' + (item.id || '-') + '</td>' +
                    '<td>' + (item.type || '-') + '</td>' +
                    '<td>' + active + '</td>' +
                    '<td>' + (item.url || '-') + '</td>' +
                    '<td>' + (item.lastError || '-') + '</td>' +
                '</tr>';
            }).join('') || '<tr><td colspan="5" class="muted">Ingen webhooks funnet.</td></tr>';
        }

        function renderTenantListRows(tenants) {
            tenantListRowsEl.innerHTML = tenants.map((tenant) => {
                return '<tr>' +
                    '<td>' + tenant.tenant_key + '</td>' +
                    '<td>' + (tenant.name || '-') + '</td>' +
                    '<td>' + statusTag(tenant.active ? 'ACTIVE' : 'INACTIVE') + '</td>' +
                    '<td>' + statusTag(tenant.has_tripletex_tokens ? 'OK' : 'MISSING') + '</td>' +
                    '<td>' + statusTag(tenant.has_susoft_credentials ? 'OK' : 'MISSING') + '</td>' +
                    '<td><button class="secondary tenant-select-btn" type="button" data-tenant-key="' + encodeURIComponent(tenant.tenant_key) + '">Velg</button></td>' +
                '</tr>';
            }).join('') || '<tr><td colspan="6" class="muted">Ingen tenants funnet.</td></tr>';
            bindTenantSelectButtons();
        }

        function bindTenantSelectButtons() {
            document.querySelectorAll('.tenant-select-btn').forEach((button) => {
                button.addEventListener('click', async () => {
                    const tenantKey = decodeURIComponent(button.getAttribute('data-tenant-key') || '');
                    if (!tenantKey) return;
                    tenantEl.value = tenantKey;
                    await loadTenantConnections();
                    await loadWebhooks();
                    await loadTenantData();
                    setMenu('menuSupport');
                });
            });
        }

        function syncExistingTenantSelect(tenants) {
            const currentValue = String(cfgExistingTenantSelectEl.value || '');
            cfgExistingTenantSelectEl.innerHTML = '<option value="">-- Ny tenant --</option>';
            tenants.forEach((tenant) => {
                const option = document.createElement('option');
                option.value = tenant.tenant_key;
                option.textContent = tenant.tenant_key + ' (' + (tenant.name || '-') + ')';
                cfgExistingTenantSelectEl.appendChild(option);
            });
            if (currentValue && tenants.some((tenant) => tenant.tenant_key === currentValue)) {
                cfgExistingTenantSelectEl.value = currentValue;
            }
        }

        function setMenu(activeMenuId) {
            ['menuAddTenant', 'menuAllTenants', 'menuSupport'].forEach((id) => {
                document.getElementById(id).classList.toggle('active', id === activeMenuId);
            });
            const panelByMenu = {
                menuAddTenant: 'menuAddTenantPanel',
                menuAllTenants: 'menuAllTenantsPanel',
                menuSupport: 'menuSupportPanel',
            };
            Object.values(panelByMenu).forEach((panelId) => {
                document.getElementById(panelId).classList.remove('active');
            });
            document.getElementById(panelByMenu[activeMenuId]).classList.add('active');
        }

        function appUrl(path) {
            return new URL(path, window.location.origin).toString();
        }

        async function api(url, opts) {
            const r = await fetch(appUrl(url), opts || {});
            if (!r.ok) {
                const t = await r.text();
                throw new Error(r.status + ': ' + t);
            }
            return await r.json();
        }

        async function loadTenants() {
            const current = tenantEl.value;
            const tenants = await api('/api/tenants');
            tenantEl.innerHTML = '';
            tenants.forEach((t) => {
                const o = document.createElement('option');
                o.value = t.tenant_key;
                const ttBadge = t.has_tripletex_tokens ? 'TT' : 'no-TT';
                const ssBadge = t.has_susoft_credentials ? 'SS' : 'no-SS';
                o.textContent = t.tenant_key + ' (' + t.name + ') [' + ttBadge + '/' + ssBadge + ']';
                tenantEl.appendChild(o);
            });
            if (!tenants.length) {
                const o = document.createElement('option');
                o.value = '';
                o.textContent = 'Ingen tenants';
                tenantEl.appendChild(o);
            } else if (current && tenants.some((t) => t.tenant_key === current)) {
                tenantEl.value = current;
            }
            renderTenantListRows(tenants);
            syncExistingTenantSelect(tenants);
            return tenants;
        }

        async function loadTenantConnections() {
            const tenant = tenantEl.value;
            if (!tenant) {
                cfgStatusEl.textContent = 'Ingen tenant valgt.';
                return;
            }
            try {
                const info = await api('/api/tenants/' + encodeURIComponent(tenant) + '/connections');
                cfgExistingTenantSelectEl.value = info.tenant_key || '';
                cfgTenantKeyEl.value = info.tenant_key || tenant;
                cfgTenantNameEl.value = info.name || '';
                cfgTripletexBaseUrlEl.value = info.tripletex_base_url || '';
                cfgSusoftBaseUrlEl.value = info.susoft_base_url || '';
                cfgTripletexConsumerTokenEl.value = '';
                cfgTripletexEmployeeTokenEl.value = '';
                cfgSusoftShopUrlKeyEl.value = '';
                cfgSusoftUsernameEl.value = '';
                cfgSusoftPasswordEl.value = '';
                cfgAutoPaidSyncEnabledEl.value = String(info.auto_paid_sync_enabled !== false);
                cfgAutoPaidSyncIntervalMinutesEl.value = String(info.auto_paid_sync_interval_minutes || 1);
                cfgDailyDirectSalesSyncEnabledEl.value = String(info.daily_direct_sales_sync_enabled === true);
                cfgDailyDirectSalesSyncTimeEl.value = String(info.daily_direct_sales_sync_time || '23:00');
                cfgDirectSalesDefaultIncomeAccountEl.value = String(info.direct_sales_default_income_account || '');
                cfgDirectSalesSettlementOffsetAccountEl.value = String(info.direct_sales_settlement_offset_account || '1900');
                const tt = info.has_tripletex_tokens ? 'TT OK' : 'TT mangler';
                const ss = info.has_susoft_credentials ? 'Susoft OK' : 'Susoft mangler';
                const autoPolling = (info.auto_paid_sync_enabled !== false)
                    ? ('Auto paid sync: hver ' + String(info.auto_paid_sync_interval_minutes || 1) + ' min')
                    : 'Auto paid sync: av';
                const dailySettlement = (info.daily_direct_sales_sync_enabled === true)
                    ? ('Direktesalg sync: daglig kl ' + String(info.daily_direct_sales_sync_time || '23:00'))
                    : 'Direktesalg sync: av';
                cfgStatusEl.innerHTML = statusTag(info.active ? 'ACTIVE' : 'INACTIVE') + ' ' + tt + ' / ' + ss + ' / ' + autoPolling + ' / ' + dailySettlement;
            } catch (err) {
                cfgStatusEl.textContent = 'Kunne ikke lese tenant-tilkobling: ' + String(err);
            }
        }

        async function saveTenantConfig() {
            const saveBtn = document.getElementById('saveTenantConfig');
            const editingTenantKey = String(cfgExistingTenantSelectEl.value || '').trim();
            const tenantKey = String(cfgTenantKeyEl.value || '').trim();
            const tenantName = String(cfgTenantNameEl.value || '').trim();
            if (!tenantKey || !tenantName) {
                const message = 'Feil: tenant key og name er påkrevd';
                cfgSaveMessageEl.textContent = message;
                cfgStatusEl.textContent = message;
                logEl.textContent = message;
                return;
            }
            if (editingTenantKey && tenantKey !== editingTenantKey) {
                const message = 'Feil: Du er i edit-modus for ' + editingTenantKey + '. Hold tenant key lik eller velg Clear Edit Mode.';
                cfgSaveMessageEl.textContent = message;
                cfgStatusEl.textContent = message;
                logEl.textContent = message;
                return;
            }

            const payload = {
                tenant_key: tenantKey,
                name: tenantName,
                active: true,
                tripletex_base_url: String(cfgTripletexBaseUrlEl.value || '').trim(),
                tripletex_consumer_token: String(cfgTripletexConsumerTokenEl.value || '').trim(),
                tripletex_employee_token: String(cfgTripletexEmployeeTokenEl.value || '').trim(),
                susoft_base_url: String(cfgSusoftBaseUrlEl.value || '').trim(),
                susoft_shop_url_key: String(cfgSusoftShopUrlKeyEl.value || '').trim(),
                susoft_username: String(cfgSusoftUsernameEl.value || '').trim(),
                susoft_password: String(cfgSusoftPasswordEl.value || '').trim(),
                auto_paid_sync_enabled: String(cfgAutoPaidSyncEnabledEl.value || 'true') === 'true',
                auto_paid_sync_interval_minutes: Number(cfgAutoPaidSyncIntervalMinutesEl.value || 1),
                daily_direct_sales_sync_enabled: String(cfgDailyDirectSalesSyncEnabledEl.value || 'false') === 'true',
                daily_direct_sales_sync_time: String(cfgDailyDirectSalesSyncTimeEl.value || '23:00'),
                direct_sales_default_income_account: String(cfgDirectSalesDefaultIncomeAccountEl.value || '').trim(),
                direct_sales_settlement_offset_account: String(cfgDirectSalesSettlementOffsetAccountEl.value || '1900').trim(),
            };

            try {
                saveBtn.setAttribute('disabled', 'disabled');
                saveBtn.textContent = 'Lagrer...';
                cfgSaveMessageEl.textContent = 'Lagrer tenant...';
                logEl.textContent = 'Lagrer tenant: ' + tenantKey;

                const result = await api('/api/tenants', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(payload),
                });

                cfgSaveMessageEl.textContent = 'Lagret: ' + result.tenant_key;
                cfgStatusEl.textContent = 'Tenant lagret.';
                logEl.textContent = JSON.stringify(result, null, 2);
                await loadTenants();
                tenantEl.value = tenantKey;
                await loadTenantConnections();
                await loadWebhooks();
                await loadSusoftWebhooks();
                try {
                    await loadTenantData();
                } catch (err) {
                    logEl.textContent = 'Tenant lagret, men kunne ikke laste supportdata: ' + String(err);
                }
            } catch (err) {
                const errorText = 'Feil ved tenant-lagring: ' + String(err);
                cfgSaveMessageEl.textContent = errorText;
                cfgStatusEl.textContent = errorText;
                logEl.textContent = errorText;
            } finally {
                saveBtn.removeAttribute('disabled');
                saveBtn.textContent = 'Save Tenant Config';
            }
        }

        async function loadStatus() {
            const status = await api('/api/status');
            document.getElementById('tenantCount').textContent = status.tenant_count;
            document.getElementById('runningJobs').textContent = status.running_jobs;
            document.getElementById('latestJob').textContent = status.latest_job_run
                ? (status.latest_job_run.job_name + ' / ' + status.latest_job_run.status)
                : '-';
            const health = await api('/health');
            document.getElementById('health').innerHTML = health.database_ok
                ? '<span class="tag ok">DB OK</span>'
                : '<span class="tag err">DB DOWN</span>';
        }

        async function loadWebhooks() {
            const tenant = tenantEl.value;
            if (!tenant) {
                webhookRowsEl.innerHTML = '<tr><td colspan="4" class="muted">Velg tenant for webhook-oppsett.</td></tr>';
                return;
            }
            try {
                const response = await api('/api/tripletex/webhooks/subscriptions?tenant_key=' + encodeURIComponent(tenant));
                latestWebhooks = Array.isArray(response.subscriptions) ? response.subscriptions : [];
                renderWebhookRows(latestWebhooks);
                if (!webhookTargetUrlEl.value) {
                    webhookTargetUrlEl.value = appUrl('/webhooks/tripletex/order');
                }
            } catch (err) {
                webhookRowsEl.innerHTML = '<tr><td colspan="4" class="muted">Kunne ikke laste subscriptions: ' + String(err) + '</td></tr>';
            }
        }

        async function loadSusoftWebhooks() {
            const tenant = tenantEl.value;
            if (!tenant) {
                susoftWebhookRowsEl.innerHTML = '<tr><td colspan="5" class="muted">Velg tenant for Susoft webhook-oppsett.</td></tr>';
                return;
            }
            try {
                const response = await api('/api/susoft/webhooks?tenant_key=' + encodeURIComponent(tenant) + '&webhook_type=ON_ORDER_INVOICED');
                latestSusoftWebhooks = Array.isArray(response.webhooks) ? response.webhooks : [];
                renderSusoftWebhookRows(latestSusoftWebhooks);
                if (!susoftWebhookTargetUrlEl.value) {
                    susoftWebhookTargetUrlEl.value = appUrl('/webhooks/susoft/payment');
                }
            } catch (err) {
                susoftWebhookRowsEl.innerHTML = '<tr><td colspan="5" class="muted">Kunne ikke laste Susoft webhooks: ' + String(err) + '</td></tr>';
            }
        }

        async function loadTenantData() {
            const tenant = tenantEl.value;
            if (!tenant) return;
            try {
                const limit = Math.max(1, Math.min(500, Number(limitEl.value || 50)));
                const sendable = await api('/api/tenants/' + encodeURIComponent(tenant) + '/sendable-orders?limit=' + limit);
                const orders = await api('/api/order-sync?tenant_key=' + encodeURIComponent(tenant) + '&limit=' + limit);
                const events = await api('/api/events?tenant_key=' + encodeURIComponent(tenant) + '&limit=' + limit);
                latestEvents = (Array.isArray(events) ? events : []).slice(0, 10);
                if (!selectedEventId && latestEvents.length) {
                    selectedEventId = latestEvents[0].id;
                }
                if (selectedEventId && !latestEvents.some((event) => String(event.id) === String(selectedEventId))) {
                    selectedEventId = latestEvents.length ? latestEvents[0].id : null;
                }

                const visibleEvents = latestEvents.filter(matchesLevelFilter);
                if (selectedEventId && !visibleEvents.some((event) => String(event.id) === String(selectedEventId))) {
                    selectedEventId = visibleEvents.length ? visibleEvents[0].id : null;
                }

                document.getElementById('sendableCount').textContent = sendable.sendable_count;
                document.getElementById('handledCount').textContent = sendable.already_handled_count;
                document.getElementById('sendableRows').innerHTML = sendable.sendable_orders.map((r) =>
                    '<tr>' +
                        '<td>' + r.tripletex_order_id + '</td>' +
                        '<td>' + (r.order_number || '-') + '</td>' +
                        '<td>' + (r.order_date || '-') + '</td>' +
                        '<td>' + statusTag(r.local_status || 'NEW') + '</td>' +
                        '<td>' + (r.susoft_uuid || '-') + '</td>' +
                    '</tr>'
                ).join('') || '<tr><td colspan="5" class="muted">Ingen sendbare ordre akkurat nå.</td></tr>';

                document.getElementById('orderRows').innerHTML = orders.map((r) =>
                    '<tr>' +
                        '<td>' + r.id + '</td>' +
                        '<td>' + r.tripletex_order_id + '</td>' +
                        '<td>' + statusTag(r.status) + '</td>' +
                        '<td>' + (r.susoft_uuid || '-') + '</td>' +
                        '<td>' + (r.last_error || '-') + '</td>' +
                        '<td>' + r.updated_at + '</td>' +
                    '</tr>'
                ).join('');

                document.getElementById('eventRows').innerHTML = visibleEvents.map((e) => {
                    const selectedClass = String(e.id) === String(selectedEventId) ? ' selected' : '';
                    return '<tr class="clickable-row' + selectedClass + '" data-event-id="' + e.id + '">' +
                        '<td>' + e.id + '</td>' +
                        '<td>' + statusTag(e.level) + '</td>' +
                        '<td>' + e.event_type + '</td>' +
                        '<td>' + e.created_at + '</td>' +
                    '</tr>';
                }).join('') || '<tr><td colspan="4" class="muted">Ingen events for valgt filter.</td></tr>';

                const detailRows = visibleEvents.find((event) => String(event.id) === String(selectedEventId)) || visibleEvents[0] || null;
                renderEventDetail(detailRows);
                bindEventRowClicks();
            } catch (err) {
                document.getElementById('sendableCount').textContent = '-';
                document.getElementById('handledCount').textContent = '-';
                document.getElementById('sendableRows').innerHTML = '<tr><td colspan="5" class="muted">Kunne ikke laste sendbare ordre: ' + String(err) + '</td></tr>';
                document.getElementById('orderRows').innerHTML = '<tr><td colspan="6" class="muted">Kunne ikke laste order sync: ' + String(err) + '</td></tr>';
                document.getElementById('eventRows').innerHTML = '<tr><td colspan="4" class="muted">Kunne ikke laste events: ' + String(err) + '</td></tr>';
                renderEventDetail(null);
                throw err;
            }
        }

        async function action(url) {
            try {
                logEl.textContent = 'Kjorer: ' + url;
                const data = await api(url, { method: 'POST' });
                logEl.textContent = JSON.stringify(data, null, 2);
                await loadAll();
            } catch (err) {
                logEl.textContent = 'Feil: ' + err;
            }
        }

        async function loadAll() {
            stamp();
            await loadTenants();
            await loadStatus();
            await loadTenantConnections();
            await loadWebhooks();
            await loadSusoftWebhooks();
            await loadTenantData();
        }

        document.getElementById('refresh').addEventListener('click', loadAll);
        tenantEl.addEventListener('change', async () => {
            await loadTenantConnections();
            await loadWebhooks();
            await loadSusoftWebhooks();
            await loadTenantData();
        });
        document.getElementById('dry').addEventListener('click', () => {
            const t = tenantEl.value;
            const l = Number(limitEl.value || 50);
            action('/api/tenants/' + encodeURIComponent(t) + '/sync/manual?dry_run=true&limit=' + l);
        });
        document.getElementById('exec').addEventListener('click', () => {
            const t = tenantEl.value;
            const l = Number(limitEl.value || 50);
            action('/api/tenants/' + encodeURIComponent(t) + '/sync/manual?dry_run=false&limit=' + l);
        });
        document.getElementById('retry').addEventListener('click', () => {
            const t = tenantEl.value;
            const l = Number(limitEl.value || 50);
            action('/api/tenants/' + encodeURIComponent(t) + '/sync/retry-failed?limit=' + l);
        });
        document.getElementById('paid').addEventListener('click', () => {
            const t = tenantEl.value;
            const l = Number(limitEl.value || 50);
            action('/api/tenants/' + encodeURIComponent(t) + '/sync/paid-from-susoft?limit=' + l + '&payment_type_id=20756819');
        });

        document.getElementById('refreshWebhooks').addEventListener('click', loadWebhooks);
        document.getElementById('createOrderWebhook').addEventListener('click', () => {
            const targetUrl = String(webhookTargetUrlEl.value || '').trim();
            const tenant = tenantEl.value;
            if (!tenant) {
                logEl.textContent = 'Feil: velg tenant først';
                return;
            }
            if (!targetUrl) {
                logEl.textContent = 'Feil: callback URL mangler';
                return;
            }
            action('/api/tripletex/webhooks/subscriptions/order-create?tenant_key=' + encodeURIComponent(tenant) + '&target_url=' + encodeURIComponent(targetUrl));
        });

        document.getElementById('refreshSusoftWebhooks').addEventListener('click', loadSusoftWebhooks);
        document.getElementById('createSusoftWebhook').addEventListener('click', () => {
            const targetUrl = String(susoftWebhookTargetUrlEl.value || '').trim();
            const tenant = tenantEl.value;
            if (!tenant) {
                logEl.textContent = 'Feil: velg tenant først';
                return;
            }
            if (!targetUrl) {
                logEl.textContent = 'Feil: Susoft callback URL mangler';
                return;
            }
            action('/api/susoft/webhooks/order-invoiced?tenant_key=' + encodeURIComponent(tenant) + '&target_url=' + encodeURIComponent(targetUrl));
        });

        document.getElementById('saveTenantConfig').addEventListener('click', saveTenantConfig);
        document.getElementById('cfgLoadExistingTenant').addEventListener('click', async () => {
            const selected = String(cfgExistingTenantSelectEl.value || '').trim();
            if (!selected) {
                cfgSaveMessageEl.textContent = 'Velg tenant i listen for a laste den inn.';
                return;
            }
            tenantEl.value = selected;
            await loadTenantConnections();
            cfgSaveMessageEl.textContent = 'Edit mode: ' + selected;
            cfgStatusEl.textContent = 'Tenant lastet for redigering.';
        });
        document.getElementById('cfgClearExistingTenant').addEventListener('click', () => {
            cfgExistingTenantSelectEl.value = '';
            cfgTenantKeyEl.value = '';
            cfgTenantNameEl.value = '';
            cfgTripletexBaseUrlEl.value = '';
            cfgSusoftBaseUrlEl.value = '';
            cfgTripletexConsumerTokenEl.value = '';
            cfgTripletexEmployeeTokenEl.value = '';
            cfgSusoftShopUrlKeyEl.value = '';
            cfgSusoftUsernameEl.value = '';
            cfgSusoftPasswordEl.value = '';
            cfgAutoPaidSyncEnabledEl.value = 'true';
            cfgAutoPaidSyncIntervalMinutesEl.value = '1';
            cfgDailyDirectSalesSyncEnabledEl.value = 'false';
            cfgDailyDirectSalesSyncTimeEl.value = '23:00';
            cfgDirectSalesDefaultIncomeAccountEl.value = '';
            cfgDirectSalesSettlementOffsetAccountEl.value = '1900';
            cfgSaveMessageEl.textContent = 'Edit mode ryddet. Klar for ny tenant.';
            cfgStatusEl.textContent = 'Ny tenant-modus.';
        });
        document.getElementById('menuAddTenant').addEventListener('click', () => setMenu('menuAddTenant'));
        document.getElementById('menuAllTenants').addEventListener('click', () => setMenu('menuAllTenants'));
        document.getElementById('menuSupport').addEventListener('click', () => setMenu('menuSupport'));

        document.getElementById('eventsShortTab').addEventListener('click', () => {
            document.getElementById('eventsShortTab').classList.add('active');
            document.getElementById('eventsDetailTab').classList.remove('active');
            document.getElementById('eventsShortPanel').classList.add('active');
            document.getElementById('eventsDetailPanel').classList.remove('active');
        });

        document.getElementById('eventsDetailTab').addEventListener('click', () => {
            document.getElementById('eventsShortTab').classList.remove('active');
            document.getElementById('eventsDetailTab').classList.add('active');
            document.getElementById('eventsShortPanel').classList.remove('active');
            document.getElementById('eventsDetailPanel').classList.add('active');
        });

        eventsLevelFilterEl.addEventListener('change', () => {
            selectedEventId = null;
            loadTenantData();
        });

        loadAll();
    </script>
</body>
</html>
"""
