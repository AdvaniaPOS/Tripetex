from __future__ import annotations

import secrets
from datetime import UTC, datetime

from fastapi import Depends, FastAPI, Header, HTTPException, Query, status
from fastapi.responses import HTMLResponse
from sqlalchemy import desc, func, select

from src.auth import require_dashboard_auth
from src.config import get_settings
from src.db import db_health_check, db_session, init_db
from src.models import JobRun, OrderSync, SyncEvent, Tenant
from src.sync_service import (
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


def _keep_or_replace_secret(current: str | None, incoming: str | None) -> str | None:
    if incoming is None:
        return current
    value = incoming.strip()
    return current if not value else value


@app.on_event("startup")
def on_startup() -> None:
    app.state.startup_error = None
    if settings.app_auto_create_tables:
        try:
            init_db()
        except Exception as exc:
            app.state.startup_error = f"database init failed: {exc}"


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
def webhook_tripletex_order(payload: dict[str, object]) -> dict[str, object]:
    tenant_key = str(payload.get("tenant_key") or payload.get("tenantKey") or "").strip()
    if not tenant_key:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="tenant_key mangler")

    raw_order_id = payload.get("order_id") or payload.get("orderId") or payload.get("tripletex_order_id") or payload.get("tripletexOrderId")
    if raw_order_id is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="order_id mangler")

    try:
        order_id = int(str(raw_order_id))
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="order_id må være et tall") from exc

    dry_run = bool(payload.get("dry_run") or payload.get("dryRun") or False)
    try:
        return process_tripletex_order_by_id_for_tenant(tenant_key, order_id, dry_run=dry_run)
    except RuntimeError as exc:
        detail = str(exc)
        status_code = status.HTTP_404_NOT_FOUND if "Fant ikke" in detail or "finnes ikke" in detail else status.HTTP_400_BAD_REQUEST
        raise HTTPException(status_code=status_code, detail=detail) from exc


@app.post("/webhooks/susoft/payment", dependencies=[Depends(_require_webhook_secret)])
def webhook_susoft_payment(payload: dict[str, object]) -> dict[str, object]:
    tenant_key = str(payload.get("tenant_key") or payload.get("tenantKey") or "").strip()
    if not tenant_key:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="tenant_key mangler")

    susoft_uuid = str(payload.get("susoft_uuid") or payload.get("susoftUuid") or payload.get("uuid") or "").strip()
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
            tenant_key,
            susoft_uuid,
            payment_type_id=payment_type_id,
            paid_amount=paid_amount,
            payment_date=payment_date,
        )
    except RuntimeError as exc:
        detail = str(exc)
        status_code = status.HTTP_404_NOT_FOUND if "Fant" in detail or "finnes ikke" in detail else status.HTTP_400_BAD_REQUEST
        raise HTTPException(status_code=status_code, detail=detail) from exc


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
    result = create_event_subscription(
        token,
        event="order.create",
        target_url=target_url,
        overrides=overrides,
        auth_header_name="X-Webhook-Secret" if settings.webhook_shared_secret.strip() else None,
        auth_header_value=settings.webhook_shared_secret.strip() or None,
        fields="id,number,orderDate,customer(id,name),orderLines(id,description,count,product(id,number,name),currency(id),discount,markup,unitPriceExcludingVatCurrency,unitPriceIncludingVatCurrency,amountExcludingVatCurrency,amountIncludingVatCurrency,vatType(id,number,name,percentage))",
    )
    return result


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

        session.commit()
        session.refresh(row)

    return {
        "id": row.id,
        "tenant_key": row.tenant_key,
        "name": row.name,
        "active": row.active,
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
        const cfgTripletexBaseUrlEl = document.getElementById('cfgTripletexBaseUrl');
        const cfgSusoftBaseUrlEl = document.getElementById('cfgSusoftBaseUrl');
        const cfgTripletexConsumerTokenEl = document.getElementById('cfgTripletexConsumerToken');
        const cfgTripletexEmployeeTokenEl = document.getElementById('cfgTripletexEmployeeToken');
        const cfgSusoftShopUrlKeyEl = document.getElementById('cfgSusoftShopUrlKey');
        const cfgSusoftUsernameEl = document.getElementById('cfgSusoftUsername');
        const cfgSusoftPasswordEl = document.getElementById('cfgSusoftPassword');
        const cfgStatusEl = document.getElementById('cfgStatus');
        const cfgSaveMessageEl = document.getElementById('cfgSaveMessage');
        const tenantListRowsEl = document.getElementById('tenantListRows');
        const webhookTargetUrlEl = document.getElementById('webhookTargetUrl');
        const logEl = document.getElementById('log');
        const eventsLevelFilterEl = document.getElementById('eventsLevelFilter');
        const webhookRowsEl = document.getElementById('webhookRows');
        let latestEvents = [];
        let selectedEventId = null;
        let latestWebhooks = [];

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

        function renderTenantListRows(tenants) {
            tenantListRowsEl.innerHTML = tenants.map((tenant) => {
                return '<tr>' +
                    '<td>' + tenant.tenant_key + '</td>' +
                    '<td>' + (tenant.name || '-') + '</td>' +
                    '<td>' + statusTag(tenant.active ? 'ACTIVE' : 'INACTIVE') + '</td>' +
                    '<td>' + statusTag(tenant.has_tripletex_tokens ? 'OK' : 'MISSING') + '</td>' +
                    '<td>' + statusTag(tenant.has_susoft_credentials ? 'OK' : 'MISSING') + '</td>' +
                    '<td><button class="secondary" type="button" onclick="selectTenantFromList(\'' + tenant.tenant_key + '\')">Velg</button></td>' +
                '</tr>';
            }).join('') || '<tr><td colspan="6" class="muted">Ingen tenants funnet.</td></tr>';
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

        window.selectTenantFromList = async function(tenantKey) {
            tenantEl.value = tenantKey;
            await loadTenantConnections();
            await loadWebhooks();
            await loadTenantData();
            setMenu('menuSupport');
        };

        async function api(url, opts) {
            const r = await fetch(url, opts || {});
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
                cfgTenantKeyEl.value = info.tenant_key || tenant;
                cfgTenantNameEl.value = info.name || '';
                cfgTripletexBaseUrlEl.value = info.tripletex_base_url || '';
                cfgSusoftBaseUrlEl.value = info.susoft_base_url || '';
                cfgTripletexConsumerTokenEl.value = '';
                cfgTripletexEmployeeTokenEl.value = '';
                cfgSusoftShopUrlKeyEl.value = '';
                cfgSusoftUsernameEl.value = '';
                cfgSusoftPasswordEl.value = '';
                const tt = info.has_tripletex_tokens ? 'TT OK' : 'TT mangler';
                const ss = info.has_susoft_credentials ? 'Susoft OK' : 'Susoft mangler';
                cfgStatusEl.innerHTML = statusTag(info.active ? 'ACTIVE' : 'INACTIVE') + ' ' + tt + ' / ' + ss;
            } catch (err) {
                cfgStatusEl.textContent = 'Kunne ikke lese tenant-tilkobling: ' + String(err);
            }
        }

        async function saveTenantConfig() {
            const saveBtn = document.getElementById('saveTenantConfig');
            const tenantKey = String(cfgTenantKeyEl.value || '').trim();
            const tenantName = String(cfgTenantNameEl.value || '').trim();
            if (!tenantKey || !tenantName) {
                const message = 'Feil: tenant key og name er påkrevd';
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
                logEl.textContent = JSON.stringify(result, null, 2);
                await loadTenants();
                tenantEl.value = tenantKey;
                await loadTenantConnections();
                await loadWebhooks();
                await loadTenantData();
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
                    webhookTargetUrlEl.value = window.location.origin + '/webhooks/tripletex/order';
                }
            } catch (err) {
                webhookRowsEl.innerHTML = '<tr><td colspan="4" class="muted">Kunne ikke laste subscriptions: ' + String(err) + '</td></tr>';
            }
        }

        async function loadTenantData() {
            const tenant = tenantEl.value;
            if (!tenant) return;
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
            await loadTenantData();
        }

        document.getElementById('refresh').addEventListener('click', loadAll);
        tenantEl.addEventListener('change', async () => {
            await loadTenantConnections();
            await loadWebhooks();
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

        document.getElementById('saveTenantConfig').addEventListener('click', saveTenantConfig);
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
