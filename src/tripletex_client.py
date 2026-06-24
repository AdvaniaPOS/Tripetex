from __future__ import annotations

import base64
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import requests

from src.config import get_settings


TIMEOUT_SECONDS = 30


def _resolve_tripletex_settings(overrides: dict[str, str] | None = None) -> dict[str, str]:
    settings = get_settings()
    data = overrides or {}
    return {
        "base_url": str(data.get("tripletex_base_url") or settings.tripletex_base_url),
        "consumer_token": str(data.get("tripletex_consumer_token") or settings.tripletex_consumer_token),
        "employee_token": str(data.get("tripletex_employee_token") or settings.tripletex_employee_token),
        "timezone": str(data.get("tripletex_timezone") or settings.tripletex_timezone),
    }


def _auth_headers(session_token: str) -> dict[str, str]:
    basic_credentials = base64.b64encode(f"0:{session_token}".encode("utf-8")).decode("ascii")
    return {
        "Authorization": f"Basic {basic_credentials}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def build_fields() -> str:
    return (
        "id,number,orderDate,customer(id,name),"
        "orderLines("
        "id,description,count,"
        "product(id,number,name),"
        "currency(id),"
        "discount,markup,"
        "unitPriceExcludingVatCurrency,unitPriceIncludingVatCurrency,"
        "amountExcludingVatCurrency,amountIncludingVatCurrency,"
        "vatType(id,number,name,percentage)"
        ")"
    )


def build_order_params(*, tripletex_timezone: str | None = None) -> dict[str, str]:
    settings = get_settings()
    tz_name = tripletex_timezone or settings.tripletex_timezone
    today_oslo = datetime.now(ZoneInfo(tz_name)).date()
    order_date_from = (today_oslo - timedelta(days=365)).isoformat()
    order_date_to = (today_oslo + timedelta(days=1)).isoformat()
    return {
        "isSent": "true",
        "isInvoiced": "false",
        "orderDateFrom": order_date_from,
        "orderDateTo": order_date_to,
        "fields": build_fields(),
    }


def create_session_token(*, overrides: dict[str, str] | None = None) -> str:
    resolved = _resolve_tripletex_settings(overrides)
    if not resolved["consumer_token"] or not resolved["employee_token"]:
        raise RuntimeError("Tripletex tokens mangler i miljokonfigurasjon (.env).")

    expiration_date = (datetime.now(ZoneInfo(resolved["timezone"])).date() + timedelta(days=1)).isoformat()

    url = f"{resolved['base_url']}/token/session/:create"
    params = {
        "consumerToken": resolved["consumer_token"],
        "employeeToken": resolved["employee_token"],
        "expirationDate": expiration_date,
    }

    try:
        response = requests.put(url, params=params, timeout=TIMEOUT_SECONDS)
    except requests.RequestException as exc:
        raise RuntimeError(f"Nettverksfeil ved oppretting av Tripletex session token: {exc}") from exc

    if not response.ok:
        raise RuntimeError(f"Tripletex session token feilet med status {response.status_code}: {response.text}")

    payload = response.json()
    value = payload.get("value")
    if isinstance(value, str) and value:
        return value
    if isinstance(value, dict) and isinstance(value.get("token"), str) and value.get("token"):
        return value["token"]
    raise RuntimeError("Tripletex session token finnes ikke i respons.")


def fetch_open_orders(session_token: str, *, overrides: dict[str, str] | None = None) -> dict[str, Any]:
    resolved = _resolve_tripletex_settings(overrides)
    url = f"{resolved['base_url']}/order"
    params = build_order_params(tripletex_timezone=resolved["timezone"])

    bearer_headers = {"Authorization": f"Bearer {session_token}", "Accept": "application/json"}
    basic_headers = {"Authorization": _auth_headers(session_token)["Authorization"], "Accept": "application/json"}

    try:
        response = requests.get(url, headers=bearer_headers, params=params, timeout=TIMEOUT_SECONDS)
    except requests.RequestException as exc:
        raise RuntimeError(f"Nettverksfeil ved henting av Tripletex-ordrer: {exc}") from exc

    if response.status_code == 401:
        try:
            response = requests.get(url, headers=basic_headers, params=params, timeout=TIMEOUT_SECONDS)
        except requests.RequestException as exc:
            raise RuntimeError(f"Nettverksfeil ved henting av Tripletex-ordrer (basic fallback): {exc}") from exc

    if not response.ok:
        raise RuntimeError(f"Tripletex ordre-kall feilet med status {response.status_code}: {response.text}")

    payload = response.json()
    if not isinstance(payload, dict):
        raise RuntimeError("Ugyldig responsformat fra Tripletex /order.")
    return payload


def list_event_subscriptions(session_token: str, *, overrides: dict[str, str] | None = None) -> list[dict[str, Any]]:
    resolved = _resolve_tripletex_settings(overrides)
    url = f"{resolved['base_url']}/event/subscription"
    headers = _auth_headers(session_token)

    try:
        response = requests.get(url, headers=headers, timeout=TIMEOUT_SECONDS)
    except requests.RequestException as exc:
        raise RuntimeError(f"Nettverksfeil ved henting av Tripletex webhook-subscriptions: {exc}") from exc

    if not response.ok:
        raise RuntimeError(f"Tripletex subscription-kall feilet med status {response.status_code}: {response.text}")

    payload = response.json()
    values = payload.get("values") if isinstance(payload, dict) else None
    return values if isinstance(values, list) else []


def create_event_subscription(
    session_token: str,
    *,
    event: str,
    target_url: str,
    overrides: dict[str, str] | None = None,
    fields: str | None = None,
    auth_header_name: str | None = None,
    auth_header_value: str | None = None,
) -> dict[str, Any]:
    resolved = _resolve_tripletex_settings(overrides)
    url = f"{resolved['base_url']}/event/subscription"
    headers = _auth_headers(session_token)

    body: dict[str, Any] = {
        "event": event,
        "targetUrl": target_url,
    }
    if fields:
        body["fields"] = fields
    if auth_header_name and auth_header_value:
        body["authHeaderName"] = auth_header_name
        body["authHeaderValue"] = auth_header_value

    try:
        response = requests.post(url, headers=headers, json=body, timeout=TIMEOUT_SECONDS)
    except requests.RequestException as exc:
        raise RuntimeError(f"Nettverksfeil ved oppretting av Tripletex webhook-subscription: {exc}") from exc

    if not response.ok:
        raise RuntimeError(f"Tripletex subscription-opprettelse feilet med status {response.status_code}: {response.text}")

    payload = response.json()
    if not isinstance(payload, dict):
        raise RuntimeError("Ugyldig responsformat fra Tripletex subscription-opprettelse.")
    return payload
