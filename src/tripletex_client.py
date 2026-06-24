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
        "id,number,orderDate,isClosed,isSubscription,customer(id,name),"
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
        # Tripletex /order supports isClosed/isSubscription filtering.
        # We only fetch open, non-subscription orders for Susoft sync.
        "isClosed": "false",
        "isSubscription": "false",
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


def find_voucher_by_external_number(
    session_token: str,
    *,
    external_voucher_number: str,
    overrides: dict[str, str] | None = None,
) -> dict[str, Any] | None:
    resolved = _resolve_tripletex_settings(overrides)
    url = f"{resolved['base_url']}/ledger/voucher/>externalVoucherNumber"
    headers = _auth_headers(session_token)
    params = {
        "externalVoucherNumber": external_voucher_number,
        "count": "10",
        "fields": "id,number,date,description,externalVoucherNumber",
    }

    try:
        response = requests.get(url, headers=headers, params=params, timeout=TIMEOUT_SECONDS)
    except requests.RequestException as exc:
        raise RuntimeError(f"Nettverksfeil ved oppslag av Tripletex voucher via externalVoucherNumber: {exc}") from exc

    if not response.ok:
        raise RuntimeError(f"Tripletex voucher-oppslag feilet med status {response.status_code}: {response.text}")

    payload = response.json()
    values = payload.get("values") if isinstance(payload, dict) else None
    if not isinstance(values, list):
        return None

    for item in values:
        if not isinstance(item, dict):
            continue
        if str(item.get("externalVoucherNumber") or "") == external_voucher_number:
            return item
    return None


def resolve_account_ids_by_number(
    session_token: str,
    *,
    account_numbers: list[str],
    overrides: dict[str, str] | None = None,
) -> dict[str, int]:
    numbers = [str(item).strip() for item in account_numbers if str(item).strip()]
    unique_numbers = sorted(set(numbers))
    if not unique_numbers:
        return {}

    resolved = _resolve_tripletex_settings(overrides)
    url = f"{resolved['base_url']}/ledger/account"
    headers = _auth_headers(session_token)
    params = {
        "number": ",".join(unique_numbers),
        "count": str(max(100, len(unique_numbers) * 3)),
        "fields": "id,number,isInactive",
    }

    try:
        response = requests.get(url, headers=headers, params=params, timeout=TIMEOUT_SECONDS)
    except requests.RequestException as exc:
        raise RuntimeError(f"Nettverksfeil ved oppslag av Tripletex kontoer: {exc}") from exc

    if not response.ok:
        raise RuntimeError(f"Tripletex konto-oppslag feilet med status {response.status_code}: {response.text}")

    payload = response.json()
    values = payload.get("values") if isinstance(payload, dict) else None
    accounts = values if isinstance(values, list) else []

    result: dict[str, int] = {}
    for account in accounts:
        if not isinstance(account, dict):
            continue
        account_number = str(account.get("number") or "").strip()
        account_id = account.get("id")
        if not account_number or not isinstance(account_id, int):
            continue
        result[account_number] = account_id

    missing = [number for number in unique_numbers if number not in result]
    if missing:
        raise RuntimeError(f"Fant ikke Tripletex konto(er): {', '.join(missing)}")

    return result


def create_ledger_voucher(
    session_token: str,
    *,
    voucher_date: str,
    description: str,
    external_voucher_number: str,
    postings: list[dict[str, Any]],
    send_to_ledger: bool = True,
    overrides: dict[str, str] | None = None,
) -> dict[str, Any]:
    if not postings:
        raise RuntimeError("Voucher-postering krever minst en postering")

    resolved = _resolve_tripletex_settings(overrides)
    url = f"{resolved['base_url']}/ledger/voucher"
    headers = _auth_headers(session_token)
    params = {"sendToLedger": "true" if send_to_ledger else "false"}

    body_postings: list[dict[str, Any]] = []
    for row in postings:
        account_id = row.get("account_id")
        amount = row.get("amount")
        if not isinstance(account_id, int):
            raise RuntimeError("Voucher-postering mangler gyldig account_id")
        if not isinstance(amount, (int, float)):
            raise RuntimeError("Voucher-postering mangler gyldig belop")

        body_row: dict[str, Any] = {
            "account": {"id": account_id},
            "amount": round(float(amount), 2),
        }
        description_row = str(row.get("description") or "").strip()
        if description_row:
            body_row["description"] = description_row
        body_postings.append(body_row)

    body = {
        "date": voucher_date,
        "description": description,
        "externalVoucherNumber": external_voucher_number,
        "postings": body_postings,
    }

    try:
        response = requests.post(url, headers=headers, params=params, json=body, timeout=TIMEOUT_SECONDS)
    except requests.RequestException as exc:
        raise RuntimeError(f"Nettverksfeil ved oppretting av Tripletex voucher: {exc}") from exc

    if not response.ok:
        raise RuntimeError(f"Tripletex voucher-opprettelse feilet med status {response.status_code}: {response.text}")

    payload = response.json()
    value = payload.get("value") if isinstance(payload, dict) else None
    if isinstance(value, dict):
        return value
    if isinstance(payload, dict):
        return payload
    raise RuntimeError("Ugyldig responsformat fra Tripletex voucher-opprettelse")
