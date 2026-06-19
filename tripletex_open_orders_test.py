from __future__ import annotations

import json
import base64
from datetime import datetime, timedelta
from typing import Any, cast
from zoneinfo import ZoneInfo

import requests

BASE_URL = "https://api-test.tripletex.tech/v2"
CONSUMER_NAME = "473e6fb1-9c40-45ab-b43c-b46c54882cd0"
CONSUMER_TOKEN = "eyJ0b2tlbklkIjoyNjg1LCJ0b2tlbiI6InRlc3QtZjE1YzhjNTktMjVjNy00YzJjLThiOTAtNjFlMGRlMWYwZjJjIn0="
EMPLOYEE_TOKEN = "eyJ0b2tlbklkIjo0NDAzLCJ0b2tlbiI6InRlc3QtN2Q4MDBkNmYtNDhlYS00NTU1LWE3MDgtMzg0YmJlMTBjNjE4In0="
TIMEOUT_SECONDS = 30


def print_api_error(step: str, response: requests.Response) -> None:
    """Prints detailed API error info for easier debugging."""
    print(f"[{step}] Feil ved kall mot Tripletex API")
    print(f"Statuskode: {response.status_code}")
    try:
        payload = response.json()
        print("Respons:")
        print(json.dumps(payload, indent=2, ensure_ascii=False))
    except ValueError:
        print("Respons (råtekst):")
        print(response.text)


def create_session_token() -> str:
    """Creates a temporary session token in Tripletex test environment."""
    expiration_date = (datetime.now(ZoneInfo("Europe/Oslo")).date() + timedelta(days=1)).isoformat()

    url = f"{BASE_URL}/token/session/:create"
    params = {
        "consumerToken": CONSUMER_TOKEN,
        "employeeToken": EMPLOYEE_TOKEN,
        "expirationDate": expiration_date,
    }

    try:
        response = requests.put(url, params=params, timeout=TIMEOUT_SECONDS)
    except requests.RequestException as exc:
        raise RuntimeError(f"Nettverksfeil ved oppretting av session token: {exc}") from exc

    if not response.ok:
        print_api_error("Opprett session token", response)
        raise RuntimeError("Kunne ikke opprette session token.")

    try:
        data: dict[str, Any] = response.json()
    except ValueError as exc:
        raise RuntimeError("Klarte ikke å parse JSON fra session token-respons.") from exc

    value = data.get("value")
    if isinstance(value, str) and value:
        return value

    if isinstance(value, dict):
        value_dict = cast(dict[str, Any], value)
        nested_token = value_dict.get("token")
        if isinstance(nested_token, str) and nested_token:
            return nested_token

    raise RuntimeError(f"Fant ikke gyldig session token i respons: {data}")


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


def build_order_params() -> dict[str, str]:
    today_oslo = datetime.now(ZoneInfo("Europe/Oslo")).date()
    order_date_from = (today_oslo - timedelta(days=365)).isoformat()
    # Tripletex treats orderDateTo as "to and excluding", so include today by using tomorrow.
    order_date_to = (today_oslo + timedelta(days=1)).isoformat()
    return {
        "isSent": "true",
        "isInvoiced": "false",
        "orderDateFrom": order_date_from,
        "orderDateTo": order_date_to,
        "fields": build_fields(),
    }


def fetch_open_orders(session_token: str) -> dict[str, Any]:
    """Fetches sent but not invoiced orders with a minimal field set."""
    url = f"{BASE_URL}/order"

    basic_credentials = base64.b64encode(f"0:{session_token}".encode("utf-8")).decode("ascii")
    bearer_headers = {
        "Authorization": f"Bearer {session_token}",
        "Accept": "application/json",
    }
    basic_headers = {
        "Authorization": f"Basic {basic_credentials}",
        "Accept": "application/json",
    }
    params = build_order_params()
    auth_label = "Bearer"

    try:
        response = requests.get(url, headers=bearer_headers, params=params, timeout=TIMEOUT_SECONDS)
    except requests.RequestException as exc:
        raise RuntimeError(f"Nettverksfeil ved henting av ordrer: {exc}") from exc

    if response.status_code == 401:
        try:
            response = requests.get(url, headers=basic_headers, params=params, timeout=TIMEOUT_SECONDS)
            auth_label = "Basic (0:sessionToken)"
        except requests.RequestException as exc:
            raise RuntimeError(f"Nettverksfeil ved henting av ordrer (basic fallback): {exc}") from exc

    print(f"Auth-metode brukt mot /order: {auth_label}")

    if not response.ok:
        print_api_error("Hent åpne ordrer", response)
        raise RuntimeError("Kunne ikke hente åpne ordrer.")

    try:
        return response.json()
    except ValueError as exc:
        raise RuntimeError("Klarte ikke å parse JSON fra ordre-respons.") from exc


def main() -> None:
    if CONSUMER_TOKEN.startswith("<") or EMPLOYEE_TOKEN.startswith("<"):
        print("Sett CONSUMER_TOKEN og EMPLOYEE_TOKEN øverst i skriptet før kjøring.")
        return

    try:
        token = create_session_token()
        orders_response = fetch_open_orders(token)
    except RuntimeError as exc:
        print(f"Feil: {exc}")
        return

    print("Åpne ordrer (isSent=true, isInvoiced=false):")
    print(json.dumps(orders_response, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
