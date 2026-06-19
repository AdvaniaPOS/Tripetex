from __future__ import annotations

import argparse
import base64
import json
from datetime import datetime
from typing import Any

import requests

from tripletex_open_orders_test import BASE_URL, TIMEOUT_SECONDS, create_session_token, print_api_error


class AlreadyInvoicedError(RuntimeError):
    pass


def build_basic_headers(session_token: str) -> dict[str, str]:
    credentials = base64.b64encode(f"0:{session_token}".encode("utf-8")).decode("ascii")
    return {
        "Authorization": f"Basic {credentials}",
        "Accept": "application/json",
    }


def list_payment_types(headers: dict[str, str]) -> list[dict[str, Any]]:
    url = f"{BASE_URL}/invoice/paymentType"
    response = requests.get(url, headers=headers, params={"count": "100"}, timeout=TIMEOUT_SECONDS)
    if not response.ok:
        print_api_error("Hent payment types", response)
        return []

    data = response.json()
    values = data.get("values", []) if isinstance(data, dict) else []
    return values if isinstance(values, list) else []


def get_order(order_id: int, headers: dict[str, str]) -> dict[str, Any]:
    url = f"{BASE_URL}/order/{order_id}"
    params = {
        "fields": "id,number,isInvoiced",
    }
    response = requests.get(url, headers=headers, params=params, timeout=TIMEOUT_SECONDS)
    if not response.ok:
        print_api_error("Hent ordre", response)
        raise RuntimeError("Kunne ikke hente ordrestatus.")

    data = response.json()
    value = data.get("value") if isinstance(data, dict) else None
    if not isinstance(value, dict):
        raise RuntimeError("Ugyldig respons ved henting av ordre.")
    return value


def find_existing_invoice_id_for_order(order_id: int, headers: dict[str, str]) -> int | None:
    url = f"{BASE_URL}/invoice"
    candidate_params = [
        {"orderId": str(order_id), "count": "100", "fields": "id,invoiceDate"},
        {"order.id": str(order_id), "count": "100", "fields": "id,invoiceDate"},
    ]

    for params in candidate_params:
        response = requests.get(url, headers=headers, params=params, timeout=TIMEOUT_SECONDS)
        if not response.ok:
            continue

        data = response.json()
        values = data.get("values", []) if isinstance(data, dict) else []
        if not isinstance(values, list):
            continue

        ids: list[int] = []
        for item in values:
            if isinstance(item, dict):
                invoice_id = item.get("id")
                if isinstance(invoice_id, int):
                    ids.append(invoice_id)

        if ids:
            return max(ids)

    return None


def create_invoice(order_id: int, invoice_date: str, headers: dict[str, str], dry_run: bool) -> int:
    url = f"{BASE_URL}/order/{order_id}/:invoice"
    params = {
        "invoiceDate": invoice_date,
        "sendToCustomer": "false",
        "sendType": "MANUAL",
    }

    if dry_run:
        print("[DRY RUN] Ville opprettet faktura:")
        print(f"PUT {url}")
        print(json.dumps(params, indent=2, ensure_ascii=False))
        return -1

    response = requests.put(url, headers=headers, params=params, timeout=TIMEOUT_SECONDS)
    if response.ok:
        payload = response.json()
        value = payload.get("value") if isinstance(payload, dict) else None
        invoice_id = value.get("id") if isinstance(value, dict) else None
        if isinstance(invoice_id, int):
            print(f"Faktura opprettet. invoiceId={invoice_id}")
            return invoice_id
        raise RuntimeError("Fikk ikke invoiceId fra fakturering.")

    # Idempotent behavior: if invoice already exists, re-use existing invoice id.
    if response.status_code in (409, 422):
        existing_invoice_id = find_existing_invoice_id_for_order(order_id, headers)
        if existing_invoice_id is not None:
            print(f"Ordre er allerede fakturert. Gjenbruker invoiceId={existing_invoice_id}")
            return existing_invoice_id
        raise AlreadyInvoicedError("Ordre er allerede fakturert i Tripletex, men invoiceId kunne ikke hentes automatisk.")

    print_api_error("Fakturer ordre", response)
    raise RuntimeError("Kunne ikke fakturere ordre.")


def register_payment(
    invoice_id: int,
    payment_date: str,
    payment_type_id: int,
    paid_amount: float,
    headers: dict[str, str],
    dry_run: bool,
) -> dict[str, Any] | None:
    url = f"{BASE_URL}/invoice/{invoice_id}/:payment"
    params = {
        "paymentDate": payment_date,
        "paymentTypeId": str(payment_type_id),
        "paidAmount": str(paid_amount),
        "paidAmountCurrency": str(paid_amount),
    }

    if dry_run:
        print("[DRY RUN] Ville registrert betaling:")
        print(f"PUT {url}")
        print(json.dumps(params, indent=2, ensure_ascii=False))
        return None

    response = requests.put(url, headers=headers, params=params, timeout=TIMEOUT_SECONDS)
    if not response.ok:
        print_api_error("Registrer betaling", response)
        raise RuntimeError("Kunne ikke registrere betaling på faktura.")

    print(f"Betaling registrert på invoiceId={invoice_id}")
    return response.json()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fakturer Tripletex-ordre og registrer betaling etter mottatt betaling i Susoft.",
    )
    parser.add_argument("--order-id", type=int, required=True, help="Tripletex order id")
    parser.add_argument(
        "--invoice-date",
        default=datetime.now().date().isoformat(),
        help="Fakturadato (yyyy-mm-dd). Default: i dag.",
    )
    parser.add_argument(
        "--register-payment",
        action="store_true",
        help="Registrer betaling etter fakturering.",
    )
    parser.add_argument(
        "--payment-date",
        default=datetime.now().date().isoformat(),
        help="Betalingsdato (yyyy-mm-dd). Default: i dag.",
    )
    parser.add_argument("--payment-type-id", type=int, help="Tripletex paymentTypeId")
    parser.add_argument("--paid-amount", type=float, help="Betalt belop i fakturavaluta")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Vis hvilke kall som ville blitt gjort, uten a endre data.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    try:
        session_token = create_session_token()
    except RuntimeError as exc:
        print(f"Feil ved token-oppretting: {exc}")
        return

    headers = build_basic_headers(session_token)

    if args.register_payment and (args.payment_type_id is None or args.paid_amount is None):
        print("Du ma oppgi --payment-type-id og --paid-amount nar --register-payment brukes.")
        payment_types = list_payment_types(headers)
        if payment_types:
            print("Tilgjengelige payment types:")
            for payment_type in payment_types:
                if isinstance(payment_type, dict):
                    print(
                        f"- id={payment_type.get('id')} name={payment_type.get('name')} "
                        f"number={payment_type.get('number')}"
                    )
        return

    try:
        invoice_id = create_invoice(args.order_id, args.invoice_date, headers, args.dry_run)
        if args.register_payment:
            if invoice_id < 0:
                print("[DRY RUN] Hopper over faktisk betalingsregistrering.")
            else:
                register_payment(
                    invoice_id=invoice_id,
                    payment_date=args.payment_date,
                    payment_type_id=args.payment_type_id,
                    paid_amount=args.paid_amount,
                    headers=headers,
                    dry_run=args.dry_run,
                )
    except RuntimeError as exc:
        print(f"Feil: {exc}")
        return

    print("Ferdig.")


if __name__ == "__main__":
    main()
