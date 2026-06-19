from __future__ import annotations

from typing import Any

import requests

from src.config import get_settings


def _base_headers() -> dict[str, str]:
    settings = get_settings()
    if not settings.susoft_shop_url_key:
        raise RuntimeError("Susoft shop key mangler i miljokonfigurasjon (.env).")
    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "X-Shop-Url-Key": settings.susoft_shop_url_key,
    }


def _authenticate() -> str:
    settings = get_settings()
    if not settings.susoft_username or not settings.susoft_password:
        raise RuntimeError("Susoft brukernavn/passord mangler i miljokonfigurasjon (.env).")

    url = f"{settings.susoft_base_url}/user/auth"
    body = {
        "login": settings.susoft_username,
        "password": settings.susoft_password,
    }
    headers = _base_headers()

    try:
        response = requests.post(url, headers=headers, json=body, timeout=settings.request_timeout_seconds)
    except requests.RequestException as exc:
        raise RuntimeError(f"Nettverksfeil ved auth mot Susoft: {exc}") from exc

    if not response.ok:
        raise RuntimeError(f"Susoft auth feilet med status {response.status_code}: {response.text}")

    payload = response.json()
    token = payload.get("token") if isinstance(payload, dict) else None
    if not isinstance(token, str) or not token:
        raise RuntimeError("Susoft auth-respons mangler token.")
    return token


def authenticate() -> str:
    return _authenticate()


def create_order(order_payload: dict[str, Any]) -> dict[str, Any]:
    settings = get_settings()
    token = _authenticate()
    # Use shopping cart endpoint so order is not finalized by integration and can continue in POS.
    url = f"{settings.susoft_base_url}/shopping-cart"
    headers = _base_headers()
    headers["Authorization"] = f"Bearer {token}"

    try:
        response = requests.post(url, headers=headers, json=order_payload, timeout=settings.request_timeout_seconds)
    except requests.RequestException as exc:
        raise RuntimeError(f"Nettverksfeil ved opprettelse av Susoft-ordre: {exc}") from exc

    # Idempotent behavior: if cart with same external reference already exists,
    # fetch and return the existing cart instead of failing the sync.
    if response.status_code == 409:
        ext_id = str(order_payload.get("externalRef", "")).strip()
        if ext_id:
            existing = find_cart_by_external_id(ext_id, token)
            if existing is not None:
                return existing

    if not response.ok:
        raise RuntimeError(f"Susoft shopping-cart opprettelse feilet med status {response.status_code}: {response.text}")

    data = response.json()
    if not isinstance(data, dict):
        raise RuntimeError("Ugyldig responsformat ved opprettelse av Susoft shopping-cart.")
    return data


def find_cart_by_external_id(ext_id: str, token: str | None = None) -> dict[str, Any] | None:
    settings = get_settings()
    auth_token = token or _authenticate()
    url = f"{settings.susoft_base_url}/shopping-cart/external-id"

    headers = _base_headers()
    headers["Authorization"] = f"Bearer {auth_token}"
    params = {"extId": ext_id}

    try:
        response = requests.get(url, headers=headers, params=params, timeout=settings.request_timeout_seconds)
    except requests.RequestException as exc:
        raise RuntimeError(f"Nettverksfeil ved lesing av Susoft shopping-cart per externalRef: {exc}") from exc

    if response.status_code == 404:
        return None
    if not response.ok:
        raise RuntimeError(f"Susoft lookup per externalRef feilet med status {response.status_code}: {response.text}")

    data = response.json()
    if not isinstance(data, dict):
        raise RuntimeError("Ugyldig responsformat ved oppslag av Susoft shopping-cart per externalRef.")
    return data


def find_cart_by_uuid(uuid: str, token: str | None = None) -> dict[str, Any] | None:
    settings = get_settings()
    auth_token = token or _authenticate()
    url = f"{settings.susoft_base_url}/shopping-cart/uuid"

    headers = _base_headers()
    headers["Authorization"] = f"Bearer {auth_token}"
    params = {"uuid": uuid}

    try:
        response = requests.get(url, headers=headers, params=params, timeout=settings.request_timeout_seconds)
    except requests.RequestException as exc:
        raise RuntimeError(f"Nettverksfeil ved lesing av Susoft shopping-cart per uuid: {exc}") from exc

    if response.status_code == 404:
        return None
    if not response.ok:
        raise RuntimeError(f"Susoft shopping-cart lookup per uuid feilet med status {response.status_code}: {response.text}")

    data = response.json()
    if not isinstance(data, dict):
        raise RuntimeError("Ugyldig responsformat ved oppslag av Susoft shopping-cart per uuid.")
    return data


def find_order_by_uuid(uuid: str, token: str | None = None) -> dict[str, Any] | None:
    settings = get_settings()
    auth_token = token or _authenticate()
    url = f"{settings.susoft_base_url}/order/uuid"

    headers = _base_headers()
    headers["Authorization"] = f"Bearer {auth_token}"
    params = {"uuid": uuid}

    try:
        response = requests.get(url, headers=headers, params=params, timeout=settings.request_timeout_seconds)
    except requests.RequestException as exc:
        raise RuntimeError(f"Nettverksfeil ved lesing av Susoft order per uuid: {exc}") from exc

    if response.status_code == 404:
        return None
    if not response.ok:
        raise RuntimeError(f"Susoft order lookup per uuid feilet med status {response.status_code}: {response.text}")

    data = response.json()
    if not isinstance(data, dict):
        raise RuntimeError("Ugyldig responsformat ved oppslag av Susoft order per uuid.")
    return data
