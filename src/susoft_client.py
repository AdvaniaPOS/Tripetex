from __future__ import annotations

import time
from typing import Any

import requests

from src.config import get_settings


RATE_LIMIT_MAX_RETRIES = 3
RATE_LIMIT_DEFAULT_BACKOFF_SECONDS = 1.0


def _retry_after_seconds(response: requests.Response, attempt: int) -> float:
    retry_after = response.headers.get("Retry-After") if response.headers is not None else None
    if retry_after:
        try:
            parsed = float(str(retry_after).strip())
            if parsed > 0:
                return parsed
        except Exception:
            pass
    # Linear backoff with a small default keeps behavior predictable for ops.
    return RATE_LIMIT_DEFAULT_BACKOFF_SECONDS * float(attempt)


def _request_with_rate_limit_retry(
    method: str,
    url: str,
    *,
    timeout: int,
    headers: dict[str, str],
    json_body: dict[str, Any] | None = None,
    params: dict[str, Any] | None = None,
) -> requests.Response:
    last_response: requests.Response | None = None
    for attempt in range(1, RATE_LIMIT_MAX_RETRIES + 1):
        response = requests.request(
            method,
            url,
            headers=headers,
            json=json_body,
            params=params,
            timeout=timeout,
        )
        if response.status_code != 429:
            return response

        last_response = response
        if attempt >= RATE_LIMIT_MAX_RETRIES:
            return response

        time.sleep(_retry_after_seconds(response, attempt))

    # Defensive fallback; the loop always returns.
    if last_response is not None:
        return last_response
    raise RuntimeError("Uventet feil i rate-limit retry-logikk mot Susoft API")


def _resolve_susoft_settings(overrides: dict[str, str] | None = None) -> dict[str, str]:
    settings = get_settings()
    data = overrides or {}
    return {
        "base_url": str(data.get("susoft_base_url") or settings.susoft_base_url),
        "shop_url_key": str(data.get("susoft_shop_url_key") or settings.susoft_shop_url_key),
        "username": str(data.get("susoft_username") or settings.susoft_username),
        "password": str(data.get("susoft_password") or settings.susoft_password),
        "timeout": str(settings.request_timeout_seconds),
    }


def _base_headers(*, overrides: dict[str, str] | None = None) -> dict[str, str]:
    resolved = _resolve_susoft_settings(overrides)
    if not resolved["shop_url_key"]:
        raise RuntimeError("Susoft shop key mangler i miljokonfigurasjon (.env).")
    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "X-Shop-Url-Key": resolved["shop_url_key"],
    }


def _authenticate(*, overrides: dict[str, str] | None = None) -> str:
    resolved = _resolve_susoft_settings(overrides)
    if not resolved["username"] or not resolved["password"]:
        raise RuntimeError("Susoft brukernavn/passord mangler i miljokonfigurasjon (.env).")

    url = f"{resolved['base_url']}/user/auth"
    body = {
        "login": resolved["username"],
        "password": resolved["password"],
    }
    headers = _base_headers(overrides=overrides)

    try:
        response = _request_with_rate_limit_retry(
            "POST",
            url,
            timeout=int(resolved["timeout"]),
            headers=headers,
            json_body=body,
        )
    except requests.RequestException as exc:
        raise RuntimeError(f"Nettverksfeil ved auth mot Susoft: {exc}") from exc

    if not response.ok:
        raise RuntimeError(f"Susoft auth feilet med status {response.status_code}: {response.text}")

    payload = response.json()
    token = payload.get("token") if isinstance(payload, dict) else None
    if not isinstance(token, str) or not token:
        raise RuntimeError("Susoft auth-respons mangler token.")
    return token


def authenticate(*, overrides: dict[str, str] | None = None) -> str:
    return _authenticate(overrides=overrides)


def create_order(
    order_payload: dict[str, Any],
    token: str | None = None,
    *,
    overrides: dict[str, str] | None = None,
) -> dict[str, Any]:
    resolved = _resolve_susoft_settings(overrides)
    auth_token = token or _authenticate(overrides=overrides)
    # Use shopping cart endpoint so order is not finalized by integration and can continue in POS.
    url = f"{resolved['base_url']}/shopping-cart"
    headers = _base_headers(overrides=overrides)
    headers["Authorization"] = f"Bearer {auth_token}"

    try:
        response = _request_with_rate_limit_retry(
            "POST",
            url,
            timeout=int(resolved["timeout"]),
            headers=headers,
            json_body=order_payload,
        )
    except requests.RequestException as exc:
        raise RuntimeError(f"Nettverksfeil ved opprettelse av Susoft-ordre: {exc}") from exc

    # Idempotent behavior: if cart with same external reference already exists,
    # fetch and return the existing cart instead of failing the sync.
    if response.status_code == 409:
        ext_id = str(order_payload.get("externalRef", "")).strip()
        if ext_id:
            existing = find_cart_by_external_id(ext_id, auth_token, overrides=overrides)
            if existing is not None:
                return existing

    if not response.ok:
        raise RuntimeError(f"Susoft shopping-cart opprettelse feilet med status {response.status_code}: {response.text}")

    data = response.json()
    if not isinstance(data, dict):
        raise RuntimeError("Ugyldig responsformat ved opprettelse av Susoft shopping-cart.")
    return data


def find_cart_by_external_id(
    ext_id: str,
    token: str | None = None,
    *,
    overrides: dict[str, str] | None = None,
) -> dict[str, Any] | None:
    resolved = _resolve_susoft_settings(overrides)
    auth_token = token or _authenticate(overrides=overrides)
    url = f"{resolved['base_url']}/shopping-cart/external-id"

    headers = _base_headers(overrides=overrides)
    headers["Authorization"] = f"Bearer {auth_token}"
    params = {"extId": ext_id}

    try:
        response = _request_with_rate_limit_retry(
            "GET",
            url,
            timeout=int(resolved["timeout"]),
            headers=headers,
            params=params,
        )
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


def find_cart_by_uuid(
    uuid: str,
    token: str | None = None,
    *,
    overrides: dict[str, str] | None = None,
) -> dict[str, Any] | None:
    resolved = _resolve_susoft_settings(overrides)
    auth_token = token or _authenticate(overrides=overrides)
    url = f"{resolved['base_url']}/shopping-cart/uuid"

    headers = _base_headers(overrides=overrides)
    headers["Authorization"] = f"Bearer {auth_token}"
    params = {"uuid": uuid}

    try:
        response = _request_with_rate_limit_retry(
            "GET",
            url,
            timeout=int(resolved["timeout"]),
            headers=headers,
            params=params,
        )
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


def find_order_by_uuid(
    uuid: str,
    token: str | None = None,
    *,
    overrides: dict[str, str] | None = None,
) -> dict[str, Any] | None:
    resolved = _resolve_susoft_settings(overrides)
    auth_token = token or _authenticate(overrides=overrides)
    url = f"{resolved['base_url']}/order/uuid"

    headers = _base_headers(overrides=overrides)
    headers["Authorization"] = f"Bearer {auth_token}"
    params = {"uuid": uuid}

    try:
        response = _request_with_rate_limit_retry(
            "GET",
            url,
            timeout=int(resolved["timeout"]),
            headers=headers,
            params=params,
        )
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
