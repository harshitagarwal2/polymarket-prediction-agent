from __future__ import annotations

import importlib
import socket
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode

from . import http_client


def configure_client_timeout(adapter: Any) -> None:
    try:
        helpers = importlib.import_module("py_clob_client.http_helpers.helpers")
        httpx = importlib.import_module("httpx")
    except ImportError:
        return
    http_client = getattr(helpers, "_http_client", None)
    if http_client is None:
        return
    http_client.timeout = httpx.Timeout(adapter.config.request_timeout_seconds)


def retryable_status_code(status_code: int | None) -> bool:
    if status_code is None:
        return False
    return status_code in {425, 429} or 500 <= status_code < 600


def exception_status_code(exc: Exception) -> int | None:
    status_code = getattr(exc, "status_code", None)
    if isinstance(status_code, int):
        return status_code
    if isinstance(exc, HTTPError):
        return int(exc.code)
    return None


def is_retryable_error(exc: Exception) -> bool:
    status_code = exception_status_code(exc)
    if retryable_status_code(status_code):
        return True
    if status_code is not None:
        return False
    if exc.__class__.__name__ == "PolyApiException":
        return True
    if isinstance(exc, (TimeoutError, socket.timeout, ConnectionError, URLError)):
        return True
    return False


def call_with_retry(adapter: Any, operation: str, func, *args, **kwargs):
    attempts = max(1, adapter.config.retry_max_attempts)
    backoff = max(0.0, adapter.config.retry_backoff_seconds)
    multiplier = max(1.0, adapter.config.retry_backoff_multiplier)
    max_backoff = max(backoff, adapter.config.retry_max_backoff_seconds)
    last_error: Exception | None = None

    for attempt in range(1, attempts + 1):
        try:
            return func(*args, **kwargs)
        except Exception as exc:
            last_error = exc
            if attempt >= attempts or not is_retryable_error(exc):
                raise
            time.sleep(min(backoff, max_backoff))
            backoff = min(max_backoff, backoff * multiplier)

    if last_error is not None:
        raise last_error
    raise RuntimeError(f"{operation} failed without returning or raising")


def call_client(adapter: Any, operation: str, method_name: str, *args, **kwargs):
    client = adapter._ensure_client()
    method = getattr(client, method_name)
    return call_with_retry(adapter, operation, method, *args, **kwargs)


def ensure_client(adapter: Any):
    if adapter._client is not None:
        return adapter._client

    try:
        client_module = importlib.import_module("py_clob_client.client")
        clob_client_cls = getattr(client_module, "ClobClient")
    except ImportError as exc:
        raise RuntimeError(
            "py-clob-client is not installed. Install it with `pip install -e upstreams/py-clob-client` or `pip install py-clob-client`."
        ) from exc

    if adapter.config.private_key:
        client = clob_client_cls(
            adapter.config.host,
            key=adapter.config.private_key,
            chain_id=adapter.config.chain_id,
            signature_type=adapter.config.signature_type,
            funder=adapter.config.funder,
        )
        client.set_api_creds(
            client.create_or_derive_api_creds(adapter.config.api_creds_nonce)
        )
    else:
        client = clob_client_cls(adapter.config.host)

    configure_client_timeout(adapter)
    adapter._client = client
    return client


def account_address(adapter: Any) -> str | None:
    if adapter.config.account_address:
        return adapter.config.account_address
    if adapter.config.funder:
        return adapter.config.funder
    try:
        client = adapter._ensure_client()
        return client.get_address()
    except Exception:
        return None


def fetch_data_api(adapter: Any, path: str, params: dict[str, Any], *, client=None) -> Any:
    clean_params = {key: value for key, value in params.items() if value is not None}
    url = f"{adapter.config.data_api_host}{path}"
    def fetch() -> Any:
        return http_client.get_json(
            url,
            params=clean_params,
            timeout_seconds=adapter.config.request_timeout_seconds,
            client=client,
        )

    return call_with_retry(adapter, f"data api fetch {path}", fetch)
