"""
OpenAPI fetch helper — T-014.

``fetch_openapi(url, anon_key)`` issues ``GET {url}/rest/v1/_openapi.json``
with the anon-key header and returns the parsed document.  Mirrors basin-js
``fetchOpenAPI`` (``src/openapi/fetch.ts``); the engine route is confirmed
against ``basin-rest/src/routes/openapi.rs``.
"""

from __future__ import annotations

from typing import cast

import httpx

from ..errors import BasinError
from .types import OpenAPIDocument

_OPENAPI_PATH = "/rest/v1/_openapi.json"


async def fetch_openapi(
    url: str,
    anon_key: str,
    *,
    client: httpx.AsyncClient | None = None,
    headers: dict[str, str] | None = None,
) -> OpenAPIDocument:
    """
    Fetch and parse the engine's per-project OpenAPI document.

    Raises ``BasinError`` with code ``network`` on transport failure,
    ``not_found`` on 404, ``invalid_response`` on a non-2xx status or a body
    that is not valid JSON.
    """
    base = url.rstrip("/")
    endpoint = base + _OPENAPI_PATH

    request_headers: dict[str, str] = {
        "Accept": "application/json",
        "apikey": anon_key,
    }
    if headers:
        request_headers.update(headers)

    owned = client is None
    http = client or httpx.AsyncClient()
    try:
        try:
            resp = await http.get(endpoint, headers=request_headers)
        except httpx.TransportError as exc:
            raise BasinError(
                "network",
                f"failed to fetch OpenAPI document: {exc}",
            ) from exc

        if resp.status_code == 404:
            raise BasinError(
                "not_found",
                f"OpenAPI document not found at {endpoint}",
                status=404,
            )
        if not resp.is_success:
            raise BasinError(
                "invalid_response",
                f"OpenAPI fetch returned HTTP {resp.status_code}",
                status=resp.status_code,
            )

        try:
            doc = resp.json()
        except Exception as exc:
            raise BasinError(
                "invalid_response",
                "OpenAPI document is not valid JSON",
                status=resp.status_code,
            ) from exc
    finally:
        if owned:
            await http.aclose()

    if not isinstance(doc, dict):
        raise BasinError(
            "invalid_response",
            "OpenAPI document is not a JSON object",
        )

    return cast(OpenAPIDocument, doc)
