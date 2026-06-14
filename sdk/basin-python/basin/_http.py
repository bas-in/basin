"""Minimal HTTP helpers shared by all sub-clients.

Wraps httpx.Client / httpx.AsyncClient with:
- Bearer-token injection (auto-refreshed sessions beat the static key)
- Error-envelope decoding → BasinApiError
- URL building with typed query-pair lists
"""

from __future__ import annotations

import json
from typing import Any, Callable, Awaitable, Optional, Union
from urllib.parse import quote, urlencode

import httpx

from .errors import BasinApiError, BasinNetworkError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_url(base: str, path: str, query: list[tuple[str, str]] | None) -> str:
    url = base.rstrip("/") + path
    if query:
        url += "?" + urlencode(query)
    return url


def _encode_object_path(path: str) -> str:
    """Percent-encode each segment of an object path, preserving '/' separators."""
    return "/".join(quote(seg, safe="") for seg in path.split("/"))


def _raise_for_response(response: httpx.Response) -> None:
    """Parse a non-2xx response into a BasinApiError."""
    code = "E_UNKNOWN"
    message = f"HTTP {response.status_code}"
    sqlstate: str | None = None
    try:
        text = response.text
        if text:
            try:
                body = json.loads(text)
                if isinstance(body, dict):
                    if isinstance(body.get("code"), str):
                        code = body["code"]
                    if isinstance(body.get("message"), str):
                        message = body["message"]
                    else:
                        message = text
                    if isinstance(body.get("sqlstate"), str):
                        sqlstate = body["sqlstate"]
                else:
                    message = text
            except (json.JSONDecodeError, ValueError):
                message = text
    except Exception:
        pass
    raise BasinApiError(code, message, response.status_code, sqlstate)


def _decode_json_response(response: httpx.Response) -> Any:
    """Return parsed JSON, or None for 204 / empty body."""
    if response.status_code == 204:
        return None
    text = response.text
    if not text:
        return None
    return json.loads(text)


# ---------------------------------------------------------------------------
# Sync transport
# ---------------------------------------------------------------------------


class SyncTransport:
    """Thin wrapper around httpx.Client used by BasinClient."""

    def __init__(
        self,
        base_url: str,
        key: Optional[str] = None,
        client: Optional[httpx.Client] = None,
        timeout: float = 30.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self._key = key
        self._client = client or httpx.Client(timeout=timeout)
        self._get_token: Optional[Callable[[], Optional[str]]] = None

    # The AuthClient wires itself in after construction so the transport always
    # uses the freshest session token.
    def set_token_getter(self, fn: Callable[[], Optional[str]]) -> None:
        self._get_token = fn

    def _bearer(self) -> Optional[str]:
        if self._get_token is not None:
            tok = self._get_token()
            if tok is not None:
                return tok
        return self._key

    def request(
        self,
        method: str,
        path: str,
        *,
        query: list[tuple[str, str]] | None = None,
        body: Any = None,
        raw_body: bytes | None = None,
        headers: dict[str, str] | None = None,
        auth: bool = True,
    ) -> httpx.Response:
        hdrs: dict[str, str] = dict(headers or {})
        if auth:
            tok = self._bearer()
            if tok is not None:
                hdrs["authorization"] = f"Bearer {tok}"

        content: bytes | None = None
        if raw_body is not None:
            content = raw_body
        elif body is not None:
            hdrs.setdefault("content-type", "application/json")
            content = json.dumps(body).encode()

        url = _build_url(self.base_url, path, query)
        try:
            response = self._client.request(
                method, url, content=content, headers=hdrs
            )
        except httpx.TransportError as exc:
            raise BasinNetworkError(str(exc)) from exc

        if not response.is_success:
            _raise_for_response(response)
        return response

    def request_json(
        self,
        method: str,
        path: str,
        **kwargs: Any,
    ) -> Any:
        return _decode_json_response(self.request(method, path, **kwargs))

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "SyncTransport":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()


# ---------------------------------------------------------------------------
# Async transport
# ---------------------------------------------------------------------------


class AsyncTransport:
    """Thin wrapper around httpx.AsyncClient used by AsyncBasinClient."""

    def __init__(
        self,
        base_url: str,
        key: Optional[str] = None,
        client: Optional[httpx.AsyncClient] = None,
        timeout: float = 30.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self._key = key
        self._client = client or httpx.AsyncClient(timeout=timeout)
        self._get_token: Optional[Callable[[], Awaitable[Optional[str]]]] = None

    def set_token_getter(self, fn: Callable[[], Awaitable[Optional[str]]]) -> None:
        self._get_token = fn

    async def _bearer(self) -> Optional[str]:
        if self._get_token is not None:
            tok = await self._get_token()
            if tok is not None:
                return tok
        return self._key

    async def request(
        self,
        method: str,
        path: str,
        *,
        query: list[tuple[str, str]] | None = None,
        body: Any = None,
        raw_body: bytes | None = None,
        headers: dict[str, str] | None = None,
        auth: bool = True,
    ) -> httpx.Response:
        hdrs: dict[str, str] = dict(headers or {})
        if auth:
            tok = await self._bearer()
            if tok is not None:
                hdrs["authorization"] = f"Bearer {tok}"

        content: bytes | None = None
        if raw_body is not None:
            content = raw_body
        elif body is not None:
            hdrs.setdefault("content-type", "application/json")
            content = json.dumps(body).encode()

        url = _build_url(self.base_url, path, query)
        try:
            response = await self._client.request(
                method, url, content=content, headers=hdrs
            )
        except httpx.TransportError as exc:
            raise BasinNetworkError(str(exc)) from exc

        if not response.is_success:
            _raise_for_response(response)
        return response

    async def request_json(
        self,
        method: str,
        path: str,
        **kwargs: Any,
    ) -> Any:
        return _decode_json_response(await self.request(method, path, **kwargs))

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> "AsyncTransport":
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.aclose()
