from __future__ import annotations

from typing import Any

import httpx

from .errors import BasinError


class HttpTransport:
    """
    Thin wrapper around ``httpx.AsyncClient``.  All basin sub-clients share
    the same transport instance (one connection pool) via ``Client``.
    """

    def __init__(
        self,
        base_url: str,
        headers: dict[str, str],
        *,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._default_headers = dict(headers)
        self._transport = transport
        client_kwargs: dict[str, Any] = {
            "headers": self._default_headers,
            "follow_redirects": True,
        }
        if transport is not None:
            client_kwargs["transport"] = transport
        self._client = httpx.AsyncClient(**client_kwargs)

    async def request(
        self,
        method: str,
        path: str,
        *,
        json: Any = None,
        params: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
        content: bytes | None = None,
    ) -> httpx.Response:
        url = self._base_url + path
        merged_headers: dict[str, str] = {}
        if headers:
            merged_headers.update(headers)
        try:
            resp = await self._client.request(
                method,
                url,
                json=json,
                params=params,
                headers=merged_headers if merged_headers else None,
                content=content,
            )
        except httpx.TransportError as exc:
            raise BasinError("network", str(exc)) from exc
        return resp

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> HttpTransport:
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.aclose()
