from __future__ import annotations

import asyncio
from typing import Any

import httpx

from ._retry import RetryConfig, is_idempotent, next_delay_ms, should_retry_response
from .errors import BasinError


class HttpTransport:
    """
    Thin wrapper around ``httpx.AsyncClient``.  All basin sub-clients share
    the same transport instance (one connection pool) via ``Client``.

    Transient failures (connect/read errors, 5xx, 429) are retried with
    jittered exponential backoff per ``RetryConfig``.  Only idempotent methods
    are retried unless the policy opts in to retrying writes.
    """

    def __init__(
        self,
        base_url: str,
        headers: dict[str, str],
        *,
        transport: httpx.AsyncBaseTransport | None = None,
        retry: RetryConfig | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._default_headers = dict(headers)
        self._transport = transport
        self._retry = retry if retry is not None else RetryConfig()
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
        retry: RetryConfig | None = None,
    ) -> httpx.Response:
        url = self._base_url + path
        merged_headers: dict[str, str] = {}
        if headers:
            merged_headers.update(headers)

        config = retry if retry is not None else self._retry
        retryable_method = config.retry_writes or is_idempotent(method)
        max_attempts = config.max_attempts if retryable_method else 1

        attempt = 0
        while True:
            attempt += 1
            resp: httpx.Response | None = None
            transport_exc: httpx.TransportError | None = None
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
                transport_exc = exc

            if attempt >= max_attempts:
                if transport_exc is not None:
                    raise BasinError("network", str(transport_exc)) from transport_exc
                assert resp is not None
                return resp

            if transport_exc is None:
                assert resp is not None
                if not should_retry_response(resp, config):
                    return resp

            delay_ms = next_delay_ms(attempt, resp, config)
            await asyncio.sleep(delay_ms / 1000.0)

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> HttpTransport:
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.aclose()
