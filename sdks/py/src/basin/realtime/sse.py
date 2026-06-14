"""
SSE transport for realtime — T-030.

Streams ``GET /realtime/v1/sse/:project/:table`` over httpx streaming response.
Parses SSE ``data:`` frames into ``SseEvent`` dicts.  Tolerates 15-second
heartbeats (comment-only lines starting with ``:``).  Sends ``Last-Event-Id``
on reconnect for replay.

Engine route: separate port 5435 for realtime (wsgi-level); the SSE path is
``/realtime/v1/sse/:project/:table``.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from typing import Any

import httpx

from ..errors import BasinError

_BASE_BACKOFF_MS = 1.0
_CAP_BACKOFF_MS = 30.0


def _backoff(attempt: int) -> float:
    return min(_BASE_BACKOFF_MS * float(2**attempt), _CAP_BACKOFF_MS)


SseEvent = dict[str, Any]
SseEventCallback = Callable[[SseEvent], None]
SseErrorCallback = Callable[[BasinError], None]


def _build_url(base_url: str, project: str, table: str) -> str:
    from urllib.parse import quote
    return (
        f"{base_url}/realtime/v1/sse"
        f"/{quote(project, safe='')}/{quote(table, safe='')}"
    )


class SseSubscription:
    """
    Single SSE subscription to a ``(project, table)`` feed.

    Call ``.start()`` to begin; ``.stop()`` to cancel.  Reconnects with
    exponential back-off on error.  Passes every parsed JSON event to
    ``on_event``.
    """

    def __init__(
        self,
        base_url: str,
        project: str,
        table: str,
        jwt: str,
        on_event: SseEventCallback,
        *,
        on_error: SseErrorCallback | None = None,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self._base_url = base_url
        self._project = project
        self._table = table
        self._jwt = jwt
        self._on_event = on_event
        self._on_error = on_error
        self._http_client = http_client
        self._last_seq: int | None = None
        self._stopped = False
        self._attempt = 0
        self._task: asyncio.Task[None] | None = None

    def start(self) -> None:
        self._stopped = False
        self._task = asyncio.ensure_future(self._loop())

    def stop(self) -> None:
        self._stopped = True
        if self._task is not None:
            self._task.cancel()
            self._task = None

    async def _loop(self) -> None:
        while not self._stopped:
            if self._attempt > 0:
                await asyncio.sleep(_backoff(self._attempt - 1))
            if self._stopped:
                break
            try:
                await self._connect()
                self._attempt = 0
            except asyncio.CancelledError:
                break
            except BasinError as err:
                if self._stopped:
                    break
                if self._on_error is not None:
                    self._on_error(err)
                self._attempt += 1
            except Exception as exc:
                if self._stopped:
                    break
                wrapped = BasinError("network", str(exc))
                if self._on_error is not None:
                    self._on_error(wrapped)
                self._attempt += 1

    async def _connect(self) -> None:
        url = _build_url(self._base_url, self._project, self._table)
        headers: dict[str, str] = {
            "Authorization": f"Bearer {self._jwt}",
            "Accept": "text/event-stream",
        }
        if self._last_seq is not None:
            headers["Last-Event-Id"] = str(self._last_seq)

        client_owned = self._http_client is None
        client = self._http_client or httpx.AsyncClient()
        try:
            async with client.stream("GET", url, headers=headers) as resp:
                if not resp.is_success:
                    raise BasinError(
                        "network",
                        f"SSE connect failed: {resp.status_code}",
                        status=resp.status_code,
                    )
                async for line in resp.aiter_lines():
                    if self._stopped:
                        return
                    self._process_line(line)
        finally:
            if client_owned:
                await client.aclose()

    def _process_line(self, line: str) -> None:
        if line.startswith(":") or not line.strip():
            return
        if line.startswith("data:"):
            payload = line[5:].lstrip(" ")
            try:
                parsed = json.loads(payload)
            except json.JSONDecodeError as exc:
                raise BasinError(
                    "invalid_response",
                    f"SSE data line is not valid JSON: {exc}",
                ) from exc
            ev = parsed
            if isinstance(ev, dict) and isinstance(ev.get("seq"), int):
                self._last_seq = ev["seq"]
            self._on_event(ev)
