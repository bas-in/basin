"""
WebSocket multiplex transport — T-031.

``websockets`` (under ``[realtime]`` extra).  JSON control plane:
subscribe/unsubscribe, event/error frames, ``seq`` gap detection.

Engine WS route: ``/realtime/v1/ws/:project`` (separate port 5436 for realtime).
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable
from typing import Any

from ..errors import BasinError

_log = logging.getLogger(__name__)

_BASE_BACKOFF_S = 1.0
_CAP_BACKOFF_S = 30.0


def _backoff(attempt: int) -> float:
    return min(_BASE_BACKOFF_S * float(2**attempt), _CAP_BACKOFF_S)


WsEvent = dict[str, Any]
WsEventCallback = Callable[[WsEvent], None]


class _Subscription:
    def __init__(
        self,
        filter: str | None,
        on_event: WsEventCallback,
        on_lag: Callable[[str, int], None] | None,
    ) -> None:
        self.filter = filter
        self.on_event = on_event
        self.on_lag = on_lag
        self.last_seq = 0


class PresenceMessageHandler:
    def handle_message(self, msg: Any) -> None:
        ...


class WsConnection:
    """
    Multiplexed WebSocket connection to ``/realtime/v1/ws/:project``.

    Requires ``websockets`` (install ``basin-sdk[realtime]``).
    """

    def __init__(
        self,
        project: str,
        *,
        url: str,
        headers: dict[str, str] | None = None,
    ) -> None:
        self._project = project
        self._base_url = url
        self._headers = headers or {}
        self._subs: dict[str, _Subscription] = {}
        self._presence_handlers: dict[str, PresenceMessageHandler] = {}
        self._closed = False
        self._reconnect_attempt = 0
        self._ws: Any = None
        self._task: asyncio.Task[None] | None = None
        self._pending: dict[str, asyncio.Future[None]] = {}

    def _build_ws_url(self) -> str:
        base = self._base_url
        if base.startswith("https://"):
            base = "wss://" + base[8:]
        elif base.startswith("http://"):
            base = "ws://" + base[7:]
        url = f"{base}/realtime/v1/ws/{self._project}"
        jwt = self._headers.get("Authorization", "")
        if jwt.startswith("Bearer "):
            token = jwt[7:]
            url += f"?apikey={token}"
        return url

    def connect(self) -> None:
        self._closed = False
        self._task = asyncio.ensure_future(self._run())

    async def _run(self) -> None:
        try:
            import websockets  # type: ignore[import-not-found]
        except ImportError as exc:
            raise BasinError(
                "not_implemented",
                "WebSocket realtime requires `pip install basin-sdk[realtime]`",
            ) from exc

        while not self._closed:
            if self._reconnect_attempt > 0:
                await asyncio.sleep(_backoff(self._reconnect_attempt - 1))
            if self._closed:
                break
            try:
                async with websockets.connect(self._build_ws_url()) as ws:
                    self._ws = ws
                    self._reconnect_attempt = 0
                    for table, sub in self._subs.items():
                        frame: dict[str, str] = {"type": "subscribe", "table": table}
                        if sub.filter:
                            frame["filter"] = sub.filter
                        await ws.send(json.dumps(frame))
                    async for raw in ws:
                        if self._closed:
                            break
                        try:
                            msg = json.loads(raw)
                        except json.JSONDecodeError:
                            continue
                        self._dispatch(msg)
            except asyncio.CancelledError:
                break
            except Exception:
                if self._closed:
                    break
                self._reconnect_attempt += 1
        self._ws = None

    def _dispatch(self, msg: Any) -> None:
        if not isinstance(msg, dict):
            return
        kind = msg.get("type")
        if kind == "subscribed":
            table = msg.get("table", "")
            fut = self._pending.pop(table, None)
            if fut and not fut.done():
                fut.set_result(None)
        elif kind == "unsubscribed":
            table = msg.get("table", "")
            fut = self._pending.pop(f"unsub:{table}", None)
            if fut and not fut.done():
                fut.set_result(None)
        elif kind == "event":
            table = msg.get("table", "")
            sub = self._subs.get(table)
            if sub is None:
                return
            seq = msg.get("seq", 0)
            if sub.last_seq != 0 and seq > sub.last_seq + 1 and sub.on_lag:
                sub.on_lag(table, seq - sub.last_seq - 1)
            sub.last_seq = seq
            sub.on_event(
                {
                    "table": table,
                    "op": msg.get("op"),
                    "after": msg.get("after"),
                    "seq": seq,
                }
            )
        elif kind == "error":
            if msg.get("code") == "lag":
                table = msg.get("table", "")
                sub = self._subs.get(table)
                if sub and sub.on_lag:
                    sub.on_lag(table, msg.get("missed", 0))
        elif kind in ("presence_state", "presence_diff"):
            channel = msg.get("channel")
            if channel:
                handler = self._presence_handlers.get(channel)
                if handler:
                    handler.handle_message(msg)

    def subscribe(
        self,
        table: str,
        *,
        filter: str | None = None,
        on_event: WsEventCallback,
        on_lag: Callable[[str, int], None] | None = None,
    ) -> asyncio.Future[None]:
        sub = _Subscription(filter=filter, on_event=on_event, on_lag=on_lag)
        self._subs[table] = sub
        loop = asyncio.get_event_loop()
        fut: asyncio.Future[None] = loop.create_future()
        self._pending[table] = fut
        if self._ws is not None:
            frame: dict[str, str] = {"type": "subscribe", "table": table}
            if filter:
                frame["filter"] = filter
            asyncio.ensure_future(self._ws.send(json.dumps(frame)))
        return fut

    def unsubscribe(self, table: str) -> asyncio.Future[None]:
        self._subs.pop(table, None)
        loop = asyncio.get_event_loop()
        fut: asyncio.Future[None] = loop.create_future()
        if self._ws is not None:
            self._pending[f"unsub:{table}"] = fut
            asyncio.ensure_future(
                self._ws.send(json.dumps({"type": "unsubscribe", "table": table}))
            )
        else:
            fut.set_result(None)
        return fut

    def register_presence(self, channel: str, handler: PresenceMessageHandler) -> None:
        self._presence_handlers[channel] = handler

    def unregister_presence(self, channel: str) -> None:
        self._presence_handlers.pop(channel, None)

    def send(self, frame: Any) -> None:
        if self._ws is not None:
            asyncio.ensure_future(self._ws.send(json.dumps(frame)))

    def close(self) -> None:
        self._closed = True
        for fut in self._pending.values():
            if not fut.done():
                fut.set_exception(Exception("connection closed"))
        self._pending.clear()
        if self._task is not None:
            self._task.cancel()
            self._task = None
        if self._ws is not None:
            asyncio.ensure_future(self._ws.close())
            self._ws = None

    @property
    def project(self) -> str:
        return self._project
