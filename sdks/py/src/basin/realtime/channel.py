from __future__ import annotations

"""
RealtimeChannel + RealtimeClient — T-033.

Transport routing (same rule as basin-js):
  SSE  — channel has exactly one ``postgres_changes`` binding, no presence,
          no per-binding filter string.
  WS   — presence bindings, multiple table bindings, or any binding with a
          filter string.

``basin.channel(topic)`` is a convenience shim on the Client; it delegates to
``basin.realtime.channel(topic)``.
"""

import uuid
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import urlparse

from ..errors import BasinError
from .presence import PresenceCallback, PresenceChannel, PresenceMember
from .sse import SseEvent, SseSubscription
from .ws import WsConnection


RealtimeEvent = str


class PostgresChangesFilter:
    def __init__(
        self,
        *,
        event: RealtimeEvent,
        table: str,
        schema: str = "public",
        filter: Optional[str] = None,
    ) -> None:
        self.event = event
        self.table = table
        self.schema = schema
        self.filter = filter


RealtimeListener = Callable[[Dict[str, Any]], None]


class _PostgresBinding:
    def __init__(self, filter: PostgresChangesFilter, callback: RealtimeListener) -> None:
        self.kind = "postgres_changes"
        self.filter = filter
        self.callback = callback


class _PresenceBinding:
    def __init__(self, filter: Dict[str, Any], callback: PresenceCallback) -> None:
        self.kind = "presence"
        self.filter = filter
        self.callback = callback


class RealtimeChannel:
    """
    Chainable channel builder.  ``.on()`` registers bindings;
    ``.subscribe()`` picks SSE or WS and connects.

    Example::

        channel = client.channel("orders")
        channel.on("postgres_changes", {"event": "INSERT", "table": "orders"}, cb)
        channel.subscribe()
    """

    def __init__(
        self,
        topic: str,
        *,
        url: str,
        project: str,
        headers: Dict[str, str],
    ) -> None:
        self._topic = topic
        self._url = url
        self._project = project
        self._headers = headers

        self._bindings: List[Any] = []
        self._sse_sub: Optional[SseSubscription] = None
        self._ws_conn: Optional[Any] = None
        self._presence_channel: Optional[PresenceChannel] = None
        self._subscribed = False

    @property
    def topic(self) -> str:
        return self._topic

    def on(
        self,
        type: str,
        filter: Dict[str, Any],
        callback: Callable[..., Any],
    ) -> "RealtimeChannel":
        if type == "postgres_changes":
            pg_filter = PostgresChangesFilter(
                event=filter.get("event", "*"),
                table=filter.get("table", ""),
                schema=filter.get("schema", "public"),
                filter=filter.get("filter"),
            )
            self._bindings.append(_PostgresBinding(filter=pg_filter, callback=callback))
        elif type == "presence":
            self._bindings.append(_PresenceBinding(filter=filter, callback=callback))
        return self

    def subscribe(self) -> "RealtimeChannel":
        if self._subscribed:
            return self
        self._subscribed = True

        pg_bindings = [b for b in self._bindings if isinstance(b, _PostgresBinding)]
        presence_bindings = [b for b in self._bindings if isinstance(b, _PresenceBinding)]

        use_sse = (
            len(pg_bindings) == 1
            and len(presence_bindings) == 0
            and not pg_bindings[0].filter.filter
        )

        if use_sse:
            self._start_sse(pg_bindings[0])
        else:
            self._start_ws(pg_bindings, presence_bindings)

        return self

    def unsubscribe(self) -> "RealtimeChannel":
        if self._sse_sub is not None:
            self._sse_sub.stop()
            self._sse_sub = None
        if self._presence_channel is not None:
            self._presence_channel.close()
            self._presence_channel = None
        if self._ws_conn is not None:
            self._ws_conn.close()
            self._ws_conn = None
        self._subscribed = False
        return self

    def _start_sse(self, binding: _PostgresBinding) -> None:
        jwt = self._extract_jwt()
        table = binding.filter.table

        def on_sse_event(event: SseEvent) -> None:
            filter_event = binding.filter.event
            op = event.get("op", "")
            if filter_event != "*" and filter_event != op:
                return
            payload: Dict[str, Any] = {
                "schema": binding.filter.schema,
                "table": event.get("table", ""),
                "commit_timestamp": "",
                "eventType": op,
                "new": event.get("after", {}),
                "old": {},
            }
            binding.callback(payload)

        self._sse_sub = SseSubscription(
            self._url,
            self._project,
            table,
            jwt,
            on_sse_event,
        )
        self._sse_sub.start()

    def _start_ws(
        self,
        pg_bindings: List[_PostgresBinding],
        presence_bindings: List[_PresenceBinding],
    ) -> None:
        conn = WsConnection(
            self._project,
            url=self._url,
            headers=self._headers,
        )
        self._ws_conn = conn
        conn.connect()

        for binding in pg_bindings:
            flt = binding.filter.filter

            def make_cb(b: _PostgresBinding) -> Callable[[Dict[str, Any]], None]:
                def cb(ws_event: Dict[str, Any]) -> None:
                    fe = b.filter.event
                    op = ws_event.get("op", "")
                    if fe != "*" and fe != op:
                        return
                    payload: Dict[str, Any] = {
                        "schema": b.filter.schema,
                        "table": ws_event.get("table", ""),
                        "commit_timestamp": "",
                        "eventType": op,
                        "new": ws_event.get("after", {}),
                        "old": {},
                    }
                    b.callback(payload)
                return cb

            conn.subscribe(
                binding.filter.table,
                filter=flt,
                on_event=make_cb(binding),
            )

        if presence_bindings:
            presence_ch = PresenceChannel(
                self._topic,
                client_id=str(uuid.uuid4()),
                send=conn.send,
            )
            self._presence_channel = presence_ch
            conn.register_presence(self._topic, presence_ch)  # type: ignore[arg-type]

            for pb in presence_bindings:
                presence_ch.on("presence", pb.filter, pb.callback)

    def _extract_jwt(self) -> str:
        auth = self._headers.get("Authorization", "")
        return auth[7:] if auth.startswith("Bearer ") else auth

    @property
    def _transport(self) -> str:
        if self._sse_sub is not None:
            return "sse"
        if self._ws_conn is not None:
            return "ws"
        return "none"


class RealtimeClient:
    """
    ``client.realtime`` — top-level realtime namespace.

    Usage::

        client.realtime.channel("orders")
            .on("postgres_changes", {"event": "INSERT", "table": "orders"}, cb)
            .subscribe()
    """

    enabled = True

    def __init__(
        self,
        *,
        url: str,
        headers: Dict[str, str],
    ) -> None:
        self._url = url
        self._headers = headers

    def channel(self, topic: str) -> RealtimeChannel:
        if not topic:
            raise BasinError("invalid_request", "realtime.channel requires a topic")
        project = self._extract_project()
        return RealtimeChannel(
            topic,
            url=self._url,
            project=project,
            headers=self._headers,
        )

    def connect(self) -> None:
        pass

    def disconnect(self) -> None:
        pass

    def _extract_project(self) -> str:
        try:
            host = urlparse(self._url).hostname or ""
            return host.split(".")[0] or "default"
        except Exception:
            return "default"
