from __future__ import annotations

"""
Presence over WebSocket — T-032.

``track``/``untrack``/``heartbeat``; ``presence_state``/``presence_diff``.
Mirrors basin-js ``PresenceChannel`` semantics.
"""

import asyncio
from typing import Any, Callable, Dict, List, Optional

PresenceEvent = str
PresenceMember = Dict[str, Any]
PresenceCallback = Callable[[List[PresenceMember]], None]


class _Binding:
    def __init__(self, event: PresenceEvent, callback: PresenceCallback) -> None:
        self.event = event
        self.callback = callback


class PresenceChannel:
    """
    Presence channel built on top of a ``WsConnection``.

    Manages ``presence_track`` / ``presence_untrack`` / ``heartbeat`` frames
    and dispatches ``presence_state`` / ``presence_diff`` messages to
    registered listeners.
    """

    _HEARTBEAT_INTERVAL = 30.0

    def __init__(
        self,
        channel: str,
        client_id: str,
        send: Callable[[Any], None],
    ) -> None:
        self._channel = channel
        self._client_id = client_id
        self._send = send
        self._presences: Dict[str, PresenceMember] = {}
        self._bindings: List[_Binding] = []
        self._heartbeat_task: Optional[asyncio.Task[None]] = None
        self._tracked = False

    def track(self, metadata: Any = None) -> None:
        """Send ``presence_track`` and start heartbeat."""
        self._tracked = True
        self._send(
            {
                "type": "presence_track",
                "channel": self._channel,
                "client_id": self._client_id,
                "metadata": metadata,
            }
        )
        self._start_heartbeat()

    def untrack(self) -> None:
        """Send ``presence_untrack`` and stop heartbeat."""
        self._tracked = False
        self._stop_heartbeat()
        self._send(
            {
                "type": "presence_untrack",
                "channel": self._channel,
                "client_id": self._client_id,
            }
        )

    def presence_state(self) -> List[PresenceMember]:
        """Return current snapshot of tracked members."""
        return list(self._presences.values())

    def on(
        self,
        type: str,
        filter: Dict[str, Any],
        callback: PresenceCallback,
    ) -> "PresenceChannel":
        event = filter.get("event", "sync")
        self._bindings.append(_Binding(event=event, callback=callback))
        return self

    def handle_message(self, msg: Any) -> None:
        if not isinstance(msg, dict):
            return
        if msg.get("channel") != self._channel:
            return
        kind = msg.get("type")
        if kind == "presence_state":
            presences: List[PresenceMember] = msg.get("presences") or []
            self._presences.clear()
            for p in presences:
                if isinstance(p, dict):
                    self._presences[str(p.get("client_id", ""))] = p
            self._emit("sync", presences)
        elif kind == "presence_diff":
            joins: List[PresenceMember] = msg.get("joins") or []
            leaves: List[PresenceMember] = msg.get("leaves") or []
            for p in joins:
                if isinstance(p, dict):
                    self._presences[str(p.get("client_id", ""))] = p
            for p in leaves:
                if isinstance(p, dict):
                    self._presences.pop(str(p.get("client_id", "")), None)
            if joins:
                self._emit("join", joins)
            if leaves:
                self._emit("leave", leaves)

    def close(self) -> None:
        self._stop_heartbeat()
        if self._tracked:
            self._tracked = False
            self._send(
                {
                    "type": "presence_untrack",
                    "channel": self._channel,
                    "client_id": self._client_id,
                }
            )

    def _emit(self, event: PresenceEvent, members: List[PresenceMember]) -> None:
        for b in self._bindings:
            if b.event == event:
                b.callback(members)

    def _start_heartbeat(self) -> None:
        self._stop_heartbeat()
        self._heartbeat_task = asyncio.ensure_future(self._heartbeat_loop())

    def _stop_heartbeat(self) -> None:
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            self._heartbeat_task = None

    async def _heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(self._HEARTBEAT_INTERVAL)
            self._send(
                {
                    "type": "heartbeat",
                    "channel": self._channel,
                    "client_id": self._client_id,
                }
            )
