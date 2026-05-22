from __future__ import annotations

"""
Realtime tests — T-030, T-031, T-032, T-033.

SSE and WS tests use mocks/fakes (no live server, no websockets dep required
for SSE tests).  WS tests mock the connection object directly so the
``websockets`` optional dep is not imported.
"""

import asyncio
import json
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest

from basin import create_client, BasinError
from basin.realtime.sse import SseSubscription
from basin.realtime.presence import PresenceChannel
from basin.realtime.channel import RealtimeChannel, RealtimeClient

BASE = "http://test.basin.run"
KEY = "test-key"


# ── RealtimeClient.channel ─────────────────────────────────────────────────────

def test_channel_returns_realtime_channel() -> None:
    client = create_client(BASE, KEY)
    ch = client.channel("orders")
    assert isinstance(ch, RealtimeChannel)
    assert ch.topic == "orders"


def test_channel_empty_topic_raises() -> None:
    client = create_client(BASE, KEY)
    with pytest.raises(BasinError) as exc_info:
        client.channel("")
    assert exc_info.value.code == "invalid_request"


def test_realtime_client_enabled() -> None:
    client = create_client(BASE, KEY)
    assert client.realtime.enabled is True


def test_channel_extract_project_from_url() -> None:
    from basin.realtime.channel import RealtimeClient
    rt = RealtimeClient(url="http://acme.basin.run", headers={})
    assert rt._extract_project() == "acme"


def test_channel_extract_project_localhost() -> None:
    from basin.realtime.channel import RealtimeClient
    rt = RealtimeClient(url="http://localhost:5434", headers={})
    assert rt._extract_project() == "localhost"


# ── RealtimeChannel transport selection ────────────────────────────────────────

def test_single_pg_binding_no_filter_chooses_sse() -> None:
    """One postgres_changes binding, no filter → SSE transport."""
    events: List[Dict[str, Any]] = []

    with patch.object(SseSubscription, "start", return_value=None):
        ch = RealtimeChannel(
            "orders",
            url=BASE,
            project="proj",
            headers={"Authorization": "Bearer tok"},
        )
        ch.on("postgres_changes", {"event": "INSERT", "table": "orders"}, events.append)
        ch.subscribe()
        assert ch._transport == "sse"
        ch.unsubscribe()


def _make_ws_mock() -> MagicMock:
    loop = asyncio.new_event_loop()
    fut: asyncio.Future[None] = loop.create_future()
    fut.set_result(None)
    ws_mock = MagicMock()
    ws_mock.connect = MagicMock()
    ws_mock.subscribe = MagicMock(return_value=fut)
    ws_mock.send = MagicMock()
    ws_mock.register_presence = MagicMock()
    ws_mock.close = MagicMock()
    loop.close()
    return ws_mock


def test_multiple_pg_bindings_chooses_ws() -> None:
    """Multiple postgres_changes → WS transport."""
    events: List[Any] = []

    with patch("basin.realtime.channel.WsConnection", return_value=_make_ws_mock()):
        ch = RealtimeChannel(
            "multi",
            url=BASE,
            project="proj",
            headers={"Authorization": "Bearer tok"},
        )
        ch.on("postgres_changes", {"event": "*", "table": "t1"}, events.append)
        ch.on("postgres_changes", {"event": "*", "table": "t2"}, events.append)
        ch.subscribe()
        assert ch._transport == "ws"
        ch.unsubscribe()


def test_filter_string_chooses_ws() -> None:
    """postgres_changes with filter= → WS transport."""
    events: List[Any] = []

    with patch("basin.realtime.channel.WsConnection", return_value=_make_ws_mock()):
        ch = RealtimeChannel(
            "filtered",
            url=BASE,
            project="proj",
            headers={"Authorization": "Bearer tok"},
        )
        ch.on(
            "postgres_changes",
            {"event": "*", "table": "orders", "filter": "id=eq.42"},
            events.append,
        )
        ch.subscribe()
        assert ch._transport == "ws"
        ch.unsubscribe()


def test_presence_binding_chooses_ws() -> None:
    """Presence binding → WS transport."""
    events: List[Any] = []

    with patch("basin.realtime.channel.WsConnection", return_value=_make_ws_mock()):
        ch = RealtimeChannel(
            "room1",
            url=BASE,
            project="proj",
            headers={"Authorization": "Bearer tok"},
        )
        ch.on("presence", {"event": "sync"}, events.append)
        ch.subscribe()
        assert ch._transport == "ws"
        ch.unsubscribe()


def test_no_bindings_is_none_transport() -> None:
    ch = RealtimeChannel(
        "empty",
        url=BASE,
        project="proj",
        headers={},
    )
    assert ch._transport == "none"


# ── SSE subscription unit tests ────────────────────────────────────────────────

def test_sse_process_line_dispatches_event() -> None:
    received: List[Any] = []
    sub = SseSubscription(BASE, "proj", "orders", "tok", received.append)
    sub._process_line('data: {"op":"INSERT","table":"orders","after":{"id":1},"seq":1}')
    assert len(received) == 1
    assert received[0]["op"] == "INSERT"
    assert sub._last_seq == 1


def test_sse_process_line_ignores_comments() -> None:
    received: List[Any] = []
    sub = SseSubscription(BASE, "proj", "orders", "tok", received.append)
    sub._process_line(": heartbeat")
    assert received == []


def test_sse_process_line_ignores_blank() -> None:
    received: List[Any] = []
    sub = SseSubscription(BASE, "proj", "orders", "tok", received.append)
    sub._process_line("")
    assert received == []


def test_sse_process_line_invalid_json_raises() -> None:
    received: List[Any] = []
    sub = SseSubscription(BASE, "proj", "orders", "tok", received.append)
    with pytest.raises(BasinError) as exc_info:
        sub._process_line("data: {not-json}")
    assert exc_info.value.code == "invalid_response"


def test_sse_last_seq_updated() -> None:
    received: List[Any] = []
    sub = SseSubscription(BASE, "proj", "t", "tok", received.append)
    sub._process_line('data: {"op":"INSERT","table":"t","after":{},"seq":5}')
    assert sub._last_seq == 5


# ── Presence unit tests ────────────────────────────────────────────────────────

def test_presence_state_initially_empty() -> None:
    sent: List[Any] = []
    pc = PresenceChannel("room1", "cid1", sent.append)
    assert pc.presence_state() == []


def test_presence_track_sends_frame() -> None:
    sent: List[Any] = []
    pc = PresenceChannel("room1", "cid1", sent.append)
    pc._stop_heartbeat()  # prevent asyncio task in sync context
    with patch.object(pc, "_start_heartbeat", return_value=None):
        pc.track({"name": "Alice"})
    assert sent[0]["type"] == "presence_track"
    assert sent[0]["metadata"] == {"name": "Alice"}
    assert sent[0]["channel"] == "room1"


def test_presence_untrack_sends_frame() -> None:
    sent: List[Any] = []
    pc = PresenceChannel("room1", "cid1", sent.append)
    with patch.object(pc, "_start_heartbeat", return_value=None):
        with patch.object(pc, "_stop_heartbeat", return_value=None):
            pc.track(None)
            pc.untrack()
    untrack_frames = [f for f in sent if f.get("type") == "presence_untrack"]
    assert len(untrack_frames) == 1


def test_presence_handle_state_message() -> None:
    sent: List[Any] = []
    received_sync: List[Any] = []
    pc = PresenceChannel("room1", "cid1", sent.append)
    pc.on("presence", {"event": "sync"}, received_sync.append)
    pc.handle_message(
        {
            "type": "presence_state",
            "channel": "room1",
            "presences": [{"client_id": "cid2", "metadata": {"name": "Bob"}}],
        }
    )
    assert len(received_sync) == 1
    state = pc.presence_state()
    assert len(state) == 1
    assert state[0]["client_id"] == "cid2"


def test_presence_handle_diff_message() -> None:
    sent: List[Any] = []
    joined: List[Any] = []
    left: List[Any] = []
    pc = PresenceChannel("room1", "cid1", sent.append)
    pc.on("presence", {"event": "join"}, joined.append)
    pc.on("presence", {"event": "leave"}, left.append)

    pc.handle_message(
        {
            "type": "presence_diff",
            "channel": "room1",
            "joins": [{"client_id": "cid2", "metadata": {}}],
            "leaves": [],
        }
    )
    assert len(joined) == 1

    pc.handle_message(
        {
            "type": "presence_diff",
            "channel": "room1",
            "joins": [],
            "leaves": [{"client_id": "cid2", "metadata": {}}],
        }
    )
    assert len(left) == 1
    assert pc.presence_state() == []


def test_presence_ignores_wrong_channel() -> None:
    sent: List[Any] = []
    received: List[Any] = []
    pc = PresenceChannel("room1", "cid1", sent.append)
    pc.on("presence", {"event": "sync"}, received.append)
    pc.handle_message(
        {
            "type": "presence_state",
            "channel": "room2",
            "presences": [{"client_id": "x", "metadata": {}}],
        }
    )
    assert received == []


# ── SSE event dispatch through channel ────────────────────────────────────────

def test_sse_event_dispatched_to_channel_callback() -> None:
    events: List[Dict[str, Any]] = []

    with patch.object(SseSubscription, "start", return_value=None):
        ch = RealtimeChannel(
            "orders",
            url=BASE,
            project="proj",
            headers={"Authorization": "Bearer tok"},
        )
        ch.on("postgres_changes", {"event": "INSERT", "table": "orders"}, events.append)
        ch.subscribe()

        assert ch._sse_sub is not None
        ch._sse_sub._process_line(
            'data: {"op":"INSERT","table":"orders","after":{"id":1},"seq":1}'
        )
        assert len(events) == 1
        assert events[0]["eventType"] == "INSERT"
        assert events[0]["new"] == {"id": 1}
        ch.unsubscribe()


def test_sse_event_filtered_by_event_type() -> None:
    events: List[Dict[str, Any]] = []

    with patch.object(SseSubscription, "start", return_value=None):
        ch = RealtimeChannel(
            "orders",
            url=BASE,
            project="proj",
            headers={"Authorization": "Bearer tok"},
        )
        ch.on("postgres_changes", {"event": "INSERT", "table": "orders"}, events.append)
        ch.subscribe()

        assert ch._sse_sub is not None
        ch._sse_sub._process_line(
            'data: {"op":"DELETE","table":"orders","after":{},"seq":2}'
        )
        assert events == []
        ch.unsubscribe()


# ── subscribe idempotence ──────────────────────────────────────────────────────

def test_subscribe_is_idempotent() -> None:
    with patch.object(SseSubscription, "start", return_value=None):
        ch = RealtimeChannel(
            "orders",
            url=BASE,
            project="proj",
            headers={"Authorization": "Bearer tok"},
        )
        ch.on("postgres_changes", {"event": "*", "table": "orders"}, lambda e: None)
        ch.subscribe()
        first_sub = ch._sse_sub
        ch.subscribe()
        assert ch._sse_sub is first_sub
        ch.unsubscribe()
