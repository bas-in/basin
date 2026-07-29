from __future__ import annotations

import http.server
import json
import threading
from typing import Any
from unittest.mock import patch

import httpx
import respx

from basin import create_sync_client
from basin.realtime.sse import SseSubscription

BASE = "http://localhost:5434"
KEY = "basin_anon"

SESSION_BODY = {
    "user": {
        "id": "u1",
        "email": "test@example.com",
        "email_verified": True,
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-01T00:00:00Z",
    },
    "session": {
        "access_token": "access_sync",
        "refresh_token": "refresh_sync",
        "expires_at": "2099-01-01T00:00:00Z",
    },
}


def test_sync_select():
    with respx.mock:
        respx.get(f"{BASE}/rest/v1/items").mock(
            return_value=httpx.Response(200, json=[{"id": 1}])
        )
        with create_sync_client(BASE, KEY) as c:
            result = c.from_("items").select().execute()
    assert result.error is None
    assert result.data == [{"id": 1}]


def test_sync_sign_in():
    with respx.mock:
        respx.post(f"{BASE}/auth/v1/signin").mock(
            return_value=httpx.Response(200, json=SESSION_BODY)
        )
        with create_sync_client(BASE, KEY) as c:
            session, err = c.auth.sign_in_with_password(
                email="a@b.com", password="pw"
            )
    assert err is None
    assert session is not None
    assert session.access_token == "access_sync"


def test_sync_select_error():
    with respx.mock:
        respx.get(f"{BASE}/rest/v1/secret").mock(
            return_value=httpx.Response(403, json={"message": "forbidden"})
        )
        with create_sync_client(BASE, KEY) as c:
            result = c.from_("secret").select().execute()
    assert result.error is not None
    assert result.error.code == "forbidden"


def _ndjson(rows: list[dict[str, Any]], cursor: str | None) -> bytes:
    lines = [json.dumps(r) for r in rows]
    if cursor is not None:
        lines.append(json.dumps({"_basin_next_cursor": cursor}))
    return ("\n".join(lines) + "\n").encode()


def test_sync_paginate_walks_pages():
    pages = [
        httpx.Response(
            200,
            content=_ndjson([{"id": 1}, {"id": 2}], "p2"),
            headers={"content-type": "application/x-ndjson"},
        ),
        httpx.Response(
            200,
            content=_ndjson([{"id": 3}], None),
            headers={"content-type": "application/x-ndjson"},
        ),
    ]
    with respx.mock:
        respx.get(f"{BASE}/rest/v1/events").mock(side_effect=pages)
        with create_sync_client(BASE, KEY) as c:
            rows = list(c.from_("events").select().paginate(page_size=2))
    assert [r["id"] for r in rows] == [1, 2, 3]


def test_sync_paginate_propagates_error():
    import pytest

    from basin import BasinError

    with respx.mock:
        respx.get(f"{BASE}/rest/v1/events").mock(
            return_value=httpx.Response(500, json={"message": "boom"})
        )
        with create_sync_client(BASE, KEY) as c:
            gen = c.from_("events").select().paginate()
            with pytest.raises(BasinError):
                list(gen)


def test_sync_stream_yields_rows():
    body = '{"id":1}\n{"id":2}\n{"id":3}\n{"_basin_next_cursor":"x"}\n'
    with respx.mock:
        respx.get(f"{BASE}/rest/v1/events").mock(
            return_value=httpx.Response(
                200,
                content=body.encode(),
                headers={"content-type": "application/x-ndjson"},
            )
        )
        with create_sync_client(BASE, KEY) as c:
            rows = list(c.from_("events").select().stream())
    assert [r["id"] for r in rows] == [1, 2, 3]


def test_sync_multiple_requests_keepalive_server():
    """Regression: a real keep-alive connection parks in the httpx pool bound to
    the event loop.  If each sync call spun up a fresh ``asyncio.run`` loop, the
    second request would fail with ``RuntimeError: Event loop is closed``.  This
    drives a real HTTP/1.1 keep-alive server (respx's mock transport holds no
    live socket, so it can't catch this)."""

    class _Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"  # keep-alive

        def do_GET(self) -> None:  # noqa: N802
            body = b'[{"id":1}]'
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *a: Any) -> None:
            pass

    srv = http.server.HTTPServer(("127.0.0.1", 0), _Handler)
    port = srv.server_address[1]
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    try:
        with create_sync_client(f"http://127.0.0.1:{port}", KEY) as c:
            for _ in range(3):
                result = c.from_("items").select().execute()
                assert result.error is None
                assert result.data == [{"id": 1}]
    finally:
        srv.shutdown()
        thread.join(timeout=5.0)


def test_sync_channel_lifecycle():
    started: list[bool] = []
    stopped: list[bool] = []
    with (
        patch.object(SseSubscription, "start", lambda self: started.append(True)),
        patch.object(SseSubscription, "stop", lambda self: stopped.append(True)),
        create_sync_client(BASE, KEY) as c,
    ):
        ch = c.channel("orders")
        assert ch.topic == "orders"
        ch.on("postgres_changes", {"event": "*", "table": "orders"}, lambda e: None)
        ch.subscribe()
        assert started == [True]
        ch.unsubscribe()
    assert stopped == [True]
