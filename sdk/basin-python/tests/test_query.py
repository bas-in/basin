"""Tests for QueryBuilder — offline, respx-mocked transport."""

import io
import json
import struct

import httpx
import pytest
import respx

from basin.client import create_client, create_async_client
from basin.errors import BasinApiError
from basin.query import QueryResult, _literal, _normalize_get, ARROW_STREAM_MIME

BASE = "http://basin.test"
KEY = "testapikey"


# ---------------------------------------------------------------------------
# Arrow IPC fixture helpers
# ---------------------------------------------------------------------------


def _make_arrow_ipc_bytes(schema_names: list, schema_types, columns: list) -> bytes:
    """Construct a minimal Arrow IPC stream in pure Python (no pyarrow needed at
    definition time; pyarrow is used at runtime when the test is invoked, and
    the test itself skips if pyarrow is not available).

    Returns raw bytes of an Arrow IPC streaming-format payload containing one
    RecordBatch.
    """
    try:
        import pyarrow as pa
        import pyarrow.ipc as pa_ipc
    except ImportError:
        pytest.skip("pyarrow not installed")

    fields = [pa.field(name, dtype) for name, dtype in zip(schema_names, schema_types)]
    schema = pa.schema(fields)
    arrays = [pa.array(col, type=dtype) for col, dtype in zip(columns, schema_types)]
    batch = pa.record_batch(arrays, schema=schema)
    sink = pa.BufferOutputStream()
    writer = pa_ipc.new_stream(sink, schema)
    writer.write_batch(batch)
    writer.close()
    return sink.getvalue().to_pybytes()


def _make_simple_ipc() -> bytes:
    """Arrow IPC stream with id: int64, name: utf8 — two rows."""
    try:
        import pyarrow as pa
    except ImportError:
        pytest.skip("pyarrow not installed")
    return _make_arrow_ipc_bytes(
        ["id", "name"],
        [pa.int64(), pa.utf8()],
        [[1, 2], ["alice", "bob"]],
    )


def _make_i64_extremes_ipc() -> bytes:
    """Arrow IPC stream with a single int64 column containing MIN/MAX values."""
    try:
        import pyarrow as pa
    except ImportError:
        pytest.skip("pyarrow not installed")
    import sys
    i64_min = -(2**63)
    i64_max = 2**63 - 1
    return _make_arrow_ipc_bytes(
        ["v"],
        [pa.int64()],
        [[i64_min, -1, 0, 1, i64_max]],
    )


def _make_timestamp_ipc() -> bytes:
    """Arrow IPC stream with a TimestampMicrosecond column."""
    try:
        import pyarrow as pa
    except ImportError:
        pytest.skip("pyarrow not installed")
    return _make_arrow_ipc_bytes(
        ["ts"],
        [pa.timestamp("us", tz="UTC")],
        [[0, -1_000_000, 32_503_680_000_000_000]],
    )


# ---------------------------------------------------------------------------
# Unit: helpers
# ---------------------------------------------------------------------------


def test_literal_null():
    assert _literal(None) == "null"


def test_literal_bool():
    assert _literal(True) == "true"
    assert _literal(False) == "false"


def test_literal_int():
    assert _literal(42) == "42"


def test_literal_float():
    assert _literal(3.14) == "3.14"


def test_literal_string():
    assert _literal("hello") == "hello"


def test_normalize_get_array():
    r = _normalize_get([{"id": 1}, {"id": 2}])
    assert len(r.rows) == 2
    assert r.next_cursor is None


def test_normalize_get_paginated():
    r = _normalize_get({"rows": [{"id": 1}], "next_cursor": "tok123"})
    assert r.rows == [{"id": 1}]
    assert r.next_cursor == "tok123"


def test_normalize_get_exec_tag():
    r = _normalize_get({"ok": True, "tag": "INSERT 0 1"})
    assert r.rows == []
    assert r.next_cursor is None


def test_normalize_get_null():
    r = _normalize_get(None)
    assert r.rows == []


# ---------------------------------------------------------------------------
# QueryBuilder filter syntax
# ---------------------------------------------------------------------------

def _client():
    return create_client(BASE, KEY)


@respx.mock
def test_select_eq_filter_url():
    route = respx.get(f"{BASE}/rest/v1/orders").mock(
        return_value=httpx.Response(200, json=[{"id": 1, "total": 200}])
    )
    client = _client()
    result = client.table("orders").select("id,total").eq("status", "paid").run()
    assert len(result.rows) == 1
    request = route.calls.last.request
    assert "select=id%2Ctotal" in str(request.url) or "select=id,total" in str(request.url)
    assert "status=eq.paid" in str(request.url)


@respx.mock
def test_gte_filter():
    respx.get(f"{BASE}/rest/v1/orders").mock(
        return_value=httpx.Response(200, json=[{"id": 1}])
    )
    client = _client()
    result = client.table("orders").gte("total", 100).run()
    assert len(result.rows) == 1


@respx.mock
def test_in_filter():
    route = respx.get(f"{BASE}/rest/v1/orders").mock(
        return_value=httpx.Response(200, json=[])
    )
    client = _client()
    client.table("orders").in_("status", ["paid", "pending"]).run()
    url = str(route.calls.last.request.url)
    assert "in." in url
    assert "paid" in url
    assert "pending" in url


@respx.mock
def test_is_null_filter():
    route = respx.get(f"{BASE}/rest/v1/orders").mock(
        return_value=httpx.Response(200, json=[])
    )
    client = _client()
    client.table("orders").is_("deleted_at", "null").run()
    url = str(route.calls.last.request.url)
    assert "is.null" in url


@respx.mock
def test_order_limit_offset():
    route = respx.get(f"{BASE}/rest/v1/orders").mock(
        return_value=httpx.Response(200, json={"rows": [], "next_cursor": None})
    )
    client = _client()
    result = client.table("orders").order("total", ascending=False).limit(10).offset(20).run()
    url = str(route.calls.last.request.url)
    assert "order=total.desc" in url
    assert "limit=10" in url
    assert "offset=20" in url
    assert result.next_cursor is None


@respx.mock
def test_cursor_pagination():
    route = respx.get(f"{BASE}/rest/v1/orders").mock(
        return_value=httpx.Response(
            200, json={"rows": [{"id": 2}], "next_cursor": "tok456"}
        )
    )
    client = _client()
    result = client.table("orders").cursor("tok123").run()
    url = str(route.calls.last.request.url)
    assert "cursor=tok123" in url
    assert result.next_cursor == "tok456"


@respx.mock
def test_stream_ndjson():
    ndjson = (
        '{"id":1,"val":"a"}\n'
        '{"id":2,"val":"b"}\n'
        '{"_basin_next_cursor":"tok789"}\n'
    )
    respx.get(f"{BASE}/rest/v1/events").mock(
        return_value=httpx.Response(200, text=ndjson)
    )
    client = _client()
    gen = client.table("events").stream()
    rows = list(gen)
    assert len(rows) == 2
    assert rows[0]["id"] == 1
    assert rows[1]["val"] == "b"


@respx.mock
def test_insert():
    respx.post(f"{BASE}/rest/v1/orders").mock(
        return_value=httpx.Response(201, json={"ok": True, "tag": "INSERT 0 1"})
    )
    client = _client()
    result = client.table("orders").insert({"total": 50, "status": "new"})
    assert result["ok"] is True


@respx.mock
def test_update():
    respx.patch(f"{BASE}/rest/v1/orders").mock(
        return_value=httpx.Response(200, json={"ok": True, "tag": "UPDATE 1"})
    )
    client = _client()
    result = client.table("orders").eq("id", 1).update({"status": "paid"})
    assert result["ok"] is True


@respx.mock
def test_delete():
    respx.delete(f"{BASE}/rest/v1/orders").mock(
        return_value=httpx.Response(200, json={"ok": True, "tag": "DELETE 1"})
    )
    client = _client()
    result = client.table("orders").eq("id", 1).delete()
    assert result["ok"] is True


@respx.mock
def test_delete_engine_unsupported():
    respx.delete(f"{BASE}/rest/v1/orders").mock(
        return_value=httpx.Response(
            501,
            json={"code": "E_ENGINE_UNSUPPORTED", "message": "DELETE not supported"},
        )
    )
    client = _client()
    with pytest.raises(BasinApiError) as exc_info:
        client.table("orders").delete()
    assert exc_info.value.code == "E_ENGINE_UNSUPPORTED"
    assert exc_info.value.status == 501


# ---------------------------------------------------------------------------
# Arrow conversion (client-side, legacy path)
# ---------------------------------------------------------------------------


def test_query_result_to_arrow():
    result = QueryResult(
        rows=[{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
        next_cursor=None,
    )
    try:
        table = result.to_arrow()
        assert table.num_rows == 2
        assert "id" in table.schema.names
    except ImportError:
        pytest.skip("pyarrow not installed")


def test_query_result_to_arrow_empty():
    result = QueryResult(rows=[], next_cursor=None)
    try:
        table = result.to_arrow()
        assert table.num_rows == 0
    except ImportError:
        pytest.skip("pyarrow not installed")


@respx.mock
def test_builder_to_arrow():
    respx.get(f"{BASE}/rest/v1/orders").mock(
        return_value=httpx.Response(
            200, json=[{"id": 1, "total": 100}, {"id": 2, "total": 200}]
        )
    )
    client = _client()
    try:
        table = client.table("orders").to_arrow()
        assert table.num_rows == 2
    except ImportError:
        pytest.skip("pyarrow not installed")


# ---------------------------------------------------------------------------
# Arrow IPC transport — native path
# ---------------------------------------------------------------------------


@respx.mock
def test_to_arrow_native_ipc():
    """to_arrow() decodes an Arrow IPC response natively when the server
    responds with Content-Type: application/vnd.apache.arrow.stream."""
    try:
        ipc_bytes = _make_simple_ipc()
    except Exception:
        pytest.skip("pyarrow not installed or IPC fixture failed")

    route = respx.get(f"{BASE}/rest/v1/orders").mock(
        return_value=httpx.Response(
            200,
            content=ipc_bytes,
            headers={"content-type": ARROW_STREAM_MIME},
        )
    )
    client = _client()
    table = client.table("orders").to_arrow()

    assert table.num_rows == 2
    assert "id" in table.schema.names
    assert "name" in table.schema.names
    # Schema is preserved: id is int64, not inferred-string.
    import pyarrow as pa
    assert table.schema.field("id").type == pa.int64()

    # Verify the Accept header was sent.
    req = route.calls.last.request
    assert ARROW_STREAM_MIME in req.headers.get("accept", "")


@respx.mock
def test_to_arrow_i64_extremes_no_precision_loss():
    """i64::MIN and i64::MAX round-trip exactly through the IPC path."""
    try:
        ipc_bytes = _make_i64_extremes_ipc()
        import pyarrow as pa
    except Exception:
        pytest.skip("pyarrow not installed")

    respx.get(f"{BASE}/rest/v1/extremes").mock(
        return_value=httpx.Response(
            200,
            content=ipc_bytes,
            headers={"content-type": ARROW_STREAM_MIME},
        )
    )
    client = _client()
    table = client.table("extremes").to_arrow()

    i64_min = -(2**63)
    i64_max = 2**63 - 1
    vals = table.column("v").to_pylist()
    assert vals == [i64_min, -1, 0, 1, i64_max], f"precision loss: {vals}"


@respx.mock
def test_to_arrow_timestamp_fidelity():
    """Microsecond timestamps survive the IPC path without RFC3339 string losses."""
    try:
        ipc_bytes = _make_timestamp_ipc()
        import pyarrow as pa
    except Exception:
        pytest.skip("pyarrow not installed")

    respx.get(f"{BASE}/rest/v1/ts_table").mock(
        return_value=httpx.Response(
            200,
            content=ipc_bytes,
            headers={"content-type": ARROW_STREAM_MIME},
        )
    )
    client = _client()
    table = client.table("ts_table").to_arrow()
    assert table.num_rows == 3
    # Schema preserves timestamp type.
    ts_type = table.schema.field("ts").type
    assert pa.types.is_timestamp(ts_type), f"expected timestamp, got {ts_type}"
    # Values are integers (microseconds since epoch) — not strings.
    raw = table.column("ts").cast(pa.int64()).to_pylist()
    assert raw[0] == 0
    assert raw[1] == -1_000_000
    assert raw[2] == 32_503_680_000_000_000


@respx.mock
def test_to_arrow_fallback_to_json_when_server_returns_json():
    """When the server ignores the Accept header and returns JSON, to_arrow()
    falls back to client-side JSON→Arrow conversion transparently."""
    respx.get(f"{BASE}/rest/v1/orders").mock(
        return_value=httpx.Response(
            200,
            json=[{"id": 1, "name": "x"}],
            headers={"content-type": "application/json"},
        )
    )
    client = _client()
    try:
        table = client.table("orders").to_arrow()
        assert table.num_rows == 1
    except ImportError:
        pytest.skip("pyarrow not installed")


@respx.mock
def test_to_arrow_pagination_cursor_in_metadata():
    """x-basin-next-cursor from the response header is attached as table metadata."""
    try:
        ipc_bytes = _make_simple_ipc()
    except Exception:
        pytestx.skip("pyarrow not installed")

    respx.get(f"{BASE}/rest/v1/orders").mock(
        return_value=httpx.Response(
            200,
            content=ipc_bytes,
            headers={
                "content-type": ARROW_STREAM_MIME,
                "x-basin-next-cursor": "tok_abc",
                "x-basin-row-count": "2",
            },
        )
    )
    client = _client()
    table = client.table("orders").to_arrow()
    metadata = table.schema.metadata or {}
    assert b"x-basin-next-cursor" in metadata
    assert metadata[b"x-basin-next-cursor"] == b"tok_abc"


@respx.mock
def test_to_arrow_sends_accept_header():
    """to_arrow() must send Accept: application/vnd.apache.arrow.stream."""
    try:
        import pyarrow  # noqa: F401
    except ImportError:
        pytest.skip("pyarrow not installed")

    route = respx.get(f"{BASE}/rest/v1/orders").mock(
        return_value=httpx.Response(200, json=[])
    )
    client = _client()
    client.table("orders").to_arrow()
    req = route.calls.last.request
    assert ARROW_STREAM_MIME in req.headers.get("accept", ""), (
        "to_arrow() must send Accept: application/vnd.apache.arrow.stream"
    )


# ---------------------------------------------------------------------------
# Async variants
# ---------------------------------------------------------------------------


@respx.mock
async def test_async_query_run():
    respx.get(f"{BASE}/rest/v1/orders").mock(
        return_value=httpx.Response(200, json=[{"id": 1}])
    )
    async with create_async_client(BASE, KEY) as client:
        result = await client.table("orders").eq("id", 1).run()
        assert len(result.rows) == 1


@respx.mock
async def test_async_builder_await():
    """AsyncQueryBuilder is directly awaitable as a shorthand for .run()."""
    respx.get(f"{BASE}/rest/v1/orders").mock(
        return_value=httpx.Response(200, json=[{"id": 7}])
    )
    async with create_async_client(BASE, KEY) as client:
        result = await client.table("orders")
        assert isinstance(result, QueryResult)


@respx.mock
async def test_async_insert():
    respx.post(f"{BASE}/rest/v1/orders").mock(
        return_value=httpx.Response(201, json={"ok": True, "tag": "INSERT 0 1"})
    )
    async with create_async_client(BASE, KEY) as client:
        result = await client.table("orders").insert({"total": 99})
        assert result["ok"] is True


@respx.mock
async def test_async_to_arrow_native_ipc():
    """Async to_arrow() decodes native IPC when the server sends it."""
    try:
        ipc_bytes = _make_simple_ipc()
    except Exception:
        pytest.skip("pyarrow not installed")

    respx.get(f"{BASE}/rest/v1/orders").mock(
        return_value=httpx.Response(
            200,
            content=ipc_bytes,
            headers={"content-type": ARROW_STREAM_MIME},
        )
    )
    async with create_async_client(BASE, KEY) as client:
        table = await client.table("orders").to_arrow()
        assert table.num_rows == 2
        import pyarrow as pa
        assert table.schema.field("id").type == pa.int64()


@respx.mock
async def test_async_to_arrow():
    """Async to_arrow() falls back to JSON→Arrow when server returns JSON."""
    respx.get(f"{BASE}/rest/v1/orders").mock(
        return_value=httpx.Response(200, json=[{"id": 1}])
    )
    async with create_async_client(BASE, KEY) as client:
        try:
            table = await client.table("orders").to_arrow()
            assert table.num_rows == 1
        except ImportError:
            pytest.skip("pyarrow not installed")
