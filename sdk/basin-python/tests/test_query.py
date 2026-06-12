"""Tests for QueryBuilder — offline, respx-mocked transport."""

import json

import httpx
import pytest
import respx

from basin.client import create_client, create_async_client
from basin.errors import BasinApiError
from basin.query import QueryResult, _literal, _normalize_get

BASE = "http://basin.test"
KEY = "testapikey"


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
# Arrow conversion (client-side)
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
async def test_async_to_arrow():
    respx.get(f"{BASE}/rest/v1/orders").mock(
        return_value=httpx.Response(200, json=[{"id": 1}])
    )
    async with create_async_client(BASE, KEY) as client:
        try:
            table = await client.table("orders").to_arrow()
            assert table.num_rows == 1
        except ImportError:
            pytest.skip("pyarrow not installed")
