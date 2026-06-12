"""Tests for FunctionsClient — offline, respx-mocked transport."""

import httpx
import pytest
import respx

from basin.client import create_client, create_async_client
from basin.errors import BasinApiError

BASE = "http://basin.test"
KEY = "testapikey"


@respx.mock
def test_rpc_scalar():
    respx.post(f"{BASE}/rest/v1/rpc/add").mock(
        return_value=httpx.Response(200, json=42)
    )
    client = create_client(BASE, KEY)
    result = client.rpc("add", {"a": 40, "b": 2})
    assert result == 42


@respx.mock
def test_rpc_table_result():
    respx.post(f"{BASE}/rest/v1/rpc/top_orders").mock(
        return_value=httpx.Response(
            200, json=[{"id": 1, "total": 999}, {"id": 2, "total": 800}]
        )
    )
    client = create_client(BASE, KEY)
    result = client.functions.rpc("top_orders")
    assert len(result) == 2
    assert result[0]["total"] == 999


@respx.mock
def test_invoke_success():
    respx.post(f"{BASE}/fn/v1/resize").mock(
        return_value=httpx.Response(200, json={"width": 100, "height": 50})
    )
    client = create_client(BASE, KEY)
    result = client.functions.invoke("resize", body={"width": 100})
    assert result.status == 200
    assert result.data["width"] == 100


@respx.mock
def test_invoke_non_2xx_non_envelope_not_raised():
    """A function returning 418 with non-envelope body is not a BasinApiError."""
    respx.post(f"{BASE}/fn/v1/teapot").mock(
        return_value=httpx.Response(418, text="I'm a teapot")
    )
    client = create_client(BASE, KEY)
    result = client.functions.invoke("teapot")
    assert result.status == 418
    assert "teapot" in result.data.lower()


@respx.mock
def test_invoke_basin_error_envelope_raises():
    """Basin-layer 401 (unknown function, auth failure) should raise."""
    respx.post(f"{BASE}/fn/v1/ghost").mock(
        return_value=httpx.Response(
            404, json={"code": "E_NOT_FOUND", "message": "function not found"}
        )
    )
    client = create_client(BASE, KEY)
    with pytest.raises(BasinApiError) as exc_info:
        client.functions.invoke("ghost")
    assert exc_info.value.code == "E_NOT_FOUND"
    assert exc_info.value.status == 404


@respx.mock
def test_invoke_custom_method():
    respx.get(f"{BASE}/fn/v1/status").mock(
        return_value=httpx.Response(200, json={"running": True})
    )
    client = create_client(BASE, KEY)
    result = client.functions.invoke("status", method="GET")
    assert result.data["running"] is True


# ---------------------------------------------------------------------------
# Async variants
# ---------------------------------------------------------------------------


@respx.mock
async def test_async_rpc():
    respx.post(f"{BASE}/rest/v1/rpc/add").mock(
        return_value=httpx.Response(200, json=10)
    )
    async with create_async_client(BASE, KEY) as client:
        result = await client.rpc("add", {"a": 3, "b": 7})
        assert result == 10


@respx.mock
async def test_async_invoke():
    respx.post(f"{BASE}/fn/v1/process").mock(
        return_value=httpx.Response(200, json={"done": True})
    )
    async with create_async_client(BASE, KEY) as client:
        result = await client.functions.invoke("process")
        assert result.status == 200
        assert result.data["done"] is True
