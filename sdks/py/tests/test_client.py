from __future__ import annotations

import httpx
import pytest
import respx

from basin import create_client
from basin.client import ClientOptions

BASE = "http://localhost:5434"
ANON_KEY = "basin_test_anon"


def make_client(transport: httpx.AsyncBaseTransport | None = None):
    opts = ClientOptions(transport=transport) if transport else None
    return create_client(BASE, ANON_KEY, options=opts)


@pytest.mark.asyncio
async def test_headers_present():
    with respx.mock:
        route = respx.get(f"{BASE}/rest/v1/users").mock(
            return_value=httpx.Response(200, json=[{"id": 1}])
        )
        client = make_client()
        await client.from_("users").select()
        assert route.called
        req = route.calls.last.request
        assert req.headers["apikey"] == ANON_KEY
        assert req.headers["authorization"].startswith("Bearer ")
        await client.aclose()


@pytest.mark.asyncio
async def test_200_json_parsed():
    with respx.mock:
        respx.get(f"{BASE}/rest/v1/items").mock(
            return_value=httpx.Response(200, json=[{"id": 1, "name": "foo"}])
        )
        client = make_client()
        result = await client.from_("items").select()
        assert result.error is None
        assert result.data == [{"id": 1, "name": "foo"}]
        await client.aclose()


@pytest.mark.asyncio
async def test_204_returns_none_data():
    with respx.mock:
        respx.post(f"{BASE}/rest/v1/items").mock(
            return_value=httpx.Response(204)
        )
        client = make_client()
        result = await client.from_("items").insert({"name": "x"}, returning="minimal")
        assert result.error is None
        assert result.data is None
        await client.aclose()


@pytest.mark.asyncio
async def test_4xx_typed_error():
    with respx.mock:
        respx.get(f"{BASE}/rest/v1/secret").mock(
            return_value=httpx.Response(403, json={"message": "forbidden"})
        )
        client = make_client()
        result = await client.from_("secret").select()
        assert result.error is not None
        assert result.error.code == "forbidden"
        assert result.error.status == 403
        await client.aclose()


@pytest.mark.asyncio
async def test_network_error_wrapped():
    transport = httpx.MockTransport(
        lambda request: (_ for _ in ()).throw(httpx.ConnectError("connection refused"))
    )
    client = make_client(transport)
    result = await client.from_("items").select()
    assert result.error is not None
    assert result.error.code == "network"
    await client.aclose()


@pytest.mark.asyncio
async def test_async_context_manager():
    with respx.mock:
        respx.get(f"{BASE}/rest/v1/x").mock(
            return_value=httpx.Response(200, json=[])
        )
        async with create_client(BASE, ANON_KEY) as client:
            result = await client.from_("x").select()
            assert result.error is None
