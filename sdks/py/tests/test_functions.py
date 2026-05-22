from __future__ import annotations

import json as jsonlib

import pytest
import respx
import httpx

from basin import create_client, BasinError

BASE = "http://test.basin.run"
KEY = "test-key"


@pytest.mark.asyncio
async def test_invoke_posts_to_rpc_route() -> None:
    with respx.mock(base_url=BASE) as mock:
        route = mock.post("/rest/v1/rpc/my_fn").mock(
            return_value=httpx.Response(200, json={"result": 42})
        )
        client = create_client(BASE, KEY)
        data = await client.functions.invoke("my_fn", body={"x": 1})
        assert route.called
        assert data == {"result": 42}
        req = route.calls[0].request
        body = jsonlib.loads(req.content)
        assert body == {"x": 1}
        await client.aclose()


@pytest.mark.asyncio
async def test_invoke_empty_body_sends_empty_object() -> None:
    with respx.mock(base_url=BASE) as mock:
        route = mock.post("/rest/v1/rpc/fn2").mock(
            return_value=httpx.Response(200, json={})
        )
        client = create_client(BASE, KEY)
        await client.functions.invoke("fn2")
        req = route.calls[0].request
        body = jsonlib.loads(req.content)
        assert body == {}
        await client.aclose()


@pytest.mark.asyncio
async def test_invoke_empty_name_raises_invalid_request() -> None:
    client = create_client(BASE, KEY)
    with pytest.raises(BasinError) as exc_info:
        await client.functions.invoke("")
    assert exc_info.value.code == "invalid_request"
    await client.aclose()


@pytest.mark.asyncio
async def test_invoke_401_raises_unauthorized() -> None:
    with respx.mock(base_url=BASE) as mock:
        mock.post("/rest/v1/rpc/fn3").mock(
            return_value=httpx.Response(401)
        )
        client = create_client(BASE, KEY)
        with pytest.raises(BasinError) as exc_info:
            await client.functions.invoke("fn3")
        assert exc_info.value.code == "unauthorized"
        await client.aclose()


@pytest.mark.asyncio
async def test_invoke_500_raises_internal() -> None:
    with respx.mock(base_url=BASE) as mock:
        mock.post("/rest/v1/rpc/fn4").mock(
            return_value=httpx.Response(500, json={"error": "boom"})
        )
        client = create_client(BASE, KEY)
        with pytest.raises(BasinError) as exc_info:
            await client.functions.invoke("fn4")
        assert exc_info.value.code == "internal"
        await client.aclose()


@pytest.mark.asyncio
async def test_invoke_extra_headers_merged() -> None:
    with respx.mock(base_url=BASE) as mock:
        route = mock.post("/rest/v1/rpc/fn5").mock(
            return_value=httpx.Response(200, json={"ok": True})
        )
        client = create_client(BASE, KEY)
        await client.functions.invoke("fn5", headers={"X-Custom": "value"})
        req = route.calls[0].request
        assert req.headers.get("x-custom") == "value"
        await client.aclose()


@pytest.mark.asyncio
async def test_invoke_non_json_response_raises_invalid_response() -> None:
    with respx.mock(base_url=BASE) as mock:
        mock.post("/rest/v1/rpc/fn6").mock(
            return_value=httpx.Response(200, content=b"not-json")
        )
        client = create_client(BASE, KEY)
        with pytest.raises(BasinError) as exc_info:
            await client.functions.invoke("fn6")
        assert exc_info.value.code == "invalid_response"
        await client.aclose()
