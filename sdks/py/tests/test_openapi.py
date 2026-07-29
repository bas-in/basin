from __future__ import annotations

import httpx
import pytest
import respx

from basin import fetch_openapi
from basin.errors import BasinError

BASE = "http://localhost:5434"
KEY = "basin_test_anon"
ENDPOINT = f"{BASE}/rest/v1/_openapi.json"

_DOC = {
    "openapi": "3.0.3",
    "info": {"title": "Basin REST API", "version": "0.1.0"},
    "paths": {
        "/rest/v1/users": {
            "get": {
                "responses": {
                    "200": {
                        "description": "OK",
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "required": ["id"],
                                        "properties": {"id": {"type": "integer"}},
                                    },
                                }
                            }
                        },
                    }
                }
            }
        }
    },
}


@pytest.mark.asyncio
async def test_fetch_openapi_parses_doc() -> None:
    with respx.mock:
        route = respx.get(ENDPOINT).mock(return_value=httpx.Response(200, json=_DOC))
        doc = await fetch_openapi(BASE, KEY)
        assert route.called
        assert doc["openapi"] == "3.0.3"
        assert "/rest/v1/users" in doc["paths"]


@pytest.mark.asyncio
async def test_fetch_openapi_sends_apikey_header() -> None:
    with respx.mock:
        route = respx.get(ENDPOINT).mock(return_value=httpx.Response(200, json=_DOC))
        await fetch_openapi(BASE, KEY)
        req = route.calls.last.request
        assert req.headers["apikey"] == KEY
        assert req.headers["accept"] == "application/json"


@pytest.mark.asyncio
async def test_fetch_openapi_trailing_slash_normalised() -> None:
    with respx.mock:
        route = respx.get(ENDPOINT).mock(return_value=httpx.Response(200, json=_DOC))
        await fetch_openapi(BASE + "/", KEY)
        assert route.called


@pytest.mark.asyncio
async def test_fetch_openapi_404_raises_not_found() -> None:
    with respx.mock:
        respx.get(ENDPOINT).mock(return_value=httpx.Response(404, text="nope"))
        with pytest.raises(BasinError) as exc:
            await fetch_openapi(BASE, KEY)
        assert exc.value.code == "not_found"
        assert exc.value.status == 404


@pytest.mark.asyncio
async def test_fetch_openapi_5xx_raises_invalid_response() -> None:
    with respx.mock:
        respx.get(ENDPOINT).mock(return_value=httpx.Response(500, text="boom"))
        with pytest.raises(BasinError) as exc:
            await fetch_openapi(BASE, KEY)
        assert exc.value.code == "invalid_response"


@pytest.mark.asyncio
async def test_fetch_openapi_malformed_json_raises() -> None:
    with respx.mock:
        respx.get(ENDPOINT).mock(
            return_value=httpx.Response(
                200, content=b"not json", headers={"content-type": "application/json"}
            )
        )
        with pytest.raises(BasinError) as exc:
            await fetch_openapi(BASE, KEY)
        assert exc.value.code == "invalid_response"


@pytest.mark.asyncio
async def test_fetch_openapi_transport_error_raises_network() -> None:
    transport = httpx.MockTransport(
        lambda request: (_ for _ in ()).throw(httpx.ConnectError("down"))
    )
    async with httpx.AsyncClient(transport=transport) as client:
        with pytest.raises(BasinError) as exc:
            await fetch_openapi(BASE, KEY, client=client)
        assert exc.value.code == "network"
