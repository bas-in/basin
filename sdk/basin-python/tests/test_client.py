"""Tests for top-level BasinClient / AsyncBasinClient."""

import httpx
import pytest
import respx

from basin.client import create_client, create_async_client, BasinClient, AsyncBasinClient
from basin.auth import _project_id_from_jwt
import base64
import json

BASE = "http://basin.test"
KEY = "testapikey"


def _make_jwt(project_id: str) -> str:
    """Build a minimal fake JWT with a project_id claim."""
    header = base64.urlsafe_b64encode(b'{"alg":"HS256"}').rstrip(b"=").decode()
    payload_bytes = json.dumps({"project_id": project_id, "exp": 9999999999}).encode()
    payload = base64.urlsafe_b64encode(payload_bytes).rstrip(b"=").decode()
    sig = base64.urlsafe_b64encode(b"fakesig").rstrip(b"=").decode()
    return f"{header}.{payload}.{sig}"


def test_create_client_returns_instance():
    client = create_client(BASE, KEY)
    assert isinstance(client, BasinClient)


def test_create_async_client_returns_instance():
    client = create_async_client(BASE, KEY)
    assert isinstance(client, AsyncBasinClient)


@respx.mock
def test_health():
    respx.get(f"{BASE}/health").mock(return_value=httpx.Response(200, text="ok"))
    client = create_client(BASE, KEY)
    assert client.health() == "ok"


@respx.mock
def test_context_manager():
    respx.get(f"{BASE}/health").mock(return_value=httpx.Response(200, text="ok"))
    with create_client(BASE, KEY) as client:
        assert client.health() == "ok"


@respx.mock
async def test_async_health():
    respx.get(f"{BASE}/health").mock(return_value=httpx.Response(200, text="ok"))
    async with create_async_client(BASE, KEY) as client:
        result = await client.health()
        assert result == "ok"


def test_jwt_project_id_extraction():
    jwt = _make_jwt("01JTEST000000000000000000A")
    pid = _project_id_from_jwt(jwt)
    assert pid == "01JTEST000000000000000000A"


def test_jwt_project_id_non_jwt():
    assert _project_id_from_jwt("rawkey") is None


def test_project_id_from_jwt_key():
    jwt = _make_jwt("01JPROJECT00000000000000B")
    client = create_client(BASE, jwt)
    # The client should decode project_id from the JWT key
    pid = client._resolve_project_id()
    assert pid == "01JPROJECT00000000000000B"


def test_explicit_project_id_beats_jwt():
    jwt = _make_jwt("01JFROM_JWT0000000000000A")
    client = create_client(BASE, jwt, project_id="01JEXPLICIT00000000000000")
    pid = client._resolve_project_id()
    assert pid == "01JEXPLICIT00000000000000"


@respx.mock
def test_request_escape_hatch():
    respx.get(f"{BASE}/admin/v1/projects").mock(
        return_value=httpx.Response(200, json=[{"id": "proj-1"}])
    )
    client = create_client(BASE, KEY)
    result = client.request("GET", "/admin/v1/projects")
    assert result[0]["id"] == "proj-1"


@respx.mock
def test_bearer_token_sent():
    """Ensure the static key is forwarded as Authorization: Bearer."""
    route = respx.get(f"{BASE}/rest/v1/t").mock(
        return_value=httpx.Response(200, json=[])
    )
    client = create_client(BASE, "mykey")
    client.table("t").run()
    request = route.calls.last.request
    assert request.headers.get("authorization") == "Bearer mykey"


@respx.mock
async def test_async_bearer_token_sent():
    route = respx.get(f"{BASE}/rest/v1/t").mock(
        return_value=httpx.Response(200, json=[])
    )
    async with create_async_client(BASE, "mykey") as client:
        await client.table("t").run()
    request = route.calls.last.request
    assert request.headers.get("authorization") == "Bearer mykey"
