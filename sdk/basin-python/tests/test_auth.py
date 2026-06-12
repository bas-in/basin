"""Tests for the AuthClient — offline, respx-mocked transport."""

import json
import time
from datetime import datetime, timezone, timedelta

import httpx
import pytest
import respx

from basin.client import create_client, create_async_client
from basin.errors import BasinApiError
from basin.types import Session


BASE = "http://basin.test"
PROJECT = "01JTEST000000000000000000A"
KEY = "testapikey"


def _future_ts(seconds: int = 3600) -> str:
    dt = datetime.now(tz=timezone.utc) + timedelta(seconds=seconds)
    return dt.isoformat().replace("+00:00", "Z")


def _past_ts(seconds: int = 10) -> str:
    dt = datetime.now(tz=timezone.utc) - timedelta(seconds=seconds)
    return dt.isoformat().replace("+00:00", "Z")


SESSION_DATA = {
    "access_token": "access.jwt.token",
    "refresh_token": "refresh.token",
    "access_expires_at": _future_ts(3600),
    "refresh_expires_at": _future_ts(86400),
}


@respx.mock
def test_sign_up():
    respx.post(f"{BASE}/auth/v1/signup").mock(
        return_value=httpx.Response(201, json={"ok": True, "user_id": "user-123"})
    )
    client = create_client(BASE, KEY, project_id=PROJECT)
    result = client.auth.sign_up(email="a@b.com", password="secret")
    assert result.ok is True
    assert result.user_id == "user-123"


@respx.mock
def test_sign_in_stores_session():
    respx.post(f"{BASE}/auth/v1/signin").mock(
        return_value=httpx.Response(200, json=SESSION_DATA)
    )
    client = create_client(BASE, KEY, project_id=PROJECT)
    session = client.auth.sign_in(email="a@b.com", password="secret")
    assert session.access_token == "access.jwt.token"
    assert client.auth.get_session() is session


@respx.mock
def test_sign_out_clears_session():
    respx.post(f"{BASE}/auth/v1/signin").mock(
        return_value=httpx.Response(200, json=SESSION_DATA)
    )
    client = create_client(BASE, KEY, project_id=PROJECT)
    client.auth.sign_in(email="a@b.com", password="secret")
    assert client.auth.get_session() is not None
    client.auth.sign_out()
    assert client.auth.get_session() is None


@respx.mock
def test_refresh_session():
    new_session = {**SESSION_DATA, "access_token": "new.access.token"}
    respx.post(f"{BASE}/auth/v1/signin").mock(
        return_value=httpx.Response(200, json=SESSION_DATA)
    )
    respx.post(f"{BASE}/auth/v1/refresh").mock(
        return_value=httpx.Response(200, json=new_session)
    )
    client = create_client(BASE, KEY, project_id=PROJECT)
    client.auth.sign_in(email="a@b.com", password="secret")
    refreshed = client.auth.refresh_session()
    assert refreshed.access_token == "new.access.token"
    assert client.auth.get_session().access_token == "new.access.token"


def test_refresh_without_session_raises():
    client = create_client(BASE, KEY, project_id=PROJECT)
    with pytest.raises(BasinApiError) as exc_info:
        client.auth.refresh_session()
    assert exc_info.value.code == "E_UNAUTHENTICATED"


@respx.mock
def test_auto_refresh_on_expired_token():
    expired_session = {**SESSION_DATA, "access_expires_at": _past_ts(60)}
    new_session = {**SESSION_DATA, "access_token": "fresh.token", "access_expires_at": _future_ts()}
    respx.post(f"{BASE}/auth/v1/signin").mock(
        return_value=httpx.Response(200, json=expired_session)
    )
    respx.post(f"{BASE}/auth/v1/refresh").mock(
        return_value=httpx.Response(200, json=new_session)
    )
    client = create_client(BASE, KEY, project_id=PROJECT)
    client.auth.sign_in(email="a@b.com", password="secret")
    tok = client.auth.access_token()
    assert tok == "fresh.token"


@respx.mock
def test_verify_email():
    respx.post(f"{BASE}/auth/v1/verify-email").mock(
        return_value=httpx.Response(200, json={"ok": True})
    )
    client = create_client(BASE, KEY, project_id=PROJECT)
    result = client.auth.verify_email(token="tok123")
    assert result["ok"] is True


@respx.mock
def test_request_password_reset():
    respx.post(f"{BASE}/auth/v1/request-password-reset").mock(
        return_value=httpx.Response(200, json={"ok": True})
    )
    client = create_client(BASE, KEY, project_id=PROJECT)
    result = client.auth.request_password_reset(email="a@b.com")
    assert result["ok"] is True


@respx.mock
def test_reset_password():
    respx.post(f"{BASE}/auth/v1/reset-password").mock(
        return_value=httpx.Response(200, json={"ok": True})
    )
    client = create_client(BASE, KEY, project_id=PROJECT)
    result = client.auth.reset_password(token="reset-tok", new_password="newpass")
    assert result["ok"] is True


@respx.mock
def test_magic_link_request():
    respx.post(f"{BASE}/auth/v1/magic-link").mock(
        return_value=httpx.Response(204)
    )
    client = create_client(BASE, KEY, project_id=PROJECT)
    # Should not raise
    client.auth.request_magic_link("a@b.com")


@respx.mock
def test_magic_link_consume():
    respx.post(f"{BASE}/auth/v1/magic-link/consume").mock(
        return_value=httpx.Response(200, json=SESSION_DATA)
    )
    client = create_client(BASE, KEY, project_id=PROJECT)
    session = client.auth.consume_magic_link("magic-token")
    assert session.access_token == SESSION_DATA["access_token"]
    assert client.auth.get_session() is session


@respx.mock
def test_create_api_key():
    respx.post(f"{BASE}/auth/v1/api-keys").mock(
        return_value=httpx.Response(
            201,
            json={
                "id": 1,
                "name": "ci-key",
                "secret": "sk_live_abc",
                "created_at": "2026-01-01T00:00:00Z",
            },
        )
    )
    client = create_client(BASE, KEY, project_id=PROJECT)
    key = client.auth.create_api_key("ci-key")
    assert key.id == 1
    assert key.secret == "sk_live_abc"


@respx.mock
def test_list_api_keys():
    respx.get(f"{BASE}/auth/v1/api-keys").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "id": 1,
                    "name": "ci-key",
                    "created_at": "2026-01-01T00:00:00Z",
                    "last_used_at": None,
                    "revoked_at": None,
                }
            ],
        )
    )
    client = create_client(BASE, KEY, project_id=PROJECT)
    keys = client.auth.list_api_keys()
    assert len(keys) == 1
    assert keys[0].name == "ci-key"


@respx.mock
def test_delete_api_key():
    respx.delete(f"{BASE}/auth/v1/api-keys/1").mock(
        return_value=httpx.Response(200, json={"ok": True})
    )
    client = create_client(BASE, KEY, project_id=PROJECT)
    result = client.auth.delete_api_key(1)
    assert result["ok"] is True


def test_sign_up_without_project_id_raises():
    client = create_client(BASE)  # no key, no project_id
    with pytest.raises(BasinApiError) as exc_info:
        client.auth.sign_up(email="a@b.com", password="secret")
    assert exc_info.value.code == "E_INVALID_REQUEST"


# ---------------------------------------------------------------------------
# Async variants
# ---------------------------------------------------------------------------


@respx.mock
async def test_async_sign_in():
    respx.post(f"{BASE}/auth/v1/signin").mock(
        return_value=httpx.Response(200, json=SESSION_DATA)
    )
    async with create_async_client(BASE, KEY, project_id=PROJECT) as client:
        session = await client.auth.sign_in(email="a@b.com", password="secret")
        assert session.access_token == SESSION_DATA["access_token"]


@respx.mock
async def test_async_refresh_session():
    new_session = {**SESSION_DATA, "access_token": "async.fresh.token"}
    respx.post(f"{BASE}/auth/v1/signin").mock(
        return_value=httpx.Response(200, json=SESSION_DATA)
    )
    respx.post(f"{BASE}/auth/v1/refresh").mock(
        return_value=httpx.Response(200, json=new_session)
    )
    async with create_async_client(BASE, KEY, project_id=PROJECT) as client:
        await client.auth.sign_in(email="a@b.com", password="secret")
        refreshed = await client.auth.refresh_session()
        assert refreshed.access_token == "async.fresh.token"
