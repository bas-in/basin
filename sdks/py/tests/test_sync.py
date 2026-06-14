from __future__ import annotations

import httpx
import respx

from basin import create_sync_client

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
