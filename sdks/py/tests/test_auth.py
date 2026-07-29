from __future__ import annotations

import httpx
import pytest
import respx

from basin import create_client

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
        "access_token": "access_abc",
        "refresh_token": "refresh_xyz",
        "expires_at": "2099-01-01T00:00:00Z",
        "session_id": "sid1",
    },
}


# ── sign_up ──────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_sign_up_success():
    with respx.mock:
        respx.post(f"{BASE}/auth/v1/signup").mock(
            return_value=httpx.Response(200, json=SESSION_BODY)
        )
        async with create_client(BASE, KEY) as c:
            session, err = await c.auth.sign_up(email="a@b.com", password="hunter2")
    assert err is None
    assert session is not None
    assert session.access_token == "access_abc"
    assert session.user.id == "u1"


@pytest.mark.asyncio
async def test_sign_up_missing_fields():
    async with create_client(BASE, KEY) as c:
        session, err = await c.auth.sign_up(email="", password="x")
    assert session is None
    assert err is not None
    assert err.code == "invalid_request"


# ── sign_in_with_password ────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_sign_in_stores_session():
    with respx.mock:
        respx.post(f"{BASE}/auth/v1/signin").mock(
            return_value=httpx.Response(200, json=SESSION_BODY)
        )
        async with create_client(BASE, KEY) as c:
            session, err = await c.auth.sign_in_with_password(
                email="a@b.com", password="hunter2"
            )
            assert err is None
            assert session is not None
            stored = c.auth.get_session()
    assert stored is not None
    assert stored.access_token == "access_abc"


@pytest.mark.asyncio
async def test_sign_in_flips_authorization_header():
    with respx.mock:
        respx.post(f"{BASE}/auth/v1/signin").mock(
            return_value=httpx.Response(200, json=SESSION_BODY)
        )
        rest_route = respx.get(f"{BASE}/rest/v1/items").mock(
            return_value=httpx.Response(200, json=[])
        )
        async with create_client(BASE, KEY) as c:
            await c.auth.sign_in_with_password(email="a@b.com", password="hunter2")
            await c.from_("items").select()
        req = rest_route.calls.last.request
        assert req.headers["authorization"] == "Bearer access_abc"


@pytest.mark.asyncio
async def test_sign_in_401_typed_error():
    with respx.mock:
        respx.post(f"{BASE}/auth/v1/signin").mock(
            return_value=httpx.Response(401, json={"message": "invalid credentials"})
        )
        async with create_client(BASE, KEY) as c:
            session, err = await c.auth.sign_in_with_password(
                email="a@b.com", password="wrong"
            )
    assert session is None
    assert err is not None
    assert err.code == "unauthorized"


# ── refresh_session ──────────────────────────────────────────────────────────

REFRESH_BODY = {
    "access_token": "access_new",
    "refresh_token": "refresh_new",
    "expires_at": "2099-06-01T00:00:00Z",
}


@pytest.mark.asyncio
async def test_refresh_updates_tokens():
    with respx.mock:
        respx.post(f"{BASE}/auth/v1/signin").mock(
            return_value=httpx.Response(200, json=SESSION_BODY)
        )
        respx.post(f"{BASE}/auth/v1/refresh").mock(
            return_value=httpx.Response(200, json=REFRESH_BODY)
        )
        async with create_client(BASE, KEY) as c:
            await c.auth.sign_in_with_password(email="a@b.com", password="pw")
            session, err = await c.auth.refresh_session()
    assert err is None
    assert session is not None
    assert session.access_token == "access_new"
    assert session.refresh_token == "refresh_new"


@pytest.mark.asyncio
async def test_refresh_without_session_errors():
    async with create_client(BASE, KEY) as c:
        session, err = await c.auth.refresh_session()
    assert session is None
    assert err is not None
    assert err.code == "no_session"


# ── sign_out ─────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_sign_out_clears_session():
    with respx.mock:
        respx.post(f"{BASE}/auth/v1/signin").mock(
            return_value=httpx.Response(200, json=SESSION_BODY)
        )
        async with create_client(BASE, KEY) as c:
            await c.auth.sign_in_with_password(email="a@b.com", password="pw")
            assert c.auth.get_session() is not None
            await c.auth.sign_out()
            assert c.auth.get_session() is None


# ── OAuth (path-segment form) ─────────────────────────────────────────────────

def test_oauth_url_uses_path_segment():
    import asyncio
    c = create_client(BASE, KEY)
    result, err = c.auth.sign_in_with_oauth(
        provider="github",
        redirect_to="https://myapp.com/callback",
    )
    asyncio.run(c.aclose())
    assert err is None
    assert result is not None
    url: str = result["url"]
    # MUST use /auth/v1/oauth/github/authorize (provider in path)
    assert "/oauth/github/authorize" in url
    # NOT the buggy ?provider=github form
    assert "?provider=" not in url
    assert "redirect_to=https" in url


def test_oauth_url_provider_in_path_not_query():
    import asyncio
    c = create_client(BASE, KEY)
    result, err = c.auth.sign_in_with_oauth(provider="google")
    asyncio.run(c.aclose())
    assert "/oauth/google/authorize" in result["url"]


# ── magic link ────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_magic_link_sent():
    with respx.mock:
        route = respx.post(f"{BASE}/auth/v1/magic-link").mock(
            return_value=httpx.Response(204)
        )
        async with create_client(BASE, KEY) as c:
            data, err = await c.auth.sign_in_with_magic_link(email="a@b.com")
    assert err is None
    assert route.called


# ── MFA routes ────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_mfa_enroll_calls_correct_route():
    with respx.mock:
        route = respx.post(f"{BASE}/auth/v1/factors").mock(
            return_value=httpx.Response(200, json={"factor": "totp", "qr_url": "otpauth://..."})
        )
        async with create_client(BASE, KEY) as c:
            data, err = await c.auth.mfa_enroll(factor="totp")
    assert route.called
    assert err is None
    assert data["factor"] == "totp"


@pytest.mark.asyncio
async def test_mfa_verify_route():
    with respx.mock:
        route = respx.post(f"{BASE}/auth/v1/factors/fid1/verify").mock(
            return_value=httpx.Response(200, json={"enabled": True})
        )
        async with create_client(BASE, KEY) as c:
            data, err = await c.auth.mfa_verify(
                factor_id="fid1",
                body={"factor": "totp", "code": "123456"},
            )
    assert route.called
    assert err is None


@pytest.mark.asyncio
async def test_mfa_challenge_route():
    with respx.mock:
        route = respx.post(f"{BASE}/auth/v1/factors/fid1/challenge").mock(
            return_value=httpx.Response(200, json={"id": "cid1", "type": "totp", "expires_at": 9999})
        )
        async with create_client(BASE, KEY) as c:
            data, err = await c.auth.mfa_challenge(factor_id="fid1")
    assert route.called


@pytest.mark.asyncio
async def test_mfa_unenroll_route():
    with respx.mock:
        route = respx.delete(f"{BASE}/auth/v1/factors/fid1").mock(
            return_value=httpx.Response(204)
        )
        async with create_client(BASE, KEY) as c:
            data, err = await c.auth.mfa_unenroll(factor_id="fid1")
    assert route.called
