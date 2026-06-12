"""Offline tests for the OAuth and MFA auth wrappers.

All tests use respx to mock the HTTP transport — no server required.
Fixtures match the exact JSON serde shapes from
crates/basin-rest/src/routes/auth.rs.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import httpx
import pytest
import respx

from basin.client import create_client, create_async_client
from basin.errors import BasinApiError
from basin.types import (
    FactorDescriptor,
    MfaChallengeResult,
    MfaEnrollResult,
    OAuthAuthorizeResult,
    Session,
    TotpChallengeResult,
    TotpEnrollResult,
    VerifyFactorResult,
    WebAuthnChallengeResult,
    WebAuthnEnrollResult,
)

BASE = "http://basin.test"
PROJECT = "01JTEST000000000000000000A"
KEY = "testapikey"
FACTOR_ID = "11111111-1111-1111-1111-111111111111"
CHALLENGE_ID = "22222222-2222-2222-2222-222222222222"


def _future_ts(seconds: int = 3600) -> str:
    dt = datetime.now(tz=timezone.utc) + timedelta(seconds=seconds)
    return dt.isoformat().replace("+00:00", "Z")


SESSION_DATA = {
    "access_token": "access.jwt.token",
    "refresh_token": "refresh.token",
    "access_expires_at": _future_ts(3600),
    "refresh_expires_at": _future_ts(86400),
}

# ---------------------------------------------------------------------------
# Fixture JSON (matches server serde shapes exactly)
# ---------------------------------------------------------------------------

OAUTH_AUTHORIZE_RESPONSE = {
    "redirect_url": (
        "https://accounts.google.com/o/oauth2/v2/auth"
        "?response_type=code&client_id=my-client-id&scope=openid%20email%20profile"
        "&state=abc123.sig&code_challenge=chall&code_challenge_method=S256"
    ),
    "state": "abc123.sig",
}

TOTP_ENROLL_RESPONSE = {
    "factor_id": FACTOR_ID,
    "factor_type": "totp",
    "secret_b32": "JBSWY3DPEHPK3PXP",
    "otpauth_uri": "otpauth://totp/Basin%3Aalice%40example.com?secret=JBSWY3DPEHPK3PXP&issuer=Basin&algorithm=SHA1&digits=6&period=30",
}

WEBAUTHN_ENROLL_RESPONSE = {
    "factor_id": FACTOR_ID,
    "factor_type": "webauthn",
    "challenge_id": CHALLENGE_ID,
    "creation_options_json": '{"rp":{"name":"Basin","id":"localhost"},"challenge":"abc","pubKeyCredParams":[]}',
}

FACTOR_LIST_RESPONSE = [
    {
        "id": FACTOR_ID,
        "factor_type": "totp",
        "status": "verified",
        "friendly_name": "My App",
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-01T00:00:00Z",
    }
]

VERIFY_FACTOR_RESPONSE_WITH_CODES = {
    "ok": True,
    "recovery_codes": [
        "aabbccddeeff0011",
        "aabbccddeeff0022",
        "aabbccddeeff0033",
        "aabbccddeeff0044",
        "aabbccddeeff0055",
        "aabbccddeeff0066",
        "aabbccddeeff0077",
        "aabbccddeeff0088",
    ],
}

VERIFY_FACTOR_RESPONSE_NO_CODES = {"ok": True}

TOTP_CHALLENGE_RESPONSE = {"challenge_id": CHALLENGE_ID}

WEBAUTHN_CHALLENGE_RESPONSE = {
    "challenge_id": CHALLENGE_ID,
    "request_options_json": '{"challenge":"xyz","allowCredentials":[]}',
}

# Challenge verify returns the standard token body (Session).
CHALLENGE_VERIFY_RESPONSE = SESSION_DATA

ERROR_RESPONSE = {"code": "E_FORBIDDEN", "message": "requires aal2"}


# ===========================================================================
# OAuth — sync
# ===========================================================================


@respx.mock
def test_get_oauth_authorize_url_sync():
    respx.get(f"{BASE}/auth/v1/oauth/google/authorize").mock(
        return_value=httpx.Response(200, json=OAUTH_AUTHORIZE_RESPONSE)
    )
    client = create_client(BASE, KEY, project_id=PROJECT)
    result = client.auth.get_oauth_authorize_url("google", redirect_to="https://app.example.com/cb")

    assert isinstance(result, OAuthAuthorizeResult)
    assert result.redirect_url == OAUTH_AUTHORIZE_RESPONSE["redirect_url"]
    assert result.state == "abc123.sig"


@respx.mock
def test_get_oauth_authorize_url_no_redirect():
    """redirect_to defaults to empty string; still a valid call."""
    respx.get(f"{BASE}/auth/v1/oauth/github/authorize").mock(
        return_value=httpx.Response(200, json=OAUTH_AUTHORIZE_RESPONSE)
    )
    client = create_client(BASE, KEY, project_id=PROJECT)
    result = client.auth.get_oauth_authorize_url("github")
    assert result.redirect_url.startswith("https://")


@respx.mock
def test_oauth_authorize_error_propagates_sync():
    """Server errors (e.g. provider not configured) raise BasinApiError."""
    respx.get(f"{BASE}/auth/v1/oauth/unknown/authorize").mock(
        return_value=httpx.Response(
            400, json={"code": "E_INVALID_REQUEST", "message": "invalid provider"}
        )
    )
    client = create_client(BASE, KEY, project_id=PROJECT)
    with pytest.raises(BasinApiError) as exc_info:
        client.auth.get_oauth_authorize_url("unknown")
    assert exc_info.value.code == "E_INVALID_REQUEST"


# ===========================================================================
# MFA enroll — sync
# ===========================================================================


@respx.mock
def test_enroll_totp_sync():
    respx.post(f"{BASE}/auth/v1/factors").mock(
        return_value=httpx.Response(201, json=TOTP_ENROLL_RESPONSE)
    )
    client = create_client(BASE, KEY, project_id=PROJECT)
    result = client.auth.enroll_factor("totp", friendly_name="My App")

    assert isinstance(result, TotpEnrollResult)
    assert result.factor_id == FACTOR_ID
    assert result.factor_type == "totp"
    assert result.secret_b32 == "JBSWY3DPEHPK3PXP"
    assert result.otpauth_uri.startswith("otpauth://totp/")


@respx.mock
def test_enroll_webauthn_sync():
    respx.post(f"{BASE}/auth/v1/factors").mock(
        return_value=httpx.Response(201, json=WEBAUTHN_ENROLL_RESPONSE)
    )
    client = create_client(BASE, KEY, project_id=PROJECT)
    result = client.auth.enroll_factor("webauthn", friendly_name="YubiKey")

    assert isinstance(result, WebAuthnEnrollResult)
    assert result.factor_id == FACTOR_ID
    assert result.factor_type == "webauthn"
    assert result.challenge_id == CHALLENGE_ID
    assert "rp" in result.creation_options_json


@respx.mock
def test_enroll_factor_error_sync():
    """E_UNAUTHENTICATED is raised when no JWT is present."""
    respx.post(f"{BASE}/auth/v1/factors").mock(
        return_value=httpx.Response(
            401, json={"code": "E_UNAUTHENTICATED", "message": "missing token"}
        )
    )
    client = create_client(BASE, KEY, project_id=PROJECT)
    with pytest.raises(BasinApiError) as exc_info:
        client.auth.enroll_factor("totp")
    assert exc_info.value.code == "E_UNAUTHENTICATED"
    assert exc_info.value.status == 401


# ===========================================================================
# MFA list_factors — sync
# ===========================================================================


@respx.mock
def test_list_factors_sync():
    respx.get(f"{BASE}/auth/v1/factors").mock(
        return_value=httpx.Response(200, json=FACTOR_LIST_RESPONSE)
    )
    client = create_client(BASE, KEY, project_id=PROJECT)
    factors = client.auth.list_factors()

    assert len(factors) == 1
    f = factors[0]
    assert isinstance(f, FactorDescriptor)
    assert f.id == FACTOR_ID
    assert f.factor_type == "totp"
    assert f.status == "verified"
    assert f.friendly_name == "My App"


@respx.mock
def test_list_factors_empty_sync():
    respx.get(f"{BASE}/auth/v1/factors").mock(
        return_value=httpx.Response(200, json=[])
    )
    client = create_client(BASE, KEY, project_id=PROJECT)
    factors = client.auth.list_factors()
    assert factors == []


# ===========================================================================
# MFA verify_factor — sync
# ===========================================================================


@respx.mock
def test_verify_totp_factor_with_recovery_codes_sync():
    """First verified factor returns recovery_codes."""
    respx.post(f"{BASE}/auth/v1/factors/{FACTOR_ID}/verify").mock(
        return_value=httpx.Response(200, json=VERIFY_FACTOR_RESPONSE_WITH_CODES)
    )
    client = create_client(BASE, KEY, project_id=PROJECT)
    result = client.auth.verify_factor(FACTOR_ID, code="123456")

    assert isinstance(result, VerifyFactorResult)
    assert result.ok is True
    assert result.recovery_codes is not None
    assert len(result.recovery_codes) == 8


@respx.mock
def test_verify_totp_factor_no_recovery_codes_sync():
    """Subsequent factor verifications omit recovery_codes."""
    respx.post(f"{BASE}/auth/v1/factors/{FACTOR_ID}/verify").mock(
        return_value=httpx.Response(200, json=VERIFY_FACTOR_RESPONSE_NO_CODES)
    )
    client = create_client(BASE, KEY, project_id=PROJECT)
    result = client.auth.verify_factor(FACTOR_ID, code="654321")

    assert result.ok is True
    assert result.recovery_codes is None


@respx.mock
def test_verify_webauthn_factor_sync():
    respx.post(f"{BASE}/auth/v1/factors/{FACTOR_ID}/verify").mock(
        return_value=httpx.Response(200, json=VERIFY_FACTOR_RESPONSE_WITH_CODES)
    )
    client = create_client(BASE, KEY, project_id=PROJECT)
    result = client.auth.verify_factor(
        FACTOR_ID,
        attestation='{"type":"webauthn.create"}',
        challenge_id=CHALLENGE_ID,
    )
    assert result.ok is True


@respx.mock
def test_verify_factor_error_sync():
    """Invalid TOTP code returns E_INVALID_REQUEST."""
    respx.post(f"{BASE}/auth/v1/factors/{FACTOR_ID}/verify").mock(
        return_value=httpx.Response(
            400, json={"code": "E_INVALID_REQUEST", "message": "invalid TOTP code"}
        )
    )
    client = create_client(BASE, KEY, project_id=PROJECT)
    with pytest.raises(BasinApiError) as exc_info:
        client.auth.verify_factor(FACTOR_ID, code="000000")
    assert exc_info.value.code == "E_INVALID_REQUEST"


# ===========================================================================
# MFA challenge_factor — sync
# ===========================================================================


@respx.mock
def test_challenge_totp_factor_sync():
    respx.post(f"{BASE}/auth/v1/factors/{FACTOR_ID}/challenge").mock(
        return_value=httpx.Response(200, json=TOTP_CHALLENGE_RESPONSE)
    )
    client = create_client(BASE, KEY, project_id=PROJECT)
    result = client.auth.challenge_factor(FACTOR_ID)

    assert isinstance(result, TotpChallengeResult)
    assert result.challenge_id == CHALLENGE_ID


@respx.mock
def test_challenge_webauthn_factor_sync():
    respx.post(f"{BASE}/auth/v1/factors/{FACTOR_ID}/challenge").mock(
        return_value=httpx.Response(200, json=WEBAUTHN_CHALLENGE_RESPONSE)
    )
    client = create_client(BASE, KEY, project_id=PROJECT)
    result = client.auth.challenge_factor(FACTOR_ID)

    assert isinstance(result, WebAuthnChallengeResult)
    assert result.challenge_id == CHALLENGE_ID
    assert result.request_options_json is not None
    assert "challenge" in result.request_options_json


@respx.mock
def test_challenge_factor_error_sync():
    respx.post(f"{BASE}/auth/v1/factors/{FACTOR_ID}/challenge").mock(
        return_value=httpx.Response(
            404, json={"code": "E_NOT_FOUND", "message": "factor not found"}
        )
    )
    client = create_client(BASE, KEY, project_id=PROJECT)
    with pytest.raises(BasinApiError) as exc_info:
        client.auth.challenge_factor(FACTOR_ID)
    assert exc_info.value.code == "E_NOT_FOUND"


# ===========================================================================
# MFA verify_challenge — sync
# ===========================================================================


@respx.mock
def test_verify_totp_challenge_sync():
    """Successful challenge verify returns a new Session (aal2 JWT)."""
    respx.post(f"{BASE}/auth/v1/factors/{FACTOR_ID}/challenge/verify").mock(
        return_value=httpx.Response(200, json=CHALLENGE_VERIFY_RESPONSE)
    )
    client = create_client(BASE, KEY, project_id=PROJECT)
    session = client.auth.verify_challenge(FACTOR_ID, CHALLENGE_ID, code="123456")

    assert isinstance(session, Session)
    assert session.access_token == SESSION_DATA["access_token"]
    # verify_challenge stores the session
    assert client.auth.get_session() is session


@respx.mock
def test_verify_webauthn_challenge_sync():
    respx.post(f"{BASE}/auth/v1/factors/{FACTOR_ID}/challenge/verify").mock(
        return_value=httpx.Response(200, json=CHALLENGE_VERIFY_RESPONSE)
    )
    client = create_client(BASE, KEY, project_id=PROJECT)
    session = client.auth.verify_challenge(
        FACTOR_ID,
        CHALLENGE_ID,
        assertion='{"type":"webauthn.get"}',
    )
    assert isinstance(session, Session)


@respx.mock
def test_verify_challenge_error_sync():
    respx.post(f"{BASE}/auth/v1/factors/{FACTOR_ID}/challenge/verify").mock(
        return_value=httpx.Response(
            400, json={"code": "E_INVALID_REQUEST", "message": "invalid TOTP code"}
        )
    )
    client = create_client(BASE, KEY, project_id=PROJECT)
    with pytest.raises(BasinApiError) as exc_info:
        client.auth.verify_challenge(FACTOR_ID, CHALLENGE_ID, code="000000")
    assert exc_info.value.code == "E_INVALID_REQUEST"


# ===========================================================================
# MFA unenroll_factor — sync
# ===========================================================================


@respx.mock
def test_unenroll_factor_sync():
    respx.delete(f"{BASE}/auth/v1/factors/{FACTOR_ID}").mock(
        return_value=httpx.Response(200, json={"ok": True})
    )
    client = create_client(BASE, KEY, project_id=PROJECT)
    result = client.auth.unenroll_factor(FACTOR_ID)
    assert result["ok"] is True


@respx.mock
def test_unenroll_factor_forbidden_sync():
    """Unenroll with aal1 token is rejected by the server."""
    respx.delete(f"{BASE}/auth/v1/factors/{FACTOR_ID}").mock(
        return_value=httpx.Response(403, json=ERROR_RESPONSE)
    )
    client = create_client(BASE, KEY, project_id=PROJECT)
    with pytest.raises(BasinApiError) as exc_info:
        client.auth.unenroll_factor(FACTOR_ID)
    assert exc_info.value.code == "E_FORBIDDEN"
    assert exc_info.value.status == 403


# ===========================================================================
# Async variants
# ===========================================================================


@respx.mock
async def test_get_oauth_authorize_url_async():
    respx.get(f"{BASE}/auth/v1/oauth/google/authorize").mock(
        return_value=httpx.Response(200, json=OAUTH_AUTHORIZE_RESPONSE)
    )
    async with create_async_client(BASE, KEY, project_id=PROJECT) as client:
        result = await client.auth.get_oauth_authorize_url(
            "google", redirect_to="https://app.example.com/cb"
        )
    assert isinstance(result, OAuthAuthorizeResult)
    assert result.state == "abc123.sig"


@respx.mock
async def test_enroll_totp_async():
    respx.post(f"{BASE}/auth/v1/factors").mock(
        return_value=httpx.Response(201, json=TOTP_ENROLL_RESPONSE)
    )
    async with create_async_client(BASE, KEY, project_id=PROJECT) as client:
        result = await client.auth.enroll_factor("totp", friendly_name="My App")
    assert isinstance(result, TotpEnrollResult)
    assert result.secret_b32 == "JBSWY3DPEHPK3PXP"


@respx.mock
async def test_enroll_webauthn_async():
    respx.post(f"{BASE}/auth/v1/factors").mock(
        return_value=httpx.Response(201, json=WEBAUTHN_ENROLL_RESPONSE)
    )
    async with create_async_client(BASE, KEY, project_id=PROJECT) as client:
        result = await client.auth.enroll_factor("webauthn")
    assert isinstance(result, WebAuthnEnrollResult)
    assert result.challenge_id == CHALLENGE_ID


@respx.mock
async def test_list_factors_async():
    respx.get(f"{BASE}/auth/v1/factors").mock(
        return_value=httpx.Response(200, json=FACTOR_LIST_RESPONSE)
    )
    async with create_async_client(BASE, KEY, project_id=PROJECT) as client:
        factors = await client.auth.list_factors()
    assert len(factors) == 1
    assert isinstance(factors[0], FactorDescriptor)
    assert factors[0].factor_type == "totp"


@respx.mock
async def test_verify_factor_with_recovery_codes_async():
    respx.post(f"{BASE}/auth/v1/factors/{FACTOR_ID}/verify").mock(
        return_value=httpx.Response(200, json=VERIFY_FACTOR_RESPONSE_WITH_CODES)
    )
    async with create_async_client(BASE, KEY, project_id=PROJECT) as client:
        result = await client.auth.verify_factor(FACTOR_ID, code="123456")
    assert isinstance(result, VerifyFactorResult)
    assert result.recovery_codes is not None
    assert len(result.recovery_codes) == 8


@respx.mock
async def test_verify_factor_no_codes_async():
    respx.post(f"{BASE}/auth/v1/factors/{FACTOR_ID}/verify").mock(
        return_value=httpx.Response(200, json=VERIFY_FACTOR_RESPONSE_NO_CODES)
    )
    async with create_async_client(BASE, KEY, project_id=PROJECT) as client:
        result = await client.auth.verify_factor(FACTOR_ID, code="654321")
    assert result.recovery_codes is None


@respx.mock
async def test_challenge_totp_factor_async():
    respx.post(f"{BASE}/auth/v1/factors/{FACTOR_ID}/challenge").mock(
        return_value=httpx.Response(200, json=TOTP_CHALLENGE_RESPONSE)
    )
    async with create_async_client(BASE, KEY, project_id=PROJECT) as client:
        result = await client.auth.challenge_factor(FACTOR_ID)
    assert isinstance(result, TotpChallengeResult)
    assert result.challenge_id == CHALLENGE_ID


@respx.mock
async def test_challenge_webauthn_factor_async():
    respx.post(f"{BASE}/auth/v1/factors/{FACTOR_ID}/challenge").mock(
        return_value=httpx.Response(200, json=WEBAUTHN_CHALLENGE_RESPONSE)
    )
    async with create_async_client(BASE, KEY, project_id=PROJECT) as client:
        result = await client.auth.challenge_factor(FACTOR_ID)
    assert isinstance(result, WebAuthnChallengeResult)
    assert result.request_options_json is not None


@respx.mock
async def test_verify_totp_challenge_async():
    respx.post(f"{BASE}/auth/v1/factors/{FACTOR_ID}/challenge/verify").mock(
        return_value=httpx.Response(200, json=CHALLENGE_VERIFY_RESPONSE)
    )
    async with create_async_client(BASE, KEY, project_id=PROJECT) as client:
        session = await client.auth.verify_challenge(
            FACTOR_ID, CHALLENGE_ID, code="123456"
        )
    assert isinstance(session, Session)
    assert session.access_token == SESSION_DATA["access_token"]


@respx.mock
async def test_verify_challenge_stores_session_async():
    respx.post(f"{BASE}/auth/v1/factors/{FACTOR_ID}/challenge/verify").mock(
        return_value=httpx.Response(200, json=CHALLENGE_VERIFY_RESPONSE)
    )
    async with create_async_client(BASE, KEY, project_id=PROJECT) as client:
        session = await client.auth.verify_challenge(
            FACTOR_ID, CHALLENGE_ID, code="123456"
        )
        stored = client.auth.get_session()
    assert stored is session


@respx.mock
async def test_unenroll_factor_async():
    respx.delete(f"{BASE}/auth/v1/factors/{FACTOR_ID}").mock(
        return_value=httpx.Response(200, json={"ok": True})
    )
    async with create_async_client(BASE, KEY, project_id=PROJECT) as client:
        result = await client.auth.unenroll_factor(FACTOR_ID)
    assert result["ok"] is True


@respx.mock
async def test_enroll_factor_error_async():
    respx.post(f"{BASE}/auth/v1/factors").mock(
        return_value=httpx.Response(
            401, json={"code": "E_UNAUTHENTICATED", "message": "missing token"}
        )
    )
    async with create_async_client(BASE, KEY, project_id=PROJECT) as client:
        with pytest.raises(BasinApiError) as exc_info:
            await client.auth.enroll_factor("totp")
    assert exc_info.value.code == "E_UNAUTHENTICATED"


@respx.mock
async def test_unenroll_factor_forbidden_async():
    respx.delete(f"{BASE}/auth/v1/factors/{FACTOR_ID}").mock(
        return_value=httpx.Response(403, json=ERROR_RESPONSE)
    )
    async with create_async_client(BASE, KEY, project_id=PROJECT) as client:
        with pytest.raises(BasinApiError) as exc_info:
            await client.auth.unenroll_factor(FACTOR_ID)
    assert exc_info.value.code == "E_FORBIDDEN"


@respx.mock
async def test_oauth_authorize_error_propagates_async():
    respx.get(f"{BASE}/auth/v1/oauth/unknown/authorize").mock(
        return_value=httpx.Response(
            400, json={"code": "E_INVALID_REQUEST", "message": "invalid provider"}
        )
    )
    async with create_async_client(BASE, KEY, project_id=PROJECT) as client:
        with pytest.raises(BasinApiError) as exc_info:
            await client.auth.get_oauth_authorize_url("unknown")
    assert exc_info.value.code == "E_INVALID_REQUEST"
