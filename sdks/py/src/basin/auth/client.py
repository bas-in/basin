from __future__ import annotations

import contextlib
import math
import time
from collections.abc import Callable
from typing import Any

import httpx

from ..errors import BasinError, _code_for_status
from .types import AuthSession, AuthUser, OAuthProvider

_SESSION_KEY = "basin.auth.session"


class AuthClient:
    """
    Authentication client.  Mirrors basin-js ``AuthClient``.

    Engine routes (verified against basin-rest/src/server.rs):
      POST /auth/v1/signup
      POST /auth/v1/signin
      POST /auth/v1/refresh
      POST /auth/v1/magic-link
      POST /auth/v1/magic-link/consume
      POST /auth/v1/verify-email
      POST /auth/v1/request-password-reset
      POST /auth/v1/reset-password
      GET  /auth/v1/oauth/:provider/authorize    ← provider is a PATH SEGMENT
      GET  /auth/v1/oauth/:provider/callback
      POST /auth/v1/factors                      (enroll / list)
      POST /auth/v1/factors/:id/verify
      POST /auth/v1/factors/:id/challenge
      POST /auth/v1/factors/:id/challenge/verify
      DELETE /auth/v1/factors/:id
    """

    def __init__(
        self,
        *,
        base_url: str,
        anon_key: str,
        get_headers: Callable[[], dict[str, str]],
        http: Any,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._anon_key = anon_key
        self._get_headers = get_headers
        self._http = http
        self._session: AuthSession | None = None
        self._listeners: list[Callable[[str, AuthSession | None], None]] = []

    # ── Public API ──────────────────────────────────────────────────────

    async def sign_up(
        self,
        *,
        email: str,
        password: str,
        data: dict[str, Any] | None = None,
        email_redirect_to: str | None = None,
    ) -> tuple[AuthSession | None, BasinError | None]:
        if not email or not password:
            return None, BasinError("invalid_request", "sign_up requires email and password")
        body: dict[str, Any] = {"email": email, "password": password}
        resp = await self._post("/auth/v1/signup", body)
        if isinstance(resp, BasinError):
            return None, resp
        session, err = _parse_session_response(resp)
        if err:
            return None, err
        if session:
            self._adopt(session, "SIGNED_IN")
        return session, None

    async def sign_in_with_password(
        self,
        *,
        email: str,
        password: str,
    ) -> tuple[AuthSession | None, BasinError | None]:
        if not email or not password:
            return None, BasinError(
                "invalid_request",
                "sign_in_with_password requires email and password",
            )
        body: dict[str, Any] = {"email": email, "password": password}
        resp = await self._post("/auth/v1/signin", body)
        if isinstance(resp, BasinError):
            return None, resp
        if (
            isinstance(resp, dict)
            and resp.get("requires_totp")
        ):
            return None, BasinError(
                "mfa_required",
                "Two-factor verification required; complete via auth.mfa_verify()",
                details={"partial_token": resp.get("partial_token", "")},
            )
        session, err = _parse_session_response(resp)
        if err:
            return None, err
        if session:
            self._adopt(session, "SIGNED_IN")
        return session, None

    async def sign_in_with_magic_link(
        self,
        *,
        email: str,
    ) -> tuple[None, BasinError | None]:
        if not email:
            return None, BasinError(
                "invalid_request",
                "sign_in_with_magic_link requires email",
            )
        resp = await self._post("/auth/v1/magic-link", {"email": email})
        if isinstance(resp, BasinError):
            return None, resp
        return None, None

    async def consume_magic_link(
        self,
        *,
        token: str,
    ) -> tuple[AuthSession | None, BasinError | None]:
        if not token:
            return None, BasinError(
                "invalid_request",
                "consume_magic_link requires a token",
            )
        resp = await self._post("/auth/v1/magic-link/consume", {"token": token})
        if isinstance(resp, BasinError):
            return None, resp
        if isinstance(resp, dict) and resp.get("requires_totp"):
            return None, BasinError(
                "mfa_required",
                "Two-factor verification required",
                details={"partial_token": resp.get("partial_token", "")},
            )
        session, err = _parse_session_response(resp)
        if err:
            return None, err
        if session:
            self._adopt(session, "SIGNED_IN")
        return session, None

    async def verify_email(
        self,
        *,
        token: str,
    ) -> tuple[None, BasinError | None]:
        if not token:
            return None, BasinError("invalid_request", "verify_email requires a token")
        resp = await self._post("/auth/v1/verify-email", {"token": token})
        if isinstance(resp, BasinError):
            return None, resp
        return None, None

    async def request_password_reset(
        self,
        *,
        email: str,
    ) -> tuple[None, BasinError | None]:
        if not email:
            return None, BasinError(
                "invalid_request",
                "request_password_reset requires email",
            )
        resp = await self._post("/auth/v1/request-password-reset", {"email": email})
        if isinstance(resp, BasinError):
            return None, resp
        return None, None

    async def reset_password(
        self,
        *,
        token: str,
        new_password: str,
    ) -> tuple[None, BasinError | None]:
        if not token or not new_password:
            return None, BasinError(
                "invalid_request",
                "reset_password requires token and new_password",
            )
        resp = await self._post(
            "/auth/v1/reset-password",
            {"token": token, "new_password": new_password},
        )
        if isinstance(resp, BasinError):
            return None, resp
        return None, None

    async def sign_out(self) -> tuple[None, None]:
        self._session = None
        self._emit("SIGNED_OUT", None)
        return None, None

    def get_session(self) -> AuthSession | None:
        return self._session

    def get_user(self) -> AuthUser | None:
        s = self._session
        return s.user if s else None

    async def refresh_session(
        self,
    ) -> tuple[AuthSession | None, BasinError | None]:
        current = self._session
        if not current or not current.refresh_token:
            return None, BasinError(
                "no_session",
                "refresh_session requires an active session with a refresh_token",
            )
        resp = await self._post(
            "/auth/v1/refresh",
            {"refresh_token": current.refresh_token},
        )
        if isinstance(resp, BasinError):
            if resp.status == 401:
                self._session = None
                self._emit("SIGNED_OUT", None)
                return None, BasinError(
                    "refresh_failed",
                    "Refresh token was rejected; signed out locally",
                    status=401,
                )
            return None, resp

        body = resp if isinstance(resp, dict) else {}
        at = body.get("access_token")
        rt = body.get("refresh_token")
        ea = body.get("expires_at")
        if not at or not rt or ea is None:
            return None, BasinError(
                "invalid_response",
                "Refresh response missing access_token, refresh_token, or expires_at",
            )
        expires_at = _parse_expires_at(ea)
        next_session = AuthSession(
            access_token=str(at),
            refresh_token=str(rt),
            token_type="bearer",
            expires_at=expires_at,
            user=current.user,
        )
        self._session = next_session
        self._emit("TOKEN_REFRESHED", next_session)
        return next_session, None

    def sign_in_with_oauth(
        self,
        *,
        provider: OAuthProvider,
        redirect_to: str | None = None,
        scopes: str | None = None,
        query_params: dict[str, str] | None = None,
    ) -> tuple[dict[str, Any] | None, BasinError | None]:
        """
        Build the OAuth authorize URL.

        Engine route (server.rs): GET /auth/v1/oauth/:provider/authorize
        The provider is a PATH SEGMENT — NOT a query parameter.
        This differs from what basin-js 0.1 builds (that is a known bug
        in basin-js; we follow the engine here).
        """
        if not provider:
            return None, BasinError(
                "invalid_request",
                "sign_in_with_oauth requires a provider",
            )
        params: dict[str, str] = {}
        if redirect_to:
            params["redirect_to"] = redirect_to
        if scopes:
            params["scopes"] = scopes
        if query_params:
            params.update(query_params)
        qs = ""
        if params:
            import urllib.parse
            qs = "?" + urllib.parse.urlencode(params)
        url = f"{self._base_url}/auth/v1/oauth/{provider}/authorize{qs}"
        return {"url": url, "provider": provider}, None

    # ── MFA helpers ─────────────────────────────────────────────────────

    async def mfa_enroll(
        self,
        *,
        factor: str,
    ) -> tuple[dict[str, Any] | None, BasinError | None]:
        resp = await self._post("/auth/v1/factors", {"factor": factor})
        if isinstance(resp, BasinError):
            return None, resp
        return resp if isinstance(resp, dict) else {}, None

    async def mfa_list(
        self,
    ) -> tuple[list[Any] | None, BasinError | None]:
        resp = await self._get_req("/auth/v1/factors")
        if isinstance(resp, BasinError):
            return None, resp
        return resp if isinstance(resp, list) else [], None

    async def mfa_verify(
        self,
        *,
        factor_id: str,
        body: dict[str, Any],
    ) -> tuple[dict[str, Any] | None, BasinError | None]:
        resp = await self._post(f"/auth/v1/factors/{factor_id}/verify", body)
        if isinstance(resp, BasinError):
            return None, resp
        return resp if isinstance(resp, dict) else {}, None

    async def mfa_challenge(
        self,
        *,
        factor_id: str,
    ) -> tuple[dict[str, Any] | None, BasinError | None]:
        resp = await self._post(f"/auth/v1/factors/{factor_id}/challenge", {})
        if isinstance(resp, BasinError):
            return None, resp
        return resp if isinstance(resp, dict) else {}, None

    async def mfa_challenge_verify(
        self,
        *,
        factor_id: str,
        challenge_id: str,
        body: dict[str, Any],
    ) -> tuple[dict[str, Any] | None, BasinError | None]:
        payload = {"challenge_id": challenge_id, **body}
        resp = await self._post(
            f"/auth/v1/factors/{factor_id}/challenge/verify",
            payload,
        )
        if isinstance(resp, BasinError):
            return None, resp
        return resp if isinstance(resp, dict) else {}, None

    async def mfa_unenroll(
        self,
        *,
        factor_id: str,
    ) -> tuple[dict[str, Any] | None, BasinError | None]:
        headers = self._authed_headers()
        try:
            raw = await self._http.request(
                "DELETE",
                f"/auth/v1/factors/{factor_id}",
                headers=headers,
            )
        except BasinError as exc:
            return None, exc
        if not raw.is_success:
            return None, BasinError.from_response(raw)
        return {}, None

    def on_auth_state_change(
        self,
        callback: Callable[[str, AuthSession | None], None],
    ) -> Callable[[], None]:
        self._listeners.append(callback)

        def unsubscribe() -> None:
            with contextlib.suppress(ValueError):
                self._listeners.remove(callback)

        return unsubscribe

    # ── Internals ───────────────────────────────────────────────────────

    def _adopt(self, session: AuthSession, event: str) -> None:
        self._session = session
        self._emit(event, session)

    def _emit(self, event: str, session: AuthSession | None) -> None:
        for cb in list(self._listeners):
            with contextlib.suppress(Exception):
                cb(event, session)

    def _authed_headers(self) -> dict[str, str]:
        headers = dict(self._get_headers())
        if self._session and self._session.access_token:
            headers["Authorization"] = f"Bearer {self._session.access_token}"
        return headers

    async def _post(
        self,
        path: str,
        body: Any,
    ) -> Any:
        headers = self._authed_headers()
        try:
            raw: httpx.Response = await self._http.request(
                "POST",
                path,
                json=body,
                headers=headers,
            )
        except BasinError as exc:
            return exc
        if not raw.is_success:
            return BasinError.from_response(raw)
        if raw.status_code == 204 or not raw.content:
            return {}
        try:
            data = raw.json()
        except Exception:
            return BasinError(
                "invalid_response",
                f"auth response was not JSON (HTTP {raw.status_code})",
                status=raw.status_code,
            )
        if isinstance(data, dict) and ("data" in data or "error" in data):
            env_err = data.get("error")
            if env_err:
                code = env_err.get("code", _code_for_status(raw.status_code)) if isinstance(env_err, dict) else "internal"
                msg = env_err.get("message", "auth error") if isinstance(env_err, dict) else str(env_err)
                return BasinError(code, msg, status=raw.status_code)
            return data.get("data", {})
        return data

    async def _get_req(self, path: str) -> Any:
        headers = self._authed_headers()
        try:
            raw: httpx.Response = await self._http.request(
                "GET",
                path,
                headers=headers,
            )
        except BasinError as exc:
            return exc
        if not raw.is_success:
            return BasinError.from_response(raw)
        try:
            return raw.json()
        except Exception:
            return BasinError(
                "invalid_response",
                f"auth response was not JSON (HTTP {raw.status_code})",
                status=raw.status_code,
            )


def _parse_session_response(
    body: Any,
) -> tuple[AuthSession | None, BasinError | None]:
    if not isinstance(body, dict):
        return None, BasinError("invalid_response", "auth response body is not an object")
    user_raw = body.get("user")
    session_raw = body.get("session")
    if not user_raw or not session_raw:
        return None, BasinError(
            "invalid_response",
            "auth response missing user or session fields",
        )
    u = user_raw
    s = session_raw
    if not u.get("id") or not s.get("access_token") or not s.get("refresh_token"):
        return None, BasinError(
            "invalid_response",
            "auth session fields incomplete",
        )
    expires_at = _parse_expires_at(s.get("expires_at", 0))
    user = AuthUser(
        id=u["id"],
        email=u.get("email"),
        email_confirmed_at=u.get("updated_at") if u.get("email_verified") else None,
        phone=None,
        created_at=u.get("created_at", ""),
        updated_at=u.get("updated_at", ""),
        app_metadata={},
        user_metadata={},
    )
    session = AuthSession(
        access_token=s["access_token"],
        refresh_token=s["refresh_token"],
        token_type="bearer",
        expires_at=expires_at,
        user=user,
    )
    return session, None


def _parse_expires_at(value: Any) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return math.floor(value)
    if isinstance(value, str):
        try:
            from datetime import datetime
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return math.floor(dt.timestamp())
        except Exception:
            pass
        try:
            return int(float(value))
        except Exception:
            pass
    return math.floor(time.time()) + 3600
