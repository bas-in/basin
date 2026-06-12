"""Auth client — bindings for /auth/v1/*.

Route sources (verified against crates/basin-rest/src/server.rs):
- POST /auth/v1/signup                              server.rs:250
- POST /auth/v1/signin                              server.rs:251
- POST /auth/v1/refresh                             server.rs:252
- POST /auth/v1/signout                             server.rs:253
- POST /auth/v1/verify-email                        server.rs:254
- POST /auth/v1/reset-password                      server.rs:255
- POST /auth/v1/request-password-reset              server.rs:256
- POST /auth/v1/magic-link                          server.rs:262
- POST /auth/v1/magic-link/consume                  server.rs:263
- POST|GET /auth/v1/api-keys                        server.rs:267
- DELETE /auth/v1/api-keys/:id                      server.rs:271
- GET /auth/v1/oauth/:provider/authorize            server.rs:277
- GET /auth/v1/oauth/:provider/callback             server.rs:281 (server-side only)
- POST|GET /auth/v1/factors                         server.rs:286-287
- POST /auth/v1/factors/:id/verify                  server.rs:290
- POST /auth/v1/factors/:id/challenge               server.rs:294
- POST /auth/v1/factors/:id/challenge/verify        server.rs:298
- DELETE /auth/v1/factors/:id                       server.rs:302

sign_out() calls POST /auth/v1/signout with the stored refresh token, writing
a server-side revocation row, then clears the local session. The local session
is cleared regardless of server response (404 from older servers and network
errors are tolerated gracefully). When no session is active the call is a
no-op (already signed out).

Auth is per-project: signup/signin take a project_id in the body.
"""

from __future__ import annotations

import base64
import json
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Optional

from .errors import BasinApiError
from .types import (
    ApiKeyDescriptor,
    ApiKeyIssued,
    FactorDescriptor,
    MfaChallengeResult,
    MfaEnrollResult,
    OAuthAuthorizeResult,
    Session,
    SignUpResult,
    TotpChallengeResult,
    TotpEnrollResult,
    VerifyFactorResult,
    WebAuthnChallengeResult,
    WebAuthnEnrollResult,
)

if TYPE_CHECKING:
    from ._http import SyncTransport, AsyncTransport

# Refresh this many seconds before the access token's stated expiry.
_EXPIRY_SKEW_S = 10


def _project_id_from_jwt(token: str) -> Optional[str]:
    """Decode the project_id claim from a Basin JWT payload segment.

    Returns None for non-JWT tokens (e.g. raw API keys).
    """
    parts = token.split(".")
    if len(parts) != 3:
        return None
    try:
        b64 = parts[1].replace("-", "+").replace("_", "/")
        # Add padding
        b64 += "=" * ((4 - len(b64) % 4) % 4)
        payload = json.loads(base64.b64decode(b64).decode("utf-8"))
        val = payload.get("project_id")
        return val if isinstance(val, str) else None
    except Exception:
        return None


class AuthClient:
    """Sync auth client for /auth/v1/* routes."""

    def __init__(
        self,
        transport: "SyncTransport",
        default_project_id: Optional[str] = None,
    ) -> None:
        self._transport = transport
        self._default_project_id = default_project_id
        self._session: Optional[Session] = None

    # ------------------------------------------------------------------
    # Session management
    # ------------------------------------------------------------------

    def get_session(self) -> Optional[Session]:
        """Return the current session, or None when signed out."""
        return self._session

    def set_session(self, session: Optional[Session]) -> None:
        """Adopt an externally obtained token pair (e.g. restored from storage)."""
        self._session = session

    def _resolve_project_id(self, project_id: Optional[str] = None) -> str:
        pid = (
            project_id
            or self._default_project_id
            or self._transport._key and _project_id_from_jwt(self._transport._key or "")
        )
        if pid is None:
            raise BasinApiError(
                "E_INVALID_REQUEST",
                "project id required: pass project_id or set it in create_client()",
                0,
            )
        return pid

    def access_token(self) -> Optional[str]:
        """Return a valid access token, auto-refreshing if near expiry.

        Returns None when there is no session (callers fall back to the
        static key).
        """
        s = self._session
        if s is None:
            return None
        try:
            expiry = datetime.fromisoformat(
                s.access_expires_at.replace("Z", "+00:00")
            ).timestamp()
            if expiry - _EXPIRY_SKEW_S <= time.time():
                s = self.refresh_session()
        except (ValueError, AttributeError):
            pass
        return s.access_token

    # ------------------------------------------------------------------
    # Auth flows
    # ------------------------------------------------------------------

    def sign_up(
        self,
        *,
        email: str,
        password: str,
        project_id: Optional[str] = None,
    ) -> SignUpResult:
        """POST /auth/v1/signup → { ok, user_id } (201)."""
        data = self._transport.request_json(
            "POST",
            "/auth/v1/signup",
            body={
                "project_id": self._resolve_project_id(project_id),
                "email": email,
                "password": password,
            },
            auth=False,
        )
        return SignUpResult.from_dict(data)

    def sign_in(
        self,
        *,
        email: str,
        password: str,
        project_id: Optional[str] = None,
    ) -> Session:
        """POST /auth/v1/signin → token body; stored as the live session."""
        data = self._transport.request_json(
            "POST",
            "/auth/v1/signin",
            body={
                "project_id": self._resolve_project_id(project_id),
                "email": email,
                "password": password,
            },
            auth=False,
        )
        session = Session.from_dict(data)
        self._session = session
        return session

    def sign_out(self) -> None:
        """Revoke the current refresh token server-side, then clear the local session.

        Calls POST /auth/v1/signout with the stored refresh token, writing a
        server-side revocation row so that any subsequent refresh_session() call
        with the old token returns 401.

        - If there is no active session this is a no-op (already signed out).
        - 404 responses (older servers without the signout route) are tolerated
          silently — the local session is still cleared.
        - Network errors are tolerated silently — the local session is still
          cleared. The client ends up signed out regardless.
        - The local session is ALWAYS cleared, even when the server call fails,
          so the client always ends up in the signed-out state.
        """
        current = self._session
        # Clear local state first — even if the server call fails the client
        # should be considered signed out.
        self._session = None
        if current is None:
            return
        try:
            self._transport.request_json(
                "POST",
                "/auth/v1/signout",
                body={"refresh_token": current.refresh_token},
                auth=False,
            )
        except BasinApiError as exc:
            if exc.status == 404:
                # Older server without the signout route — tolerate silently.
                return
            raise
        except Exception:
            # Network error or other transient failure — local clear already done.
            return

    def refresh_session(self) -> Session:
        """POST /auth/v1/refresh with the stored refresh token.

        Rotates both tokens. Throws E_REVOKED_TOKEN if the token was already
        rotated.
        """
        current = self._session
        if current is None:
            raise BasinApiError("E_UNAUTHENTICATED", "no session to refresh", 0)
        data = self._transport.request_json(
            "POST",
            "/auth/v1/refresh",
            body={"refresh_token": current.refresh_token},
            auth=False,
        )
        session = Session.from_dict(data)
        self._session = session
        return session

    def verify_email(
        self,
        *,
        token: str,
        project_id: Optional[str] = None,
    ) -> dict:
        """POST /auth/v1/verify-email → { ok: true }."""
        return self._transport.request_json(
            "POST",
            "/auth/v1/verify-email",
            body={
                "project_id": self._resolve_project_id(project_id),
                "token": token,
            },
            auth=False,
        )

    def request_password_reset(
        self,
        *,
        email: str,
        project_id: Optional[str] = None,
    ) -> dict:
        """POST /auth/v1/request-password-reset → { ok: true }."""
        return self._transport.request_json(
            "POST",
            "/auth/v1/request-password-reset",
            body={
                "project_id": self._resolve_project_id(project_id),
                "email": email,
            },
            auth=False,
        )

    def reset_password(
        self,
        *,
        token: str,
        new_password: str,
        project_id: Optional[str] = None,
    ) -> dict:
        """POST /auth/v1/reset-password → { ok: true }."""
        return self._transport.request_json(
            "POST",
            "/auth/v1/reset-password",
            body={
                "project_id": self._resolve_project_id(project_id),
                "token": token,
                "new_password": new_password,
            },
            auth=False,
        )

    def request_magic_link(self, email: str) -> None:
        """POST /auth/v1/magic-link — 204 always (never confirms address)."""
        self._transport.request_json(
            "POST",
            "/auth/v1/magic-link",
            body={"email": email},
            auth=False,
        )

    def consume_magic_link(self, token: str) -> Session:
        """POST /auth/v1/magic-link/consume → token body; stored as session."""
        data = self._transport.request_json(
            "POST",
            "/auth/v1/magic-link/consume",
            body={"token": token},
            auth=False,
        )
        session = Session.from_dict(data)
        self._session = session
        return session

    def create_api_key(self, name: str) -> ApiKeyIssued:
        """POST /auth/v1/api-keys (JWT-gated) → issued key incl. one-time secret."""
        data = self._transport.request_json(
            "POST", "/auth/v1/api-keys", body={"name": name}
        )
        return ApiKeyIssued.from_dict(data)

    def list_api_keys(self) -> list[ApiKeyDescriptor]:
        """GET /auth/v1/api-keys (JWT-gated)."""
        data = self._transport.request_json("GET", "/auth/v1/api-keys")
        return [ApiKeyDescriptor.from_dict(d) for d in (data or [])]

    def delete_api_key(self, key_id: int) -> dict:
        """DELETE /auth/v1/api-keys/:id (JWT-gated) → { ok: true }."""
        return self._transport.request_json(
            "DELETE", f"/auth/v1/api-keys/{key_id}"
        )

    # ------------------------------------------------------------------
    # OAuth (Phase 5.10.O — crates/basin-rest/src/routes/auth.rs)
    # ------------------------------------------------------------------

    def get_oauth_authorize_url(
        self,
        provider: str,
        *,
        redirect_to: str = "",
        project_id: Optional[str] = None,
    ) -> OAuthAuthorizeResult:
        """GET /auth/v1/oauth/:provider/authorize → { redirect_url, state }.

        Builds the provider authorize URL server-side (with PKCE + signed CSRF
        state) and returns it.  The SDK cannot perform the browser redirect
        itself — redirect the user's browser to ``result.redirect_url``.

        After the OAuth flow completes, the server's callback handler
        (``GET /auth/v1/oauth/:provider/callback``) exchanges the code and
        issues Basin JWT + refresh tokens via the normal token body.  If your
        app receives those tokens in a redirect URL, use
        ``parse_oauth_session_from_url`` to decode them from the query string.

        Supported providers (preset): google, github, apple, bitbucket,
        discord, figma, gitlab, linkedin, microsoft (azure_ad), notion, slack,
        spotify, twitch, twitter_x (twitter).  Custom OIDC providers are
        registered server-side; pass their name here.

        Parameters
        ----------
        provider:
            Lower-case provider name, e.g. ``"google"`` or ``"github"``.
        redirect_to:
            URL to redirect to after the flow completes.  Validated server-side
            against the per-project ``redirect_uri`` allowlist.  May be empty
            or a relative path.
        project_id:
            Project for which this OAuth flow is initiated.  Defaults to the
            client's ``default_project_id`` or the project encoded in the JWT.
        """
        pid = self._resolve_project_id(project_id)
        data = self._transport.request_json(
            "GET",
            f"/auth/v1/oauth/{provider}/authorize",
            query=[("project_id", pid), ("redirect_to", redirect_to)],
            auth=False,
        )
        return OAuthAuthorizeResult.from_dict(data)

    # ------------------------------------------------------------------
    # MFA (Phase 5.10.M — crates/basin-rest/src/routes/auth.rs)
    # ------------------------------------------------------------------

    def enroll_factor(
        self,
        factor_type: str,
        *,
        friendly_name: str = "",
    ) -> MfaEnrollResult:
        """POST /auth/v1/factors — begin MFA factor enrollment (JWT-gated).

        Parameters
        ----------
        factor_type:
            ``"totp"`` or ``"webauthn"``.
        friendly_name:
            Human-readable label, e.g. ``"Authenticator App"`` or ``"YubiKey"``.

        Returns
        -------
        TotpEnrollResult
            For ``factor_type="totp"``: contains ``factor_id``, ``secret_b32``
            (base-32 TOTP secret), and ``otpauth_uri`` for QR display.
        WebAuthnEnrollResult
            For ``factor_type="webauthn"``: contains ``factor_id``,
            ``challenge_id``, and ``creation_options_json`` to pass to
            ``navigator.credentials.create()``.

        The factor is ``unverified`` until ``verify_factor`` succeeds.
        """
        data = self._transport.request_json(
            "POST",
            "/auth/v1/factors",
            body={"factor_type": factor_type, "friendly_name": friendly_name},
        )
        if data.get("factor_type") == "webauthn":
            return WebAuthnEnrollResult.from_dict(data)
        return TotpEnrollResult.from_dict(data)

    def list_factors(self) -> list[FactorDescriptor]:
        """GET /auth/v1/factors — list MFA factors for the current user (JWT-gated).

        Returns metadata descriptors only; the secret / credential bytes are
        never included.
        """
        data = self._transport.request_json("GET", "/auth/v1/factors")
        return [FactorDescriptor.from_dict(f) for f in (data or [])]

    def verify_factor(
        self,
        factor_id: str,
        *,
        code: Optional[str] = None,
        attestation: Optional[str] = None,
        challenge_id: Optional[str] = None,
    ) -> VerifyFactorResult:
        """POST /auth/v1/factors/:id/verify — confirm enrollment (JWT-gated).

        Call after ``enroll_factor`` to promote the factor from ``unverified``
        to ``verified``.

        Parameters
        ----------
        factor_id:
            UUID string returned by ``enroll_factor``.
        code:
            TOTP path: 6-digit OTP code from the authenticator app.
        attestation:
            WebAuthn path: attestation JSON from ``navigator.credentials.create()``.
        challenge_id:
            WebAuthn path: ``challenge_id`` returned by ``enroll_factor``.

        Returns
        -------
        VerifyFactorResult
            ``ok=True`` always on success.  ``recovery_codes`` (list of 8 hex
            strings) is present only on the **first** verified factor for a
            user — store these securely, they are shown exactly once.
        """
        body: dict = {}
        if code is not None:
            body["code"] = code
        if attestation is not None:
            body["attestation"] = attestation
        if challenge_id is not None:
            body["challenge_id"] = challenge_id
        data = self._transport.request_json(
            "POST", f"/auth/v1/factors/{factor_id}/verify", body=body
        )
        return VerifyFactorResult.from_dict(data)

    def challenge_factor(self, factor_id: str) -> MfaChallengeResult:
        """POST /auth/v1/factors/:id/challenge — begin a step-up challenge (JWT-gated).

        Parameters
        ----------
        factor_id:
            UUID string of a ``verified`` factor.

        Returns
        -------
        TotpChallengeResult
            For TOTP factors: ``challenge_id`` only — present the TOTP code
            to the user and call ``verify_challenge``.
        WebAuthnChallengeResult
            For WebAuthn factors: ``challenge_id`` + ``request_options_json``
            to pass to ``navigator.credentials.get()``.

        The server auto-detects factor type and returns the appropriate shape.
        """
        data = self._transport.request_json(
            "POST", f"/auth/v1/factors/{factor_id}/challenge", body={}
        )
        if data.get("request_options_json") is not None:
            return WebAuthnChallengeResult.from_dict(data)
        return TotpChallengeResult.from_dict(data)

    def verify_challenge(
        self,
        factor_id: str,
        challenge_id: str,
        *,
        code: Optional[str] = None,
        assertion: Optional[str] = None,
    ) -> Session:
        """POST /auth/v1/factors/:id/challenge/verify — complete step-up → aal2 JWT.

        On success returns a new Session whose access token carries ``aal2``
        in its JWT claims.  Store this session to elevate the client's
        authorization level for operations that require AAL2.

        Parameters
        ----------
        factor_id:
            UUID string of the factor being challenged.
        challenge_id:
            ``challenge_id`` returned by ``challenge_factor``.
        code:
            TOTP path: 6-digit OTP code.
        assertion:
            WebAuthn path: assertion JSON from ``navigator.credentials.get()``.
        """
        body: dict = {"challenge_id": challenge_id}
        if code is not None:
            body["code"] = code
        if assertion is not None:
            body["assertion"] = assertion
        data = self._transport.request_json(
            "POST", f"/auth/v1/factors/{factor_id}/challenge/verify", body=body
        )
        session = Session.from_dict(data)
        self._session = session
        return session

    def unenroll_factor(self, factor_id: str) -> dict:
        """DELETE /auth/v1/factors/:id — unenroll an MFA factor (requires aal2 JWT).

        The caller must hold an ``aal2`` access token (obtained via
        ``verify_challenge``) — the server enforces this and will return
        ``E_FORBIDDEN`` if the session is ``aal1`` only.

        Parameters
        ----------
        factor_id:
            UUID string of the factor to remove.

        Returns
        -------
        dict
            ``{ "ok": True }`` on success.
        """
        return self._transport.request_json(
            "DELETE", f"/auth/v1/factors/{factor_id}"
        )


class AsyncAuthClient:
    """Async auth client for /auth/v1/* routes."""

    def __init__(
        self,
        transport: "AsyncTransport",
        default_project_id: Optional[str] = None,
    ) -> None:
        self._transport = transport
        self._default_project_id = default_project_id
        self._session: Optional[Session] = None

    def get_session(self) -> Optional[Session]:
        return self._session

    def set_session(self, session: Optional[Session]) -> None:
        self._session = session

    def _resolve_project_id(self, project_id: Optional[str] = None) -> str:
        pid = (
            project_id
            or self._default_project_id
            or (self._transport._key and _project_id_from_jwt(self._transport._key or ""))
        )
        if pid is None:
            raise BasinApiError(
                "E_INVALID_REQUEST",
                "project id required: pass project_id or set it in create_async_client()",
                0,
            )
        return pid

    async def access_token(self) -> Optional[str]:
        s = self._session
        if s is None:
            return None
        try:
            expiry = datetime.fromisoformat(
                s.access_expires_at.replace("Z", "+00:00")
            ).timestamp()
            if expiry - _EXPIRY_SKEW_S <= time.time():
                s = await self.refresh_session()
        except (ValueError, AttributeError):
            pass
        return s.access_token

    async def sign_up(
        self,
        *,
        email: str,
        password: str,
        project_id: Optional[str] = None,
    ) -> SignUpResult:
        data = await self._transport.request_json(
            "POST",
            "/auth/v1/signup",
            body={
                "project_id": self._resolve_project_id(project_id),
                "email": email,
                "password": password,
            },
            auth=False,
        )
        return SignUpResult.from_dict(data)

    async def sign_in(
        self,
        *,
        email: str,
        password: str,
        project_id: Optional[str] = None,
    ) -> Session:
        data = await self._transport.request_json(
            "POST",
            "/auth/v1/signin",
            body={
                "project_id": self._resolve_project_id(project_id),
                "email": email,
                "password": password,
            },
            auth=False,
        )
        session = Session.from_dict(data)
        self._session = session
        return session

    async def sign_out(self) -> None:
        """Revoke the current refresh token server-side, then clear the local session.

        Calls POST /auth/v1/signout with the stored refresh token, writing a
        server-side revocation row so that any subsequent refresh_session() call
        with the old token returns 401.

        - If there is no active session this is a no-op (already signed out).
        - 404 responses (older servers without the signout route) are tolerated
          silently — the local session is still cleared.
        - Network errors are tolerated silently — the local session is still
          cleared. The client ends up signed out regardless.
        - The local session is ALWAYS cleared, even when the server call fails,
          so the client always ends up in the signed-out state.
        """
        current = self._session
        # Clear local state first — even if the server call fails the client
        # should be considered signed out.
        self._session = None
        if current is None:
            return
        try:
            await self._transport.request_json(
                "POST",
                "/auth/v1/signout",
                body={"refresh_token": current.refresh_token},
                auth=False,
            )
        except BasinApiError as exc:
            if exc.status == 404:
                # Older server without the signout route — tolerate silently.
                return
            raise
        except Exception:
            # Network error or other transient failure — local clear already done.
            return

    async def refresh_session(self) -> Session:
        current = self._session
        if current is None:
            raise BasinApiError("E_UNAUTHENTICATED", "no session to refresh", 0)
        data = await self._transport.request_json(
            "POST",
            "/auth/v1/refresh",
            body={"refresh_token": current.refresh_token},
            auth=False,
        )
        session = Session.from_dict(data)
        self._session = session
        return session

    async def verify_email(
        self,
        *,
        token: str,
        project_id: Optional[str] = None,
    ) -> dict:
        return await self._transport.request_json(
            "POST",
            "/auth/v1/verify-email",
            body={
                "project_id": self._resolve_project_id(project_id),
                "token": token,
            },
            auth=False,
        )

    async def request_password_reset(
        self,
        *,
        email: str,
        project_id: Optional[str] = None,
    ) -> dict:
        return await self._transport.request_json(
            "POST",
            "/auth/v1/request-password-reset",
            body={
                "project_id": self._resolve_project_id(project_id),
                "email": email,
            },
            auth=False,
        )

    async def reset_password(
        self,
        *,
        token: str,
        new_password: str,
        project_id: Optional[str] = None,
    ) -> dict:
        return await self._transport.request_json(
            "POST",
            "/auth/v1/reset-password",
            body={
                "project_id": self._resolve_project_id(project_id),
                "token": token,
                "new_password": new_password,
            },
            auth=False,
        )

    async def request_magic_link(self, email: str) -> None:
        await self._transport.request_json(
            "POST",
            "/auth/v1/magic-link",
            body={"email": email},
            auth=False,
        )

    async def consume_magic_link(self, token: str) -> Session:
        data = await self._transport.request_json(
            "POST",
            "/auth/v1/magic-link/consume",
            body={"token": token},
            auth=False,
        )
        session = Session.from_dict(data)
        self._session = session
        return session

    async def create_api_key(self, name: str) -> ApiKeyIssued:
        data = await self._transport.request_json(
            "POST", "/auth/v1/api-keys", body={"name": name}
        )
        return ApiKeyIssued.from_dict(data)

    async def list_api_keys(self) -> list[ApiKeyDescriptor]:
        data = await self._transport.request_json("GET", "/auth/v1/api-keys")
        return [ApiKeyDescriptor.from_dict(d) for d in (data or [])]

    async def delete_api_key(self, key_id: int) -> dict:
        return await self._transport.request_json(
            "DELETE", f"/auth/v1/api-keys/{key_id}"
        )

    # ------------------------------------------------------------------
    # OAuth (Phase 5.10.O)
    # ------------------------------------------------------------------

    async def get_oauth_authorize_url(
        self,
        provider: str,
        *,
        redirect_to: str = "",
        project_id: Optional[str] = None,
    ) -> OAuthAuthorizeResult:
        """GET /auth/v1/oauth/:provider/authorize → { redirect_url, state }.

        See ``AuthClient.get_oauth_authorize_url`` for full documentation.
        """
        pid = self._resolve_project_id(project_id)
        data = await self._transport.request_json(
            "GET",
            f"/auth/v1/oauth/{provider}/authorize",
            query=[("project_id", pid), ("redirect_to", redirect_to)],
            auth=False,
        )
        return OAuthAuthorizeResult.from_dict(data)

    # ------------------------------------------------------------------
    # MFA (Phase 5.10.M)
    # ------------------------------------------------------------------

    async def enroll_factor(
        self,
        factor_type: str,
        *,
        friendly_name: str = "",
    ) -> MfaEnrollResult:
        """POST /auth/v1/factors — begin MFA factor enrollment (JWT-gated).

        See ``AuthClient.enroll_factor`` for full documentation.
        """
        data = await self._transport.request_json(
            "POST",
            "/auth/v1/factors",
            body={"factor_type": factor_type, "friendly_name": friendly_name},
        )
        if data.get("factor_type") == "webauthn":
            return WebAuthnEnrollResult.from_dict(data)
        return TotpEnrollResult.from_dict(data)

    async def list_factors(self) -> list[FactorDescriptor]:
        """GET /auth/v1/factors — list MFA factors for the current user (JWT-gated).

        See ``AuthClient.list_factors`` for full documentation.
        """
        data = await self._transport.request_json("GET", "/auth/v1/factors")
        return [FactorDescriptor.from_dict(f) for f in (data or [])]

    async def verify_factor(
        self,
        factor_id: str,
        *,
        code: Optional[str] = None,
        attestation: Optional[str] = None,
        challenge_id: Optional[str] = None,
    ) -> VerifyFactorResult:
        """POST /auth/v1/factors/:id/verify — confirm enrollment (JWT-gated).

        See ``AuthClient.verify_factor`` for full documentation.
        """
        body: dict = {}
        if code is not None:
            body["code"] = code
        if attestation is not None:
            body["attestation"] = attestation
        if challenge_id is not None:
            body["challenge_id"] = challenge_id
        data = await self._transport.request_json(
            "POST", f"/auth/v1/factors/{factor_id}/verify", body=body
        )
        return VerifyFactorResult.from_dict(data)

    async def challenge_factor(self, factor_id: str) -> MfaChallengeResult:
        """POST /auth/v1/factors/:id/challenge — begin step-up challenge (JWT-gated).

        See ``AuthClient.challenge_factor`` for full documentation.
        """
        data = await self._transport.request_json(
            "POST", f"/auth/v1/factors/{factor_id}/challenge", body={}
        )
        if data.get("request_options_json") is not None:
            return WebAuthnChallengeResult.from_dict(data)
        return TotpChallengeResult.from_dict(data)

    async def verify_challenge(
        self,
        factor_id: str,
        challenge_id: str,
        *,
        code: Optional[str] = None,
        assertion: Optional[str] = None,
    ) -> Session:
        """POST /auth/v1/factors/:id/challenge/verify → aal2 Session (JWT-gated).

        See ``AuthClient.verify_challenge`` for full documentation.
        """
        body: dict = {"challenge_id": challenge_id}
        if code is not None:
            body["code"] = code
        if assertion is not None:
            body["assertion"] = assertion
        data = await self._transport.request_json(
            "POST", f"/auth/v1/factors/{factor_id}/challenge/verify", body=body
        )
        session = Session.from_dict(data)
        self._session = session
        return session

    async def unenroll_factor(self, factor_id: str) -> dict:
        """DELETE /auth/v1/factors/:id — unenroll a factor (requires aal2 JWT).

        See ``AuthClient.unenroll_factor`` for full documentation.
        """
        return await self._transport.request_json(
            "DELETE", f"/auth/v1/factors/{factor_id}"
        )
