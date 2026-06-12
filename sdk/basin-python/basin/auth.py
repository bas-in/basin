"""Auth client — bindings for /auth/v1/*.

Route sources (verified against crates/basin-rest/src/server.rs):
- POST /auth/v1/signup                 server.rs:250
- POST /auth/v1/signin                 server.rs:251
- POST /auth/v1/refresh                server.rs:252
- POST /auth/v1/verify-email           server.rs:253
- POST /auth/v1/reset-password         server.rs:254
- POST /auth/v1/request-password-reset server.rs:255
- POST /auth/v1/magic-link             server.rs:262
- POST /auth/v1/magic-link/consume     server.rs:263
- POST|GET /auth/v1/api-keys           server.rs:267
- DELETE /auth/v1/api-keys/:id         server.rs:271

NOTE: There is NO server-side sign-out route (verified against
crates/basin-rest/src/server.rs). sign_out() only clears the local session.
Revocation happens via refresh-token rotation on the server.

Auth is per-project: signup/signin take a project_id in the body.
"""

from __future__ import annotations

import base64
import json
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Optional

from .errors import BasinApiError
from .types import ApiKeyDescriptor, ApiKeyIssued, Session, SignUpResult

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
        """Clear the local session.

        The server exposes no sign-out / token-revoke route (verified against
        crates/basin-rest/src/server.rs). The refresh token simply expires or
        is invalidated by rotation.

        SERVER GAP: no /auth/v1/signout route exists server-side. This method
        is local-only by design.
        """
        self._session = None

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

    def sign_out(self) -> None:
        """Clear the local session (local-only; no server route exists)."""
        self._session = None

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
