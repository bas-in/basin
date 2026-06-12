"""Shared wire types mirrored from the Rust handlers they bind to."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Optional, Union


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------


@dataclass
class Session:
    """Token body returned by /auth/v1/signin, /auth/v1/refresh,
    /auth/v1/magic-link/consume (crates/basin-rest/src/routes/auth.rs
    token_body)."""

    access_token: str
    refresh_token: str
    access_expires_at: str  # RFC 3339
    refresh_expires_at: str  # RFC 3339

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Session":
        return cls(
            access_token=d["access_token"],
            refresh_token=d["refresh_token"],
            access_expires_at=d["access_expires_at"],
            refresh_expires_at=d["refresh_expires_at"],
        )


@dataclass
class SignUpResult:
    """POST /auth/v1/signup response."""

    ok: bool
    user_id: str

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "SignUpResult":
        return cls(ok=d["ok"], user_id=d["user_id"])


@dataclass
class ApiKeyIssued:
    """POST /auth/v1/api-keys response (the secret is shown exactly once)."""

    id: int
    name: str
    secret: str
    created_at: str

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "ApiKeyIssued":
        return cls(
            id=d["id"],
            name=d["name"],
            secret=d["secret"],
            created_at=d["created_at"],
        )


@dataclass
class ApiKeyDescriptor:
    """GET /auth/v1/api-keys element."""

    id: int
    name: str
    created_at: str
    last_used_at: Optional[str]
    revoked_at: Optional[str]

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "ApiKeyDescriptor":
        return cls(
            id=d["id"],
            name=d["name"],
            created_at=d["created_at"],
            last_used_at=d.get("last_used_at"),
            revoked_at=d.get("revoked_at"),
        )


# ---------------------------------------------------------------------------
# Data / query
# ---------------------------------------------------------------------------

Row = dict[str, Any]


@dataclass
class ExecTag:
    """Non-row result for writes: { ok: true, tag: "..." }."""

    ok: bool
    tag: str

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "ExecTag":
        return cls(ok=d["ok"], tag=d["tag"])


@dataclass
class Page:
    """Wrapped GET response when limit or cursor is supplied."""

    rows: list[Row]
    next_cursor: Optional[str]


# ---------------------------------------------------------------------------
# Realtime wire frames (crates/basin-realtime/src/ws.rs ClientMsg/ServerMsg)
# ---------------------------------------------------------------------------

ChangeOp = Literal["INSERT", "UPDATE", "DELETE"]


@dataclass
class RealtimeEvent:
    """{"type":"event",...} server frame."""

    type: str
    project: str
    table: str
    op: ChangeOp
    seq: int
    before: Optional[dict[str, Any]] = None
    after: Optional[dict[str, Any]] = None

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "RealtimeEvent":
        return cls(
            type=d["type"],
            project=d["project"],
            table=d["table"],
            op=d["op"],
            seq=d["seq"],
            before=d.get("before"),
            after=d.get("after"),
        )


@dataclass
class RealtimeErrorFrame:
    type: str
    code: str
    table: str
    missed: Optional[int] = None

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "RealtimeErrorFrame":
        return cls(
            type=d["type"],
            code=d["code"],
            table=d["table"],
            missed=d.get("missed"),
        )


@dataclass
class RealtimeGapFrame:
    """Reconnect-resume gap: missed events were evicted; cold re-sync needed."""

    type: str
    table: str
    last_event_id: int
    oldest_in_ring: int
    newest_in_ring: int

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "RealtimeGapFrame":
        return cls(
            type=d["type"],
            table=d["table"],
            last_event_id=d["last_event_id"],
            oldest_in_ring=d["oldest_in_ring"],
            newest_in_ring=d["newest_in_ring"],
        )


# ---------------------------------------------------------------------------
# Storage (crates/basin-rest/src/routes/storage.rs, storage_sign.rs)
# ---------------------------------------------------------------------------


@dataclass
class Bucket:
    id: str
    name: str
    public: bool
    file_size_limit: Optional[int]
    allowed_mime_types: list[str]
    created_at: str
    updated_at: str

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Bucket":
        return cls(
            id=d["id"],
            name=d["name"],
            public=d["public"],
            file_size_limit=d.get("file_size_limit"),
            allowed_mime_types=d.get("allowed_mime_types", []),
            created_at=d["created_at"],
            updated_at=d["updated_at"],
        )


@dataclass
class StorageObject:
    id: str
    bucket_id: str
    path: str
    size: int
    mime_type: Optional[str]
    metadata: Any
    owner: Optional[str]
    etag: str
    created_at: str
    updated_at: str

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "StorageObject":
        return cls(
            id=d["id"],
            bucket_id=d["bucket_id"],
            path=d["path"],
            size=d["size"],
            mime_type=d.get("mime_type"),
            metadata=d.get("metadata"),
            owner=d.get("owner"),
            etag=d["etag"],
            created_at=d["created_at"],
            updated_at=d["updated_at"],
        )


@dataclass
class SignedUrl:
    """POST /storage/v1/object/sign/upload/:bucket/*path response.

    signedUrl is server-relative; absolute_url is the full URL built by
    prepending the base URL.
    """

    signed_url: str  # server-relative URL
    expires_at: str  # RFC 3339
    absolute_url: str  # full URL (base + signed_url)

    @classmethod
    def from_dict(cls, d: dict[str, Any], base_url: str) -> "SignedUrl":
        rel = d.get("signedUrl") or d.get("signed_url", "")
        return cls(
            signed_url=rel,
            expires_at=d.get("expiresAt") or d.get("expires_at", ""),
            absolute_url=base_url + rel,
        )


# ---------------------------------------------------------------------------
# OAuth (crates/basin-rest/src/routes/auth.rs oauth_authorize / oauth_callback)
# ---------------------------------------------------------------------------


@dataclass
class OAuthAuthorizeResult:
    """GET /auth/v1/oauth/:provider/authorize response.

    The SDK cannot perform the browser redirect dance itself.  Redirect the
    user's browser to ``redirect_url``; after the provider redirects back to
    Basin's callback endpoint the server exchanges the code and issues tokens
    — the SDK method ``get_oauth_authorize_url`` returns this object.  The
    server-side callback is ``GET /auth/v1/oauth/:provider/callback`` and it
    returns a standard token body (Session); use ``parse_oauth_callback_url``
    to extract those tokens from the final redirect URL if your client-side
    code receives them via a URL fragment or query string.
    """

    redirect_url: str
    """Full provider authorize URL — redirect the browser here."""
    state: str
    """CSRF state value included in redirect_url; echoed back on callback."""

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "OAuthAuthorizeResult":
        return cls(redirect_url=d["redirect_url"], state=d["state"])


# ---------------------------------------------------------------------------
# MFA (crates/basin-rest/src/routes/auth.rs enroll_factor / factors*)
# ---------------------------------------------------------------------------


@dataclass
class TotpEnrollResult:
    """POST /auth/v1/factors response for factor_type="totp".

    The factor is ``unverified`` until ``POST /auth/v1/factors/:id/verify``
    succeeds with the first valid OTP code.
    """

    factor_id: str
    factor_type: str  # "totp"
    secret_b32: str
    """Base-32 TOTP secret — display in the QR code or give to an auth app."""
    otpauth_uri: str
    """``otpauth://totp/...`` URI suitable for QR code display."""

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "TotpEnrollResult":
        return cls(
            factor_id=d["factor_id"],
            factor_type=d["factor_type"],
            secret_b32=d["secret_b32"],
            otpauth_uri=d["otpauth_uri"],
        )


@dataclass
class WebAuthnEnrollResult:
    """POST /auth/v1/factors response for factor_type="webauthn".

    Pass ``creation_options_json`` to ``navigator.credentials.create()`` (via
    a JS bridge) and then call ``verify_factor`` with the attestation response
    and the returned ``challenge_id``.
    """

    factor_id: str
    factor_type: str  # "webauthn"
    challenge_id: str
    creation_options_json: str
    """Serialised ``PublicKeyCredentialCreationOptions`` JSON."""

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "WebAuthnEnrollResult":
        return cls(
            factor_id=d["factor_id"],
            factor_type=d["factor_type"],
            challenge_id=d["challenge_id"],
            creation_options_json=d["creation_options_json"],
        )


# Union type for enroll_factor return
MfaEnrollResult = Union["TotpEnrollResult", "WebAuthnEnrollResult"]


@dataclass
class FactorDescriptor:
    """Element of GET /auth/v1/factors array.

    Mirrors ``FactorDescriptor`` in ``crates/basin-auth/src/mfa.rs``.
    The secret / credential bytes are never included.
    """

    id: str
    factor_type: str  # "totp" | "webauthn"
    status: str  # "unverified" | "verified"
    friendly_name: str
    created_at: str  # RFC 3339
    updated_at: str  # RFC 3339

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "FactorDescriptor":
        return cls(
            id=d["id"],
            factor_type=d["factor_type"],
            status=d["status"],
            friendly_name=d["friendly_name"],
            created_at=d["created_at"],
            updated_at=d["updated_at"],
        )


@dataclass
class VerifyFactorResult:
    """POST /auth/v1/factors/:id/verify response.

    ``recovery_codes`` is only present on the **first** verified factor for a
    user — it will be ``None`` for all subsequent factor verifications.
    Store these codes securely; they are shown exactly once.
    """

    ok: bool
    recovery_codes: Optional[list[str]]

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "VerifyFactorResult":
        return cls(ok=d.get("ok", True), recovery_codes=d.get("recovery_codes"))


@dataclass
class TotpChallengeResult:
    """POST /auth/v1/factors/:id/challenge response for a TOTP factor."""

    challenge_id: str

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "TotpChallengeResult":
        return cls(challenge_id=d["challenge_id"])


@dataclass
class WebAuthnChallengeResult:
    """POST /auth/v1/factors/:id/challenge response for a WebAuthn factor.

    Pass ``request_options_json`` to ``navigator.credentials.get()`` to
    obtain the assertion, then call ``verify_challenge`` with the result.
    """

    challenge_id: str
    request_options_json: Optional[str]
    """Serialised ``PublicKeyCredentialRequestOptions`` JSON; present for
    WebAuthn factors, absent for TOTP."""

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "WebAuthnChallengeResult":
        return cls(
            challenge_id=d["challenge_id"],
            request_options_json=d.get("request_options_json"),
        )


# Unified challenge result — callers can inspect challenge_id from both.
MfaChallengeResult = Union["TotpChallengeResult", "WebAuthnChallengeResult"]


# ---------------------------------------------------------------------------
# Functions (ANY /fn/v1/:name, crates/basin-rest/src/routes/fn_handler.rs)
# ---------------------------------------------------------------------------


@dataclass
class FunctionInvokeResult:
    """Proxied response from an HTTP-handler function."""

    status: int
    headers: dict[str, str]
    data: Any
