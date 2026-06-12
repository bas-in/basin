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
# Functions (ANY /fn/v1/:name, crates/basin-rest/src/routes/fn_handler.rs)
# ---------------------------------------------------------------------------


@dataclass
class FunctionInvokeResult:
    """Proxied response from an HTTP-handler function."""

    status: int
    headers: dict[str, str]
    data: Any
