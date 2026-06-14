"""Typed errors mirroring basin-rest's error envelope.

Every failed request returns {"code": "...", "message": "..."} with an
appropriate HTTP status (crates/basin-rest/src/errors.rs). `code` is the
stable contract; `message` is human-readable and not promised to be stable.
"""

from __future__ import annotations

from typing import Optional

# Stable error codes emitted by basin-rest (errors.rs ErrorCode::as_str).
ERROR_CODES = (
    "E_UNAUTHENTICATED",
    "E_FORBIDDEN",
    "E_NOT_FOUND",
    "E_INVALID_REQUEST",
    "E_RATE_LIMITED",
    "E_ENGINE_UNSUPPORTED",
    "E_INTERNAL",
    "E_EMAIL_DISABLED",
    "E_REVOKED_TOKEN",
)


class BasinError(Exception):
    """Base for all Basin SDK errors."""


class BasinApiError(BasinError):
    """Raised for any non-2xx response from a Basin server.

    Attributes
    ----------
    code:
        Stable error code from the response body (``E_*``).  Known codes are
        listed in :data:`ERROR_CODES`; unknown codes pass through as plain
        strings so a newer server does not break an older SDK.
    status:
        HTTP status of the response (0 when the error was synthesised
        client-side without a round-trip).
    message:
        Human-readable detail — do **not** match on this; use ``code``.
    sqlstate:
        5-character Postgres SQLSTATE code (e.g. ``"23505"`` for a unique
        violation), present for SQL-layer errors; ``None`` otherwise.
    """

    def __init__(
        self,
        code: str,
        message: str,
        status: int,
        sqlstate: Optional[str] = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.status = status
        self.sqlstate = sqlstate

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"BasinApiError(code={self.code!r}, status={self.status}, "
            f"sqlstate={self.sqlstate!r}, message={str(self)!r})"
        )

    @classmethod
    def from_response_body(cls, body: dict, status: int) -> "BasinApiError":
        code = body.get("code", "E_UNKNOWN")
        if not isinstance(code, str):
            code = "E_UNKNOWN"
        message = body.get("message", f"HTTP {status}")
        if not isinstance(message, str):
            message = f"HTTP {status}"
        sqlstate = body.get("sqlstate")
        if not isinstance(sqlstate, str):
            sqlstate = None
        return cls(code, message, status, sqlstate)


class BasinNetworkError(BasinError):
    """Raised when the transport layer fails (connection refused, timeout,
    etc.) before a server response is received."""
