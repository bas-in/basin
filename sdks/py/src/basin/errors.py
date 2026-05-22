from __future__ import annotations

from typing import Any


BasinErrorCode = str


class BasinError(Exception):
    """
    Single error type for all basin SDK failures. Branch on `.code`:

      ``network``           — transport-level failure (DNS, TLS, timeout)
      ``invalid_response``  — server returned non-JSON or a malformed envelope
      ``unauthorized``      — 401/403
      ``forbidden``         — 403 (subset; engine may emit either code)
      ``not_found``         — 404
      ``invalid_request``   — 400/other 4xx
      ``conflict``          — 409
      ``rate_limited``      — 429
      ``internal``          — 5xx server error
      ``unsupported``       — feature not supported by this engine version
      ``token_expired``     — access or refresh token has expired
      ``not_implemented``   — SDK method exists but engine route is not yet live
    """

    code: BasinErrorCode
    message: str
    status: int | None
    details: Any

    def __init__(
        self,
        code: BasinErrorCode,
        message: str,
        *,
        status: int | None = None,
        details: Any = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status
        self.details = details

    def __str__(self) -> str:
        return f"{self.code}: {self.message}"

    def __repr__(self) -> str:
        return f"BasinError(code={self.code!r}, message={self.message!r}, status={self.status!r})"

    @classmethod
    def from_response(cls, resp: Any, *, body: Any = None) -> BasinError:
        """
        Build a ``BasinError`` from an ``httpx.Response``.  Extracts the
        human-readable message from ``{message|error|msg}`` when the body is
        JSON; falls back to a generic phrase otherwise.
        """
        status: int = resp.status_code
        code = _code_for_status(status)

        message: str = f"request failed (HTTP {status})"
        extracted_details: Any = None

        if body is None:
            try:
                body = resp.json()
            except Exception:
                try:
                    text = resp.text
                    if text:
                        message = text
                except Exception:
                    pass
                return cls(code, message, status=status)

        if isinstance(body, dict):
            msg_candidate = body.get("message") or body.get("error") or body.get("msg")
            if isinstance(msg_candidate, str) and msg_candidate:
                message = msg_candidate
            code_candidate = body.get("code")
            if isinstance(code_candidate, str) and code_candidate:
                code = code_candidate
            extracted_details = body.get("details")

        return cls(code, message, status=status, details=extracted_details)


def _code_for_status(status: int) -> str:
    if status == 401:
        return "unauthorized"
    if status == 403:
        return "forbidden"
    if status == 404:
        return "not_found"
    if status == 409:
        return "conflict"
    if status == 429:
        return "rate_limited"
    if status == 501:
        return "not_implemented"
    if status >= 500:
        return "internal"
    return "invalid_request"
