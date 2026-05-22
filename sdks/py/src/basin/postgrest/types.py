from __future__ import annotations

from typing import Any, Generic, List, Optional, TypeVar

T = TypeVar("T")


class APIResponse(Generic[T]):
    """
    Returned by every awaited ``QueryBuilder`` call.

    ``data``       — the rows (or ``None`` on error/204).
    ``error``      — a ``BasinError`` when the call failed, else ``None``.
    ``count``      — row count from ``Content-Range`` (when requested).
    ``status``     — HTTP status code.
    ``next_cursor`` — keyset cursor from an NDJSON sentinel, when present.
    """

    __slots__ = ("data", "error", "count", "status", "next_cursor")

    def __init__(
        self,
        *,
        data: Optional[List[T]],
        error: Any,
        count: Optional[int] = None,
        status: int = 200,
        next_cursor: Optional[str] = None,
    ) -> None:
        self.data = data
        self.error = error
        self.count = count
        self.status = status
        self.next_cursor = next_cursor

    def __repr__(self) -> str:
        return (
            f"APIResponse(data={self.data!r}, error={self.error!r}, "
            f"count={self.count!r}, status={self.status!r})"
        )
