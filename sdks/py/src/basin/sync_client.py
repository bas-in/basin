from __future__ import annotations

import asyncio
from typing import Any, Dict, Generator, Optional, TypeVar

from .client import Client, ClientOptions, create_client
from .auth.types import AuthSession, AuthUser
from .postgrest.builder import QueryBuilder
from .postgrest.types import APIResponse

T = TypeVar("T")


def _run(coro: Any) -> Any:
    """Run a coroutine synchronously using a fresh event loop."""
    return asyncio.run(coro)


class SyncQueryBuilder:
    """
    Synchronous wrapper around ``QueryBuilder``.

    Call ``execute()`` (or ``single()``/``maybe_single()``) rather than
    ``await``-ing; everything else is the same chainable interface.
    """

    def __init__(self, async_builder: QueryBuilder[Any]) -> None:
        self._b = async_builder

    # ── delegating filter/modifier methods ─────────────────────────────

    def select(self, columns: str = "*", **kw: Any) -> SyncQueryBuilder:
        self._b.select(columns, **kw)
        return self

    def insert(self, rows: Any, **kw: Any) -> SyncQueryBuilder:
        self._b.insert(rows, **kw)
        return self

    def update(self, values: Any, **kw: Any) -> SyncQueryBuilder:
        self._b.update(values, **kw)
        return self

    def upsert(self, rows: Any, **kw: Any) -> SyncQueryBuilder:
        self._b.upsert(rows, **kw)
        return self

    def delete(self, **kw: Any) -> SyncQueryBuilder:
        self._b.delete(**kw)
        return self

    def eq(self, column: str, value: Any) -> SyncQueryBuilder:
        self._b.eq(column, value)
        return self

    def neq(self, column: str, value: Any) -> SyncQueryBuilder:
        self._b.neq(column, value)
        return self

    def gt(self, column: str, value: Any) -> SyncQueryBuilder:
        self._b.gt(column, value)
        return self

    def gte(self, column: str, value: Any) -> SyncQueryBuilder:
        self._b.gte(column, value)
        return self

    def lt(self, column: str, value: Any) -> SyncQueryBuilder:
        self._b.lt(column, value)
        return self

    def lte(self, column: str, value: Any) -> SyncQueryBuilder:
        self._b.lte(column, value)
        return self

    def like(self, column: str, pattern: str) -> SyncQueryBuilder:
        self._b.like(column, pattern)
        return self

    def ilike(self, column: str, pattern: str) -> SyncQueryBuilder:
        self._b.ilike(column, pattern)
        return self

    def is_(self, column: str, value: Any) -> SyncQueryBuilder:
        self._b.is_(column, value)
        return self

    def in_(self, column: str, values: Any) -> SyncQueryBuilder:
        self._b.in_(column, values)
        return self

    def contains(self, column: str, value: Any) -> SyncQueryBuilder:
        self._b.contains(column, value)
        return self

    def order(self, column: str, **kw: Any) -> SyncQueryBuilder:
        self._b.order(column, **kw)
        return self

    def limit(self, n: int) -> SyncQueryBuilder:
        self._b.limit(n)
        return self

    def range(self, from_: int, to: int) -> SyncQueryBuilder:
        self._b.range(from_, to)
        return self

    def single(self) -> SyncQueryBuilder:
        self._b.single()
        return self

    def maybe_single(self) -> SyncQueryBuilder:
        self._b.maybe_single()
        return self

    def cursor(self, token: Optional[str]) -> SyncQueryBuilder:
        self._b.cursor(token)
        return self

    def headers(self, extra: Dict[str, str]) -> SyncQueryBuilder:
        self._b.headers(extra)
        return self

    # ── terminal ───────────────────────────────────────────────────────

    def execute(self) -> APIResponse[Any]:
        return _run(self._b.execute())

    def paginate(self, *, page_size: int = 1000) -> Generator[Any, None, None]:
        """Sync generator that walks all pages via cursor pagination."""

        async def collect() -> list[Any]:
            rows = []
            async for row in self._b.paginate(page_size=page_size):
                rows.append(row)
            return rows

        yield from _run(collect())


class SyncAuthClient:
    def __init__(self, async_auth: Any) -> None:
        self._a = async_auth

    def sign_up(self, *, email: str, password: str, **kw: Any) -> Any:
        return _run(self._a.sign_up(email=email, password=password, **kw))

    def sign_in_with_password(self, *, email: str, password: str) -> Any:
        return _run(self._a.sign_in_with_password(email=email, password=password))

    def sign_in_with_magic_link(self, *, email: str) -> Any:
        return _run(self._a.sign_in_with_magic_link(email=email))

    def consume_magic_link(self, *, token: str) -> Any:
        return _run(self._a.consume_magic_link(token=token))

    def sign_out(self) -> Any:
        return _run(self._a.sign_out())

    def get_session(self) -> Optional[AuthSession]:
        return self._a.get_session()

    def get_user(self) -> Optional[AuthUser]:
        return self._a.get_user()

    def refresh_session(self) -> Any:
        return _run(self._a.refresh_session())

    def sign_in_with_oauth(self, *, provider: str, **kw: Any) -> Any:
        return self._a.sign_in_with_oauth(provider=provider, **kw)


class SyncClient:
    """
    Synchronous facade over the async ``Client``.

    Intended for scripts, notebooks, and sync frameworks (Django, Flask).
    The async client is the source of truth; this facade delegates
    every call via ``asyncio.run``.
    """

    def __init__(
        self,
        base_url: str,
        anon_key: str,
        *,
        options: Optional[ClientOptions] = None,
    ) -> None:
        self._async = create_client(base_url, anon_key, options=options)
        self.auth = SyncAuthClient(self._async.auth)

    def from_(self, table: str) -> SyncQueryBuilder:
        return SyncQueryBuilder(self._async.from_(table))

    def close(self) -> None:
        _run(self._async.aclose())

    def __enter__(self) -> SyncClient:
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()


def create_sync_client(
    url: str,
    key: str,
    *,
    options: Optional[ClientOptions] = None,
) -> SyncClient:
    """Synchronous counterpart to ``create_client``."""
    return SyncClient(url, key, options=options)
