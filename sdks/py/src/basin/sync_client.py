from __future__ import annotations

import asyncio
import queue
import threading
from collections.abc import AsyncIterator, Callable, Coroutine, Generator
from typing import Any, TypeVar

from .auth.client import AuthClient
from .auth.types import AuthSession, AuthUser
from .client import ClientOptions, create_client
from .postgrest.builder import QueryBuilder
from .postgrest.types import APIResponse

T = TypeVar("T")

_SENTINEL = object()


class _LoopThread:
    """A single persistent event loop running on a daemon background thread.

    Every sync call is driven on this one loop, so the underlying
    ``httpx.AsyncClient`` connection pool stays bound to a single event loop
    for its entire lifetime.  Using a fresh ``asyncio.run`` per call (as an
    earlier version did) closes the loop after each request and leaves parked
    keep-alive connections bound to a dead loop — the next request then fails
    with ``RuntimeError: Event loop is closed``.
    """

    def __init__(self) -> None:
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._run_forever, name="basin-sync-loop", daemon=True
        )
        self._thread.start()

    def _run_forever(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def run(self, coro: Coroutine[Any, Any, T]) -> T:
        """Run a coroutine to completion on the background loop and return its
        result (re-raising any exception on the calling thread)."""
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result()

    def iterate(
        self, factory: Callable[[], AsyncIterator[Any]]
    ) -> Generator[Any, None, None]:
        """Drive an async iterator on the background loop, yielding its items
        lazily to the synchronous caller via a bounded queue."""
        items: queue.Queue[Any] = queue.Queue(maxsize=64)

        async def pump() -> None:
            try:
                async for item in factory():
                    items.put((False, item))
            except Exception as exc:  # surfaced to the sync side below
                items.put((True, exc))
            finally:
                items.put((False, _SENTINEL))

        future = asyncio.run_coroutine_threadsafe(pump(), self._loop)
        try:
            while True:
                is_error, payload = items.get()
                if is_error:
                    raise payload
                if payload is _SENTINEL:
                    break
                yield payload
        finally:
            future.cancel()

    def close(self) -> None:
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=5.0)


class SyncQueryBuilder:
    """
    Synchronous wrapper around ``QueryBuilder``.

    Call ``execute()`` (or ``single()``/``maybe_single()``) rather than
    ``await``-ing; everything else is the same chainable interface.
    """

    def __init__(self, async_builder: QueryBuilder[Any], loop: _LoopThread) -> None:
        self._b = async_builder
        self._loop = loop

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

    def cursor(self, token: str | None) -> SyncQueryBuilder:
        self._b.cursor(token)
        return self

    def headers(self, extra: dict[str, str]) -> SyncQueryBuilder:
        self._b.headers(extra)
        return self

    # ── terminal ───────────────────────────────────────────────────────

    def execute(self) -> APIResponse[Any]:
        return self._loop.run(self._b.execute())

    def paginate(self, *, page_size: int = 1000) -> Generator[Any, None, None]:
        """Sync generator that walks all pages lazily via cursor pagination."""
        yield from self._loop.iterate(lambda: self._b.paginate(page_size=page_size))

    def stream(self) -> Generator[Any, None, None]:
        """Sync generator that yields NDJSON rows as they arrive."""
        yield from self._loop.iterate(self._b.stream)


class SyncRealtimeChannel:
    """
    Synchronous wrapper around the async ``RealtimeChannel``.

    A dedicated background thread runs an event loop that drives the SSE/WS
    transports; ``.on()`` registers callbacks (invoked on that loop thread) and
    ``.subscribe()``/``.unsubscribe()`` are scheduled onto it.  Keep a reference
    to the channel for as long as you want to receive events; call
    ``.unsubscribe()`` (or ``SyncClient.close()``) to tear it down.
    """

    def __init__(self, async_channel: Any) -> None:
        self._ch = async_channel
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

    def _run_loop(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def _call(self, fn: Callable[[], Any]) -> Any:
        future = asyncio.run_coroutine_threadsafe(self._async_call(fn), self._loop)
        return future.result()

    async def _async_call(self, fn: Callable[[], Any]) -> Any:
        return fn()

    def on(
        self,
        type: str,
        filter: dict[str, Any],
        callback: Callable[..., Any],
    ) -> SyncRealtimeChannel:
        self._call(lambda: self._ch.on(type, filter, callback))
        return self

    def subscribe(self) -> SyncRealtimeChannel:
        self._call(self._ch.subscribe)
        return self

    def unsubscribe(self) -> SyncRealtimeChannel:
        self._call(self._ch.unsubscribe)
        return self

    @property
    def topic(self) -> str:
        topic: str = self._ch.topic
        return topic

    def close(self) -> None:
        try:
            self._call(self._ch.unsubscribe)
        finally:
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join(timeout=5.0)


class SyncAuthClient:
    def __init__(self, async_auth: AuthClient, loop: _LoopThread) -> None:
        self._a = async_auth
        self._loop = loop

    def sign_up(self, *, email: str, password: str, **kw: Any) -> Any:
        return self._loop.run(self._a.sign_up(email=email, password=password, **kw))

    def sign_in_with_password(self, *, email: str, password: str) -> Any:
        return self._loop.run(
            self._a.sign_in_with_password(email=email, password=password)
        )

    def sign_in_with_magic_link(self, *, email: str) -> Any:
        return self._loop.run(self._a.sign_in_with_magic_link(email=email))

    def consume_magic_link(self, *, token: str) -> Any:
        return self._loop.run(self._a.consume_magic_link(token=token))

    def sign_out(self) -> Any:
        return self._loop.run(self._a.sign_out())

    def get_session(self) -> AuthSession | None:
        return self._a.get_session()

    def get_user(self) -> AuthUser | None:
        return self._a.get_user()

    def refresh_session(self) -> Any:
        return self._loop.run(self._a.refresh_session())

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
        options: ClientOptions | None = None,
    ) -> None:
        self._loop = _LoopThread()
        self._async = create_client(base_url, anon_key, options=options)
        self.auth = SyncAuthClient(self._async.auth, self._loop)
        self._channels: list[SyncRealtimeChannel] = []

    def from_(self, table: str) -> SyncQueryBuilder:
        return SyncQueryBuilder(self._async.from_(table), self._loop)

    def channel(self, topic: str) -> SyncRealtimeChannel:
        """Open a realtime channel driven by a background event-loop thread."""
        ch = SyncRealtimeChannel(self._async.channel(topic))
        self._channels.append(ch)
        return ch

    def close(self) -> None:
        for ch in self._channels:
            ch.close()
        self._channels.clear()
        try:
            self._loop.run(self._async.aclose())
        finally:
            self._loop.close()

    def __enter__(self) -> SyncClient:
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()


def create_sync_client(
    url: str,
    key: str,
    *,
    options: ClientOptions | None = None,
) -> SyncClient:
    """Synchronous counterpart to ``create_client``."""
    return SyncClient(url, key, options=options)
