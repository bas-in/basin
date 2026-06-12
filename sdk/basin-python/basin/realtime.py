"""Realtime WebSocket client for Basin.

Connects to ``GET /realtime/v1/ws/:project`` and delivers change events as
async generators.

Protocol source of truth: ``crates/basin-realtime/src/ws.rs``
(``ClientMsg`` / ``ServerMsg`` + ``presence.rs`` serialize_presence_* helpers).

Auth: ``Sec-WebSocket-Protocol: basin-v1, <token>`` subprotocol header — the
server also accepts ``Authorization: Bearer`` but browsers and the ``websockets``
library make the subprotocol form simpler for client-originated connections.

Requires the optional ``realtime`` extra::

    pip install "basin-sdk[realtime]"

Usage::

    import asyncio
    from basin import create_async_client

    async def main():
        async with create_async_client("http://localhost:8080", "my-key") as basin:
            async for event in basin.realtime.listen("orders"):
                print(event.op, event.after)

    asyncio.run(main())
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from typing import (
    Any,
    AsyncGenerator,
    Callable,
    Dict,
    List,
    Optional,
    Awaitable,
)

from .errors import BasinApiError
from .types import ChangeOp, RealtimeEvent, RealtimeErrorFrame, RealtimeGapFrame

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Optional websockets import — deferred so the core package works without it.
# ---------------------------------------------------------------------------

_MISSING_EXTRA = (
    "The 'realtime' extra is required for WebSocket support.\n"
    "Install it with:  pip install 'basin-sdk[realtime]'"
)


def _import_websockets() -> Any:
    try:
        import websockets.asyncio.client as _ws  # websockets ≥ 13

        return _ws
    except ImportError:
        pass
    try:
        import websockets.legacy.client as _ws  # websockets 10–12

        return _ws
    except ImportError:
        pass
    raise ImportError(_MISSING_EXTRA)


# ---------------------------------------------------------------------------
# Additional wire frame types (presence)
# ---------------------------------------------------------------------------


@dataclass
class PresenceEntry:
    """A single presence member — shape mirrors ``presence.rs PresenceEntry``."""

    client_id: str
    metadata: Any = None
    joined_at: Optional[str] = None

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "PresenceEntry":
        return cls(
            client_id=d.get("client_id", ""),
            metadata=d.get("metadata"),
            joined_at=d.get("joined_at"),
        )


@dataclass
class PresenceStateFrame:
    """``{"type":"presence_state","channel":…,"presences":[…]}``"""

    type: str
    channel: str
    presences: List[PresenceEntry] = field(default_factory=list)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "PresenceStateFrame":
        return cls(
            type=d["type"],
            channel=d["channel"],
            presences=[PresenceEntry.from_dict(p) for p in d.get("presences", [])],
        )


@dataclass
class PresenceDiffFrame:
    """``{"type":"presence_diff","channel":…,"joins":[…],"leaves":[…]}``"""

    type: str
    channel: str
    joins: List[PresenceEntry] = field(default_factory=list)
    leaves: List[PresenceEntry] = field(default_factory=list)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "PresenceDiffFrame":
        return cls(
            type=d["type"],
            channel=d["channel"],
            joins=[PresenceEntry.from_dict(p) for p in d.get("joins", [])],
            leaves=[PresenceEntry.from_dict(p) for p in d.get("leaves", [])],
        )


@dataclass
class PresenceErrorFrame:
    """``{"type":"presenceerror","code":…,"channel":…,"message":…}``

    Note: the server serialises ``PresenceError`` variant with
    ``rename_all = "lowercase"`` → wire type is ``"presenceerror"``
    (no underscore).  The JS SDK matches this (types.ts line 106).
    """

    type: str
    code: str
    channel: str
    message: str

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "PresenceErrorFrame":
        return cls(
            type=d["type"],
            code=d["code"],
            channel=d["channel"],
            message=d.get("message", ""),
        )


@dataclass
class SubscribedFrame:
    """``{"type":"subscribed","table":…}``"""

    type: str
    table: str

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "SubscribedFrame":
        return cls(type=d["type"], table=d["table"])


@dataclass
class UnsubscribedFrame:
    """``{"type":"unsubscribed","table":…}``"""

    type: str
    table: str

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "UnsubscribedFrame":
        return cls(type=d["type"], table=d["table"])


# Union of all server → client frame types.
ServerFrame = (
    RealtimeEvent
    | RealtimeErrorFrame
    | RealtimeGapFrame
    | SubscribedFrame
    | UnsubscribedFrame
    | PresenceStateFrame
    | PresenceDiffFrame
    | PresenceErrorFrame
)

# ---------------------------------------------------------------------------
# Frame deserializer
# ---------------------------------------------------------------------------

_FRAME_CTORS: dict[str, Callable[[dict[str, Any]], Any]] = {
    "event": RealtimeEvent.from_dict,
    "error": RealtimeErrorFrame.from_dict,
    "gap": RealtimeGapFrame.from_dict,
    "subscribed": SubscribedFrame.from_dict,
    "unsubscribed": UnsubscribedFrame.from_dict,
    "presence_state": PresenceStateFrame.from_dict,
    "presence_diff": PresenceDiffFrame.from_dict,
    # "presenceerror" — no underscore, from rename_all="lowercase" on PresenceError
    "presenceerror": PresenceErrorFrame.from_dict,
}


def _parse_frame(raw: str) -> Optional[Any]:
    """Parse a raw text frame from the server into a typed dataclass.

    Returns None for unknown or malformed frames (caller should log and skip).
    """
    try:
        d = json.loads(raw)
    except json.JSONDecodeError:
        log.debug("realtime: ignoring non-JSON frame: %.120s", raw)
        return None
    if not isinstance(d, dict):
        return None
    ftype = d.get("type")
    if not isinstance(ftype, str):
        return None
    ctor = _FRAME_CTORS.get(ftype)
    if ctor is None:
        log.debug("realtime: unknown frame type %r; ignoring", ftype)
        return None
    try:
        return ctor(d)
    except (KeyError, TypeError) as exc:
        log.debug("realtime: failed to deserialise %r frame: %s", ftype, exc)
        return None


# ---------------------------------------------------------------------------
# Reconnect / backoff parameters
# ---------------------------------------------------------------------------

_BACKOFF_BASE = 0.5  # seconds
_BACKOFF_MAX = 30.0  # seconds
_BACKOFF_FACTOR = 2.0


def _backoff(attempt: int) -> float:
    delay = _BACKOFF_BASE * (_BACKOFF_FACTOR ** attempt)
    return min(delay, _BACKOFF_MAX)


# ---------------------------------------------------------------------------
# RealtimeClient
# ---------------------------------------------------------------------------


class RealtimeClient:
    """Async WebSocket client for ``GET /realtime/v1/ws/:project``.

    Obtain via ``AsyncBasinClient.realtime`` — do not construct directly.

    The primary API is the async-generator ``listen()``.  For callback-based
    consumption see ``subscribe()``.

    Reconnect behaviour
    -------------------
    On any unexpected disconnect the client waits with exponential backoff
    (0.5 s → 1 s → 2 s … capped at 30 s) and re-connects.  All active
    subscriptions are reinstated automatically on reconnect.  Callers can
    pass ``last_event_id`` to ``listen()`` / ``subscribe()`` for server-side
    replay of missed events (closes basin #54 P0).

    Connection management
    ---------------------
    The connection is opened lazily on the first ``listen()`` / ``subscribe()``
    / ``connect()`` call.  Call ``disconnect()`` to close it.  The client is
    **not** thread-safe; use it from a single asyncio event loop.

    Example::

        async with create_async_client(url, key) as basin:
            async for event in basin.realtime.listen("orders"):
                print(event.op, event.table, event.after)
    """

    def __init__(
        self,
        base_url: str,
        get_token: Callable[[], Awaitable[Optional[str]]],
        project_id: Callable[[], Optional[str]],
        *,
        connect_fn: Optional[Any] = None,
    ) -> None:
        """
        Parameters
        ----------
        base_url:
            HTTP base URL of the Basin server (``http://…`` or ``https://…``).
        get_token:
            Async callable returning the current Bearer token.  May return
            ``None`` when there is no session (the static API key is then used
            instead).
        project_id:
            Callable returning the project ULID, or ``None`` when unknown.
        connect_fn:
            Injected WebSocket connect coroutine (for tests).  When ``None``
            the real ``websockets`` library is used.
        """
        self._base_url = base_url
        self._get_token = get_token
        self._project_id = project_id
        self._connect_fn = connect_fn  # injectable for tests

        # Active WebSocket connection (None when disconnected).
        self._ws: Optional[Any] = None

        # Pending ack futures: table → Future
        self._pending_subscribed: Dict[str, asyncio.Future] = {}
        self._pending_unsubscribed: Dict[str, asyncio.Future] = {}

        # Subscription bookkeeping: table → (opts, list[Queue])
        # Each active listen() call gets its own queue.
        self._subscriptions: Dict[
            str,
            Dict[str, Any],  # options (filter, last_event_id)
        ] = {}
        self._queues: Dict[str, List[asyncio.Queue]] = {}

        # Task running the receive loop.
        self._recv_task: Optional[asyncio.Task] = None

        # Protect against concurrent connect() calls.
        self._connect_lock = asyncio.Lock()

        # Reconnect control.
        self._closed = False  # permanent close requested by caller

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """Open the WebSocket connection.  No-op if already connected."""
        async with self._connect_lock:
            if self._ws is not None:
                return
            await self._do_connect()

    async def _do_connect(self) -> None:
        """Internal connect — must be called with ``_connect_lock`` held."""
        project = self._project_id()
        if project is None:
            raise BasinApiError(
                "E_INVALID_REQUEST",
                "realtime requires a project id — pass project_id to create_async_client()",
                0,
            )

        token = await self._get_token()
        if token is None:
            raise BasinApiError(
                "E_UNAUTHENTICATED",
                "realtime requires a JWT or API key",
                0,
            )

        # Build the ws:// URL from the http:// base.
        ws_base = self._base_url.rstrip("/")
        ws_base = ws_base.replace("https://", "wss://").replace("http://", "ws://")
        url = f"{ws_base}/realtime/v1/ws/{project}"

        # Auth via subprotocol (mirrors JS SDK — browser-friendly path that
        # does not require a custom Authorization header).
        subprotocols = ["basin-v1", token]

        connect_fn = self._connect_fn
        if connect_fn is None:
            ws_module = _import_websockets()
            connect_fn = ws_module.connect

        log.debug("realtime: connecting to %s", url)
        # websockets.connect() returns a context manager; we use __aenter__.
        cm = connect_fn(url, subprotocols=subprotocols)
        self._ws = await cm.__aenter__()
        self._ws_cm = cm

        # Start the receive loop.
        self._recv_task = asyncio.ensure_future(self._recv_loop())
        log.debug("realtime: connected")

    async def disconnect(self) -> None:
        """Close the WebSocket connection and cancel all listen() generators."""
        self._closed = True
        if self._recv_task is not None:
            self._recv_task.cancel()
            try:
                await self._recv_task
            except asyncio.CancelledError:
                pass
            self._recv_task = None
        await self._close_ws()
        # Drain all queues with a sentinel so generators exit cleanly.
        self._drain_queues(None)
        log.debug("realtime: disconnected")

    async def _close_ws(self) -> None:
        if self._ws is not None:
            try:
                await self._ws_cm.__aexit__(None, None, None)
            except Exception:
                pass
            self._ws = None
            self._ws_cm = None

    @property
    def is_connected(self) -> bool:
        return self._ws is not None

    # ------------------------------------------------------------------
    # Send helpers
    # ------------------------------------------------------------------

    async def _send(self, msg: dict) -> None:
        if self._ws is None:
            raise BasinApiError("E_UNKNOWN", "realtime socket is not open", 0)
        await self._ws.send(json.dumps(msg))

    # ------------------------------------------------------------------
    # Receive loop
    # ------------------------------------------------------------------

    async def _recv_loop(self) -> None:
        """Read frames from the WebSocket and dispatch them until closed."""
        try:
            async for raw in self._ws:
                if not isinstance(raw, str):
                    continue  # binary frames not used in this protocol
                frame = _parse_frame(raw)
                if frame is None:
                    continue
                self._dispatch(frame)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.debug("realtime: recv loop exited: %s", exc)
        finally:
            self._ws = None

        # Unexpected disconnect — attempt reconnect unless closed by caller.
        if not self._closed:
            asyncio.ensure_future(self._reconnect())

    def _dispatch(self, frame: Any) -> None:
        """Route an incoming server frame to the appropriate listener."""
        if isinstance(frame, RealtimeEvent):
            for q in self._queues.get(frame.table, []):
                q.put_nowait(frame)
        elif isinstance(frame, (RealtimeErrorFrame, RealtimeGapFrame)):
            # Route errors/gaps to the table's queues too, so generators see them.
            table = frame.table
            for q in self._queues.get(table, []):
                q.put_nowait(frame)
        elif isinstance(frame, SubscribedFrame):
            fut = self._pending_subscribed.pop(frame.table, None)
            if fut is not None and not fut.done():
                fut.set_result(None)
        elif isinstance(frame, UnsubscribedFrame):
            fut = self._pending_unsubscribed.pop(frame.table, None)
            if fut is not None and not fut.done():
                fut.set_result(None)
        # Presence / error frames — callers get them via queues too.
        elif isinstance(frame, PresenceStateFrame):
            for q in self._queues.get(frame.channel, []):
                q.put_nowait(frame)
        elif isinstance(frame, PresenceDiffFrame):
            for q in self._queues.get(frame.channel, []):
                q.put_nowait(frame)
        elif isinstance(frame, PresenceErrorFrame):
            for q in self._queues.get(frame.channel, []):
                q.put_nowait(frame)

    def _drain_queues(self, sentinel: Any) -> None:
        for queues in self._queues.values():
            for q in queues:
                q.put_nowait(sentinel)

    # ------------------------------------------------------------------
    # Reconnect
    # ------------------------------------------------------------------

    async def _reconnect(self) -> None:
        attempt = 0
        while not self._closed:
            delay = _backoff(attempt)
            log.debug("realtime: reconnect in %.1fs (attempt %d)", delay, attempt + 1)
            await asyncio.sleep(delay)
            attempt += 1
            try:
                async with self._connect_lock:
                    if self._ws is not None:
                        return  # another coroutine already reconnected
                    await self._do_connect()
                # Re-issue all active subscriptions.
                await self._resubscribe()
                return
            except Exception as exc:
                log.debug("realtime: reconnect attempt %d failed: %s", attempt, exc)

    async def _resubscribe(self) -> None:
        """Re-send subscribe messages for all active subscriptions after reconnect."""
        for table, opts in list(self._subscriptions.items()):
            try:
                await self._send_subscribe(table, opts)
                # Wait for the server ack (with a timeout).
                fut: asyncio.Future = asyncio.get_event_loop().create_future()
                self._pending_subscribed[table] = fut
                await asyncio.wait_for(asyncio.shield(fut), timeout=10.0)
            except Exception as exc:
                log.debug("realtime: resubscribe %r failed: %s", table, exc)

    # ------------------------------------------------------------------
    # Subscribe / unsubscribe
    # ------------------------------------------------------------------

    async def _send_subscribe(self, table: str, opts: dict) -> None:
        msg: dict[str, Any] = {"type": "subscribe", "table": table}
        if opts.get("filter"):
            msg["filter"] = opts["filter"]
        if opts.get("last_event_id") is not None:
            msg["last_event_id"] = opts["last_event_id"]
        await self._send(msg)

    async def _subscribe(self, table: str, opts: dict) -> None:
        """Send a subscribe message and wait for the server ack."""
        await self.connect()
        self._subscriptions[table] = opts
        if table not in self._queues:
            self._queues[table] = []

        loop = asyncio.get_event_loop()
        fut: asyncio.Future = loop.create_future()
        self._pending_subscribed[table] = fut

        await self._send_subscribe(table, opts)
        await asyncio.wait_for(fut, timeout=30.0)

    async def _unsubscribe(self, table: str) -> None:
        """Send an unsubscribe message and wait for the server ack."""
        if self._ws is None:
            return
        loop = asyncio.get_event_loop()
        fut: asyncio.Future = loop.create_future()
        self._pending_unsubscribed[table] = fut
        await self._send({"type": "unsubscribe", "table": table})
        try:
            await asyncio.wait_for(fut, timeout=10.0)
        except asyncio.TimeoutError:
            pass
        self._subscriptions.pop(table, None)

    # ------------------------------------------------------------------
    # Public API — async generator (primary)
    # ------------------------------------------------------------------

    async def listen(
        self,
        table: str,
        *,
        filter: Optional[str] = None,
        last_event_id: Optional[int] = None,
    ) -> AsyncGenerator[RealtimeEvent | RealtimeErrorFrame | RealtimeGapFrame, None]:
        """Async generator that yields change events for ``table``.

        Subscribes to the table on entry and unsubscribes on exit.  Multiple
        concurrent ``listen()`` calls for the same table share one subscription
        (the server deduplicates idempotently).

        Parameters
        ----------
        table:
            Table name to subscribe to.
        filter:
            Optional server-side SQL predicate, e.g. ``"NEW.status = 'paid'"``.
            The server parses and validates this; invalid predicates arrive as
            ``RealtimeErrorFrame(code="invalid_filter")``.
        last_event_id:
            Reconnect cursor — the last ``seq`` the client successfully
            processed.  When given, the server replays missed events from its
            replay ring, or emits a ``RealtimeGapFrame`` if the ring has been
            evicted.

        Yields
        ------
        ``RealtimeEvent`` for INSERT / UPDATE / DELETE rows, or
        ``RealtimeErrorFrame`` / ``RealtimeGapFrame`` for protocol-level
        notifications.

        Example::

            async for event in client.listen("orders", filter="NEW.status='paid'"):
                print(event.after)
        """
        opts = {"filter": filter, "last_event_id": last_event_id}
        queue: asyncio.Queue = asyncio.Queue()

        if table not in self._queues:
            self._queues[table] = []
        self._queues[table].append(queue)

        try:
            await self._subscribe(table, opts)
            while True:
                item = await queue.get()
                if item is None:
                    # Sentinel — client or server closed.
                    break
                yield item
        finally:
            self._queues.get(table, [None])  # keep reference alive
            queues = self._queues.get(table, [])
            if queue in queues:
                queues.remove(queue)
            if not queues and table in self._subscriptions:
                await self._unsubscribe(table)

    # ------------------------------------------------------------------
    # Public API — callback-based (adapter over listen())
    # ------------------------------------------------------------------

    async def subscribe(
        self,
        table: str,
        callback: Callable[[RealtimeEvent | RealtimeErrorFrame | RealtimeGapFrame], Any],
        *,
        filter: Optional[str] = None,
        last_event_id: Optional[int] = None,
    ) -> "ChannelHandle":
        """Subscribe to ``table`` with a callback.

        Spawns a background task that drives the ``listen()`` generator and
        invokes ``callback`` for each event.  Call ``handle.unsubscribe()``
        to stop.

        Example::

            async def on_event(ev):
                print(ev)

            handle = await client.subscribe("orders", on_event)
            # … later:
            await handle.unsubscribe()
        """
        stop_event = asyncio.Event()

        async def _driver() -> None:
            async for ev in self.listen(table, filter=filter, last_event_id=last_event_id):
                if stop_event.is_set():
                    break
                result = callback(ev)
                if asyncio.iscoroutine(result):
                    await result

        task = asyncio.ensure_future(_driver())
        return ChannelHandle(table=table, _task=task, _stop=stop_event)

    # ------------------------------------------------------------------
    # Presence API
    # ------------------------------------------------------------------

    async def presence_track(
        self,
        channel: str,
        client_id: str,
        metadata: Any = None,
    ) -> None:
        """Send ``{"type":"presence_track",…}`` to register in a presence channel.

        The server enforces that ``client_id`` matches the authenticated JWT
        identity (6.SEC.P1); a mismatch returns a ``presenceerror`` frame.
        """
        await self.connect()
        msg: dict[str, Any] = {
            "type": "presence_track",
            "channel": channel,
            "client_id": client_id,
            "metadata": metadata if metadata is not None else {},
        }
        await self._send(msg)

    async def presence_untrack(self, channel: str, client_id: str) -> None:
        """Send ``{"type":"presence_untrack",…}``."""
        await self.connect()
        await self._send(
            {"type": "presence_untrack", "channel": channel, "client_id": client_id}
        )

    async def presence_heartbeat(self, channel: str, client_id: str) -> None:
        """Send ``{"type":"heartbeat",…}`` to refresh the presence TTL."""
        await self.connect()
        await self._send(
            {"type": "heartbeat", "channel": channel, "client_id": client_id}
        )

    async def listen_presence(
        self, channel: str
    ) -> AsyncGenerator[PresenceStateFrame | PresenceDiffFrame | PresenceErrorFrame, None]:
        """Async generator that yields presence frames for ``channel``.

        Frames begin arriving after a ``presence_track()`` call (the server
        sends a ``presence_state`` snapshot on join, then ``presence_diff``
        deltas).
        """
        queue: asyncio.Queue = asyncio.Queue()
        if channel not in self._queues:
            self._queues[channel] = []
        self._queues[channel].append(queue)
        try:
            while True:
                item = await queue.get()
                if item is None:
                    break
                yield item
        finally:
            queues = self._queues.get(channel, [])
            if queue in queues:
                queues.remove(queue)


@dataclass
class ChannelHandle:
    """Handle returned by ``RealtimeClient.subscribe()`` (callback API)."""

    table: str
    _task: asyncio.Task = field(repr=False)
    _stop: asyncio.Event = field(repr=False)

    async def unsubscribe(self) -> None:
        """Stop receiving events.  Cancels the background driver task."""
        self._stop.set()
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
