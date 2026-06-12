"""QueryBuilder — REST query builder for /rest/v1/:table.

Route source (verified): crates/basin-rest/src/server.rs:243-249 —
GET|POST|PATCH|DELETE /rest/v1/:table.

The filter grammar is Basin's PostgREST-*style* dialect (parser.rs):
- select=<cols,csv>
- <col>=<op>.<value> with ops eq|neq|gt|gte|lt|lte|in|is
  (in.(a,b,c) parenthesised; is.null / is.notnull)
- order=<col>[.asc|.desc][,...], limit=N, offset=N
- cursor=<token> keyset pagination, stream=true NDJSON

NOT full PostgREST: no or=, not., like|ilike, embedded resource selects,
Prefer headers. Filters AND together.

Response shapes (crates/basin-rest/src/routes/data.rs):
- plain GET → JSON array of rows
- GET with limit or cursor → { rows, next_cursor }
- GET with ?stream=true → NDJSON, final line {"_basin_next_cursor":"..."} when paginating
- POST → 201, { ok, tag } (or rows); PATCH/DELETE → { ok, tag }
- DELETE may surface 501 E_ENGINE_UNSUPPORTED

Arrow IPC transport (crates/basin-rest/src/arrow_ipc.rs):
- GET /rest/v1/:table with Accept: application/vnd.apache.arrow.stream
  → Arrow IPC stream, Content-Type: application/vnd.apache.arrow.stream
- POST /rest/v1/rpc/:fn with the same Accept header → IPC for multi-row results
- Pagination state is in response headers (not body):
    x-basin-next-cursor   opaque cursor token (absent when no next page)
    x-basin-row-count     decimal total row count across all IPC batches
- The to_arrow() and stream_arrow() methods below send this Accept header and
  decode the IPC response natively via pyarrow. Falls back to JSON→Arrow
  conversion when the server returns JSON (older server, no IPC support).
"""

from __future__ import annotations

import io
import json
from typing import TYPE_CHECKING, Any, Generator, Iterator, Optional, Union

from .types import ExecTag, Page, Row

if TYPE_CHECKING:
    from ._http import SyncTransport, AsyncTransport

Scalar = Union[str, int, float, bool, None]

# MIME type for the Arrow IPC streaming format — matches the server constant
# in crates/basin-rest/src/arrow_ipc.rs.
ARROW_STREAM_MIME = "application/vnd.apache.arrow.stream"


def _literal(v: Scalar) -> str:
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "true" if v else "false"
    return str(v)


def _try_import_pyarrow():
    """Return pyarrow module or raise ImportError with install hint."""
    try:
        import pyarrow as pa
        import pyarrow.ipc as pa_ipc
        return pa, pa_ipc
    except ImportError as exc:
        raise ImportError(
            "pyarrow is required for Arrow IPC transport: "
            "pip install basin-sdk[arrow]"
        ) from exc


def _decode_arrow_ipc(raw: bytes) -> Any:
    """Decode Arrow IPC stream bytes into a pyarrow.Table."""
    pa, pa_ipc = _try_import_pyarrow()
    buf = pa.py_buffer(raw)
    reader = pa_ipc.open_stream(buf)
    batches = []
    while True:
        try:
            batches.append(reader.read_next_batch())
        except StopIteration:
            break
    if batches:
        return pa.Table.from_batches(batches, schema=reader.schema)
    return pa.table({}, schema=reader.schema)


def _is_arrow_response(response: Any) -> bool:
    """Return True when the response Content-Type is Arrow IPC stream."""
    ct = response.headers.get("content-type", "")
    return ARROW_STREAM_MIME in ct


class QueryResult:
    """Normalized result of a GET: rows plus the pagination cursor."""

    __slots__ = ("rows", "next_cursor")

    def __init__(self, rows: list[Row], next_cursor: Optional[str]) -> None:
        self.rows = rows
        self.next_cursor = next_cursor

    def __repr__(self) -> str:  # pragma: no cover
        return f"QueryResult(rows={len(self.rows)}, next_cursor={self.next_cursor!r})"

    def to_arrow(self) -> Any:
        """Convert rows to a pyarrow.Table (client-side JSON→Arrow conversion).

        Use QueryBuilder.to_arrow() for the zero-loss native IPC path when the
        server supports it. This method is a fallback for pre-populated
        QueryResult objects that hold rows as Python dicts.

        Install the 'arrow' extra (``pip install basin-sdk[arrow]``) to use this.
        """
        try:
            import pyarrow as pa
        except ImportError as exc:
            raise ImportError(
                "pyarrow is required for Arrow conversion: "
                "pip install basin-sdk[arrow]"
            ) from exc
        if not self.rows:
            return pa.table({})
        return pa.Table.from_pylist(self.rows)


def _normalize_get(body: Any) -> QueryResult:
    if isinstance(body, list):
        return QueryResult(rows=body, next_cursor=None)
    if isinstance(body, dict):
        if "rows" in body:
            return QueryResult(
                rows=body.get("rows") or [],
                next_cursor=body.get("next_cursor"),
            )
        # { ok, tag } empty-result shape (ExecResult::Empty) → no rows.
    return QueryResult(rows=[], next_cursor=None)


class QueryBuilder:
    """Fluent query builder for /rest/v1/:table.

    Usage (sync)::

        result = client.table("orders").select("id,total").gte("total", 100).run()
        for row in result.rows:
            print(row)

    Awaitable (async) shorthand via AsyncQueryBuilder.
    """

    def __init__(self, transport: "SyncTransport", table: str) -> None:
        self._transport = transport
        self._table = table
        self._query: list[tuple[str, str]] = []

    # ------------------------------------------------------------------
    # Filter / projection / ordering methods (all return self for chaining)
    # ------------------------------------------------------------------

    def select(self, columns: Optional[Union[str, list[str]]] = None) -> "QueryBuilder":
        """select=<cols> projection; omit or pass '*' for all columns."""
        if isinstance(columns, list):
            cols = ",".join(columns)
        else:
            cols = columns or "*"
        self._query.append(("select", cols))
        return self

    def eq(self, column: str, value: Scalar) -> "QueryBuilder":
        self._query.append((column, f"eq.{_literal(value)}"))
        return self

    def neq(self, column: str, value: Scalar) -> "QueryBuilder":
        self._query.append((column, f"neq.{_literal(value)}"))
        return self

    def gt(self, column: str, value: Scalar) -> "QueryBuilder":
        self._query.append((column, f"gt.{_literal(value)}"))
        return self

    def gte(self, column: str, value: Scalar) -> "QueryBuilder":
        self._query.append((column, f"gte.{_literal(value)}"))
        return self

    def lt(self, column: str, value: Scalar) -> "QueryBuilder":
        self._query.append((column, f"lt.{_literal(value)}"))
        return self

    def lte(self, column: str, value: Scalar) -> "QueryBuilder":
        self._query.append((column, f"lte.{_literal(value)}"))
        return self

    def in_(self, column: str, values: list[Scalar]) -> "QueryBuilder":
        """<col>=in.(a,b,c) — parenthesised list per parser.rs parse_in_list."""
        self._query.append((column, f"in.({','.join(_literal(v) for v in values)})"))
        return self

    def is_(self, column: str, value: str) -> "QueryBuilder":
        """<col>=is.null / <col>=is.notnull."""
        self._query.append((column, f"is.{value}"))
        return self

    def order(self, column: str, *, ascending: bool = True) -> "QueryBuilder":
        """order=<col>.asc|desc (repeatable)."""
        direction = "asc" if ascending else "desc"
        self._query.append(("order", f"{column}.{direction}"))
        return self

    def limit(self, n: int) -> "QueryBuilder":
        """limit=N. Switches the GET response to { rows, next_cursor }."""
        self._query.append(("limit", str(n)))
        return self

    def offset(self, n: int) -> "QueryBuilder":
        self._query.append(("offset", str(n)))
        return self

    def cursor(self, token: str) -> "QueryBuilder":
        """Resume keyset pagination from a next_cursor token."""
        self._query.append(("cursor", token))
        return self

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def run(self) -> QueryResult:
        """Execute as GET and normalize both response shapes."""
        body = self._transport.request_json(
            "GET", f"/rest/v1/{self._table}", query=self._query
        )
        return _normalize_get(body)

    def rows(self) -> list[Row]:
        """Execute as GET and return rows only."""
        return self.run().rows

    def page(self) -> Page:
        """Execute as paginated GET ({ rows, next_cursor } shape)."""
        r = self.run()
        return Page(rows=r.rows, next_cursor=r.next_cursor)

    def stream(self) -> Generator[Row, None, Optional[str]]:
        """Execute as GET with ?stream=true (NDJSON).

        Yields rows. Returns the next_cursor when the generator is exhausted
        (accessible via the return value of next() after StopIteration, or via
        send()/throw() on the generator object — call gen.close() to get it).

        The trailing {"_basin_next_cursor": ...} line is captured, not yielded.
        """
        query = list(self._query) + [("stream", "true")]
        response = self._transport.request(
            "GET", f"/rest/v1/{self._table}", query=query
        )
        next_cursor: Optional[str] = None
        for line in response.text.splitlines():
            line = line.strip()
            if not line:
                continue
            parsed = json.loads(line)
            if "_basin_next_cursor" in parsed:
                next_cursor = parsed["_basin_next_cursor"]
                continue
            yield parsed
        return next_cursor

    def to_arrow(self) -> Any:
        """Run the query and return the result as a pyarrow.Table.

        Sends Accept: application/vnd.apache.arrow.stream. If the server
        responds with an Arrow IPC stream (Content-Type matches), the bytes are
        decoded natively via pyarrow — zero JSON round-trip, full i64/timestamp
        fidelity. If the server returns JSON (e.g. older server without IPC
        support), falls back to client-side JSON→Arrow conversion transparently.

        Pagination: the x-basin-next-cursor response header is returned as
        table metadata under the key "x-basin-next-cursor" so callers can
        continue paginating without re-running a JSON request.

        Install the 'arrow' extra (``pip install basin-sdk[arrow]``) to use.
        """
        _try_import_pyarrow()  # early ImportError before hitting the network
        response = self._transport.request(
            "GET",
            f"/rest/v1/{self._table}",
            query=self._query,
            headers={"accept": ARROW_STREAM_MIME},
        )
        if _is_arrow_response(response):
            table = _decode_arrow_ipc(response.content)
            # Attach pagination cursor as table-level metadata so callers can
            # page through results without losing the cursor.
            cursor = response.headers.get("x-basin-next-cursor")
            if cursor:
                existing = dict(table.schema.metadata or {})
                existing[b"x-basin-next-cursor"] = cursor.encode()
                table = table.replace_schema_metadata(existing)
            return table
        # Fallback: server returned JSON — convert client-side.
        body = json.loads(response.text) if response.text else []
        result = _normalize_get(body)
        return result.to_arrow()

    def insert(self, values: Union[Row, list[Row]]) -> Any:
        """POST /rest/v1/:table — insert one object or an array (201)."""
        return self._transport.request_json(
            "POST", f"/rest/v1/{self._table}", body=values
        )

    def update(self, values: Row) -> Any:
        """PATCH /rest/v1/:table?<filters> — update rows matching the filters."""
        return self._transport.request_json(
            "PATCH",
            f"/rest/v1/{self._table}",
            query=self._query,
            body=values,
        )

    def delete(self) -> Any:
        """DELETE /rest/v1/:table?<filters>.

        May raise BasinApiError with code E_ENGINE_UNSUPPORTED (501) on
        engines without DELETE support.
        """
        return self._transport.request_json(
            "DELETE",
            f"/rest/v1/{self._table}",
            query=self._query,
        )


class AsyncQueryBuilder:
    """Async version of QueryBuilder for use with AsyncBasinClient."""

    def __init__(self, transport: "AsyncTransport", table: str) -> None:
        self._transport = transport
        self._table = table
        self._query: list[tuple[str, str]] = []

    def select(self, columns: Optional[Union[str, list[str]]] = None) -> "AsyncQueryBuilder":
        if isinstance(columns, list):
            cols = ",".join(columns)
        else:
            cols = columns or "*"
        self._query.append(("select", cols))
        return self

    def eq(self, column: str, value: Scalar) -> "AsyncQueryBuilder":
        self._query.append((column, f"eq.{_literal(value)}"))
        return self

    def neq(self, column: str, value: Scalar) -> "AsyncQueryBuilder":
        self._query.append((column, f"neq.{_literal(value)}"))
        return self

    def gt(self, column: str, value: Scalar) -> "AsyncQueryBuilder":
        self._query.append((column, f"gt.{_literal(value)}"))
        return self

    def gte(self, column: str, value: Scalar) -> "AsyncQueryBuilder":
        self._query.append((column, f"gte.{_literal(value)}"))
        return self

    def lt(self, column: str, value: Scalar) -> "AsyncQueryBuilder":
        self._query.append((column, f"lt.{_literal(value)}"))
        return self

    def lte(self, column: str, value: Scalar) -> "AsyncQueryBuilder":
        self._query.append((column, f"lte.{_literal(value)}"))
        return self

    def in_(self, column: str, values: list[Scalar]) -> "AsyncQueryBuilder":
        self._query.append((column, f"in.({','.join(_literal(v) for v in values)})"))
        return self

    def is_(self, column: str, value: str) -> "AsyncQueryBuilder":
        self._query.append((column, f"is.{value}"))
        return self

    def order(self, column: str, *, ascending: bool = True) -> "AsyncQueryBuilder":
        direction = "asc" if ascending else "desc"
        self._query.append(("order", f"{column}.{direction}"))
        return self

    def limit(self, n: int) -> "AsyncQueryBuilder":
        self._query.append(("limit", str(n)))
        return self

    def offset(self, n: int) -> "AsyncQueryBuilder":
        self._query.append(("offset", str(n)))
        return self

    def cursor(self, token: str) -> "AsyncQueryBuilder":
        self._query.append(("cursor", token))
        return self

    async def run(self) -> QueryResult:
        body = await self._transport.request_json(
            "GET", f"/rest/v1/{self._table}", query=self._query
        )
        return _normalize_get(body)

    async def rows(self) -> list[Row]:
        return (await self.run()).rows

    async def page(self) -> Page:
        r = await self.run()
        return Page(rows=r.rows, next_cursor=r.next_cursor)

    async def stream(self) -> "AsyncGenerator[Row, None]":
        """Execute as GET with ?stream=true (NDJSON), yielding rows."""
        query = list(self._query) + [("stream", "true")]
        response = await self._transport.request(
            "GET", f"/rest/v1/{self._table}", query=query
        )
        for line in response.text.splitlines():
            line = line.strip()
            if not line:
                continue
            parsed = json.loads(line)
            if "_basin_next_cursor" in parsed:
                continue
            yield parsed

    async def to_arrow(self) -> Any:
        """Run the query and return the result as a pyarrow.Table.

        Sends Accept: application/vnd.apache.arrow.stream. Decodes the Arrow
        IPC response natively for zero-loss columnar results, or falls back to
        JSON→Arrow conversion for older servers transparently.

        Pagination cursor is attached as table metadata under key
        "x-basin-next-cursor" when present.
        """
        _try_import_pyarrow()  # early ImportError before hitting the network
        response = await self._transport.request(
            "GET",
            f"/rest/v1/{self._table}",
            query=self._query,
            headers={"accept": ARROW_STREAM_MIME},
        )
        if _is_arrow_response(response):
            table = _decode_arrow_ipc(response.content)
            cursor = response.headers.get("x-basin-next-cursor")
            if cursor:
                existing = dict(table.schema.metadata or {})
                existing[b"x-basin-next-cursor"] = cursor.encode()
                table = table.replace_schema_metadata(existing)
            return table
        body = json.loads(response.text) if response.text else []
        result = _normalize_get(body)
        return result.to_arrow()

    async def insert(self, values: Union[Row, list[Row]]) -> Any:
        return await self._transport.request_json(
            "POST", f"/rest/v1/{self._table}", body=values
        )

    async def update(self, values: Row) -> Any:
        return await self._transport.request_json(
            "PATCH",
            f"/rest/v1/{self._table}",
            query=self._query,
            body=values,
        )

    async def delete(self) -> Any:
        return await self._transport.request_json(
            "DELETE",
            f"/rest/v1/{self._table}",
            query=self._query,
        )

    # Make the builder awaitable as a shorthand for .run()
    def __await__(self):
        return self.run().__await__()
