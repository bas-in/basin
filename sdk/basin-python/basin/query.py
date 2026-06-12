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

Arrow transport: no native Arrow IPC endpoint exists in basin-rest (confirmed
by grepping for arrow content-types in the server source). The to_arrow()
method converts the JSON result set to a pyarrow.Table client-side. Native
Arrow streaming is pending server support — see follow-ups.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Generator, Iterator, Optional, Union

from .types import ExecTag, Page, Row

if TYPE_CHECKING:
    from ._http import SyncTransport, AsyncTransport

Scalar = Union[str, int, float, bool, None]


def _literal(v: Scalar) -> str:
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "true" if v else "false"
    return str(v)


class QueryResult:
    """Normalized result of a GET: rows plus the pagination cursor."""

    __slots__ = ("rows", "next_cursor")

    def __init__(self, rows: list[Row], next_cursor: Optional[str]) -> None:
        self.rows = rows
        self.next_cursor = next_cursor

    def __repr__(self) -> str:  # pragma: no cover
        return f"QueryResult(rows={len(self.rows)}, next_cursor={self.next_cursor!r})"

    def to_arrow(self) -> Any:
        """Convert rows to a pyarrow.Table (client-side conversion).

        Native Arrow IPC transport is not yet available from the server — this
        method performs JSON→Arrow conversion locally. Install the 'arrow'
        extra (``pip install basin-sdk[arrow]``) to use this.

        NOTE: The server does not expose an Arrow/IPC endpoint. This is a
        fallback that may lose type fidelity (all columns become string or
        object unless pyarrow infers better). A native server-side Arrow
        endpoint would yield correct schema. Track progress server-side.
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

        This is a client-side JSON→Arrow conversion. No native Arrow IPC
        transport is available from the server yet.
        """
        return self.run().to_arrow()

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
        """Run the query and return the result as a pyarrow.Table (client-side conversion)."""
        result = await self.run()
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
