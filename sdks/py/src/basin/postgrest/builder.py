from __future__ import annotations

import json
from typing import (
    Any,
    AsyncIterator,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)
from urllib.parse import urlencode

import httpx

from ..errors import BasinError, _code_for_status
from .types import APIResponse

T = TypeVar("T")

_PGRST_OBJECT = "application/vnd.pgrst.object+json"


class QueryBuilder(APIResponse[T]):
    """
    Chainable PostgREST query builder.  Awaiting the builder executes the
    HTTP call and returns an ``APIResponse[T]``.

    ``APIResponse`` is inherited so that ``await builder`` is typed correctly
    as ``APIResponse[T]``.  The actual awaitable protocol is implemented via
    ``__await__``.

    Engine routes:
      GET    /rest/v1/{table}   — select
      POST   /rest/v1/{table}   — insert / upsert
      PATCH  /rest/v1/{table}   — update
      DELETE /rest/v1/{table}   — delete
    """

    def __init__(
        self,
        *,
        table_url: str,
        get_headers: Callable[[], Dict[str, str]],
        http: Any,
    ) -> None:
        super().__init__(data=None, error=None)
        self._table_url = table_url
        self._get_headers = get_headers
        self._http = http

        self._params: List[Tuple[str, str]] = []
        self._method: str = "GET"
        self._body: Any = None
        self._single: Optional[str] = None
        self._returning: str = "representation"
        self._extra_headers: Dict[str, str] = {}
        self._count: Optional[str] = None

    # ── select / mutations ─────────────────────────────────────────────

    def select(self, columns: str = "*", *, count: Optional[str] = None) -> QueryBuilder[T]:
        self._set_param("select", columns)
        if count:
            self._count = count
        return self

    def insert(
        self,
        rows: Union[Dict[str, Any], List[Dict[str, Any]]],
        *,
        returning: str = "representation",
    ) -> QueryBuilder[T]:
        self._method = "POST"
        self._body = rows
        self._returning = returning
        return self

    def update(
        self,
        values: Dict[str, Any],
        *,
        returning: str = "representation",
    ) -> QueryBuilder[T]:
        self._method = "PATCH"
        self._body = values
        self._returning = returning
        return self

    def upsert(
        self,
        rows: Union[Dict[str, Any], List[Dict[str, Any]]],
        *,
        on_conflict: Optional[str] = None,
        returning: str = "representation",
    ) -> QueryBuilder[T]:
        self._method = "POST"
        self._body = rows
        self._returning = returning
        if on_conflict:
            self._set_param("on_conflict", on_conflict)
        self._extra_headers["Prefer"] = (
            (self._extra_headers.get("Prefer", "") + ",resolution=merge-duplicates").lstrip(",")
        )
        return self

    def delete(self, *, force: bool = False) -> QueryBuilder[T]:
        self._method = "DELETE"
        return self

    # ── filters ────────────────────────────────────────────────────────

    def eq(self, column: str, value: Any) -> QueryBuilder[T]:
        return self._filter(column, "eq", _encode_value(value))

    def neq(self, column: str, value: Any) -> QueryBuilder[T]:
        return self._filter(column, "neq", _encode_value(value))

    def gt(self, column: str, value: Any) -> QueryBuilder[T]:
        return self._filter(column, "gt", _encode_value(value))

    def gte(self, column: str, value: Any) -> QueryBuilder[T]:
        return self._filter(column, "gte", _encode_value(value))

    def lt(self, column: str, value: Any) -> QueryBuilder[T]:
        return self._filter(column, "lt", _encode_value(value))

    def lte(self, column: str, value: Any) -> QueryBuilder[T]:
        return self._filter(column, "lte", _encode_value(value))

    def like(self, column: str, pattern: str) -> QueryBuilder[T]:
        return self._filter(column, "like", _encode_value(pattern))

    def ilike(self, column: str, pattern: str) -> QueryBuilder[T]:
        return self._filter(column, "ilike", _encode_value(pattern))

    def is_(self, column: str, value: Optional[bool]) -> QueryBuilder[T]:
        encoded = "null" if value is None else str(value).lower()
        return self._filter(column, "is", encoded)

    def in_(self, column: str, values: Iterable[Any]) -> QueryBuilder[T]:
        inner = ",".join(_encode_value(v) for v in values)
        return self._filter(column, "in", f"({inner})")

    def contains(self, column: str, value: Any) -> QueryBuilder[T]:
        return self._filter(column, "cs", _encode_containment(value))

    def contained_by(self, column: str, value: Any) -> QueryBuilder[T]:
        return self._filter(column, "cd", _encode_containment(value))

    def overlaps(self, column: str, value: Any) -> QueryBuilder[T]:
        return self._filter(column, "ov", _encode_containment(value))

    def not_(self, column: str, operator: str, value: Any) -> QueryBuilder[T]:
        if isinstance(value, list):
            encoded = f"({','.join(_encode_value(v) for v in value)})"
        elif value is None:
            encoded = "null"
        else:
            encoded = _encode_value(value)
        return self._filter(column, f"not.{operator}", encoded)

    def or_(self, filters: str, *, foreign_table: Optional[str] = None) -> QueryBuilder[T]:
        key = f"{foreign_table}.or" if foreign_table else "or"
        self._params.append((key, f"({filters})"))
        return self

    def match(self, query: Dict[str, Any]) -> QueryBuilder[T]:
        for k, v in query.items():
            self.eq(k, v)
        return self

    def filter(self, column: str, operator: str, value: Any) -> QueryBuilder[T]:
        if isinstance(value, list):
            encoded = f"({','.join(_encode_value(v) for v in value)})"
        elif value is None:
            encoded = "null"
        else:
            encoded = _encode_value(value)
        return self._filter(column, operator, encoded)

    # ── modifiers ──────────────────────────────────────────────────────

    def order(
        self,
        column: str,
        *,
        ascending: bool = True,
        nulls_first: bool = False,
    ) -> QueryBuilder[T]:
        direction = "asc" if ascending else "desc"
        nulls = ".nullsfirst" if nulls_first else ""
        self._params.append(("order", f"{column}.{direction}{nulls}"))
        return self

    def limit(self, n: int) -> QueryBuilder[T]:
        self._set_param("limit", str(n))
        return self

    def range(self, from_: int, to: int) -> QueryBuilder[T]:
        self._set_param("offset", str(from_))
        self._set_param("limit", str(to - from_ + 1))
        return self

    def single(self) -> QueryBuilder[T]:
        self._single = "row"
        self._extra_headers["Accept"] = _PGRST_OBJECT
        return self

    def maybe_single(self) -> QueryBuilder[T]:
        self._single = "maybe"
        self._extra_headers["Accept"] = _PGRST_OBJECT
        return self

    def returning(self, mode: str) -> QueryBuilder[T]:
        self._returning = mode
        return self

    def headers(self, extra: Dict[str, str]) -> QueryBuilder[T]:
        for k, v in extra.items():
            if k.lower() == "prefer" and "Prefer" in self._extra_headers:
                self._extra_headers["Prefer"] = (
                    self._extra_headers["Prefer"] + "," + v
                )
            else:
                self._extra_headers[k] = v
        return self

    def cursor(self, token: Optional[str]) -> QueryBuilder[T]:
        if token is not None:
            self._set_param("cursor", token)
        return self

    # ── execution ──────────────────────────────────────────────────────

    def __await__(self) -> Any:
        return self._execute().__await__()

    async def execute(self) -> APIResponse[T]:
        return await self._execute()

    async def _execute(self) -> APIResponse[T]:
        url, hdrs = self._build_request()
        try:
            resp: httpx.Response = await self._http.request(
                self._method,
                url,
                headers=hdrs,
                json=self._body,
            )
        except BasinError as exc:
            return APIResponse(data=None, error=exc, status=0)
        except Exception as exc:
            return APIResponse(
                data=None,
                error=BasinError("network", str(exc)),
                status=0,
            )

        count = _parse_content_range(resp.headers.get("content-range"))

        if resp.status_code == 204:
            return APIResponse(data=None, error=None, count=count, status=204)

        ct = resp.headers.get("content-type", "")
        if "application/x-ndjson" in ct or "application/jsonl" in ct:
            return self._parse_ndjson(resp, count)

        if not resp.is_success:
            return APIResponse(
                data=None,
                error=BasinError.from_response(resp),
                count=None,
                status=resp.status_code,
            )

        try:
            raw = resp.json()
        except Exception:
            return APIResponse(
                data=None,
                error=BasinError(
                    "invalid_response",
                    f"REST response was not JSON (HTTP {resp.status_code})",
                    status=resp.status_code,
                ),
                status=resp.status_code,
            )

        payload = _unwrap_envelope(raw)

        if self._single:
            return self._unwrap_single(payload, count, resp.status_code)

        rows = payload if isinstance(payload, list) else ([] if payload is None else [payload])
        return APIResponse(data=rows, error=None, count=count, status=resp.status_code)

    def _parse_ndjson(
        self,
        resp: httpx.Response,
        count: Optional[int],
    ) -> APIResponse[T]:
        if not resp.is_success:
            return APIResponse(
                data=None,
                error=BasinError(
                    _code_for_status(resp.status_code),
                    f"request failed (HTTP {resp.status_code})",
                    status=resp.status_code,
                ),
                count=None,
                status=resp.status_code,
            )
        try:
            text = resp.text
        except Exception as exc:
            return APIResponse(
                data=None,
                error=BasinError("invalid_response", str(exc), status=resp.status_code),
                status=resp.status_code,
            )
        lines = [ln for ln in text.splitlines() if ln.strip()]
        parsed: List[Any] = []
        for line in lines:
            try:
                parsed.append(json.loads(line))
            except json.JSONDecodeError as exc:
                return APIResponse(
                    data=None,
                    error=BasinError(
                        "invalid_response",
                        f"NDJSON line is not valid JSON: {exc}",
                        status=resp.status_code,
                    ),
                    status=resp.status_code,
                )
        next_cursor: Optional[str] = None
        if parsed:
            last = parsed[-1]
            if isinstance(last, dict) and "_basin_next_cursor" in last:
                next_cursor = last.get("_basin_next_cursor")
                parsed = parsed[:-1]
        return APIResponse(
            data=parsed,
            error=None,
            count=count,
            status=resp.status_code,
            next_cursor=next_cursor,
        )

    def _unwrap_single(
        self,
        payload: Any,
        count: Optional[int],
        status: int,
    ) -> APIResponse[T]:
        if isinstance(payload, list):
            if len(payload) == 0:
                if self._single == "row":
                    return APIResponse(
                        data=None,
                        error=BasinError(
                            "not_found",
                            "single() expected one row, got zero",
                            status=status,
                        ),
                        count=count,
                        status=status,
                    )
                return APIResponse(data=None, error=None, count=count, status=status)
            if len(payload) > 1:
                return APIResponse(
                    data=None,
                    error=BasinError(
                        "invalid_response",
                        f"single() expected one row, got {len(payload)}",
                        status=status,
                    ),
                    count=count,
                    status=status,
                )
            row = payload[0]
        else:
            row = payload
        return APIResponse(data=row, error=None, count=count, status=status)  # type: ignore[arg-type]

    def _build_request(self) -> Tuple[str, Dict[str, str]]:
        params = list(self._params)
        qs = urlencode(params) if params else ""
        url = (self._table_url + ("?" + qs if qs else ""))
        headers = dict(self._get_headers())
        prefer_parts: List[str] = []
        if self._method != "GET":
            prefer_parts.append(f"return={self._returning}")
        if self._count:
            prefer_parts.append(f"count={self._count}")
        if prefer_parts:
            existing = self._extra_headers.get("Prefer", "")
            if existing:
                headers["Prefer"] = existing + "," + ",".join(prefer_parts)
            else:
                headers["Prefer"] = ",".join(prefer_parts)
        for k, v in self._extra_headers.items():
            if k == "Prefer" and "Prefer" in headers:
                continue
            headers[k] = v
        if "Prefer" not in headers and "Prefer" in self._extra_headers:
            headers["Prefer"] = self._extra_headers["Prefer"]
        return url, headers

    def _filter(self, column: str, operator: str, encoded: str) -> QueryBuilder[T]:
        self._params.append((column, f"{operator}.{encoded}"))
        return self

    def _set_param(self, key: str, value: str) -> None:
        self._params = [(k, v) for k, v in self._params if k != key]
        self._params.append((key, value))

    # ── async iterators ────────────────────────────────────────────────

    async def paginate(self, *, page_size: int = 1000) -> AsyncIterator[T]:
        if not any(k == "limit" for k, _ in self._params):
            self._set_param("limit", str(page_size))
        while True:
            result = await self._execute()
            if result.error:
                raise result.error
            rows: List[T] = result.data or []
            for row in rows:
                yield row
            if result.next_cursor is None:
                break
            self.cursor(result.next_cursor)

    async def stream(self) -> AsyncIterator[T]:
        self._set_param("stream", "true")
        url, hdrs = self._build_request()
        async with self._http._client.stream(
            self._method,
            self._http._base_url + url if not url.startswith("http") else url,
            headers=hdrs,
            json=self._body,
        ) as resp:
            if not resp.is_success:
                body_text = await resp.aread()
                raise BasinError(
                    _code_for_status(resp.status_code),
                    body_text.decode(errors="replace") or f"request failed (HTTP {resp.status_code})",
                    status=resp.status_code,
                )
            async for line in resp.aiter_lines():
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise BasinError(
                        "invalid_response",
                        f"failed to parse NDJSON line: {exc}",
                    ) from exc
                if isinstance(obj, dict) and "_basin_next_cursor" in obj:
                    return
                yield obj  # type: ignore[misc]


def _encode_value(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, str):
        return value
    return str(value)


def _encode_containment(value: Any) -> str:
    if isinstance(value, list):
        return "{" + ",".join(
            v if isinstance(v, str) else str(v) for v in value
        ) + "}"
    if isinstance(value, dict):
        return json.dumps(value)
    return _encode_value(value)


def _unwrap_envelope(raw: Any) -> Any:
    if isinstance(raw, dict) and "data" in raw and "error" in raw:
        return raw["data"]
    return raw


def _parse_content_range(header: Optional[str]) -> Optional[int]:
    if not header:
        return None
    slash = header.find("/")
    if slash < 0:
        return None
    total = header[slash + 1:].strip()
    if not total or total == "*":
        return None
    try:
        return int(total)
    except ValueError:
        return None
