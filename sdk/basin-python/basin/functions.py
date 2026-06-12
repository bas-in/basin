"""Functions client — Basin's two function-invocation surfaces.

Route sources (verified):
- ANY  /fn/v1/:name          crates/basin-rest/src/server.rs:238,
  handler crates/basin-rest/src/routes/fn_handler.rs (any_fn_handler) —
  HTTP-handler-shape Wasm functions; the guest's response (status, headers,
  body) is proxied back verbatim.
- POST /rest/v1/rpc/:fn_name crates/basin-rest/src/server.rs:236,
  handler crates/basin-rest/src/routes/rpc.rs (post_rpc) — catalog SQL /
  Wasm UDFs. Body is a JSON object of named args; scalar results unwrap to
  the bare value, table results return an array of row objects.

Both are bearer-authenticated like every other route.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Optional
from urllib.parse import quote

from .errors import BasinApiError
from .types import FunctionInvokeResult

if TYPE_CHECKING:
    from ._http import SyncTransport, AsyncTransport


def _is_error_envelope(body: Any) -> bool:
    return (
        isinstance(body, dict)
        and isinstance(body.get("code"), str)
        and body["code"].startswith("E_")
        and isinstance(body.get("message"), str)
    )


class FunctionsClient:
    """Sync functions client."""

    def __init__(self, transport: "SyncTransport") -> None:
        self._transport = transport

    def invoke(
        self,
        name: str,
        *,
        method: str = "POST",
        body: Any = None,
        headers: Optional[dict[str, str]] = None,
    ) -> FunctionInvokeResult:
        """ANY /fn/v1/:name — invoke an HTTP-handler function.

        The function's response is proxied verbatim, so non-2xx statuses are
        NOT automatically errors — unless the body is Basin's own error
        envelope (auth 401, unknown function 404, invocation failure 500),
        which raises BasinApiError as usual.
        """
        hdrs: dict[str, str] = dict(headers or {})
        tok = self._transport._bearer()
        if tok is not None:
            hdrs["authorization"] = f"Bearer {tok}"

        content: bytes | None = None
        if isinstance(body, (str, bytes)):
            content = body.encode() if isinstance(body, str) else body
        elif body is not None:
            hdrs.setdefault("content-type", "application/json")
            content = json.dumps(body).encode()

        import httpx
        from ._http import _build_url
        from .errors import BasinNetworkError

        url = _build_url(
            self._transport.base_url, f"/fn/v1/{quote(name, safe='')}", None
        )
        try:
            response = self._transport._client.request(
                method, url, content=content, headers=hdrs
            )
        except httpx.TransportError as exc:
            raise BasinNetworkError(str(exc)) from exc

        ct = response.headers.get("content-type", "")
        text = response.text
        data: Any = text
        if "json" in ct and text:
            try:
                data = json.loads(text)
            except (json.JSONDecodeError, ValueError):
                pass

        # Basin-layer failures (not the function's own responses) use the
        # standard envelope; surface those as typed errors.
        if not response.is_success and _is_error_envelope(data):
            code = data.get("code", "E_UNKNOWN")
            message = data.get("message", f"HTTP {response.status_code}")
            raise BasinApiError(code, message, response.status_code)

        return FunctionInvokeResult(
            status=response.status_code,
            headers=dict(response.headers),
            data=data,
        )

    def rpc(self, fn_name: str, args: Optional[dict[str, Any]] = None) -> Any:
        """POST /rest/v1/rpc/:fn_name — call a catalog UDF.

        Args are named; missing declared args become NULL. Returns the bare
        scalar for single-value results, or an array of rows for RETURNS TABLE.
        """
        return self._transport.request_json(
            "POST",
            f"/rest/v1/rpc/{quote(fn_name, safe='')}",
            body=args or {},
        )


class AsyncFunctionsClient:
    """Async functions client."""

    def __init__(self, transport: "AsyncTransport") -> None:
        self._transport = transport

    async def invoke(
        self,
        name: str,
        *,
        method: str = "POST",
        body: Any = None,
        headers: Optional[dict[str, str]] = None,
    ) -> FunctionInvokeResult:
        """ANY /fn/v1/:name — invoke an HTTP-handler function (async)."""
        hdrs: dict[str, str] = dict(headers or {})
        tok = await self._transport._bearer()
        if tok is not None:
            hdrs["authorization"] = f"Bearer {tok}"

        content: bytes | None = None
        if isinstance(body, (str, bytes)):
            content = body.encode() if isinstance(body, str) else body
        elif body is not None:
            hdrs.setdefault("content-type", "application/json")
            content = json.dumps(body).encode()

        import httpx
        from ._http import _build_url
        from .errors import BasinNetworkError

        url = _build_url(
            self._transport.base_url, f"/fn/v1/{quote(name, safe='')}", None
        )
        try:
            response = await self._transport._client.request(
                method, url, content=content, headers=hdrs
            )
        except httpx.TransportError as exc:
            raise BasinNetworkError(str(exc)) from exc

        ct = response.headers.get("content-type", "")
        text = response.text
        data: Any = text
        if "json" in ct and text:
            try:
                data = json.loads(text)
            except (json.JSONDecodeError, ValueError):
                pass

        if not response.is_success and _is_error_envelope(data):
            code = data.get("code", "E_UNKNOWN")
            message = data.get("message", f"HTTP {response.status_code}")
            raise BasinApiError(code, message, response.status_code)

        return FunctionInvokeResult(
            status=response.status_code,
            headers=dict(response.headers),
            data=data,
        )

    async def rpc(self, fn_name: str, args: Optional[dict[str, Any]] = None) -> Any:
        """POST /rest/v1/rpc/:fn_name — call a catalog UDF (async)."""
        return await self._transport.request_json(
            "POST",
            f"/rest/v1/rpc/{quote(fn_name, safe='')}",
            body=args or {},
        )
