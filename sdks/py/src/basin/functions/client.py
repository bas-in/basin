"""
FunctionsClient — ``client.functions.invoke(name, ...)``

Engine route (confirmed against basin-rest/src/server.rs):
  POST /rest/v1/rpc/:fn_name

Body is a JSON object of named args.  Response is the function result.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
from urllib.parse import quote

from .._http import HttpTransport
from ..errors import BasinError


class FunctionsClient:
    """
    ``client.functions`` — invoke basin user-defined functions.

    Usage::

        result = await client.functions.invoke("my_fn", body={"x": 1})
    """

    enabled = True

    def __init__(
        self,
        *,
        http: HttpTransport,
        get_headers: Callable[[], dict[str, str]],
    ) -> None:
        self._http = http
        self._get_headers = get_headers

    async def invoke(
        self,
        fn_name: str,
        *,
        body: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> Any:
        """
        ``POST /rest/v1/rpc/:fn_name`` — invoke a named function.

        ``body`` is passed as the JSON request body (named args).
        Extra ``headers`` are merged on top of client defaults.

        Returns the parsed JSON response on success.
        Raises ``BasinError`` on failure.
        """
        if not fn_name:
            raise BasinError("invalid_request", "functions.invoke requires a function name")

        url = f"/rest/v1/rpc/{quote(fn_name, safe='')}"
        call_headers = dict(self._get_headers())
        call_headers["Content-Type"] = "application/json"
        if headers:
            call_headers.update(headers)

        payload = body if body is not None else {}

        try:
            resp = await self._http.request(
                "POST",
                url,
                headers=call_headers,
                json=payload,
            )
        except BasinError:
            raise
        except Exception as exc:
            raise BasinError(
                "network",
                str(exc) or "network error reaching rpc endpoint",
            ) from exc

        if resp.status_code in (401, 403):
            raise BasinError("unauthorized", "unauthorized", status=resp.status_code)

        try:
            result = resp.json()
        except Exception as exc:
            raise BasinError(
                "invalid_response",
                f"functions.invoke('{fn_name}') response was not JSON (HTTP {resp.status_code})",
                status=resp.status_code,
            ) from exc

        if not resp.is_success:
            raise BasinError(
                "internal",
                f"functions.invoke('{fn_name}') failed (HTTP {resp.status_code})",
                status=resp.status_code,
                details=result,
            )

        return result
