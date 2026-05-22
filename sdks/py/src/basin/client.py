from __future__ import annotations

from typing import Any, Dict, Optional, TypeVar

import httpx

from ._http import HttpTransport
from .auth.client import AuthClient
from .errors import BasinError
from .postgrest.builder import QueryBuilder

T = TypeVar("T")


class ClientOptions:
    """Optional overrides for ``create_client``."""

    def __init__(
        self,
        *,
        headers: Optional[Dict[str, str]] = None,
        transport: Optional[httpx.AsyncBaseTransport] = None,
    ) -> None:
        self.headers = headers or {}
        self.transport = transport


class Client:
    """
    Async basin client.  Obtain via ``create_client(url, key)``.

    Speaks DIRECTLY to basin-engine (the OSS Rust data plane).  The engine
    URL can be a cloud-managed regional endpoint
    (``https://<region>.basin.run``) or a self-hosted engine
    (``http://localhost:5434``).
    """

    def __init__(
        self,
        base_url: str,
        anon_key: str,
        *,
        options: Optional[ClientOptions] = None,
    ) -> None:
        opts = options or ClientOptions()
        self._base_url = base_url.rstrip("/")
        self._anon_key = anon_key

        base_headers: Dict[str, str] = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "apikey": anon_key,
            "Authorization": f"Bearer {anon_key}",
        }
        base_headers.update(opts.headers)

        self._http = HttpTransport(
            self._base_url,
            base_headers,
            transport=opts.transport,
        )

        self.auth = AuthClient(
            base_url=self._base_url,
            anon_key=anon_key,
            get_headers=self._current_headers,
            http=self._http,
        )

    # ── Public surface ─────────────────────────────────────────────────

    def from_(self, table: str) -> QueryBuilder[Any]:
        """PostgREST query builder for ``table``."""
        from urllib.parse import quote
        table_url = f"/rest/v1/{quote(table, safe='')}"
        return QueryBuilder(
            table_url=table_url,
            get_headers=self._current_headers,
            http=self._http,
        )

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json: Any = None,
        params: Optional[Dict[str, str]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Any:
        """
        Low-level helper used by sub-clients.  Raises ``BasinError`` on
        non-2xx.  Returns ``None`` on 204.
        """
        try:
            resp = await self._http.request(
                method,
                path,
                json=json,
                params=params,
                headers=headers,
            )
        except BasinError:
            raise
        except Exception as exc:
            raise BasinError("network", str(exc)) from exc

        if resp.status_code == 204:
            return None
        if not resp.is_success:
            raise BasinError.from_response(resp)
        try:
            return resp.json()
        except Exception:
            raise BasinError(
                "invalid_response",
                f"response was not JSON (HTTP {resp.status_code})",
                status=resp.status_code,
            )

    async def aclose(self) -> None:
        await self._http.aclose()

    async def __aenter__(self) -> Client:
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.aclose()

    # ── Internals ──────────────────────────────────────────────────────

    def _current_headers(self) -> Dict[str, str]:
        headers = dict(self._http._default_headers)
        session = self.auth.get_session()
        if session and session.access_token:
            headers["Authorization"] = f"Bearer {session.access_token}"
        return headers


def create_client(
    url: str,
    key: str,
    *,
    options: Optional[ClientOptions] = None,
) -> Client:
    """
    Construct a basin ``Client``.

    ``url``  — basin engine HTTP base URL.  Cloud:
               ``https://<region>.basin.run``.  Self-hosted:
               ``http://localhost:5434``.
    ``key``  — public anon API key (``basin_{tenant_id}_{base64}``).
    """
    return Client(url, key, options=options)
