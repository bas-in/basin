"""Top-level BasinClient and AsyncBasinClient."""

from __future__ import annotations

import base64
import json
from typing import Any, Optional

import httpx

from ._http import AsyncTransport, SyncTransport
from .auth import AsyncAuthClient, AuthClient, _project_id_from_jwt
from .errors import BasinApiError
from .functions import AsyncFunctionsClient, FunctionsClient
from .query import AsyncQueryBuilder, QueryBuilder
from .storage import AsyncStorageClient, StorageClient
from .types import Row


class BasinClient:
    """Sync Basin client.

    ``key`` is a JWT or an API-key secret — both are accepted as
    ``Authorization: Bearer <key>`` (crates/basin-rest/src/server.rs
    ``authorize``, JWT verify first, API-key lookup fallback).

    Usage::

        from basin import create_client

        basin = create_client("http://localhost:8080", "my-api-key",
                               project_id="01J...")

        result = basin.table("orders").select("id,total").gte("total", 100).run()
        basin.table("orders").insert({"total": 50, "status": "new"})
    """

    def __init__(
        self,
        url: str,
        key: Optional[str] = None,
        *,
        project_id: Optional[str] = None,
        timeout: float = 30.0,
        http_client: Optional[httpx.Client] = None,
    ) -> None:
        self._transport = SyncTransport(
            base_url=url,
            key=key,
            client=http_client,
            timeout=timeout,
        )
        self._explicit_project_id = project_id
        self._key = key

        self.auth = AuthClient(self._transport, project_id)
        self.functions = FunctionsClient(self._transport)
        self.storage = StorageClient(self._transport, self._resolve_project_id)

        # Wire the transport's token getter to the auth client so every
        # request uses the freshest session token.
        self._transport.set_token_getter(self.auth.access_token)

    def _resolve_project_id(self) -> Optional[str]:
        if self._explicit_project_id is not None:
            return self._explicit_project_id
        session = self.auth.get_session()
        if session is not None:
            from_session = _project_id_from_jwt(session.access_token)
            if from_session is not None:
                return from_session
        if self._key is not None:
            return _project_id_from_jwt(self._key)
        return None

    def table(self, name: str) -> QueryBuilder:
        """Return a QueryBuilder for /rest/v1/:table."""
        return QueryBuilder(self._transport, name)

    # Alias matching the JS SDK's `from(table)` style.
    from_table = table

    def rpc(self, fn_name: str, args: Optional[dict[str, Any]] = None) -> Any:
        """POST /rest/v1/rpc/:fn_name — convenience alias for functions.rpc."""
        return self.functions.rpc(fn_name, args)

    def health(self) -> str:
        """GET /health → 'ok'."""
        response = self._transport.request("GET", "/health", auth=False)
        return response.text

    def request(
        self,
        method: str,
        path: str,
        *,
        query: Optional[list[tuple[str, str]]] = None,
        body: Any = None,
    ) -> Any:
        """Escape hatch for routes not yet wrapped (e.g. /admin/v1/*)."""
        return self._transport.request_json(
            method, path, query=query, body=body
        )

    def close(self) -> None:
        self._transport.close()

    def __enter__(self) -> "BasinClient":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()


class AsyncBasinClient:
    """Async Basin client backed by httpx.AsyncClient.

    Usage::

        from basin import create_async_client

        async with create_async_client("http://localhost:8080", key) as basin:
            result = await basin.table("orders").select("id,total").run()
    """

    def __init__(
        self,
        url: str,
        key: Optional[str] = None,
        *,
        project_id: Optional[str] = None,
        timeout: float = 30.0,
        http_client: Optional[httpx.AsyncClient] = None,
    ) -> None:
        self._transport = AsyncTransport(
            base_url=url,
            key=key,
            client=http_client,
            timeout=timeout,
        )
        self._explicit_project_id = project_id
        self._key = key

        self.auth = AsyncAuthClient(self._transport, project_id)
        self.functions = AsyncFunctionsClient(self._transport)
        self.storage = AsyncStorageClient(self._transport, self._resolve_project_id)

        self._transport.set_token_getter(self.auth.access_token)

        # Lazy: created on first access so the websockets dep stays optional.
        self._realtime: Optional[Any] = None

    def _resolve_project_id(self) -> Optional[str]:
        if self._explicit_project_id is not None:
            return self._explicit_project_id
        session = self.auth.get_session()
        if session is not None:
            from_session = _project_id_from_jwt(session.access_token)
            if from_session is not None:
                return from_session
        if self._key is not None:
            return _project_id_from_jwt(self._key)
        return None

    @property
    def realtime(self) -> "Any":
        """Lazy ``RealtimeClient`` for ``GET /realtime/v1/ws/:project``.

        Requires the optional ``realtime`` extra::

            pip install "basin-sdk[realtime]"

        A clear ``ImportError`` is raised on first access if ``websockets``
        is not installed.
        """
        if self._realtime is None:
            from .realtime import RealtimeClient  # lazy import, websockets optional
            self._realtime = RealtimeClient(
                base_url=self._transport.base_url,
                get_token=self.auth.access_token,
                project_id=self._resolve_project_id,
            )
        return self._realtime

    def table(self, name: str) -> AsyncQueryBuilder:
        """Return an AsyncQueryBuilder for /rest/v1/:table."""
        return AsyncQueryBuilder(self._transport, name)

    from_table = table

    async def rpc(self, fn_name: str, args: Optional[dict[str, Any]] = None) -> Any:
        return await self.functions.rpc(fn_name, args)

    async def health(self) -> str:
        response = await self._transport.request("GET", "/health", auth=False)
        return response.text

    async def request(
        self,
        method: str,
        path: str,
        *,
        query: Optional[list[tuple[str, str]]] = None,
        body: Any = None,
    ) -> Any:
        return await self._transport.request_json(
            method, path, query=query, body=body
        )

    async def aclose(self) -> None:
        await self._transport.aclose()

    async def __aenter__(self) -> "AsyncBasinClient":
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.aclose()


def create_client(
    url: str,
    key: Optional[str] = None,
    *,
    project_id: Optional[str] = None,
    timeout: float = 30.0,
    http_client: Optional[httpx.Client] = None,
) -> BasinClient:
    """Create a sync Basin client.

    Parameters
    ----------
    url:
        Base URL of the Basin server, e.g. ``http://localhost:8080``.
    key:
        JWT or raw API key — passed as ``Authorization: Bearer <key>``.
    project_id:
        Project ULID. Optional when using a JWT that carries a ``project_id``
        claim; required for public storage URLs and realtime (not yet wrapped).
    timeout:
        Default request timeout in seconds.
    http_client:
        Custom ``httpx.Client`` (for testing / advanced transport config).
    """
    return BasinClient(
        url,
        key,
        project_id=project_id,
        timeout=timeout,
        http_client=http_client,
    )


def create_async_client(
    url: str,
    key: Optional[str] = None,
    *,
    project_id: Optional[str] = None,
    timeout: float = 30.0,
    http_client: Optional[httpx.AsyncClient] = None,
) -> AsyncBasinClient:
    """Create an async Basin client.

    Parameters
    ----------
    url:
        Base URL of the Basin server.
    key:
        JWT or raw API key.
    project_id:
        Project ULID — optional when the JWT carries ``project_id``.
    timeout:
        Default request timeout in seconds.
    http_client:
        Custom ``httpx.AsyncClient`` (for testing / advanced transport config).
    """
    return AsyncBasinClient(
        url,
        key,
        project_id=project_id,
        timeout=timeout,
        http_client=http_client,
    )
