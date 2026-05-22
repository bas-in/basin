from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional
from urllib.parse import quote

from .._http import HttpTransport
from ..errors import BasinError
from .types import Credential, ProvisionResult


def _extract_connection_url(body: Any) -> str:
    """
    Extract the connection URL from the engine's provision/rotate response.

    Engine shape: ``{"project_id":…, "pgwire_user":…, "dbname":…,
    "password":…, "connection_url":"postgres://…"}``.
    Falls back to scanning all string values for a ``postgres://``-prefixed
    entry so older engine builds still work.
    """
    if isinstance(body, dict):
        if isinstance(body.get("connection_url"), str):
            return body["connection_url"]
        for v in body.values():
            if isinstance(v, str) and v.startswith("postgres://"):
                return v
    return ""


class AdminProjectsClient:
    """
    ``client.admin.projects`` — operator-grade credential management.

    Routes (engine source of truth: basin-rest/src/routes/admin.rs):
      POST /admin/v1/projects                           — provision
      POST /admin/v1/projects/:pgwire_user/rotate       — rotate_credentials
      GET  /admin/v1/projects/:project_id/credentials   — list_credentials

    All routes require ``is_admin: true`` in the JWT claims.
    """

    def __init__(
        self,
        *,
        http: HttpTransport,
        get_headers: Callable[[], Dict[str, str]],
    ) -> None:
        self._http = http
        self._get_headers = get_headers

    async def provision(
        self,
        project_id: Optional[str] = None,
        *,
        dbname: Optional[str] = None,
    ) -> ProvisionResult:
        """
        ``POST /admin/v1/projects`` — provision a new project (or the
        supplied ``project_id``) and return a ``ProvisionResult`` with
        ``connection_string``.

        Raises ``BasinError("unauthorized", …)`` when the token lacks
        ``is_admin: true``.
        """
        body: Dict[str, Any] = {}
        if project_id is not None:
            body["project_id"] = project_id
        if dbname is not None:
            body["dbname"] = dbname

        resp = await self._http.request(
            "POST",
            "/admin/v1/projects",
            json=body,
            headers=self._get_headers(),
        )
        if resp.status_code in (401, 403):
            raise BasinError(
                "unauthorized",
                "Admin endpoints require is_admin claims",
                status=resp.status_code,
            )
        if not resp.is_success:
            raise BasinError.from_response(resp)
        try:
            data = resp.json()
        except Exception:
            raise BasinError(
                "invalid_response",
                f"Admin provision response was not JSON (HTTP {resp.status_code})",
                status=resp.status_code,
            )
        return ProvisionResult(connection_string=_extract_connection_url(data))

    async def rotate_credentials(self, pgwire_user: str) -> ProvisionResult:
        """
        ``POST /admin/v1/projects/:pgwire_user/rotate`` — rotate a
        credential's password and return updated ``ProvisionResult``.

        Raises ``BasinError("not_found", …)`` for an unknown ``pgwire_user``.
        """
        resp = await self._http.request(
            "POST",
            f"/admin/v1/projects/{quote(pgwire_user, safe='')}/rotate",
            headers=self._get_headers(),
        )
        if resp.status_code in (401, 403):
            raise BasinError(
                "unauthorized",
                "Admin endpoints require is_admin claims",
                status=resp.status_code,
            )
        if resp.status_code == 404:
            raise BasinError(
                "not_found",
                f"Unknown pgwire user: {pgwire_user}",
                status=404,
            )
        if not resp.is_success:
            raise BasinError.from_response(resp)
        try:
            data = resp.json()
        except Exception:
            raise BasinError(
                "invalid_response",
                f"Admin rotateCredentials response was not JSON (HTTP {resp.status_code})",
                status=resp.status_code,
            )
        return ProvisionResult(connection_string=_extract_connection_url(data))

    async def list_credentials(self, project_id: str) -> List[Credential]:
        """
        ``GET /admin/v1/projects/:project_id/credentials`` — return
        credential descriptors for the project (no password/hash).
        """
        resp = await self._http.request(
            "GET",
            f"/admin/v1/projects/{quote(project_id, safe='')}/credentials",
            headers=self._get_headers(),
        )
        if resp.status_code in (401, 403):
            raise BasinError(
                "unauthorized",
                "Admin endpoints require is_admin claims",
                status=resp.status_code,
            )
        if not resp.is_success:
            raise BasinError.from_response(resp)
        try:
            items = resp.json()
        except Exception:
            raise BasinError(
                "invalid_response",
                f"Admin listCredentials response was not JSON (HTTP {resp.status_code})",
                status=resp.status_code,
            )
        if not isinstance(items, list):
            return []
        return [
            Credential(
                id=str(item.get("id", "")),
                project_id=str(item.get("project_id", "")),
                pgwire_user=str(item.get("pgwire_user", "")),
                dbname=str(item.get("dbname", "")),
                created_at=str(item.get("created_at", "")),
                rotated_at=item.get("rotated_at"),
            )
            for item in items
            if isinstance(item, dict)
        ]


class AdminClient:
    """
    ``client.admin`` — top-level admin namespace.

    Usage::

        result = await client.admin.projects.provision(project_id="my-proj")
        print(result.connection_string)
    """

    def __init__(
        self,
        *,
        http: HttpTransport,
        get_headers: Callable[[], Dict[str, str]],
    ) -> None:
        self.projects = AdminProjectsClient(http=http, get_headers=get_headers)
