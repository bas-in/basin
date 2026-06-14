"""
StorageClient — ``client.storage.from_(bucket)`` returns a ``StorageBucket``
with ``.upload``, ``.download``, ``.list``, ``.remove``, ``.create_signed_url``,
``.get_public_url``, ``.upload_multipart`` (not_implemented),
``.upload_resumable`` (not_implemented).

Engine routes (confirmed against basin-rest/src/server.rs + routes/storage.rs
and routes/storage_sign.rs, Phase 5.17.D):
  POST   /storage/v1/object/:bucket/*path              — upload
  GET    /storage/v1/object/:bucket/*path              — download
  POST   /storage/v1/object/list/:bucket               — list
  DELETE /storage/v1/object/:bucket                    — remove (bulk)
  POST   /storage/v1/object/sign/upload/:bucket/*path  — create_signed_url (mint)
  GET    /storage/v1/object/public/:project_id/:bucket/*path — public download

NOTE: The sign/mint path is /storage/v1/object/sign/upload/:bucket/*path
(``upload`` literal required — see ADR/server.rs comment re: route disambiguation
for axum, fixes #55).  basin-js uses /sign/:bucket/:path which was the old path;
the engine changed it post-5.17.D.  We follow the engine source of truth.
"""

from __future__ import annotations

import builtins
import contextlib
from collections.abc import Callable
from typing import Any
from urllib.parse import quote

from .._http import HttpTransport
from ..errors import BasinError
from .types import ObjectInfo

MULTIPART_THRESHOLD = 5 * 1024 * 1024


def _encode_path(path: str) -> str:
    """Encode each path segment but preserve ``/`` separators."""
    return "/".join(quote(seg, safe="") for seg in path.split("/"))


def _http_error(status: int, details: Any = None) -> BasinError:
    if status in (401, 403):
        return BasinError("unauthorized", "unauthorized", status=status, details=details)
    if status == 404:
        return BasinError("not_found", "object not found", status=status, details=details)
    return BasinError(
        "internal",
        f"storage request failed (HTTP {status})",
        status=status,
        details=details,
    )


class StorageBucket:
    """
    Per-bucket handle.  Obtain via ``client.storage.from_(bucket)``.
    """

    MULTIPART_THRESHOLD = MULTIPART_THRESHOLD

    def __init__(
        self,
        bucket: str,
        *,
        http: HttpTransport,
        get_headers: Callable[[], dict[str, str]],
    ) -> None:
        self._bucket = bucket
        self._http = http
        self._get_headers = get_headers

    def _headers(self, extra: dict[str, str] | None = None) -> dict[str, str]:
        h = dict(self._get_headers())
        if extra:
            h.update(extra)
        return h

    async def upload(
        self,
        path: str,
        data: bytes,
        *,
        content_type: str = "application/octet-stream",
        upsert: bool = False,
    ) -> dict[str, str]:
        """
        Upload ``data`` to ``{bucket}/{path}``.

        ``POST /storage/v1/object/:bucket/*path``

        Returns ``{"path": path}`` on success.
        Raises ``BasinError`` on failure.
        """
        bucket_enc = quote(self._bucket, safe="")
        path_enc = _encode_path(path)
        url = f"/storage/v1/object/{bucket_enc}/{path_enc}"

        extra: dict[str, str] = {"Content-Type": content_type}
        if upsert:
            extra["x-upsert"] = "true"
        headers = self._headers(extra)

        try:
            resp = await self._http.request("POST", url, headers=headers, content=data)
        except BasinError:
            raise
        except Exception as exc:
            raise BasinError("network", str(exc)) from exc

        if not resp.is_success:
            details: Any = None
            with contextlib.suppress(Exception):
                details = resp.json()
            raise _http_error(resp.status_code, details)

        return {"path": path}

    async def download(self, path: str) -> bytes:
        """
        Download ``{bucket}/{path}`` and return raw bytes.

        ``GET /storage/v1/object/:bucket/*path``
        """
        bucket_enc = quote(self._bucket, safe="")
        path_enc = _encode_path(path)
        url = f"/storage/v1/object/{bucket_enc}/{path_enc}"

        try:
            resp = await self._http.request("GET", url, headers=self._headers())
        except BasinError:
            raise
        except Exception as exc:
            raise BasinError("network", str(exc)) from exc

        if not resp.is_success:
            details = None
            with contextlib.suppress(Exception):
                details = resp.json()
            raise _http_error(resp.status_code, details)

        return resp.content

    async def list(
        self,
        prefix: str = "",
        *,
        limit: int | None = None,
        offset: int | None = None,
        sort_by: dict[str, str] | None = None,
    ) -> builtins.list[ObjectInfo]:
        """
        List objects in the bucket under ``prefix``.

        ``POST /storage/v1/object/list/:bucket``

        Returns ``[]`` (not ``None``) on empty result.
        """
        bucket_enc = quote(self._bucket, safe="")
        url = f"/storage/v1/object/list/{bucket_enc}"

        body: dict[str, Any] = {"prefix": prefix}
        if limit is not None:
            body["limit"] = limit
        if offset is not None:
            body["offset"] = offset
        if sort_by is not None:
            body["sortBy"] = sort_by

        try:
            resp = await self._http.request(
                "POST",
                url,
                headers=self._headers({"Content-Type": "application/json"}),
                json=body,
            )
        except BasinError:
            raise
        except Exception as exc:
            raise BasinError("network", str(exc)) from exc

        if not resp.is_success:
            details = None
            with contextlib.suppress(Exception):
                details = resp.json()
            raise _http_error(resp.status_code, details)

        try:
            items = resp.json()
        except Exception as exc:
            raise BasinError(
                "internal",
                "storage.list response was not JSON",
                status=resp.status_code,
            ) from exc

        if not isinstance(items, list):
            return []

        return [
            ObjectInfo(
                name=str(item.get("name", "")),
                size=int(item.get("size", 0)),
                content_type=str(item.get("contentType", "")),
                created_at=str(item.get("created_at", "")),
                updated_at=str(item.get("updated_at", "")),
                metadata=item.get("metadata"),
            )
            for item in items
            if isinstance(item, dict)
        ]

    async def remove(self, paths: builtins.list[str]) -> dict[str, Any]:
        """
        Remove objects in bulk.

        ``DELETE /storage/v1/object/:bucket`` with ``{"prefixes": paths}``.

        Returns ``{"paths": paths}`` on success.
        """
        bucket_enc = quote(self._bucket, safe="")
        url = f"/storage/v1/object/{bucket_enc}"

        try:
            resp = await self._http.request(
                "DELETE",
                url,
                headers=self._headers({"Content-Type": "application/json"}),
                json={"prefixes": paths},
            )
        except BasinError:
            raise
        except Exception as exc:
            raise BasinError("network", str(exc)) from exc

        if not resp.is_success:
            details = None
            with contextlib.suppress(Exception):
                details = resp.json()
            raise _http_error(resp.status_code, details)

        return {"paths": paths}

    async def create_signed_url(
        self,
        path: str,
        expires_in: int,
    ) -> dict[str, Any]:
        """
        Mint a short-lived signed URL for ``{bucket}/{path}``.

        ``POST /storage/v1/object/sign/upload/:bucket/*path``

        ``expires_in`` must be non-negative (seconds).  Raises
        ``BasinError("invalid_request", …)`` immediately for negative values.

        Returns ``{"signed_url": str, "expires_at": str}`` on success.

        NOTE: The engine route uses the ``upload`` literal segment to
        disambiguate from the download path (server.rs, Phase 5.17.D, fixes #55).
        """
        if expires_in < 0:
            raise BasinError(
                "invalid_request",
                "expires_in must be a non-negative number of seconds",
            )

        bucket_enc = quote(self._bucket, safe="")
        path_enc = _encode_path(path)
        url = f"/storage/v1/object/sign/upload/{bucket_enc}/{path_enc}"

        try:
            resp = await self._http.request(
                "POST",
                url,
                headers=self._headers({"Content-Type": "application/json"}),
                json={"expires_in": expires_in},
            )
        except BasinError:
            raise
        except Exception as exc:
            raise BasinError("network", str(exc)) from exc

        if not resp.is_success:
            details = None
            with contextlib.suppress(Exception):
                details = resp.json()
            raise _http_error(resp.status_code, details)

        try:
            body = resp.json()
        except Exception as exc:
            raise BasinError(
                "internal",
                "storage.create_signed_url response was not JSON",
                status=resp.status_code,
            ) from exc

        raw_url = ""
        if isinstance(body, dict):
            raw_url = body.get("signedUrl") or body.get("signedURL") or ""

        signed_url = raw_url
        if raw_url and not raw_url.startswith("http"):
            base = self._http._base_url
            signed_url = base + ("" if raw_url.startswith("/") else "/") + raw_url

        result: dict[str, Any] = {"signed_url": signed_url}
        if isinstance(body, dict) and body.get("expiresAt"):
            result["expires_at"] = body["expiresAt"]
        return result

    def get_public_url(self, path: str, *, project_id: str = "") -> dict[str, str]:
        """
        Construct a public URL for ``{bucket}/{path}`` synchronously.

        No network call — pure URL composition.  Only valid for public buckets.

        ``GET /storage/v1/object/public/:project_id/:bucket/*path``
        """
        bucket_enc = quote(self._bucket, safe="")
        path_enc = _encode_path(path)
        proj_enc = quote(project_id, safe="") if project_id else "public"
        public_url = (
            f"{self._http._base_url}/storage/v1/object/public"
            f"/{proj_enc}/{bucket_enc}/{path_enc}"
        )
        return {"public_url": public_url}

    async def upload_multipart(
        self,
        _path: str,
        _data: bytes,
        **_kwargs: Any,
    ) -> None:
        """
        Multipart upload for files > 5 MB.

        Returns ``BasinError("not_implemented")`` — basin-engine has no
        presigned-multipart surface.  Lands in basin v0.3+.
        """
        raise BasinError(
            "not_implemented",
            "storage.upload_multipart ships when the engine route lands — tracked in ROADMAP 0.3",
        )

    async def upload_resumable(
        self,
        _path: str,
        _data: bytes,
        **_kwargs: Any,
    ) -> None:
        """
        Resumable upload via TUS (tus.io).

        Returns ``BasinError("not_implemented")`` — basin-engine has no TUS
        proxy surface.  Lands in basin v0.3+.
        """
        raise BasinError(
            "not_implemented",
            "storage.upload_resumable (TUS) ships when the engine route lands — tracked in ROADMAP 0.3",
        )


class StorageClient:
    """
    ``client.storage`` — top-level storage namespace.

    Usage::

        bucket = client.storage.from_("my-bucket")
        await bucket.upload("folder/file.png", data)
    """

    def __init__(
        self,
        *,
        http: HttpTransport,
        get_headers: Callable[[], dict[str, str]],
    ) -> None:
        self._http = http
        self._get_headers = get_headers

    def from_(self, bucket: str) -> StorageBucket:
        return StorageBucket(bucket, http=self._http, get_headers=self._get_headers)
