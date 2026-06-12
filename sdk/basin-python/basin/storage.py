"""Storage client — bindings for /storage/v1/* (ADR 0021 surface).

Route sources (verified, crates/basin-rest/src/server.rs:373-424; handlers
in crates/basin-rest/src/routes/storage.rs and storage_sign.rs):

- POST   /storage/v1/bucket                                  create bucket
- GET    /storage/v1/bucket/:name                            bucket metadata
- DELETE /storage/v1/bucket/:name                            delete + purge
- POST   /storage/v1/object/:bucket/*path                    upload (JWT)
- GET    /storage/v1/object/:bucket/*path                    download (JWT)
- DELETE /storage/v1/object/:bucket/*path                    delete (JWT)
- POST   /storage/v1/object/list/:bucket                     list (body: prefix/limit/offset)
- DELETE /storage/v1/object/:bucket                          bulk delete (body: prefixes[])
- GET    /storage/v1/object/public/:project_id/:bucket/*path public download (no JWT)
- POST   /storage/v1/object/sign/upload/:bucket/*path        mint signed URL (JWT)
- GET    /storage/v1/object/sign/:project/:bucket/*path      signed download (no JWT)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional
from urllib.parse import quote

from .errors import BasinApiError
from .types import Bucket, SignedUrl, StorageObject

if TYPE_CHECKING:
    from ._http import SyncTransport, AsyncTransport


def _encode_object_path(path: str) -> str:
    return "/".join(quote(seg, safe="") for seg in path.split("/"))


@dataclass
class DownloadResult:
    data: bytes
    content_type: Optional[str]
    etag: Optional[str]


class StorageClient:
    """Sync storage client. Access object operations via .from_bucket(name)."""

    def __init__(
        self,
        transport: "SyncTransport",
        project_id_getter: Any = None,
    ) -> None:
        self._transport = transport
        self._project_id_getter = project_id_getter

    def create_bucket(
        self,
        name: str,
        *,
        public: bool = False,
        file_size_limit: Optional[int] = None,
        allowed_mime_types: Optional[list[str]] = None,
    ) -> Bucket:
        """POST /storage/v1/bucket (201)."""
        data = self._transport.request_json(
            "POST",
            "/storage/v1/bucket",
            body={
                "name": name,
                "public": public,
                "file_size_limit": file_size_limit,
                "allowed_mime_types": allowed_mime_types or [],
            },
        )
        return Bucket.from_dict(data)

    def get_bucket(self, name: str) -> Bucket:
        """GET /storage/v1/bucket/:name."""
        data = self._transport.request_json(
            "GET", f"/storage/v1/bucket/{quote(name, safe='')}"
        )
        return Bucket.from_dict(data)

    def delete_bucket(self, name: str) -> dict:
        """DELETE /storage/v1/bucket/:name — also purges object bytes."""
        return self._transport.request_json(
            "DELETE", f"/storage/v1/bucket/{quote(name, safe='')}"
        )

    def from_bucket(self, bucket: str) -> "StorageBucketClient":
        """Return object-level client scoped to one bucket."""
        return StorageBucketClient(
            self._transport, bucket, self._project_id_getter
        )


class StorageBucketClient:
    """Sync object operations scoped to one bucket."""

    def __init__(
        self,
        transport: "SyncTransport",
        bucket: str,
        project_id_getter: Any = None,
    ) -> None:
        self._transport = transport
        self._bucket = bucket
        self._project_id_getter = project_id_getter

    def _object_path(self, path: str) -> str:
        return f"/storage/v1/object/{quote(self._bucket, safe='')}/{_encode_object_path(path)}"

    def upload(
        self,
        path: str,
        data: bytes,
        *,
        content_type: Optional[str] = None,
    ) -> StorageObject:
        """POST /storage/v1/object/:bucket/*path."""
        headers: dict[str, str] = {}
        if content_type is not None:
            headers["content-type"] = content_type
        result = self._transport.request_json(
            "POST",
            self._object_path(path),
            raw_body=data,
            headers=headers,
        )
        return StorageObject.from_dict(result)

    def download(self, path: str) -> DownloadResult:
        """GET /storage/v1/object/:bucket/*path — authenticated download."""
        response = self._transport.request("GET", self._object_path(path))
        return DownloadResult(
            data=response.content,
            content_type=response.headers.get("content-type"),
            etag=response.headers.get("etag"),
        )

    def remove(self, path: str) -> dict:
        """DELETE /storage/v1/object/:bucket/*path — single delete."""
        return self._transport.request_json(
            "DELETE", self._object_path(path)
        )

    def list(
        self,
        *,
        prefix: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> list[StorageObject]:
        """POST /storage/v1/object/list/:bucket."""
        data = self._transport.request_json(
            "POST",
            f"/storage/v1/object/list/{quote(self._bucket, safe='')}",
            body={"prefix": prefix, "limit": limit, "offset": offset},
        )
        return [StorageObject.from_dict(d) for d in (data or [])]

    def remove_by_prefixes(self, prefixes: list[str]) -> dict:
        """DELETE /storage/v1/object/:bucket with { prefixes: [...] }.

        Bulk-delete every object matching any prefix. RLS-denied objects are
        skipped server-side.
        """
        return self._transport.request_json(
            "DELETE",
            f"/storage/v1/object/{quote(self._bucket, safe='')}",
            body={"prefixes": prefixes},
        )

    def get_public_url(self, path: str) -> str:
        """Build the no-JWT public URL.

        GET /storage/v1/object/public/:project_id/:bucket/*path
        Only works for buckets with public=True.
        """
        project = self._project_id_getter() if self._project_id_getter else None
        if project is None:
            raise BasinApiError(
                "E_INVALID_REQUEST",
                "public URLs require a project id (pass project_id to create_client())",
                0,
            )
        base = self._transport.base_url
        return (
            f"{base}/storage/v1/object/public/{project}"
            f"/{quote(self._bucket, safe='')}/{_encode_object_path(path)}"
        )

    def create_signed_url(
        self,
        path: str,
        *,
        expires_in: int = 3600,
    ) -> SignedUrl:
        """POST /storage/v1/object/sign/upload/:bucket/*path — mint signed URL.

        TTL is capped at 7 days server-side; default 3600s.
        Despite the 'upload' literal in the path this mints a download URL —
        that segment only disambiguates the axum router (see storage_sign.rs).
        """
        data = self._transport.request_json(
            "POST",
            f"/storage/v1/object/sign/upload"
            f"/{quote(self._bucket, safe='')}/{_encode_object_path(path)}",
            body={"expires_in": expires_in},
        )
        return SignedUrl.from_dict(data, self._transport.base_url)


# ---------------------------------------------------------------------------
# Async variants
# ---------------------------------------------------------------------------


class AsyncStorageClient:
    def __init__(
        self,
        transport: "AsyncTransport",
        project_id_getter: Any = None,
    ) -> None:
        self._transport = transport
        self._project_id_getter = project_id_getter

    async def create_bucket(
        self,
        name: str,
        *,
        public: bool = False,
        file_size_limit: Optional[int] = None,
        allowed_mime_types: Optional[list[str]] = None,
    ) -> Bucket:
        data = await self._transport.request_json(
            "POST",
            "/storage/v1/bucket",
            body={
                "name": name,
                "public": public,
                "file_size_limit": file_size_limit,
                "allowed_mime_types": allowed_mime_types or [],
            },
        )
        return Bucket.from_dict(data)

    async def get_bucket(self, name: str) -> Bucket:
        data = await self._transport.request_json(
            "GET", f"/storage/v1/bucket/{quote(name, safe='')}"
        )
        return Bucket.from_dict(data)

    async def delete_bucket(self, name: str) -> dict:
        return await self._transport.request_json(
            "DELETE", f"/storage/v1/bucket/{quote(name, safe='')}"
        )

    def from_bucket(self, bucket: str) -> "AsyncStorageBucketClient":
        return AsyncStorageBucketClient(
            self._transport, bucket, self._project_id_getter
        )


class AsyncStorageBucketClient:
    def __init__(
        self,
        transport: "AsyncTransport",
        bucket: str,
        project_id_getter: Any = None,
    ) -> None:
        self._transport = transport
        self._bucket = bucket
        self._project_id_getter = project_id_getter

    def _object_path(self, path: str) -> str:
        return f"/storage/v1/object/{quote(self._bucket, safe='')}/{_encode_object_path(path)}"

    async def upload(
        self,
        path: str,
        data: bytes,
        *,
        content_type: Optional[str] = None,
    ) -> StorageObject:
        headers: dict[str, str] = {}
        if content_type is not None:
            headers["content-type"] = content_type
        result = await self._transport.request_json(
            "POST",
            self._object_path(path),
            raw_body=data,
            headers=headers,
        )
        return StorageObject.from_dict(result)

    async def download(self, path: str) -> DownloadResult:
        response = await self._transport.request("GET", self._object_path(path))
        return DownloadResult(
            data=response.content,
            content_type=response.headers.get("content-type"),
            etag=response.headers.get("etag"),
        )

    async def remove(self, path: str) -> dict:
        return await self._transport.request_json(
            "DELETE", self._object_path(path)
        )

    async def list(
        self,
        *,
        prefix: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> list[StorageObject]:
        data = await self._transport.request_json(
            "POST",
            f"/storage/v1/object/list/{quote(self._bucket, safe='')}",
            body={"prefix": prefix, "limit": limit, "offset": offset},
        )
        return [StorageObject.from_dict(d) for d in (data or [])]

    async def remove_by_prefixes(self, prefixes: list[str]) -> dict:
        return await self._transport.request_json(
            "DELETE",
            f"/storage/v1/object/{quote(self._bucket, safe='')}",
            body={"prefixes": prefixes},
        )

    def get_public_url(self, path: str) -> str:
        project = self._project_id_getter() if self._project_id_getter else None
        if project is None:
            raise BasinApiError(
                "E_INVALID_REQUEST",
                "public URLs require a project id",
                0,
            )
        base = self._transport.base_url
        return (
            f"{base}/storage/v1/object/public/{project}"
            f"/{quote(self._bucket, safe='')}/{_encode_object_path(path)}"
        )

    async def create_signed_url(
        self,
        path: str,
        *,
        expires_in: int = 3600,
    ) -> SignedUrl:
        data = await self._transport.request_json(
            "POST",
            f"/storage/v1/object/sign/upload"
            f"/{quote(self._bucket, safe='')}/{_encode_object_path(path)}",
            body={"expires_in": expires_in},
        )
        return SignedUrl.from_dict(data, self._transport.base_url)
