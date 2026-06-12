"""Tests for StorageClient — offline, respx-mocked transport."""

import httpx
import pytest
import respx

from basin.client import create_client, create_async_client
from basin.errors import BasinApiError

BASE = "http://basin.test"
KEY = "testapikey"
PROJECT = "01JTEST000000000000000000A"

BUCKET_DATA = {
    "id": "bucket-uuid",
    "name": "avatars",
    "public": False,
    "file_size_limit": None,
    "allowed_mime_types": [],
    "created_at": "2026-01-01T00:00:00Z",
    "updated_at": "2026-01-01T00:00:00Z",
}

OBJECT_DATA = {
    "id": "obj-uuid",
    "bucket_id": "bucket-uuid",
    "path": "user/1.png",
    "size": 1024,
    "mime_type": "image/png",
    "metadata": {},
    "owner": None,
    "etag": '"abc123"',
    "created_at": "2026-01-01T00:00:00Z",
    "updated_at": "2026-01-01T00:00:00Z",
}


@respx.mock
def test_create_bucket():
    respx.post(f"{BASE}/storage/v1/bucket").mock(
        return_value=httpx.Response(201, json=BUCKET_DATA)
    )
    client = create_client(BASE, KEY, project_id=PROJECT)
    bucket = client.storage.create_bucket("avatars")
    assert bucket.name == "avatars"
    assert bucket.public is False


@respx.mock
def test_create_bucket_with_options():
    route = respx.post(f"{BASE}/storage/v1/bucket").mock(
        return_value=httpx.Response(
            201, json={**BUCKET_DATA, "public": True, "file_size_limit": 5000000}
        )
    )
    client = create_client(BASE, KEY, project_id=PROJECT)
    bucket = client.storage.create_bucket(
        "media", public=True, file_size_limit=5000000, allowed_mime_types=["image/jpeg"]
    )
    assert bucket.public is True
    assert bucket.file_size_limit == 5000000


@respx.mock
def test_get_bucket():
    respx.get(f"{BASE}/storage/v1/bucket/avatars").mock(
        return_value=httpx.Response(200, json=BUCKET_DATA)
    )
    client = create_client(BASE, KEY, project_id=PROJECT)
    bucket = client.storage.get_bucket("avatars")
    assert bucket.id == "bucket-uuid"


@respx.mock
def test_delete_bucket():
    respx.delete(f"{BASE}/storage/v1/bucket/avatars").mock(
        return_value=httpx.Response(200, json={"message": "deleted", "objects_purged": 0})
    )
    client = create_client(BASE, KEY, project_id=PROJECT)
    result = client.storage.delete_bucket("avatars")
    assert result["objects_purged"] == 0


@respx.mock
def test_upload_object():
    respx.post(f"{BASE}/storage/v1/object/avatars/user%2F1.png").mock(
        return_value=httpx.Response(201, json=OBJECT_DATA)
    )
    client = create_client(BASE, KEY, project_id=PROJECT)
    obj = client.storage.from_bucket("avatars").upload(
        "user/1.png", b"pngbytes", content_type="image/png"
    )
    assert obj.path == "user/1.png"
    assert obj.size == 1024


@respx.mock
def test_download_object():
    respx.get(f"{BASE}/storage/v1/object/avatars/user%2F1.png").mock(
        return_value=httpx.Response(
            200,
            content=b"pngbytes",
            headers={"content-type": "image/png", "etag": '"abc"'},
        )
    )
    client = create_client(BASE, KEY, project_id=PROJECT)
    result = client.storage.from_bucket("avatars").download("user/1.png")
    assert result.data == b"pngbytes"
    assert result.content_type == "image/png"
    assert result.etag == '"abc"'


@respx.mock
def test_remove_object():
    respx.delete(f"{BASE}/storage/v1/object/avatars/user%2F1.png").mock(
        return_value=httpx.Response(200, json={"message": "deleted"})
    )
    client = create_client(BASE, KEY, project_id=PROJECT)
    result = client.storage.from_bucket("avatars").remove("user/1.png")
    assert result["message"] == "deleted"


@respx.mock
def test_list_objects():
    respx.post(f"{BASE}/storage/v1/object/list/avatars").mock(
        return_value=httpx.Response(200, json=[OBJECT_DATA])
    )
    client = create_client(BASE, KEY, project_id=PROJECT)
    objects = client.storage.from_bucket("avatars").list(prefix="user/")
    assert len(objects) == 1
    assert objects[0].etag == '"abc123"'


@respx.mock
def test_remove_by_prefixes():
    respx.delete(f"{BASE}/storage/v1/object/avatars").mock(
        return_value=httpx.Response(200, json={"deleted": 3})
    )
    client = create_client(BASE, KEY, project_id=PROJECT)
    result = client.storage.from_bucket("avatars").remove_by_prefixes(["user/"])
    assert result["deleted"] == 3


def test_get_public_url():
    client = create_client(BASE, KEY, project_id=PROJECT)
    url = client.storage.from_bucket("avatars").get_public_url("user/1.png")
    assert url.startswith(BASE)
    assert "/storage/v1/object/public/" in url
    assert PROJECT in url
    assert "avatars" in url
    assert "user%2F1.png" in url or "user/1.png" in url


def test_get_public_url_no_project_raises():
    client = create_client(BASE, KEY)  # no project_id, key is not a JWT
    with pytest.raises(BasinApiError) as exc_info:
        client.storage.from_bucket("avatars").get_public_url("file.png")
    assert exc_info.value.code == "E_INVALID_REQUEST"


@respx.mock
def test_create_signed_url():
    respx.post(
        f"{BASE}/storage/v1/object/sign/upload/avatars/user%2F1.png"
    ).mock(
        return_value=httpx.Response(
            200,
            json={
                "signedUrl": "/storage/v1/object/sign/proj/avatars/user%2F1.png?token=xxx",
                "expiresAt": "2026-01-01T01:00:00Z",
            },
        )
    )
    client = create_client(BASE, KEY, project_id=PROJECT)
    signed = client.storage.from_bucket("avatars").create_signed_url(
        "user/1.png", expires_in=3600
    )
    assert signed.signed_url.startswith("/storage")
    assert signed.absolute_url.startswith(BASE)


# ---------------------------------------------------------------------------
# Async variants
# ---------------------------------------------------------------------------


@respx.mock
async def test_async_create_bucket():
    respx.post(f"{BASE}/storage/v1/bucket").mock(
        return_value=httpx.Response(201, json=BUCKET_DATA)
    )
    async with create_async_client(BASE, KEY, project_id=PROJECT) as client:
        bucket = await client.storage.create_bucket("avatars")
        assert bucket.name == "avatars"


@respx.mock
async def test_async_upload():
    respx.post(f"{BASE}/storage/v1/object/avatars/user%2F1.png").mock(
        return_value=httpx.Response(201, json=OBJECT_DATA)
    )
    async with create_async_client(BASE, KEY, project_id=PROJECT) as client:
        obj = await client.storage.from_bucket("avatars").upload(
            "user/1.png", b"bytes"
        )
        assert obj.id == "obj-uuid"
