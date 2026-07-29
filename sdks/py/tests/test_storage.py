from __future__ import annotations

import httpx
import pytest
import respx

from basin import BasinError, create_client

BASE = "http://test.basin.run"
KEY = "test-key"


# ── upload ─────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_upload_posts_to_correct_url() -> None:
    with respx.mock(base_url=BASE) as mock:
        route = mock.post("/storage/v1/object/my-bucket/folder%2Ffile.txt").mock(
            return_value=httpx.Response(200, json={"key": "folder/file.txt"})
        )
        client = create_client(BASE, KEY)
        result = await client.storage.from_("my-bucket").upload(
            "folder/file.txt", b"hello world"
        )
        assert route.called
        assert result["path"] == "folder/file.txt"
        await client.aclose()


@pytest.mark.asyncio
async def test_upload_sets_content_type() -> None:
    with respx.mock(base_url=BASE) as mock:
        route = mock.post("/storage/v1/object/bucket/img.png").mock(
            return_value=httpx.Response(200, json={})
        )
        client = create_client(BASE, KEY)
        await client.storage.from_("bucket").upload(
            "img.png", b"\x89PNG", content_type="image/png"
        )
        req = route.calls[0].request
        assert req.headers["content-type"] == "image/png"
        await client.aclose()


@pytest.mark.asyncio
async def test_upload_upsert_header() -> None:
    with respx.mock(base_url=BASE) as mock:
        route = mock.post("/storage/v1/object/bucket/file.txt").mock(
            return_value=httpx.Response(200, json={})
        )
        client = create_client(BASE, KEY)
        await client.storage.from_("bucket").upload("file.txt", b"x", upsert=True)
        req = route.calls[0].request
        assert req.headers.get("x-upsert") == "true"
        await client.aclose()


@pytest.mark.asyncio
async def test_upload_401_raises_unauthorized() -> None:
    with respx.mock(base_url=BASE) as mock:
        mock.post("/storage/v1/object/bucket/file.txt").mock(
            return_value=httpx.Response(401)
        )
        client = create_client(BASE, KEY)
        with pytest.raises(BasinError) as exc_info:
            await client.storage.from_("bucket").upload("file.txt", b"x")
        assert exc_info.value.code == "unauthorized"
        await client.aclose()


@pytest.mark.asyncio
async def test_upload_404_raises_not_found() -> None:
    with respx.mock(base_url=BASE) as mock:
        mock.post("/storage/v1/object/bucket/missing.txt").mock(
            return_value=httpx.Response(404)
        )
        client = create_client(BASE, KEY)
        with pytest.raises(BasinError) as exc_info:
            await client.storage.from_("bucket").upload("missing.txt", b"x")
        assert exc_info.value.code == "not_found"
        await client.aclose()


# ── download ───────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_download_returns_bytes() -> None:
    with respx.mock(base_url=BASE) as mock:
        mock.get("/storage/v1/object/my-bucket/file.txt").mock(
            return_value=httpx.Response(200, content=b"file content")
        )
        client = create_client(BASE, KEY)
        data = await client.storage.from_("my-bucket").download("file.txt")
        assert data == b"file content"
        await client.aclose()


@pytest.mark.asyncio
async def test_download_404_raises_not_found() -> None:
    with respx.mock(base_url=BASE) as mock:
        mock.get("/storage/v1/object/bucket/nope.txt").mock(
            return_value=httpx.Response(404)
        )
        client = create_client(BASE, KEY)
        with pytest.raises(BasinError) as exc_info:
            await client.storage.from_("bucket").download("nope.txt")
        assert exc_info.value.code == "not_found"
        await client.aclose()


# ── list ───────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_list_returns_object_info() -> None:
    with respx.mock(base_url=BASE) as mock:
        mock.post("/storage/v1/object/list/my-bucket").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {
                        "name": "file.txt",
                        "size": 123,
                        "contentType": "text/plain",
                        "created_at": "2024-01-01T00:00:00Z",
                        "updated_at": "2024-01-01T00:00:00Z",
                    }
                ],
            )
        )
        client = create_client(BASE, KEY)
        items = await client.storage.from_("my-bucket").list()
        assert len(items) == 1
        assert items[0].name == "file.txt"
        assert items[0].size == 123
        await client.aclose()


@pytest.mark.asyncio
async def test_list_sends_prefix_in_body() -> None:
    import json as jsonlib
    with respx.mock(base_url=BASE) as mock:
        route = mock.post("/storage/v1/object/list/bucket").mock(
            return_value=httpx.Response(200, json=[])
        )
        client = create_client(BASE, KEY)
        await client.storage.from_("bucket").list("my/prefix", limit=10)
        req = route.calls[0].request
        body = jsonlib.loads(req.content)
        assert body["prefix"] == "my/prefix"
        assert body["limit"] == 10
        await client.aclose()


@pytest.mark.asyncio
async def test_list_empty_returns_empty_list() -> None:
    with respx.mock(base_url=BASE) as mock:
        mock.post("/storage/v1/object/list/bucket").mock(
            return_value=httpx.Response(200, json=[])
        )
        client = create_client(BASE, KEY)
        items = await client.storage.from_("bucket").list()
        assert items == []
        await client.aclose()


# ── remove ─────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_remove_sends_prefixes() -> None:
    import json as jsonlib
    with respx.mock(base_url=BASE) as mock:
        route = mock.delete("/storage/v1/object/bucket").mock(
            return_value=httpx.Response(200, json={})
        )
        client = create_client(BASE, KEY)
        result = await client.storage.from_("bucket").remove(["a.txt", "b.txt"])
        req = route.calls[0].request
        body = jsonlib.loads(req.content)
        assert body["prefixes"] == ["a.txt", "b.txt"]
        assert result["paths"] == ["a.txt", "b.txt"]
        await client.aclose()


@pytest.mark.asyncio
async def test_remove_401_raises_unauthorized() -> None:
    with respx.mock(base_url=BASE) as mock:
        mock.delete("/storage/v1/object/bucket").mock(
            return_value=httpx.Response(403)
        )
        client = create_client(BASE, KEY)
        with pytest.raises(BasinError) as exc_info:
            await client.storage.from_("bucket").remove(["f.txt"])
        assert exc_info.value.code == "unauthorized"
        await client.aclose()


# ── create_signed_url ──────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_create_signed_url_uses_upload_literal_path() -> None:
    """Engine uses /sign/upload/:bucket/*path (Phase 5.17.D disambiguation)."""
    with respx.mock(base_url=BASE) as mock:
        route = mock.post("/storage/v1/object/sign/upload/bucket/file.txt").mock(
            return_value=httpx.Response(
                200,
                json={
                    "signedUrl": "/storage/v1/object/sign/proj/bucket/file.txt?token=abc&expires=9999",
                    "expiresAt": "2024-12-31T00:00:00Z",
                },
            )
        )
        client = create_client(BASE, KEY)
        result = await client.storage.from_("bucket").create_signed_url("file.txt", 3600)
        assert route.called
        assert "token=abc" in result["signed_url"]
        assert result["expires_at"] == "2024-12-31T00:00:00Z"
        await client.aclose()


@pytest.mark.asyncio
async def test_create_signed_url_negative_raises_invalid_request() -> None:
    client = create_client(BASE, KEY)
    with pytest.raises(BasinError) as exc_info:
        await client.storage.from_("bucket").create_signed_url("f.txt", -1)
    assert exc_info.value.code == "invalid_request"
    await client.aclose()


@pytest.mark.asyncio
async def test_create_signed_url_resolves_relative_url() -> None:
    with respx.mock(base_url=BASE) as mock:
        mock.post("/storage/v1/object/sign/upload/bucket/file.txt").mock(
            return_value=httpx.Response(
                200,
                json={
                    "signedUrl": "/storage/v1/object/sign/proj/bucket/file.txt?token=xyz",
                },
            )
        )
        client = create_client(BASE, KEY)
        result = await client.storage.from_("bucket").create_signed_url("file.txt", 60)
        assert result["signed_url"].startswith(BASE)
        await client.aclose()


# ── get_public_url ─────────────────────────────────────────────────────────────

def test_get_public_url_no_network() -> None:
    from basin import create_client
    client = create_client(BASE, KEY)
    result = client.storage.from_("photos").get_public_url("cat.jpg", project_id="proj1")
    assert "proj1" in result["public_url"]
    assert "photos" in result["public_url"]
    assert "cat.jpg" in result["public_url"]


# ── not_implemented stubs ──────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_upload_multipart_raises_not_implemented() -> None:
    client = create_client(BASE, KEY)
    with pytest.raises(BasinError) as exc_info:
        await client.storage.from_("bucket").upload_multipart("f.txt", b"x")
    assert exc_info.value.code == "not_implemented"
    await client.aclose()


@pytest.mark.asyncio
async def test_upload_resumable_raises_not_implemented() -> None:
    client = create_client(BASE, KEY)
    with pytest.raises(BasinError) as exc_info:
        await client.storage.from_("bucket").upload_resumable("f.txt", b"x")
    assert exc_info.value.code == "not_implemented"
    await client.aclose()
