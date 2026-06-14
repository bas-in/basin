from __future__ import annotations

import httpx
import pytest
import respx

from basin import BasinError, create_client

BASE = "http://test.basin.run"
KEY = "test-key"


def make_client(transport: httpx.MockTransport) -> object:
    from basin import ClientOptions
    return create_client(BASE, KEY, options=ClientOptions(transport=transport))


# ── provision ─────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_provision_returns_connection_string() -> None:
    with respx.mock(base_url=BASE) as mock:
        mock.post("/admin/v1/projects").mock(
            return_value=httpx.Response(
                201,
                json={
                    "project_id": "abc",
                    "pgwire_user": "user1",
                    "dbname": "basin",
                    "password": "secret",
                    "connection_url": "postgres://user1:secret@host/basin",
                },
            )
        )
        client = create_client(BASE, KEY)
        result = await client.admin.projects.provision(project_id="abc")
        assert result.connection_string == "postgres://user1:secret@host/basin"
        await client.aclose()


@pytest.mark.asyncio
async def test_provision_sends_project_id_in_body() -> None:
    with respx.mock(base_url=BASE) as mock:
        route = mock.post("/admin/v1/projects").mock(
            return_value=httpx.Response(
                201,
                json={"connection_url": "postgres://u:p@h/d"},
            )
        )
        client = create_client(BASE, KEY)
        await client.admin.projects.provision(project_id="my-proj")
        assert route.called
        request = route.calls[0].request
        import json
        body = json.loads(request.content)
        assert body["project_id"] == "my-proj"
        await client.aclose()


@pytest.mark.asyncio
async def test_provision_without_project_id_sends_empty_body() -> None:
    with respx.mock(base_url=BASE) as mock:
        route = mock.post("/admin/v1/projects").mock(
            return_value=httpx.Response(
                201,
                json={"connection_url": "postgres://u:p@h/d"},
            )
        )
        client = create_client(BASE, KEY)
        await client.admin.projects.provision()
        assert route.called
        request = route.calls[0].request
        import json
        body = json.loads(request.content)
        assert "project_id" not in body
        await client.aclose()


@pytest.mark.asyncio
async def test_provision_401_raises_unauthorized() -> None:
    with respx.mock(base_url=BASE) as mock:
        mock.post("/admin/v1/projects").mock(
            return_value=httpx.Response(401, json={"message": "not admin"})
        )
        client = create_client(BASE, KEY)
        with pytest.raises(BasinError) as exc_info:
            await client.admin.projects.provision()
        assert exc_info.value.code == "unauthorized"
        await client.aclose()


@pytest.mark.asyncio
async def test_provision_403_raises_unauthorized() -> None:
    with respx.mock(base_url=BASE) as mock:
        mock.post("/admin/v1/projects").mock(
            return_value=httpx.Response(403, json={"message": "forbidden"})
        )
        client = create_client(BASE, KEY)
        with pytest.raises(BasinError) as exc_info:
            await client.admin.projects.provision()
        assert exc_info.value.code == "unauthorized"
        await client.aclose()


@pytest.mark.asyncio
async def test_provision_fallback_to_postgres_url_scan() -> None:
    with respx.mock(base_url=BASE) as mock:
        mock.post("/admin/v1/projects").mock(
            return_value=httpx.Response(
                201,
                json={"some_field": "postgres://user:pass@host/db"},
            )
        )
        client = create_client(BASE, KEY)
        result = await client.admin.projects.provision()
        assert result.connection_string == "postgres://user:pass@host/db"
        await client.aclose()


# ── rotate_credentials ────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_rotate_credentials_hits_correct_route() -> None:
    with respx.mock(base_url=BASE) as mock:
        route = mock.post("/admin/v1/projects/user_abc/rotate").mock(
            return_value=httpx.Response(
                200,
                json={"connection_url": "postgres://user_abc:newpw@host/basin"},
            )
        )
        client = create_client(BASE, KEY)
        result = await client.admin.projects.rotate_credentials("user_abc")
        assert route.called
        assert result.connection_string == "postgres://user_abc:newpw@host/basin"
        await client.aclose()


@pytest.mark.asyncio
async def test_rotate_credentials_404_raises_not_found() -> None:
    with respx.mock(base_url=BASE) as mock:
        mock.post("/admin/v1/projects/unknown/rotate").mock(
            return_value=httpx.Response(404, json={"message": "not found"})
        )
        client = create_client(BASE, KEY)
        with pytest.raises(BasinError) as exc_info:
            await client.admin.projects.rotate_credentials("unknown")
        assert exc_info.value.code == "not_found"
        await client.aclose()


@pytest.mark.asyncio
async def test_rotate_credentials_401_raises_unauthorized() -> None:
    with respx.mock(base_url=BASE) as mock:
        mock.post("/admin/v1/projects/user1/rotate").mock(
            return_value=httpx.Response(401)
        )
        client = create_client(BASE, KEY)
        with pytest.raises(BasinError) as exc_info:
            await client.admin.projects.rotate_credentials("user1")
        assert exc_info.value.code == "unauthorized"
        await client.aclose()


# ── list_credentials ──────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_list_credentials_returns_credentials() -> None:
    with respx.mock(base_url=BASE) as mock:
        mock.get("/admin/v1/projects/proj123/credentials").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {
                        "id": "cred1",
                        "project_id": "proj123",
                        "pgwire_user": "user_abc",
                        "dbname": "basin",
                        "created_at": "2024-01-01T00:00:00Z",
                        "rotated_at": None,
                    }
                ],
            )
        )
        client = create_client(BASE, KEY)
        creds = await client.admin.projects.list_credentials("proj123")
        assert len(creds) == 1
        assert creds[0].id == "cred1"
        assert creds[0].pgwire_user == "user_abc"
        assert creds[0].rotated_at is None
        await client.aclose()


@pytest.mark.asyncio
async def test_list_credentials_empty_list() -> None:
    with respx.mock(base_url=BASE) as mock:
        mock.get("/admin/v1/projects/proj123/credentials").mock(
            return_value=httpx.Response(200, json=[])
        )
        client = create_client(BASE, KEY)
        creds = await client.admin.projects.list_credentials("proj123")
        assert creds == []
        await client.aclose()


@pytest.mark.asyncio
async def test_list_credentials_401_raises_unauthorized() -> None:
    with respx.mock(base_url=BASE) as mock:
        mock.get("/admin/v1/projects/proj123/credentials").mock(
            return_value=httpx.Response(403)
        )
        client = create_client(BASE, KEY)
        with pytest.raises(BasinError) as exc_info:
            await client.admin.projects.list_credentials("proj123")
        assert exc_info.value.code == "unauthorized"
        await client.aclose()
