from __future__ import annotations

import json
import pytest
import httpx
import respx

from basin import create_client, BasinError

BASE = "http://localhost:5434"
KEY = "basin_test"
TABLE = "products"
URL = f"{BASE}/rest/v1/{TABLE}"


def client():
    return create_client(BASE, KEY)


# ── filter rendering ────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_eq_filter():
    with respx.mock:
        route = respx.get(URL).mock(return_value=httpx.Response(200, json=[]))
        async with client() as c:
            await c.from_(TABLE).select().eq("id", 1)
        req = route.calls.last.request
        assert "id=eq.1" in str(req.url)


@pytest.mark.asyncio
async def test_neq_filter():
    with respx.mock:
        route = respx.get(URL).mock(return_value=httpx.Response(200, json=[]))
        async with client() as c:
            await c.from_(TABLE).select().neq("status", "inactive")
        qs = str(route.calls.last.request.url.query)
        assert "status=neq.inactive" in qs


@pytest.mark.asyncio
async def test_gt_gte_lt_lte():
    with respx.mock:
        route = respx.get(URL).mock(return_value=httpx.Response(200, json=[]))
        async with client() as c:
            await c.from_(TABLE).select().gt("price", 10).gte("stock", 0).lt("age", 99).lte("rank", 5)
        qs = str(route.calls.last.request.url.query)
        assert "price=gt.10" in qs
        assert "stock=gte.0" in qs
        assert "age=lt.99" in qs
        assert "rank=lte.5" in qs


@pytest.mark.asyncio
async def test_like_ilike():
    with respx.mock:
        route = respx.get(URL).mock(return_value=httpx.Response(200, json=[]))
        async with client() as c:
            await c.from_(TABLE).select().like("name", "%foo%").ilike("email", "%@example.com")
        qs = str(route.calls.last.request.url.query)
        assert "name=like.%25foo%25" in qs
        assert "email=ilike.%25%40example.com" in qs


@pytest.mark.asyncio
async def test_is_null():
    with respx.mock:
        route = respx.get(URL).mock(return_value=httpx.Response(200, json=[]))
        async with client() as c:
            await c.from_(TABLE).select().is_("deleted_at", None)
        qs = str(route.calls.last.request.url.query)
        assert "deleted_at=is.null" in qs


@pytest.mark.asyncio
async def test_in_filter():
    with respx.mock:
        route = respx.get(URL).mock(return_value=httpx.Response(200, json=[]))
        async with client() as c:
            await c.from_(TABLE).select().in_("id", [1, 2, 3])
        qs = str(route.calls.last.request.url.query)
        assert "id=in." in qs
        assert "1" in qs


@pytest.mark.asyncio
async def test_order_limit_range():
    with respx.mock:
        route = respx.get(URL).mock(return_value=httpx.Response(200, json=[]))
        async with client() as c:
            await c.from_(TABLE).select().order("price", ascending=False).limit(5).range(0, 9)
        qs = str(route.calls.last.request.url.query)
        assert "order=price.desc" in qs
        assert "limit=10" in qs
        assert "offset=0" in qs


# ── single / maybe_single ────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_single_unwraps_one_row():
    with respx.mock:
        respx.get(URL).mock(return_value=httpx.Response(200, json={"id": 1}))
        async with client() as c:
            result = await c.from_(TABLE).select().single()
        assert result.error is None
        assert result.data == {"id": 1}


@pytest.mark.asyncio
async def test_single_zero_rows_error():
    with respx.mock:
        respx.get(URL).mock(return_value=httpx.Response(200, json=[]))
        async with client() as c:
            result = await c.from_(TABLE).select().single()
        assert result.error is not None
        assert result.error.code == "not_found"


@pytest.mark.asyncio
async def test_maybe_single_zero_rows_none():
    with respx.mock:
        respx.get(URL).mock(return_value=httpx.Response(200, json=[]))
        async with client() as c:
            result = await c.from_(TABLE).select().maybe_single()
        assert result.error is None
        assert result.data is None


@pytest.mark.asyncio
async def test_single_header():
    with respx.mock:
        route = respx.get(URL).mock(return_value=httpx.Response(200, json={"id": 1}))
        async with client() as c:
            await c.from_(TABLE).select().single()
        req = route.calls.last.request
        assert "pgrst.object" in req.headers.get("accept", "")


# ── mutations ────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_insert_method_and_body():
    with respx.mock:
        route = respx.post(URL).mock(
            return_value=httpx.Response(201, json=[{"id": 1, "name": "widget"}])
        )
        async with client() as c:
            result = await c.from_(TABLE).insert({"name": "widget"})
        assert result.error is None
        req = route.calls.last.request
        assert req.method == "POST"
        body = json.loads(req.content)
        assert body == {"name": "widget"}
        assert "return=representation" in req.headers.get("prefer", "")


@pytest.mark.asyncio
async def test_update_method():
    with respx.mock:
        route = respx.patch(URL).mock(
            return_value=httpx.Response(200, json=[{"id": 1, "name": "updated"}])
        )
        async with client() as c:
            result = await c.from_(TABLE).update({"name": "updated"}).eq("id", 1)
        assert result.error is None
        req = route.calls.last.request
        assert req.method == "PATCH"


@pytest.mark.asyncio
async def test_delete_method():
    with respx.mock:
        route = respx.delete(URL).mock(return_value=httpx.Response(204))
        async with client() as c:
            result = await c.from_(TABLE).delete().eq("id", 1)
        req = route.calls.last.request
        assert req.method == "DELETE"
        qs = str(req.url.query)
        assert "id=eq.1" in qs


@pytest.mark.asyncio
async def test_upsert_prefer_header():
    with respx.mock:
        route = respx.post(URL).mock(
            return_value=httpx.Response(200, json=[{"id": 1}])
        )
        async with client() as c:
            await c.from_(TABLE).upsert({"id": 1, "name": "x"}, on_conflict="id")
        req = route.calls.last.request
        assert "merge-duplicates" in req.headers.get("prefer", "")


# ── NDJSON auto-detection ─────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_ndjson_rows_plus_cursor():
    body = '{"id":1}\n{"id":2}\n{"id":3}\n{"_basin_next_cursor":"tok123"}\n'
    with respx.mock:
        respx.get(URL).mock(
            return_value=httpx.Response(
                200,
                content=body.encode(),
                headers={"content-type": "application/x-ndjson"},
            )
        )
        async with client() as c:
            result = await c.from_(TABLE).select()
    assert result.error is None
    assert len(result.data) == 3
    assert result.next_cursor == "tok123"


@pytest.mark.asyncio
async def test_ndjson_empty_with_cursor():
    body = '{"_basin_next_cursor":"abc"}\n'
    with respx.mock:
        respx.get(URL).mock(
            return_value=httpx.Response(
                200,
                content=body.encode(),
                headers={"content-type": "application/x-ndjson"},
            )
        )
        async with client() as c:
            result = await c.from_(TABLE).select()
    assert result.data == []
    assert result.next_cursor == "abc"


@pytest.mark.asyncio
async def test_ndjson_no_sentinel():
    body = '{"id":1}\n{"id":2}\n'
    with respx.mock:
        respx.get(URL).mock(
            return_value=httpx.Response(
                200,
                content=body.encode(),
                headers={"content-type": "application/x-ndjson"},
            )
        )
        async with client() as c:
            result = await c.from_(TABLE).select()
    assert len(result.data) == 2
    assert result.next_cursor is None


@pytest.mark.asyncio
async def test_regular_json_still_works():
    with respx.mock:
        respx.get(URL).mock(return_value=httpx.Response(200, json=[{"id": 1}]))
        async with client() as c:
            result = await c.from_(TABLE).select()
    assert result.data == [{"id": 1}]
    assert result.next_cursor is None


# ── chained filters preserve order ───────────────────────────────────────────

@pytest.mark.asyncio
async def test_chained_filters_preserved():
    with respx.mock:
        route = respx.get(URL).mock(return_value=httpx.Response(200, json=[]))
        async with client() as c:
            await c.from_(TABLE).select().eq("a", 1).neq("b", 2).gt("c", 3)
        qs = str(route.calls.last.request.url.query)
        assert "a=eq.1" in qs
        assert "b=neq.2" in qs
        assert "c=gt.3" in qs
