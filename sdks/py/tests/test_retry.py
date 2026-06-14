from __future__ import annotations

import httpx
import pytest
import respx

from basin import create_client
from basin._retry import (
    RetryConfig,
    compute_backoff_ms,
    is_idempotent,
    next_delay_ms,
    parse_retry_after_ms,
)
from basin.client import ClientOptions
from basin.errors import BasinError

BASE = "http://localhost:5434"
KEY = "basin_test_anon"

# Near-zero backoff so tests stay fast.
FAST = RetryConfig(max_attempts=3, base_ms=0.0, max_ms=0.0, jitter_ms=0.0)


def _client(retry: RetryConfig) -> object:
    return create_client(BASE, KEY, options=ClientOptions(retry=retry))


@pytest.mark.asyncio
async def test_retries_5xx_then_succeeds() -> None:
    with respx.mock:
        route = respx.get(f"{BASE}/rest/v1/users").mock(
            side_effect=[
                httpx.Response(503),
                httpx.Response(503),
                httpx.Response(200, json=[{"id": 1}]),
            ]
        )
        client = _client(FAST)
        res = await client.from_("users").select()
        assert route.call_count == 3
        assert res.data == [{"id": 1}]
        await client.aclose()


@pytest.mark.asyncio
async def test_retries_429_then_succeeds() -> None:
    with respx.mock:
        route = respx.get(f"{BASE}/rest/v1/users").mock(
            side_effect=[
                httpx.Response(429),
                httpx.Response(200, json=[{"id": 1}]),
            ]
        )
        client = _client(FAST)
        res = await client.from_("users").select()
        assert route.call_count == 2
        assert res.data == [{"id": 1}]
        await client.aclose()


@pytest.mark.asyncio
async def test_retries_transport_error_then_succeeds() -> None:
    with respx.mock:
        route = respx.get(f"{BASE}/rest/v1/users").mock(
            side_effect=[
                httpx.ConnectError("down"),
                httpx.Response(200, json=[{"id": 1}]),
            ]
        )
        client = _client(FAST)
        res = await client.from_("users").select()
        assert route.call_count == 2
        assert res.data == [{"id": 1}]
        await client.aclose()


@pytest.mark.asyncio
async def test_gives_up_after_max_attempts() -> None:
    with respx.mock:
        route = respx.get(f"{BASE}/rest/v1/users").mock(
            return_value=httpx.Response(503)
        )
        client = _client(FAST)
        res = await client.from_("users").select()
        assert route.call_count == 3
        assert res.error is not None
        assert res.status == 503
        await client.aclose()


@pytest.mark.asyncio
async def test_transport_error_exhausted_returns_network_error() -> None:
    with respx.mock:
        respx.get(f"{BASE}/rest/v1/users").mock(side_effect=httpx.ConnectError("x"))
        client = _client(FAST)
        res = await client.from_("users").select()
        assert isinstance(res.error, BasinError)
        assert res.error.code == "network"
        await client.aclose()


@pytest.mark.asyncio
async def test_no_retry_when_disabled() -> None:
    with respx.mock:
        route = respx.get(f"{BASE}/rest/v1/users").mock(
            return_value=httpx.Response(503)
        )
        client = _client(RetryConfig(max_attempts=1))
        await client.from_("users").select()
        assert route.call_count == 1
        await client.aclose()


@pytest.mark.asyncio
async def test_4xx_not_retried() -> None:
    with respx.mock:
        route = respx.get(f"{BASE}/rest/v1/users").mock(
            return_value=httpx.Response(404)
        )
        client = _client(FAST)
        await client.from_("users").select()
        assert route.call_count == 1
        await client.aclose()


@pytest.mark.asyncio
async def test_post_writes_not_retried_by_default() -> None:
    with respx.mock:
        route = respx.post(f"{BASE}/rest/v1/users").mock(
            return_value=httpx.Response(503)
        )
        client = _client(FAST)
        await client.from_("users").insert({"name": "x"})
        assert route.call_count == 1
        await client.aclose()


@pytest.mark.asyncio
async def test_writes_retried_when_opted_in() -> None:
    with respx.mock:
        route = respx.post(f"{BASE}/rest/v1/users").mock(
            side_effect=[
                httpx.Response(503),
                httpx.Response(201, json=[{"id": 1}]),
            ]
        )
        client = _client(RetryConfig(max_attempts=3, base_ms=0.0, retry_writes=True))
        res = await client.from_("users").insert({"name": "x"})
        assert route.call_count == 2
        assert res.data == [{"id": 1}]
        await client.aclose()


def test_is_idempotent() -> None:
    assert is_idempotent("GET")
    assert is_idempotent("delete")
    assert not is_idempotent("POST")
    assert not is_idempotent("PATCH")


def test_parse_retry_after_seconds() -> None:
    assert parse_retry_after_ms("2") == 2000.0
    assert parse_retry_after_ms(None) is None
    assert parse_retry_after_ms("garbage") is None


def test_compute_backoff_caps() -> None:
    cfg = RetryConfig(base_ms=100.0, max_ms=300.0, jitter_ms=0.0)
    assert compute_backoff_ms(1, cfg) == 100.0
    assert compute_backoff_ms(2, cfg) == 200.0
    assert compute_backoff_ms(10, cfg) == 300.0  # capped


def test_next_delay_honours_retry_after_on_429() -> None:
    cfg = RetryConfig(base_ms=0.0, max_ms=10000.0, jitter_ms=0.0)
    resp = httpx.Response(429, headers={"Retry-After": "3"})
    assert next_delay_ms(1, resp, cfg) == 3000.0
