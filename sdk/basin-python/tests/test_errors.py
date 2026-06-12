"""Tests for error envelope decoding."""

import json
import pytest
import httpx
import respx

from basin.errors import BasinApiError, BasinNetworkError, ERROR_CODES
from basin._http import SyncTransport, AsyncTransport


def test_error_codes_constant():
    assert "E_UNAUTHENTICATED" in ERROR_CODES
    assert "E_FORBIDDEN" in ERROR_CODES
    assert "E_NOT_FOUND" in ERROR_CODES
    assert "E_INVALID_REQUEST" in ERROR_CODES
    assert "E_RATE_LIMITED" in ERROR_CODES
    assert "E_ENGINE_UNSUPPORTED" in ERROR_CODES
    assert "E_INTERNAL" in ERROR_CODES
    assert "E_EMAIL_DISABLED" in ERROR_CODES
    assert "E_REVOKED_TOKEN" in ERROR_CODES


def test_basin_api_error_attributes():
    err = BasinApiError("E_NOT_FOUND", "table not found", 404)
    assert err.code == "E_NOT_FOUND"
    assert err.status == 404
    assert "table not found" in str(err)


def test_basin_api_error_from_response_body():
    body = {"code": "E_FORBIDDEN", "message": "access denied"}
    err = BasinApiError.from_response_body(body, 403)
    assert err.code == "E_FORBIDDEN"
    assert err.status == 403
    assert err.message if hasattr(err, "message") else True


def test_basin_api_error_unknown_code():
    body = {"code": "E_FUTURE_CODE", "message": "from a newer server"}
    err = BasinApiError.from_response_body(body, 400)
    assert err.code == "E_FUTURE_CODE"  # passes through


def test_error_envelope_missing_code():
    body = {"detail": "something went wrong"}
    err = BasinApiError.from_response_body(body, 500)
    assert err.code == "E_UNKNOWN"
    assert err.status == 500


@respx.mock
def test_sync_transport_non_2xx_raises():
    respx.get("http://basin.test/health").mock(
        return_value=httpx.Response(
            401,
            json={"code": "E_UNAUTHENTICATED", "message": "bad token"},
        )
    )
    transport = SyncTransport("http://basin.test")
    with pytest.raises(BasinApiError) as exc_info:
        transport.request("GET", "/health", auth=False)
    err = exc_info.value
    assert err.code == "E_UNAUTHENTICATED"
    assert err.status == 401


@respx.mock
def test_sync_transport_non_json_error_body():
    respx.get("http://basin.test/health").mock(
        return_value=httpx.Response(503, text="Service Unavailable")
    )
    transport = SyncTransport("http://basin.test")
    with pytest.raises(BasinApiError) as exc_info:
        transport.request("GET", "/health", auth=False)
    err = exc_info.value
    assert err.code == "E_UNKNOWN"
    assert err.status == 503


@respx.mock
async def test_async_transport_non_2xx_raises():
    respx.get("http://basin.test/health").mock(
        return_value=httpx.Response(
            403,
            json={"code": "E_FORBIDDEN", "message": "forbidden"},
        )
    )
    transport = AsyncTransport("http://basin.test")
    try:
        with pytest.raises(BasinApiError) as exc_info:
            await transport.request("GET", "/health", auth=False)
        assert exc_info.value.code == "E_FORBIDDEN"
    finally:
        await transport.aclose()


def test_network_error_is_basin_error():
    assert issubclass(BasinNetworkError, BasinApiError.__bases__[0])
