from __future__ import annotations

import pytest
import httpx

from basin.errors import BasinError


def _mock_response(status_code: int, body: bytes = b"", content_type: str = "application/json") -> httpx.Response:
    return httpx.Response(
        status_code=status_code,
        content=body,
        headers={"content-type": content_type},
    )


def test_str_repr():
    err = BasinError("not_found", "resource missing")
    assert str(err) == "not_found: resource missing"


def test_attributes():
    err = BasinError("conflict", "duplicate key", status=409, details={"field": "email"})
    assert err.code == "conflict"
    assert err.message == "duplicate key"
    assert err.status == 409
    assert err.details == {"field": "email"}


@pytest.mark.parametrize(
    "status,expected_code",
    [
        (401, "unauthorized"),
        (403, "forbidden"),
        (404, "not_found"),
        (409, "conflict"),
        (429, "rate_limited"),
        (500, "internal"),
        (503, "internal"),
        (501, "not_implemented"),
        (400, "invalid_request"),
        (422, "invalid_request"),
    ],
)
def test_status_mapping(status: int, expected_code: str):
    resp = _mock_response(status, b'{"message": "oops"}')
    err = BasinError.from_response(resp)
    assert err.code == expected_code
    assert err.status == status


def test_json_body_message():
    resp = _mock_response(400, b'{"message": "bad email"}')
    err = BasinError.from_response(resp)
    assert err.message == "bad email"


def test_json_error_key_fallback():
    resp = _mock_response(400, b'{"error": "invalid input"}')
    err = BasinError.from_response(resp)
    assert err.message == "invalid input"


def test_json_msg_key_fallback():
    resp = _mock_response(400, b'{"msg": "short msg"}')
    err = BasinError.from_response(resp)
    assert err.message == "short msg"


def test_non_json_body_fallback():
    resp = _mock_response(500, b"Internal Server Error", content_type="text/plain")
    err = BasinError.from_response(resp)
    assert err.code == "internal"
    assert err.status == 500


def test_empty_body_fallback():
    resp = _mock_response(404, b"")
    err = BasinError.from_response(resp)
    assert err.code == "not_found"


def test_json_code_override():
    resp = _mock_response(400, b'{"code": "custom_code", "message": "custom message"}')
    err = BasinError.from_response(resp)
    assert err.code == "custom_code"
    assert err.message == "custom message"
