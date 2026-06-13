/// Typed errors mirroring basin-rest's error envelope.
///
/// Every failed request returns `{"code": "...", "message": "..."}` with an
/// appropriate HTTP status (crates/basin-rest/src/errors.rs). [code] is the
/// stable contract; [message] is human-readable and not promised to be stable.
library;

/// Stable error codes emitted by basin-rest (errors.rs ErrorCode::as_str).
const List<String> kErrorCodes = [
  'E_UNAUTHENTICATED',
  'E_FORBIDDEN',
  'E_NOT_FOUND',
  'E_INVALID_REQUEST',
  'E_RATE_LIMITED',
  'E_ENGINE_UNSUPPORTED',
  'E_INTERNAL',
  'E_EMAIL_DISABLED',
  'E_REVOKED_TOKEN',
];

/// Base class for all Basin SDK errors.
sealed class BasinError implements Exception {}

/// Raised for any non-2xx response from a Basin server.
///
/// [code] is the stable `E_*` code from the response body.
/// [status] is the HTTP status code (0 when synthesised client-side).
/// [message] is human-readable — do **not** match on this; use [code].
class BasinApiError extends BasinError {
  final String code;
  final int status;
  final String message;

  const BasinApiError(this.code, this.message, this.status);

  factory BasinApiError.fromBody(Map<String, dynamic> body, int status) {
    final code =
        (body['code'] is String) ? (body['code'] as String) : 'E_UNKNOWN';
    final message =
        (body['message'] is String)
            ? (body['message'] as String)
            : 'HTTP $status';
    return BasinApiError(code, message, status);
  }

  @override
  String toString() =>
      'BasinApiError(code: $code, status: $status, message: $message)';
}

/// Raised when the transport layer fails before a server response arrives
/// (connection refused, timeout, etc.).
class BasinNetworkError extends BasinError {
  final String message;
  final Object? cause;

  const BasinNetworkError(this.message, [this.cause]);

  @override
  String toString() => 'BasinNetworkError($message)';
}
