import 'package:basin_sdk/basin_sdk.dart';
import 'package:test/test.dart';

void main() {
  group('BasinApiError', () {
    test('fromBody extracts code and message', () {
      final err = BasinApiError.fromBody({
        'code': 'E_NOT_FOUND',
        'message': 'table not found',
      }, 404);
      expect(err.code, 'E_NOT_FOUND');
      expect(err.message, 'table not found');
      expect(err.status, 404);
    });

    test('fromBody surfaces sqlstate on a constraint violation', () {
      final err = BasinApiError.fromBody({
        'code': 'E_INVALID_REQUEST',
        'message': 'duplicate key value violates unique constraint',
        'sqlstate': '23505',
      }, 400);
      expect(err.code, 'E_INVALID_REQUEST');
      expect(err.sqlstate, '23505');
      expect(err.toString(), contains('23505'));
    });

    test('fromBody leaves sqlstate null when absent', () {
      final err = BasinApiError.fromBody({
        'code': 'E_NOT_FOUND',
        'message': 'gone',
      }, 404);
      expect(err.sqlstate, isNull);
    });

    test('fromBody falls back to E_UNKNOWN for missing code', () {
      final err = BasinApiError.fromBody({'message': 'oops'}, 500);
      expect(err.code, 'E_UNKNOWN');
      expect(err.status, 500);
    });

    test('fromBody falls back to HTTP status message for missing message', () {
      final err = BasinApiError.fromBody({'code': 'E_INTERNAL'}, 500);
      expect(err.message, 'HTTP 500');
    });

    test('toString includes all fields', () {
      const err = BasinApiError('E_FORBIDDEN', 'not allowed', 403);
      final s = err.toString();
      expect(s, contains('E_FORBIDDEN'));
      expect(s, contains('403'));
      expect(s, contains('not allowed'));
    });

    test('kErrorCodes contains all known stable codes', () {
      expect(kErrorCodes, contains('E_UNAUTHENTICATED'));
      expect(kErrorCodes, contains('E_REVOKED_TOKEN'));
      expect(kErrorCodes, contains('E_ENGINE_UNSUPPORTED'));
    });

    test('BasinApiError is a BasinError', () {
      const err = BasinApiError('E_INTERNAL', 'server error', 500);
      expect(err, isA<BasinError>());
      expect(err, isA<Exception>());
    });

    test('BasinNetworkError carries cause', () {
      final cause = Exception('connect refused');
      final err = BasinNetworkError('connection failed', cause);
      expect(err.cause, same(cause));
      expect(err.toString(), contains('connection failed'));
    });
  });
}
