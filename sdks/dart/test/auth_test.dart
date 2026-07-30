import 'dart:convert';

import 'package:basin_sdk/basin_sdk.dart';
import 'package:http/http.dart' as http;
import 'package:http/testing.dart';
import 'package:test/test.dart';

http.Client _mockClient(
  Map<String, dynamic> Function(http.Request) handler,
) =>
    MockClient((req) async {
      final body = handler(req);
      return http.Response(
        jsonEncode(body),
        200,
        headers: {'content-type': 'application/json'},
      );
    });

http.Client _statusMock(int status, Map<String, dynamic> body) =>
    MockClient((_) async => http.Response(
      jsonEncode(body),
      status,
      headers: {'content-type': 'application/json'},
    ));

BasinClient _client(http.Client mock) => BasinClient.create(
  url: 'http://localhost:8080',
  key: 'test-api-key',
  projectId: 'proj-01',
  httpClient: mock,
);

final _sessionJson = {
  'access_token': 'access-tok',
  'refresh_token': 'refresh-tok',
  'access_expires_at': '2099-01-01T00:00:00Z',
  'refresh_expires_at': '2099-02-01T00:00:00Z',
};

void main() {
  group('AuthClient.signUp', () {
    test('POSTs to /auth/v1/signup with correct body', () async {
      http.Request? captured;
      final mock = MockClient((req) async {
        captured = req;
        return http.Response(
          jsonEncode({'ok': true, 'user_id': 'u1'}),
          201,
          headers: {'content-type': 'application/json'},
        );
      });
      final client = _client(mock);
      final result = await client.auth.signUp(
        email: 'alice@example.com',
        password: 's3cr3t',
      );
      expect(result.ok, isTrue);
      expect(result.userId, 'u1');
      expect(captured!.url.path, '/auth/v1/signup');
      final body = jsonDecode(captured!.body) as Map;
      expect(body['email'], 'alice@example.com');
      expect(body['project_id'], 'proj-01');
      // Must NOT send Authorization header for unauthenticated routes.
      expect(captured!.headers.containsKey('Authorization'), isFalse);
    });
  });

  group('AuthClient.signIn', () {
    test('stores session and returns it', () async {
      final mock = _mockClient((_) => _sessionJson);
      final client = _client(mock);
      final session = await client.auth.signIn(
        email: 'alice@example.com',
        password: 's3cr3t',
      );
      expect(session.accessToken, 'access-tok');
      expect(client.auth.getSession()?.accessToken, 'access-tok');
    });
  });

  group('AuthClient.signOut', () {
    test('POSTs refresh_token to /auth/v1/signout and clears session', () async {
      final mock = _mockClient((_) => _sessionJson);
      final client = _client(mock);
      await client.auth.signIn(email: 'a@b.com', password: 'pw');
      expect(client.auth.getSession(), isNotNull);

      http.Request? signoutReq;
      final signoutMock = MockClient((req) async {
        signoutReq = req;
        return http.Response('{}', 200,
            headers: {'content-type': 'application/json'});
      });
      final client2 = BasinClient.create(
        url: 'http://localhost:8080',
        key: 'k',
        projectId: 'proj-01',
        httpClient: signoutMock,
      );
      client2.auth.setSession(const Session(
        accessToken: 'at',
        refreshToken: 'rt',
        accessExpiresAt: '2099-01-01T00:00:00Z',
        refreshExpiresAt: '2099-02-01T00:00:00Z',
      ));
      await client2.auth.signOut();
      expect(client2.auth.getSession(), isNull);
      expect(signoutReq?.url.path, '/auth/v1/signout');
      final body = jsonDecode(signoutReq!.body) as Map;
      expect(body['refresh_token'], 'rt');
    });

    test('tolerates 404 response (older server)', () async {
      final mock = MockClient((_) async =>
          http.Response(jsonEncode({'code': 'E_NOT_FOUND', 'message': 'x'}),
              404, headers: {'content-type': 'application/json'}));
      final client = BasinClient.create(
        url: 'http://localhost:8080',
        projectId: 'proj-01',
        httpClient: mock,
      );
      client.auth.setSession(const Session(
        accessToken: 'at',
        refreshToken: 'rt',
        accessExpiresAt: '2099-01-01T00:00:00Z',
        refreshExpiresAt: '2099-02-01T00:00:00Z',
      ));
      // Should not throw.
      await client.auth.signOut();
      expect(client.auth.getSession(), isNull);
    });

    test('no-op when already signed out', () async {
      final callCount = <int>[0];
      final mock = MockClient((_) async {
        callCount[0]++;
        return http.Response('{}', 200,
            headers: {'content-type': 'application/json'});
      });
      final client = BasinClient.create(
        url: 'http://localhost:8080',
        projectId: 'proj-01',
        httpClient: mock,
      );
      await client.auth.signOut(); // no session, must be no-op
      expect(callCount[0], 0);
    });
  });

  group('AuthClient.refreshSession', () {
    test('rotates both tokens', () async {
      final mock = _mockClient((_) => {
        'access_token': 'new-access',
        'refresh_token': 'new-refresh',
        'access_expires_at': '2099-01-01T00:00:00Z',
        'refresh_expires_at': '2099-02-01T00:00:00Z',
      });
      final client = BasinClient.create(
        url: 'http://localhost:8080',
        key: 'k',
        projectId: 'proj-01',
        httpClient: mock,
      );
      client.auth.setSession(const Session(
        accessToken: 'old-access',
        refreshToken: 'old-refresh',
        accessExpiresAt: '2099-01-01T00:00:00Z',
        refreshExpiresAt: '2099-02-01T00:00:00Z',
      ));
      final session = await client.auth.refreshSession();
      expect(session.accessToken, 'new-access');
      expect(client.auth.getSession()?.accessToken, 'new-access');
    });

    test('throws E_UNAUTHENTICATED when no session', () async {
      final client = BasinClient.create(
        url: 'http://localhost:8080',
        httpClient: MockClient((_) async => http.Response('{}', 200)),
      );
      expect(
        () => client.auth.refreshSession(),
        throwsA(
          isA<BasinApiError>().having((e) => e.code, 'code', 'E_UNAUTHENTICATED'),
        ),
      );
    });
  });

  group('auto-refresh', () {
    test('accessToken() refreshes when near expiry', () async {
      // The expiry is in the past — should trigger a refresh call.
      const expiredSession = Session(
        accessToken: 'expired-access',
        refreshToken: 'ref',
        // Far in the past — always triggers refresh.
        accessExpiresAt: '2000-01-01T00:00:00Z',
        refreshExpiresAt: '2099-01-01T00:00:00Z',
      );
      bool refreshCalled = false;
      final mock = MockClient((_) async {
        refreshCalled = true;
        return http.Response(
          jsonEncode({
            'access_token': 'fresh-access',
            'refresh_token': 'ref2',
            'access_expires_at': '2099-01-01T00:00:00Z',
            'refresh_expires_at': '2099-02-01T00:00:00Z',
          }),
          200,
          headers: {'content-type': 'application/json'},
        );
      });
      final client = BasinClient.create(
        url: 'http://localhost:8080',
        projectId: 'proj-01',
        httpClient: mock,
      );
      client.auth.setSession(expiredSession);
      final tok = await client.auth.accessToken();
      expect(refreshCalled, isTrue);
      expect(tok, 'fresh-access');
    });
  });

  group('AuthClient API keys', () {
    test('createApiKey POSTs to /auth/v1/api-keys', () async {
      http.Request? req;
      final mock = MockClient((r) async {
        req = r;
        return http.Response(
          jsonEncode({
            'id': 1,
            'name': 'ci',
            'secret': 'sk-secret',
            'created_at': '2026-01-01T00:00:00Z',
          }),
          200,
          headers: {'content-type': 'application/json'},
        );
      });
      final client = _client(mock);
      final key = await client.auth.createApiKey('ci');
      expect(key.secret, 'sk-secret');
      expect(req?.url.path, '/auth/v1/api-keys');
    });

    test('listApiKeys GETs /auth/v1/api-keys', () async {
      final mock = _mockClient((_) => [
        {
          'id': 1,
          'name': 'ci',
          'created_at': '2026-01-01T00:00:00Z',
        }
      ]);
      final client = _client(mock);
      // listApiKeys expects a List at the top level — adjust mock.
      final mock2 = MockClient((_) async => http.Response(
        jsonEncode([
          {'id': 1, 'name': 'ci', 'created_at': '2026-01-01T00:00:00Z'},
        ]),
        200,
        headers: {'content-type': 'application/json'},
      ));
      final client2 = _client(mock2);
      final keys = await client2.auth.listApiKeys();
      expect(keys, hasLength(1));
      expect(keys.first.name, 'ci');
    });
  });

  group('AuthClient OAuth', () {
    test('getOAuthAuthorizeUrl GETs with project_id and redirect_to', () async {
      http.Request? req;
      final mock = MockClient((r) async {
        req = r;
        return http.Response(
          jsonEncode({
            'redirect_url': 'https://accounts.google.com/o/oauth2/v2/auth?...',
            'state': 'csrf',
          }),
          200,
          headers: {'content-type': 'application/json'},
        );
      });
      final client = _client(mock);
      final result = await client.auth.getOAuthAuthorizeUrl(
        'google',
        redirectTo: 'https://myapp.com/callback',
      );
      expect(req?.url.path, '/auth/v1/oauth/google/authorize');
      expect(req?.url.queryParameters['project_id'], 'proj-01');
      expect(req?.url.queryParameters['redirect_to'], 'https://myapp.com/callback');
      expect(result.state, 'csrf');
    });
  });

  group('AuthClient MFA', () {
    test('enrollFactor returns TotpEnrollResult', () async {
      final mock = _mockClient((_) => {
        'factor_id': 'f1',
        'factor_type': 'totp',
        'secret_b32': 'JBSWY3DPEHPK3PXP',
        'otpauth_uri': 'otpauth://totp/Basin:alice?secret=JBSWY3DPEHPK3PXP',
      });
      final client = _client(mock);
      final result = await client.auth.enrollFactor('totp');
      expect(result, isA<TotpEnrollResult>());
    });

    test('challengeFactor returns WebAuthnChallengeResult', () async {
      final mock = _mockClient((_) => {
        'challenge_id': 'ch1',
        'request_options_json': '{}',
      });
      final client = _client(mock);
      final result = await client.auth.challengeFactor('f1');
      expect(result, isA<WebAuthnChallengeResult>());
    });

    test('verifyChallenge stores aal2 session', () async {
      final mock = _mockClient((_) => _sessionJson);
      final client = _client(mock);
      final session = await client.auth.verifyChallenge(
        'f1',
        'ch1',
        code: '123456',
      );
      expect(session.accessToken, 'access-tok');
      expect(client.auth.getSession()?.accessToken, 'access-tok');
    });
  });

  group('API error decoding', () {
    test('non-2xx raises BasinApiError', () async {
      final mock = _statusMock(
        404,
        {'code': 'E_NOT_FOUND', 'message': 'table not found'},
      );
      final client = BasinClient.create(
        url: 'http://localhost:8080',
        projectId: 'proj-01',
        httpClient: mock,
      );
      expect(
        () => client.auth.signIn(email: 'x@y.com', password: 'z'),
        throwsA(
          isA<BasinApiError>()
              .having((e) => e.code, 'code', 'E_NOT_FOUND')
              .having((e) => e.status, 'status', 404),
        ),
      );
    });
  });
}
