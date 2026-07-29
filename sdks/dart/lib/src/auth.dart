/// Auth client — bindings for /auth/v1/*.
///
/// Route sources (verified against crates/basin-rest/src/server.rs):
/// - POST /auth/v1/signup                              server.rs:250
/// - POST /auth/v1/signin                              server.rs:251
/// - POST /auth/v1/refresh                             server.rs:252
/// - POST /auth/v1/signout                             server.rs:253
/// - POST /auth/v1/verify-email                        server.rs:254
/// - POST /auth/v1/reset-password                      server.rs:255
/// - POST /auth/v1/request-password-reset              server.rs:256
/// - POST /auth/v1/magic-link                          server.rs:262
/// - POST /auth/v1/magic-link/consume                  server.rs:263
/// - POST|GET /auth/v1/api-keys                        server.rs:267
/// - DELETE /auth/v1/api-keys/:id                      server.rs:271
/// - GET /auth/v1/oauth/:provider/authorize            server.rs:277
/// - POST|GET /auth/v1/factors                         server.rs:286-287
/// - POST /auth/v1/factors/:id/verify                  server.rs:290
/// - POST /auth/v1/factors/:id/challenge               server.rs:294
/// - POST /auth/v1/factors/:id/challenge/verify        server.rs:298
/// - DELETE /auth/v1/factors/:id                       server.rs:302
///
/// [signOut] calls POST /auth/v1/signout with the stored refresh token,
/// writing a server-side revocation row, then clears the local session.
/// The local session is cleared regardless of server response (404 and
/// network errors are tolerated gracefully). When no session is active
/// the call is a no-op.
///
/// Auth is per-project: signup/signin take a project_id in the body.
library;

import 'dart:convert';

import 'errors.dart';
import 'http.dart';
import 'types.dart';

/// Refresh the access token this many seconds before its stated expiry.
const int _expirySkewSeconds = 10;

/// Decode the project_id claim from a Basin JWT payload segment.
/// Returns null for non-JWT tokens (e.g. raw API keys).
String? projectIdFromJwt(String token) {
  final parts = token.split('.');
  if (parts.length != 3) return null;
  try {
    var b64 = parts[1].replaceAll('-', '+').replaceAll('_', '/');
    // Add padding.
    while (b64.length % 4 != 0) {
      b64 += '=';
    }
    final payload = jsonDecode(utf8.decode(base64Decode(b64))) as Map;
    final val = payload['project_id'];
    return val is String ? val : null;
  } catch (_) {
    return null;
  }
}

/// Auth client for /auth/v1/* routes.
///
/// Obtained via [BasinClient.auth]; do not construct directly.
class AuthClient {
  final BasinHttpClient _http;
  final String? _defaultProjectId;
  Session? _session;

  AuthClient(this._http, this._defaultProjectId);

  // ------------------------------------------------------------------
  // Session management
  // ------------------------------------------------------------------

  /// Return the current session, or null when signed out.
  Session? getSession() => _session;

  /// Adopt an externally obtained token pair (e.g. restored from storage).
  void setSession(Session? session) {
    _session = session;
  }

  String _resolveProjectId([String? projectId]) {
    final pid =
        projectId ??
        _defaultProjectId ??
        (_http._staticKey != null
            ? projectIdFromJwt(_http._staticKey!)
            : null);
    if (pid == null) {
      throw const BasinApiError(
        'E_INVALID_REQUEST',
        'project id required: pass projectId to BasinClient.create()',
        0,
      );
    }
    return pid;
  }

  /// Return a valid access token, auto-refreshing if near expiry.
  ///
  /// Returns null when there is no session (callers fall back to the
  /// static key).
  Future<String?> accessToken() async {
    final s = _session;
    if (s == null) return null;
    try {
      final expiry = DateTime.parse(s.accessExpiresAt);
      final threshold = expiry.subtract(
        const Duration(seconds: _expirySkewSeconds),
      );
      if (DateTime.now().isAfter(threshold)) {
        final refreshed = await refreshSession();
        return refreshed.accessToken;
      }
    } catch (_) {
      // Malformed expiry — skip auto-refresh, return current token.
    }
    return s.accessToken;
  }

  // ------------------------------------------------------------------
  // Auth flows
  // ------------------------------------------------------------------

  /// POST /auth/v1/signup → { ok, user_id } (201).
  Future<SignUpResult> signUp({
    required String email,
    required String password,
    String? projectId,
  }) async {
    final data = await _http.requestJson(
      'POST',
      '/auth/v1/signup',
      body: {
        'project_id': _resolveProjectId(projectId),
        'email': email,
        'password': password,
      },
      auth: false,
    ) as Map<String, dynamic>;
    return SignUpResult.fromJson(data);
  }

  /// POST /auth/v1/signin → token body; stored as the live session.
  Future<Session> signIn({
    required String email,
    required String password,
    String? projectId,
  }) async {
    final data = await _http.requestJson(
      'POST',
      '/auth/v1/signin',
      body: {
        'project_id': _resolveProjectId(projectId),
        'email': email,
        'password': password,
      },
      auth: false,
    ) as Map<String, dynamic>;
    final session = Session.fromJson(data);
    _session = session;
    return session;
  }

  /// POST /auth/v1/signout with the stored refresh token (server-side revocation).
  ///
  /// The local session is always cleared, even if the server call fails.
  /// 404 responses (older servers) and network errors are tolerated silently.
  Future<void> signOut() async {
    final current = _session;
    _session = null; // clear local state first — client is always signed out
    if (current == null) return;
    try {
      await _http.requestJson(
        'POST',
        '/auth/v1/signout',
        body: {'refresh_token': current.refreshToken},
        auth: false,
      );
    } on BasinApiError catch (e) {
      if (e.status == 404) return; // older server without signout route
      rethrow;
    } catch (_) {
      // Network error — local clear already done.
    }
  }

  /// POST /auth/v1/refresh with the stored refresh token.
  ///
  /// Rotates both tokens. Throws E_REVOKED_TOKEN if the token was already
  /// rotated.
  Future<Session> refreshSession() async {
    final current = _session;
    if (current == null) {
      throw const BasinApiError('E_UNAUTHENTICATED', 'no session to refresh', 0);
    }
    final data = await _http.requestJson(
      'POST',
      '/auth/v1/refresh',
      body: {'refresh_token': current.refreshToken},
      auth: false,
    ) as Map<String, dynamic>;
    final session = Session.fromJson(data);
    _session = session;
    return session;
  }

  /// POST /auth/v1/verify-email → { ok: true }.
  Future<Map<String, dynamic>> verifyEmail({
    required String token,
    String? projectId,
  }) async {
    return await _http.requestJson(
          'POST',
          '/auth/v1/verify-email',
          body: {
            'project_id': _resolveProjectId(projectId),
            'token': token,
          },
          auth: false,
        )
        as Map<String, dynamic>;
  }

  /// POST /auth/v1/request-password-reset → { ok: true }.
  Future<Map<String, dynamic>> requestPasswordReset({
    required String email,
    String? projectId,
  }) async {
    return await _http.requestJson(
          'POST',
          '/auth/v1/request-password-reset',
          body: {
            'project_id': _resolveProjectId(projectId),
            'email': email,
          },
          auth: false,
        )
        as Map<String, dynamic>;
  }

  /// POST /auth/v1/reset-password → { ok: true }.
  Future<Map<String, dynamic>> resetPassword({
    required String token,
    required String newPassword,
    String? projectId,
  }) async {
    return await _http.requestJson(
          'POST',
          '/auth/v1/reset-password',
          body: {
            'project_id': _resolveProjectId(projectId),
            'token': token,
            'new_password': newPassword,
          },
          auth: false,
        )
        as Map<String, dynamic>;
  }

  /// POST /auth/v1/magic-link — 204 always (never confirms address).
  Future<void> requestMagicLink(String email) async {
    await _http.requestJson(
      'POST',
      '/auth/v1/magic-link',
      body: {'email': email},
      auth: false,
    );
  }

  /// POST /auth/v1/magic-link/consume → token body; stored as session.
  Future<Session> consumeMagicLink(String token) async {
    final data = await _http.requestJson(
      'POST',
      '/auth/v1/magic-link/consume',
      body: {'token': token},
      auth: false,
    ) as Map<String, dynamic>;
    final session = Session.fromJson(data);
    _session = session;
    return session;
  }

  // ------------------------------------------------------------------
  // API keys
  // ------------------------------------------------------------------

  /// POST /auth/v1/api-keys (JWT-gated) → issued key incl. one-time secret.
  Future<ApiKeyIssued> createApiKey(String name) async {
    final data = await _http.requestJson(
      'POST',
      '/auth/v1/api-keys',
      body: {'name': name},
    ) as Map<String, dynamic>;
    return ApiKeyIssued.fromJson(data);
  }

  /// GET /auth/v1/api-keys (JWT-gated).
  Future<List<ApiKeyDescriptor>> listApiKeys() async {
    final data = await _http.requestJson('GET', '/auth/v1/api-keys');
    return (data as List? ?? [])
        .map((e) => ApiKeyDescriptor.fromJson(e as Map<String, dynamic>))
        .toList();
  }

  /// DELETE /auth/v1/api-keys/:id (JWT-gated) → { ok: true }.
  Future<Map<String, dynamic>> deleteApiKey(int keyId) async {
    return await _http.requestJson('DELETE', '/auth/v1/api-keys/$keyId')
        as Map<String, dynamic>;
  }

  // ------------------------------------------------------------------
  // OAuth
  // ------------------------------------------------------------------

  /// GET /auth/v1/oauth/:provider/authorize → { redirect_url, state }.
  ///
  /// Builds the provider authorize URL server-side (with PKCE + signed CSRF
  /// state) and returns it. The SDK cannot perform the browser redirect itself
  /// — redirect the user's browser to [OAuthAuthorizeResult.redirectUrl].
  ///
  /// Supported providers: google, github, apple, bitbucket, discord, figma,
  /// gitlab, linkedin, microsoft (azure_ad), notion, slack, spotify, twitch,
  /// twitter_x. Custom OIDC providers are registered server-side.
  Future<OAuthAuthorizeResult> getOAuthAuthorizeUrl(
    String provider, {
    String redirectTo = '',
    String? projectId,
  }) async {
    final pid = _resolveProjectId(projectId);
    final data = await _http.requestJson(
      'GET',
      '/auth/v1/oauth/$provider/authorize',
      query: [('project_id', pid), ('redirect_to', redirectTo)],
      auth: false,
    ) as Map<String, dynamic>;
    return OAuthAuthorizeResult.fromJson(data);
  }

  // ------------------------------------------------------------------
  // MFA
  // ------------------------------------------------------------------

  /// POST /auth/v1/factors — begin MFA factor enrollment (JWT-gated).
  ///
  /// [factorType] is "totp" or "webauthn".
  /// Returns [TotpEnrollResult] or [WebAuthnEnrollResult].
  Future<MfaEnrollResult> enrollFactor(
    String factorType, {
    String friendlyName = '',
  }) async {
    final data = await _http.requestJson(
      'POST',
      '/auth/v1/factors',
      body: {'factor_type': factorType, 'friendly_name': friendlyName},
    ) as Map<String, dynamic>;
    return MfaEnrollResult.fromJson(data);
  }

  /// GET /auth/v1/factors — list MFA factors for the current user (JWT-gated).
  Future<List<FactorDescriptor>> listFactors() async {
    final data = await _http.requestJson('GET', '/auth/v1/factors');
    return (data as List? ?? [])
        .map((e) => FactorDescriptor.fromJson(e as Map<String, dynamic>))
        .toList();
  }

  /// POST /auth/v1/factors/:id/verify — confirm enrollment (JWT-gated).
  ///
  /// [code]: TOTP 6-digit OTP.
  /// [attestation]: WebAuthn attestation JSON.
  /// [challengeId]: WebAuthn challenge_id from enrollFactor.
  Future<VerifyFactorResult> verifyFactor(
    String factorId, {
    String? code,
    String? attestation,
    String? challengeId,
  }) async {
    final body = <String, dynamic>{};
    if (code != null) body['code'] = code;
    if (attestation != null) body['attestation'] = attestation;
    if (challengeId != null) body['challenge_id'] = challengeId;
    final data = await _http.requestJson(
      'POST',
      '/auth/v1/factors/$factorId/verify',
      body: body,
    ) as Map<String, dynamic>;
    return VerifyFactorResult.fromJson(data);
  }

  /// POST /auth/v1/factors/:id/challenge — begin a step-up challenge (JWT-gated).
  Future<MfaChallengeResult> challengeFactor(String factorId) async {
    final data = await _http.requestJson(
      'POST',
      '/auth/v1/factors/$factorId/challenge',
      body: {},
    ) as Map<String, dynamic>;
    return MfaChallengeResult.fromJson(data);
  }

  /// POST /auth/v1/factors/:id/challenge/verify — complete step-up → aal2 JWT.
  ///
  /// On success returns a new Session whose access token carries aal2 in its
  /// JWT claims.
  Future<Session> verifyChallenge(
    String factorId,
    String challengeId, {
    String? code,
    String? assertion,
  }) async {
    final body = <String, dynamic>{'challenge_id': challengeId};
    if (code != null) body['code'] = code;
    if (assertion != null) body['assertion'] = assertion;
    final data = await _http.requestJson(
      'POST',
      '/auth/v1/factors/$factorId/challenge/verify',
      body: body,
    ) as Map<String, dynamic>;
    final session = Session.fromJson(data);
    _session = session;
    return session;
  }

  /// DELETE /auth/v1/factors/:id — unenroll a factor (requires aal2 JWT).
  Future<Map<String, dynamic>> unenrollFactor(String factorId) async {
    return await _http.requestJson(
          'DELETE',
          '/auth/v1/factors/$factorId',
        )
        as Map<String, dynamic>;
  }
}
