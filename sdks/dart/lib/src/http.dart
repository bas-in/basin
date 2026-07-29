/// Internal HTTP transport layer.
///
/// Uses package:http which is cross-platform: works on Flutter (Android, iOS,
/// Web, desktop) and Dart CLI/server. package:http uses dart:html on Flutter
/// Web and dart:io on native — callers never see that distinction.
///
/// Token injection: [BasinHttpClient] accepts an optional [getToken] callback
/// that is called before every authenticated request. When the callback is set
/// (wired up to AuthClient.accessToken) the SDK auto-refreshes the JWT before
/// it is used, rather than waiting for a 401.
library;

import 'dart:convert';

import 'package:http/http.dart' as http;

import 'errors.dart';

/// Low-level HTTP client wrapping package:http.
///
/// All routes send `Authorization: Bearer <token>` when an auth token is
/// available. Unauthenticated routes (sign-up, sign-in, sign-out, magic-link)
/// call with [auth] = false, which bypasses the header injection so the static
/// key or session token do not leak on public endpoints.
class BasinHttpClient {
  final String baseUrl;

  /// Static key (raw API key or initial JWT supplied to [BasinClient]).
  final String? _staticKey;

  /// Package:http client — injectable for testing via MockClient.
  final http.Client _http;

  /// Callback that returns the current bearer token (may auto-refresh).
  /// Wired to AuthClient.accessToken after construction.
  Future<String?> Function()? _getToken;

  BasinHttpClient({
    required this.baseUrl,
    String? key,
    http.Client? httpClient,
  }) : _staticKey = key,
       _http = httpClient ?? http.Client();

  void setTokenGetter(Future<String?> Function() getter) {
    _getToken = getter;
  }

  Future<String?> _bearer() async {
    if (_getToken != null) {
      final tok = await _getToken!();
      if (tok != null) return tok;
    }
    return _staticKey;
  }

  Uri _buildUri(String path, List<(String, String)>? query) {
    final base = Uri.parse(baseUrl.replaceAll(RegExp(r'/$'), ''));
    final params = <String, dynamic>{};
    if (query != null) {
      for (final (k, v) in query) {
        if (params.containsKey(k)) {
          final existing = params[k];
          if (existing is List) {
            existing.add(v);
          } else {
            params[k] = [existing, v];
          }
        } else {
          params[k] = v;
        }
      }
    }
    return Uri(
      scheme: base.scheme,
      host: base.host,
      port: base.port,
      path: base.path + path,
      queryParameters: params.isEmpty ? null : params,
    );
  }

  /// Execute a request and return the raw [http.Response].
  Future<http.Response> request(
    String method,
    String path, {
    List<(String, String)>? query,
    Object? body,
    List<int>? rawBody,
    Map<String, String>? headers,
    bool auth = true,
  }) async {
    final uri = _buildUri(path, query);
    final hdrs = <String, String>{};

    if (auth) {
      final tok = await _bearer();
      if (tok != null) hdrs['Authorization'] = 'Bearer $tok';
    }

    if (headers != null) hdrs.addAll(headers);

    http.Response response;
    try {
      if (rawBody != null) {
        final req = http.Request(method, uri);
        req.headers.addAll(hdrs);
        req.bodyBytes = rawBody;
        response = await http.Response.fromStream(await _http.send(req));
      } else if (body != null) {
        hdrs.putIfAbsent('Content-Type', () => 'application/json; charset=utf-8');
        final encoded = jsonEncode(body);
        switch (method) {
          case 'GET':
            // GET with body: use Request directly
            final req = http.Request(method, uri)
              ..headers.addAll(hdrs)
              ..body = encoded;
            response = await http.Response.fromStream(await _http.send(req));
          case 'POST':
            response = await _http.post(uri, headers: hdrs, body: encoded);
          case 'PATCH':
            response = await _http.patch(uri, headers: hdrs, body: encoded);
          case 'PUT':
            response = await _http.put(uri, headers: hdrs, body: encoded);
          case 'DELETE':
            final req = http.Request('DELETE', uri)
              ..headers.addAll(hdrs)
              ..body = encoded;
            response = await http.Response.fromStream(await _http.send(req));
          default:
            final req = http.Request(method, uri)
              ..headers.addAll(hdrs)
              ..body = encoded;
            response = await http.Response.fromStream(await _http.send(req));
        }
      } else {
        switch (method) {
          case 'GET':
            response = await _http.get(uri, headers: hdrs);
          case 'DELETE':
            response = await _http.delete(uri, headers: hdrs);
          case 'POST':
            response = await _http.post(uri, headers: hdrs);
          case 'PATCH':
            response = await _http.patch(uri, headers: hdrs);
          default:
            final req = http.Request(method, uri)..headers.addAll(hdrs);
            response = await http.Response.fromStream(await _http.send(req));
        }
      }
    } catch (e) {
      throw BasinNetworkError(e.toString(), e);
    }

    if (response.statusCode < 200 || response.statusCode >= 300) {
      _throwApiError(response);
    }
    return response;
  }

  /// Execute a request and JSON-decode the response body.
  Future<dynamic> requestJson(
    String method,
    String path, {
    List<(String, String)>? query,
    Object? body,
    Map<String, String>? headers,
    bool auth = true,
  }) async {
    final response = await request(
      method,
      path,
      query: query,
      body: body,
      headers: headers,
      auth: auth,
    );
    final text = response.body;
    if (text.isEmpty) return null;
    return jsonDecode(text);
  }

  Never _throwApiError(http.Response response) {
    dynamic body;
    try {
      body = jsonDecode(response.body);
    } catch (_) {
      body = null;
    }
    if (body is Map<String, dynamic>) {
      throw BasinApiError.fromBody(body, response.statusCode);
    }
    throw BasinApiError(
      'E_UNKNOWN',
      'HTTP ${response.statusCode}',
      response.statusCode,
    );
  }

  void close() => _http.close();
}
