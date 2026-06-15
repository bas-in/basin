/// Top-level BasinClient.
///
/// The client wires together auth, query, storage, functions, and realtime
/// into a single entry point.
library;

import 'package:http/http.dart' as http;
import 'package:web_socket_channel/web_socket_channel.dart';

import 'auth.dart';
import 'errors.dart';
import 'functions.dart';
import 'http.dart';
import 'query.dart';
import 'realtime.dart';
import 'storage.dart';
import 'types.dart';

/// Basin client.
///
/// [key] is a JWT or an API-key secret — both are accepted as
/// `Authorization: Bearer <key>` (crates/basin-rest/src/server.rs `authorize`,
/// JWT verify first, API-key lookup fallback).
///
/// Create with the named constructor [BasinClient.create]:
///
/// ```dart
/// final basin = BasinClient.create(
///   url: 'https://your-project.basin.run',
///   key: 'your-api-key',
///   projectId: '01J...',  // optional when key is a JWT
/// );
///
/// final result = await basin
///     .table('orders')
///     .select('id,total')
///     .gte('total', 100)
///     .get();
/// ```
class BasinClient {
  final BasinHttpClient _http;
  final String? _explicitProjectId;
  final String? _key;

  late final AuthClient auth;
  late final FunctionsClient functions;
  late final StorageClient storage;
  late final RealtimeClient realtime;

  BasinClient._(
    this._http,
    this._explicitProjectId,
    this._key, {
    WebSocketChannel Function(Uri, {Iterable<String>? protocols})? wsFactory,
  }) {
    auth = AuthClient(_http, _explicitProjectId);
    functions = FunctionsClient(_http);
    storage = StorageClient(_http, _resolveProjectId);
    realtime = RealtimeClient(
      baseUrl: _http.baseUrl,
      getToken: auth.accessToken,
      projectId: _resolveProjectId,
      wsFactory: wsFactory,
    );

    // Wire the transport's token getter to the auth client so every
    // authenticated request uses the freshest session token.
    _http.setTokenGetter(auth.accessToken);
  }

  /// Create a [BasinClient].
  ///
  /// [url]: Base URL of the Basin server, e.g. `https://your-project.basin.run`.
  /// [key]: JWT or raw API key — passed as `Authorization: Bearer <key>`.
  /// [projectId]: Project ULID. Optional when [key] is a JWT that carries a
  ///   `project_id` claim.
  /// [httpClient]: Inject a custom [http.Client] (for testing).
  /// [wsFactory]: Inject a custom WebSocket factory (for testing).
  factory BasinClient.create({
    required String url,
    String? key,
    String? projectId,
    http.Client? httpClient,
    WebSocketChannel Function(Uri, {Iterable<String>? protocols})? wsFactory,
  }) {
    final httpTransport = BasinHttpClient(
      baseUrl: url,
      key: key,
      httpClient: httpClient,
    );
    return BasinClient._(httpTransport, projectId, key, wsFactory: wsFactory);
  }

  String? _resolveProjectId() {
    if (_explicitProjectId != null) return _explicitProjectId;
    final session = auth.getSession();
    if (session != null) {
      final fromSession = projectIdFromJwt(session.accessToken);
      if (fromSession != null) return fromSession;
    }
    if (_key != null) return projectIdFromJwt(_key!);
    return null;
  }

  /// Return a [QueryBuilder] for /rest/v1/:table.
  QueryBuilder table(String name) => QueryBuilder(_http, name);

  /// Alias matching the JS SDK's `from(table)` style.
  QueryBuilder from(String name) => table(name);

  /// POST /rest/v1/rpc/:fn_name — convenience alias for functions.rpc.
  Future<dynamic> rpc(String fnName, [Map<String, dynamic>? args]) =>
      functions.rpc(fnName, args);

  /// GET /health → 'ok'.
  Future<String> health() async {
    final response = await _http.request('GET', '/health', auth: false);
    return response.body;
  }

  /// Escape hatch for routes not yet wrapped (e.g. /admin/v1/*).
  Future<dynamic> request(
    String method,
    String path, {
    List<(String, String)>? query,
    Object? body,
  }) => _http.requestJson(method, path, query: query, body: body);

  /// Close the underlying HTTP client.
  void close() => _http.close();
}
