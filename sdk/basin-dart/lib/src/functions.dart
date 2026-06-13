/// Functions client — Basin's two function-invocation surfaces.
///
/// Route sources (verified):
/// - ANY  /fn/v1/:name         crates/basin-rest/src/server.rs:238,
///   handler crates/basin-rest/src/routes/fn_handler.rs (any_fn_handler) —
///   HTTP-handler-shape Wasm/JS functions; the guest's response (status,
///   headers, body) is proxied back verbatim.
/// - POST /rest/v1/rpc/:fn_name crates/basin-rest/src/server.rs:236,
///   handler crates/basin-rest/src/routes/rpc.rs (post_rpc) — catalog SQL /
///   Wasm UDFs. Body is a JSON object of named args; scalar results unwrap
///   to the bare value, table results return an array of row objects.
library;

import 'dart:convert';

import 'package:http/http.dart' as http;

import 'errors.dart';
import 'http.dart';
import 'types.dart';

bool _isErrorEnvelope(dynamic body) =>
    body is Map &&
    body['code'] is String &&
    (body['code'] as String).startsWith('E_') &&
    body['message'] is String;

/// Functions client for /fn/v1/:name and /rest/v1/rpc/:fn_name.
class FunctionsClient {
  final BasinHttpClient _http;

  FunctionsClient(this._http);

  /// ANY /fn/v1/:name — invoke an HTTP-handler function.
  ///
  /// The function's response is proxied verbatim, so non-2xx statuses are NOT
  /// automatically errors — unless the body is Basin's own error envelope
  /// (auth 401, unknown function 404, invocation failure 500), which raises
  /// [BasinApiError].
  Future<FunctionInvokeResult> invoke(
    String name, {
    String method = 'POST',
    Object? body,
    Map<String, String>? headers,
  }) async {
    final hdrs = <String, String>{};
    if (headers != null) hdrs.addAll(headers);

    final tok = await _http._bearer();
    if (tok != null) hdrs['Authorization'] = 'Bearer $tok';

    List<int>? rawBody;
    if (body is List<int>) {
      rawBody = body;
    } else if (body is String) {
      rawBody = utf8.encode(body);
    } else if (body != null) {
      hdrs.putIfAbsent('Content-Type', () => 'application/json; charset=utf-8');
      rawBody = utf8.encode(jsonEncode(body));
    }

    final uri = _http._buildUri(
      '/fn/v1/${Uri.encodeComponent(name)}',
      null,
    );

    http.Response response;
    try {
      final req = http.Request(method, uri)..headers.addAll(hdrs);
      if (rawBody != null) req.bodyBytes = rawBody;
      response = await http.Response.fromStream(
        await _http._http.send(req),
      );
    } catch (e) {
      throw BasinNetworkError(e.toString(), e);
    }

    final ct = response.headers['content-type'] ?? '';
    dynamic data = response.body;
    if (ct.contains('json') && response.body.isNotEmpty) {
      try {
        data = jsonDecode(response.body);
      } catch (_) {}
    }

    // Basin-layer failures surface as typed errors even for non-2xx.
    if (response.statusCode < 200 || response.statusCode >= 300) {
      if (_isErrorEnvelope(data)) {
        final d = data as Map;
        throw BasinApiError(
          d['code'] as String? ?? 'E_UNKNOWN',
          d['message'] as String? ?? 'HTTP ${response.statusCode}',
          response.statusCode,
        );
      }
    }

    return FunctionInvokeResult(
      status: response.statusCode,
      headers: Map.fromEntries(
        response.headers.entries.map((e) => MapEntry(e.key, e.value)),
      ),
      data: data,
    );
  }

  /// POST /rest/v1/rpc/:fn_name — call a catalog UDF.
  ///
  /// Args are named; missing declared args become NULL. Returns the bare
  /// scalar for single-value results, or a list of rows for RETURNS TABLE.
  Future<dynamic> rpc(String fnName, [Map<String, dynamic>? args]) =>
      _http.requestJson(
        'POST',
        '/rest/v1/rpc/${Uri.encodeComponent(fnName)}',
        body: args ?? {},
      );
}
