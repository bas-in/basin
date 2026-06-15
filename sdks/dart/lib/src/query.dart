/// QueryBuilder — REST query builder for /rest/v1/:table.
///
/// Route source (verified): crates/basin-rest/src/server.rs:243-249 —
/// GET|POST|PATCH|DELETE /rest/v1/:table.
///
/// The filter grammar is Basin's PostgREST-style dialect (parser.rs):
/// - select=<cols,csv>
/// - <col>=<op>.<value> with ops eq|neq|gt|gte|lt|lte|in|is
///   (in.(a,b,c) parenthesised; is.null / is.notnull)
/// - order=<col>[.asc|.desc][,...], limit=N, offset=N
/// - cursor=<token> keyset pagination, stream=true NDJSON
///
/// NOT full PostgREST: no or=, not., like|ilike, embedded resource selects,
/// Prefer headers. Filters AND together.
///
/// Response shapes (crates/basin-rest/src/routes/data.rs):
/// - plain GET → JSON array of rows
/// - GET with limit or cursor → { rows, next_cursor }
/// - GET with ?stream=true → NDJSON, final line {"_basin_next_cursor":"..."} when paginating
/// - POST → 201, { ok, tag } (or rows); PATCH/DELETE → { ok, tag }
/// - DELETE may surface 501 E_ENGINE_UNSUPPORTED
library;

import 'dart:convert';

import 'http.dart';
import 'types.dart';

String _literal(Object? v) {
  if (v == null) return 'null';
  if (v is bool) return v ? 'true' : 'false';
  return v.toString();
}

/// Normalized result of a GET: rows plus the optional next-page cursor.
class QueryResult {
  final List<Row> rows;
  final String? nextCursor;

  const QueryResult({required this.rows, this.nextCursor});

  @override
  String toString() =>
      'QueryResult(rows: ${rows.length}, nextCursor: $nextCursor)';
}

QueryResult _normalizeGet(dynamic body) {
  if (body is List) {
    return QueryResult(
      rows: body.map((e) => e as Row).toList(),
    );
  }
  if (body is Map<String, dynamic>) {
    if (body.containsKey('rows')) {
      return QueryResult(
        rows:
            (body['rows'] as List? ?? []).map((e) => e as Row).toList(),
        nextCursor: body['next_cursor'] as String?,
      );
    }
  }
  return const QueryResult(rows: []);
}

/// Fluent query builder for /rest/v1/:table.
///
/// All filter methods return `this` for chaining. Call [get] (or await the
/// builder directly) to execute a SELECT, or [insert]/[update]/[delete] to
/// mutate data.
///
/// Example:
/// ```dart
/// final result = await client
///     .table('orders')
///     .select('id,total,status')
///     .eq('status', 'paid')
///     .gte('total', 100)
///     .order('total', ascending: false)
///     .limit(50)
///     .get();
/// ```
class QueryBuilder {
  final BasinHttpClient _http;
  final String _table;
  final List<(String, String)> _query;

  QueryBuilder(this._http, this._table, [List<(String, String)>? query])
    : _query = query ?? [];

  QueryBuilder _copy() =>
      QueryBuilder(_http, _table, List<(String, String)>.from(_query));

  QueryBuilder _add(String key, String value) {
    return _copy().._query.add((key, value));
  }

  // ------------------------------------------------------------------
  // Filter / projection / ordering
  // ------------------------------------------------------------------

  /// select=<cols> projection; omit or pass '*' for all columns.
  QueryBuilder select([String? columns]) =>
      _add('select', columns ?? '*');

  QueryBuilder eq(String column, Object? value) =>
      _add(column, 'eq.${_literal(value)}');

  QueryBuilder neq(String column, Object? value) =>
      _add(column, 'neq.${_literal(value)}');

  QueryBuilder gt(String column, Object? value) =>
      _add(column, 'gt.${_literal(value)}');

  QueryBuilder gte(String column, Object? value) =>
      _add(column, 'gte.${_literal(value)}');

  QueryBuilder lt(String column, Object? value) =>
      _add(column, 'lt.${_literal(value)}');

  QueryBuilder lte(String column, Object? value) =>
      _add(column, 'lte.${_literal(value)}');

  /// <col>=in.(a,b,c) — parenthesised list per parser.rs parse_in_list.
  QueryBuilder inFilter(String column, List<Object?> values) =>
      _add(column, 'in.(${values.map(_literal).join(',')})');

  /// <col>=is.null / <col>=is.notnull.
  QueryBuilder is_(String column, String value) =>
      _add(column, 'is.$value');

  /// order=<col>.asc|desc (repeatable).
  QueryBuilder order(String column, {bool ascending = true}) =>
      _add('order', '$column.${ascending ? 'asc' : 'desc'}');

  /// limit=N. Switches the GET response to { rows, next_cursor }.
  QueryBuilder limit(int n) => _add('limit', '$n');

  QueryBuilder offset(int n) => _add('offset', '$n');

  /// Resume keyset pagination from a next_cursor token.
  QueryBuilder cursor(String token) => _add('cursor', token);

  // ------------------------------------------------------------------
  // Execution
  // ------------------------------------------------------------------

  /// Execute as GET and return both rows and the pagination cursor.
  Future<QueryResult> get() async {
    final body = await _http.requestJson(
      'GET',
      '/rest/v1/$_table',
      query: _query,
    );
    return _normalizeGet(body);
  }

  /// Execute as GET and return rows only.
  Future<List<Row>> rows() async => (await get()).rows;

  /// Execute as paginated GET ({ rows, next_cursor } shape).
  Future<Page> page() async {
    final r = await get();
    return Page(rows: r.rows, nextCursor: r.nextCursor);
  }

  /// Execute as GET with ?stream=true (NDJSON), yielding rows.
  ///
  /// The trailing `{"_basin_next_cursor": ...}` line is captured and
  /// accessible via [QueryResult.nextCursor] in the returned result after
  /// the stream is exhausted. Use the stream-variant [streamRows] for lazy
  /// consumption of large result sets.
  Future<QueryResult> streamCollect() async {
    final q = List<(String, String)>.from(_query)..add(('stream', 'true'));
    final response = await _http.request(
      'GET',
      '/rest/v1/$_table',
      query: q,
    );
    final rows = <Row>[];
    String? nextCursor;
    for (final line in response.body.split('\n')) {
      final trimmed = line.trim();
      if (trimmed.isEmpty) continue;
      final parsed = jsonDecode(trimmed) as Map<String, dynamic>;
      if (parsed.containsKey('_basin_next_cursor')) {
        nextCursor = parsed['_basin_next_cursor'] as String?;
        continue;
      }
      rows.add(parsed);
    }
    return QueryResult(rows: rows, nextCursor: nextCursor);
  }

  /// POST /rest/v1/:table — insert one row object or a list (201).
  Future<dynamic> insert(Object values) => _http.requestJson(
    'POST',
    '/rest/v1/$_table',
    body: values,
  );

  /// PATCH /rest/v1/:table?<filters> — update rows matching the filters.
  Future<dynamic> update(Row values) => _http.requestJson(
    'PATCH',
    '/rest/v1/$_table',
    query: _query,
    body: values,
  );

  /// DELETE /rest/v1/:table?<filters>.
  ///
  /// May throw [BasinApiError] with code E_ENGINE_UNSUPPORTED (501) on
  /// engines without DELETE support.
  Future<dynamic> delete() => _http.requestJson(
    'DELETE',
    '/rest/v1/$_table',
    query: _query,
  );

  /// Awaiting the builder itself is shorthand for [get].
  Future<QueryResult> then<R>(
    Function(QueryResult) onValue, [
    Function? onError,
  ]) => get().then(onValue, onError: onError);
}
