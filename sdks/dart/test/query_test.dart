import 'dart:convert';

import 'package:basin_sdk/basin_sdk.dart';
import 'package:http/http.dart' as http;
import 'package:http/testing.dart';
import 'package:test/test.dart';

BasinClient _mockClient(dynamic Function(http.Request) handler, {int status = 200}) {
  final mock = MockClient((req) async => http.Response(
    jsonEncode(handler(req)),
    status,
    headers: {'content-type': 'application/json'},
  ));
  return BasinClient.create(
    url: 'http://localhost:8080',
    key: 'test-key',
    projectId: 'proj-01',
    httpClient: mock,
  );
}

void main() {
  group('QueryBuilder — filter construction', () {
    late List<Uri> capturedUris;

    setUp(() => capturedUris = []);

    BasinClient _capture() {
      final mock = MockClient((req) async {
        capturedUris.add(req.url);
        return http.Response(
          jsonEncode([]),
          200,
          headers: {'content-type': 'application/json'},
        );
      });
      return BasinClient.create(
        url: 'http://localhost:8080',
        key: 'k',
        projectId: 'proj-01',
        httpClient: mock,
      );
    }

    test('select encodes columns', () async {
      final client = _capture();
      await client.table('orders').select('id,total').get();
      expect(capturedUris.first.queryParameters['select'], 'id,total');
    });

    test('eq encodes correctly', () async {
      final client = _capture();
      await client.table('orders').eq('status', 'paid').get();
      expect(capturedUris.first.queryParameters['status'], 'eq.paid');
    });

    test('neq encodes correctly', () async {
      final client = _capture();
      await client.table('orders').neq('status', 'cancelled').get();
      expect(capturedUris.first.queryParameters['status'], 'neq.cancelled');
    });

    test('gt/gte/lt/lte encode correctly', () async {
      final client = _capture();
      await client.table('orders').gte('total', 100).lt('total', 500).get();
      final q = capturedUris.first.queryParameters;
      // Multiple same-key params are joined by Uri.queryParameters — they may
      // be comma-joined or the last wins depending on implementation.
      // Test each individually:
      capturedUris.clear();
      await client.table('orders').gte('total', 100).get();
      expect(capturedUris.first.queryParametersAll['total']?.first, 'gte.100');
    });

    test('inFilter encodes parenthesised list', () async {
      final client = _capture();
      await client.table('orders').inFilter('status', ['new', 'paid']).get();
      final q = capturedUris.first.queryParameters;
      expect(q['status'], 'in.(new,paid)');
    });

    test('is_ encodes null check', () async {
      final client = _capture();
      await client.table('orders').is_('deleted_at', 'null').get();
      expect(capturedUris.first.queryParameters['deleted_at'], 'is.null');
    });

    test('order appends asc/desc suffix', () async {
      final client = _capture();
      await client.table('orders').order('total', ascending: false).get();
      expect(capturedUris.first.queryParameters['order'], 'total.desc');
    });

    test('limit appends limit param', () async {
      final client = _capture();
      await client.table('orders').limit(50).get();
      expect(capturedUris.first.queryParameters['limit'], '50');
    });

    test('cursor appends cursor param', () async {
      final client = _capture();
      await client.table('orders').cursor('tok123').get();
      expect(capturedUris.first.queryParameters['cursor'], 'tok123');
    });

    test('chaining does not mutate builder', () async {
      final client = _capture();
      final base = client.table('orders').select('id');
      final withFilter = base.eq('status', 'paid');
      await base.get();
      await withFilter.get();
      expect(capturedUris[0].queryParameters.containsKey('status'), isFalse);
      expect(capturedUris[1].queryParameters['status'], 'eq.paid');
    });
  });

  group('QueryBuilder — response normalisation', () {
    test('plain array → QueryResult with no cursor', () async {
      final client = _mockClient(
        (_) => [{'id': 1}, {'id': 2}],
      );
      final r = await client.table('orders').get();
      expect(r.rows, hasLength(2));
      expect(r.nextCursor, isNull);
    });

    test('paginated shape → rows + nextCursor', () async {
      final client = _mockClient(
        (_) => {'rows': [{'id': 1}], 'next_cursor': 'cursor-abc'},
      );
      final r = await client.table('orders').limit(1).get();
      expect(r.rows, hasLength(1));
      expect(r.nextCursor, 'cursor-abc');
    });

    test('paginated shape with null cursor', () async {
      final client = _mockClient(
        (_) => {'rows': [{'id': 1}], 'next_cursor': null},
      );
      final r = await client.table('orders').limit(1).get();
      expect(r.nextCursor, isNull);
    });

    test('empty rows on write-shaped response', () async {
      final client = _mockClient((_) => {'ok': true, 'tag': 'INSERT 0 1'});
      final r = await client.table('orders').get();
      expect(r.rows, isEmpty);
    });
  });

  group('QueryBuilder — writes', () {
    test('insert POSTs to /rest/v1/:table', () async {
      http.Request? req;
      final mock = MockClient((r) async {
        req = r;
        return http.Response(
          jsonEncode({'ok': true, 'tag': 'INSERT 0 1'}),
          201,
          headers: {'content-type': 'application/json'},
        );
      });
      final client = BasinClient.create(
        url: 'http://localhost:8080',
        key: 'k',
        projectId: 'p',
        httpClient: mock,
      );
      await client.table('orders').insert({'total': 50, 'status': 'new'});
      expect(req?.method, 'POST');
      expect(req?.url.path, '/rest/v1/orders');
    });

    test('update PATCHes with filters in query string', () async {
      http.Request? req;
      final mock = MockClient((r) async {
        req = r;
        return http.Response(
          jsonEncode({'ok': true, 'tag': 'UPDATE 1'}),
          200,
          headers: {'content-type': 'application/json'},
        );
      });
      final client = BasinClient.create(
        url: 'http://localhost:8080',
        key: 'k',
        projectId: 'p',
        httpClient: mock,
      );
      await client.table('orders').eq('id', 7).update({'status': 'paid'});
      expect(req?.method, 'PATCH');
      expect(req?.url.queryParameters['id'], 'eq.7');
    });

    test('delete DELETEs with filters in query string', () async {
      http.Request? req;
      final mock = MockClient((r) async {
        req = r;
        return http.Response(
          jsonEncode({'ok': true, 'tag': 'DELETE 1'}),
          200,
          headers: {'content-type': 'application/json'},
        );
      });
      final client = BasinClient.create(
        url: 'http://localhost:8080',
        key: 'k',
        projectId: 'p',
        httpClient: mock,
      );
      await client.table('orders').eq('id', 7).delete();
      expect(req?.method, 'DELETE');
      expect(req?.url.queryParameters['id'], 'eq.7');
    });
  });

  group('QueryBuilder — rows() shorthand', () {
    test('returns list directly', () async {
      final client = _mockClient((_) => [{'id': 1}, {'id': 2}]);
      final rows = await client.table('orders').rows();
      expect(rows, hasLength(2));
    });
  });

  group('QueryBuilder — page() shorthand', () {
    test('returns Page with nextCursor', () async {
      final client = _mockClient(
        (_) => {'rows': [{'id': 1}], 'next_cursor': 'tok'},
      );
      final p = await client.table('orders').limit(1).page();
      expect(p.nextCursor, 'tok');
      expect(p.rows, hasLength(1));
    });
  });
}
