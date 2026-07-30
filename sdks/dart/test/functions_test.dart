import 'dart:convert';

import 'package:basin_sdk/basin_sdk.dart';
import 'package:http/http.dart' as http;
import 'package:http/testing.dart';
import 'package:test/test.dart';

BasinClient _client(http.Client mock) => BasinClient.create(
  url: 'http://localhost:8080',
  key: 'k',
  projectId: 'proj-01',
  httpClient: mock,
);

void main() {
  group('FunctionsClient.rpc', () {
    test('POSTs to /rest/v1/rpc/:fn_name with args', () async {
      http.Request? req;
      final mock = MockClient((r) async {
        req = r;
        return http.Response(
          jsonEncode(42),
          200,
          headers: {'content-type': 'application/json'},
        );
      });
      final client = _client(mock);
      final result = await client.rpc('add', {'a': 40, 'b': 2});
      expect(req?.url.path, '/rest/v1/rpc/add');
      expect(req?.method, 'POST');
      expect(result, 42);
    });

    test('rpc with no args sends empty object', () async {
      http.Request? req;
      final mock = MockClient((r) async {
        req = r;
        return http.Response(
          jsonEncode({'result': true}),
          200,
          headers: {'content-type': 'application/json'},
        );
      });
      final client = _client(mock);
      await client.rpc('my_fn');
      final body = jsonDecode(req!.body) as Map;
      expect(body, isEmpty);
    });

    test('rpc with RETURNS TABLE returns list', () async {
      final mock = MockClient((_) async => http.Response(
        jsonEncode([{'id': 1}, {'id': 2}]),
        200,
        headers: {'content-type': 'application/json'},
      ));
      final client = _client(mock);
      final result = await client.rpc('list_orders');
      expect(result, isA<List>());
      expect((result as List).length, 2);
    });
  });

  group('FunctionsClient.invoke', () {
    test('sends Authorization header and returns FunctionInvokeResult', () async {
      http.Request? req;
      final mock = MockClient((r) async {
        req = r;
        return http.Response(
          jsonEncode({'resized': true}),
          200,
          headers: {'content-type': 'application/json'},
        );
      });
      final client = _client(mock);
      final result = await client.functions.invoke('resize',
          body: {'width': 100});
      expect(req?.url.path, '/fn/v1/resize');
      expect(req?.headers['Authorization'], 'Bearer k');
      expect(result.status, 200);
      expect(result.data, isA<Map>());
    });

    test('non-2xx with Basin error envelope raises BasinApiError', () async {
      final mock = MockClient((_) async => http.Response(
        jsonEncode({'code': 'E_NOT_FOUND', 'message': 'function not found'}),
        404,
        headers: {'content-type': 'application/json'},
      ));
      final client = _client(mock);
      expect(
        () => client.functions.invoke('missing_fn'),
        throwsA(
          isA<BasinApiError>().having((e) => e.code, 'code', 'E_NOT_FOUND'),
        ),
      );
    });

    test('non-2xx without Basin envelope returns result with non-200 status', () async {
      final mock = MockClient((_) async => http.Response(
        'not found',
        404,
        headers: {'content-type': 'text/plain'},
      ));
      final client = _client(mock);
      // Custom function returning its own 404 — not a Basin error envelope.
      final result = await client.functions.invoke('custom_fn');
      expect(result.status, 404);
      expect(result.data, 'not found');
    });

    test('custom method GET is passed through', () async {
      http.Request? req;
      final mock = MockClient((r) async {
        req = r;
        return http.Response('{}', 200,
            headers: {'content-type': 'application/json'});
      });
      final client = _client(mock);
      await client.functions.invoke('health_fn', method: 'GET');
      expect(req?.method, 'GET');
    });

    test('function name is URL-encoded', () async {
      http.Request? req;
      final mock = MockClient((r) async {
        req = r;
        return http.Response('{}', 200,
            headers: {'content-type': 'application/json'});
      });
      final client = _client(mock);
      await client.functions.invoke('my function');
      expect(req?.url.path, '/fn/v1/my%20function');
    });
  });
}
