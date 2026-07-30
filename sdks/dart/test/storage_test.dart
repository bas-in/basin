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

final _bucketJson = {
  'id': 'b1',
  'name': 'avatars',
  'public': false,
  'file_size_limit': null,
  'allowed_mime_types': [],
  'created_at': '2026-01-01T00:00:00Z',
  'updated_at': '2026-01-01T00:00:00Z',
};

final _objectJson = {
  'id': 'o1',
  'bucket_id': 'b1',
  'path': 'users/alice.png',
  'size': 1024,
  'mime_type': 'image/png',
  'metadata': null,
  'owner': null,
  'etag': '"abc"',
  'created_at': '2026-01-01T00:00:00Z',
  'updated_at': '2026-01-01T00:00:00Z',
};

void main() {
  group('StorageClient.createBucket', () {
    test('POSTs to /storage/v1/bucket', () async {
      http.Request? req;
      final mock = MockClient((r) async {
        req = r;
        return http.Response(jsonEncode(_bucketJson), 201,
            headers: {'content-type': 'application/json'});
      });
      final client = _client(mock);
      final bucket = await client.storage.createBucket(
        'avatars',
        public: false,
        allowedMimeTypes: ['image/png'],
      );
      expect(req?.method, 'POST');
      expect(req?.url.path, '/storage/v1/bucket');
      final body = jsonDecode(req!.body) as Map;
      expect(body['name'], 'avatars');
      expect(body['allowed_mime_types'], ['image/png']);
      expect(bucket.name, 'avatars');
    });
  });

  group('StorageClient.getBucket', () {
    test('GETs /storage/v1/bucket/:name', () async {
      http.Request? req;
      final mock = MockClient((r) async {
        req = r;
        return http.Response(jsonEncode(_bucketJson), 200,
            headers: {'content-type': 'application/json'});
      });
      final client = _client(mock);
      await client.storage.getBucket('avatars');
      expect(req?.url.path, '/storage/v1/bucket/avatars');
    });
  });

  group('StorageBucketClient.upload', () {
    test('POSTs raw bytes to /storage/v1/object/:bucket/*path', () async {
      http.Request? req;
      final mock = MockClient((r) async {
        req = r;
        return http.Response(jsonEncode(_objectJson), 200,
            headers: {'content-type': 'application/json'});
      });
      final client = _client(mock);
      final data = [0x89, 0x50, 0x4e, 0x47]; // PNG header
      await client.storage.fromBucket('avatars').upload(
        'users/alice.png',
        data,
        contentType: 'image/png',
      );
      expect(req?.url.path, '/storage/v1/object/avatars/users/alice.png');
      expect(req?.method, 'POST');
      expect(req?.headers['Content-Type'], 'image/png');
      expect(req?.bodyBytes, data);
    });
  });

  group('StorageBucketClient.download', () {
    test('GETs /storage/v1/object/:bucket/*path', () async {
      final mock = MockClient((_) async => http.Response.bytes(
        [1, 2, 3],
        200,
        headers: {'content-type': 'image/png', 'etag': '"abc"'},
      ));
      final client = _client(mock);
      final result =
          await client.storage.fromBucket('avatars').download('users/alice.png');
      expect(result.data, [1, 2, 3]);
      expect(result.contentType, 'image/png');
      expect(result.etag, '"abc"');
    });
  });

  group('StorageBucketClient.list', () {
    test('POSTs to /storage/v1/object/list/:bucket', () async {
      http.Request? req;
      final mock = MockClient((r) async {
        req = r;
        return http.Response(jsonEncode([_objectJson]), 200,
            headers: {'content-type': 'application/json'});
      });
      final client = _client(mock);
      final objs = await client.storage.fromBucket('avatars').list(
        prefix: 'users/',
        limit: 10,
      );
      expect(req?.url.path, '/storage/v1/object/list/avatars');
      expect(req?.method, 'POST');
      final body = jsonDecode(req!.body) as Map;
      expect(body['prefix'], 'users/');
      expect(objs, hasLength(1));
    });
  });

  group('StorageBucketClient.removeByPrefixes', () {
    test('DELETEs /storage/v1/object/:bucket with prefixes body', () async {
      http.Request? req;
      final mock = MockClient((r) async {
        req = r;
        return http.Response(jsonEncode({'ok': true}), 200,
            headers: {'content-type': 'application/json'});
      });
      final client = _client(mock);
      await client.storage.fromBucket('avatars').removeByPrefixes(['users/']);
      expect(req?.url.path, '/storage/v1/object/avatars');
      expect(req?.method, 'DELETE');
      final body = jsonDecode(req!.body) as Map;
      expect(body['prefixes'], ['users/']);
    });
  });

  group('StorageBucketClient.getPublicUrl', () {
    test('builds URL with project_id in path', () {
      final client = BasinClient.create(
        url: 'https://api.basin.run',
        projectId: 'proj-01',
        httpClient: MockClient((_) async => http.Response('{}', 200)),
      );
      final url =
          client.storage.fromBucket('avatars').getPublicUrl('users/alice.png');
      expect(url,
          'https://api.basin.run/storage/v1/object/public/proj-01/avatars/users/alice.png');
    });

    test('throws E_INVALID_REQUEST when project_id unknown', () {
      final client = BasinClient.create(
        url: 'https://api.basin.run',
        httpClient: MockClient((_) async => http.Response('{}', 200)),
      );
      expect(
        () => client.storage.fromBucket('avatars').getPublicUrl('file.png'),
        throwsA(isA<BasinApiError>().having(
          (e) => e.code,
          'code',
          'E_INVALID_REQUEST',
        )),
      );
    });
  });

  group('StorageBucketClient.createSignedUrl', () {
    test('POSTs to sign/upload path with expires_in', () async {
      http.Request? req;
      final mock = MockClient((r) async {
        req = r;
        return http.Response(
          jsonEncode({
            'signedUrl': '/storage/v1/object/sign/proj-01/avatars/file.png?token=tok',
            'expiresAt': '2026-06-01T01:00:00Z',
          }),
          200,
          headers: {'content-type': 'application/json'},
        );
      });
      final client = _client(mock);
      final su = await client.storage
          .fromBucket('avatars')
          .createSignedUrl('file.png', expiresIn: 7200);
      expect(req?.url.path,
          '/storage/v1/object/sign/upload/avatars/file.png');
      final body = jsonDecode(req!.body) as Map;
      expect(body['expires_in'], 7200);
      expect(su.expiresAt, '2026-06-01T01:00:00Z');
      expect(su.absoluteUrl, startsWith('http://localhost:8080'));
    });
  });

  group('path encoding', () {
    test('spaces and special chars in object path are percent-encoded', () async {
      http.Request? req;
      final mock = MockClient((r) async {
        req = r;
        return http.Response(jsonEncode(_objectJson), 200,
            headers: {'content-type': 'application/json'});
      });
      final client = _client(mock);
      await client.storage
          .fromBucket('my bucket')
          .download('my file/with spaces.txt');
      // Bucket name encoded in path
      expect(req?.url.path, contains('my%20bucket'));
      // Path segment encoded
      expect(req?.url.path, contains('my%20file'));
    });
  });
}
