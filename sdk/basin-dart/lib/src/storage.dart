/// Storage client — bindings for /storage/v1/*.
///
/// Route sources (verified, crates/basin-rest/src/server.rs:373-424):
///
/// - POST   /storage/v1/bucket                                  create bucket
/// - GET    /storage/v1/bucket/:name                            bucket metadata
/// - DELETE /storage/v1/bucket/:name                            delete + purge
/// - POST   /storage/v1/object/:bucket/*path                    upload (JWT)
/// - GET    /storage/v1/object/:bucket/*path                    download (JWT)
/// - DELETE /storage/v1/object/:bucket/*path                    delete (JWT)
/// - POST   /storage/v1/object/list/:bucket                     list
/// - DELETE /storage/v1/object/:bucket                          bulk delete
/// - GET    /storage/v1/object/public/:project_id/:bucket/*path public download
/// - POST   /storage/v1/object/sign/upload/:bucket/*path        mint signed URL
/// - GET    /storage/v1/object/sign/:project/:bucket/*path      signed download
library;

import 'package:http/http.dart' as http;

import 'errors.dart';
import 'http.dart';
import 'types.dart';

String _encodePath(String path) =>
    path.split('/').map(Uri.encodeComponent).join('/');

/// Storage client. Access object operations via [fromBucket].
class StorageClient {
  final BasinHttpClient _http;
  final String? Function() _projectIdGetter;

  StorageClient(this._http, this._projectIdGetter);

  /// POST /storage/v1/bucket (201).
  Future<Bucket> createBucket(
    String name, {
    bool public = false,
    int? fileSizeLimit,
    List<String>? allowedMimeTypes,
  }) async {
    final data = await _http.requestJson(
      'POST',
      '/storage/v1/bucket',
      body: {
        'name': name,
        'public': public,
        if (fileSizeLimit != null) 'file_size_limit': fileSizeLimit,
        'allowed_mime_types': allowedMimeTypes ?? [],
      },
    ) as Map<String, dynamic>;
    return Bucket.fromJson(data);
  }

  /// GET /storage/v1/bucket/:name.
  Future<Bucket> getBucket(String name) async {
    final data = await _http.requestJson(
      'GET',
      '/storage/v1/bucket/${Uri.encodeComponent(name)}',
    ) as Map<String, dynamic>;
    return Bucket.fromJson(data);
  }

  /// DELETE /storage/v1/bucket/:name — also purges object bytes.
  Future<Map<String, dynamic>> deleteBucket(String name) async =>
      await _http.requestJson(
            'DELETE',
            '/storage/v1/bucket/${Uri.encodeComponent(name)}',
          )
          as Map<String, dynamic>;

  /// Return an object-level client scoped to one bucket.
  StorageBucketClient fromBucket(String bucket) =>
      StorageBucketClient(_http, bucket, _projectIdGetter);
}

/// Object operations scoped to one bucket.
class StorageBucketClient {
  final BasinHttpClient _http;
  final String _bucket;
  final String? Function() _projectIdGetter;

  StorageBucketClient(this._http, this._bucket, this._projectIdGetter);

  String get _encodedBucket => Uri.encodeComponent(_bucket);

  String _objectPath(String path) =>
      '/storage/v1/object/$_encodedBucket/${_encodePath(path)}';

  /// POST /storage/v1/object/:bucket/*path — upload bytes.
  Future<StorageObject> upload(
    String path,
    List<int> data, {
    String? contentType,
  }) async {
    final headers = <String, String>{};
    if (contentType != null) headers['Content-Type'] = contentType;
    final result = await _http.requestJson(
      'POST',
      _objectPath(path),
      rawBody: data,
      headers: headers,
    ) as Map<String, dynamic>;
    return StorageObject.fromJson(result);
  }

  /// GET /storage/v1/object/:bucket/*path — authenticated download.
  Future<DownloadResult> download(String path) async {
    final response = await _http.request('GET', _objectPath(path));
    return DownloadResult(
      data: response.bodyBytes,
      contentType: response.headers['content-type'],
      etag: response.headers['etag'],
    );
  }

  /// DELETE /storage/v1/object/:bucket/*path — single delete.
  Future<Map<String, dynamic>> remove(String path) async =>
      await _http.requestJson('DELETE', _objectPath(path))
          as Map<String, dynamic>;

  /// POST /storage/v1/object/list/:bucket.
  Future<List<StorageObject>> list({
    String? prefix,
    int? limit,
    int? offset,
  }) async {
    final data = await _http.requestJson(
      'POST',
      '/storage/v1/object/list/$_encodedBucket',
      body: {
        if (prefix != null) 'prefix': prefix,
        if (limit != null) 'limit': limit,
        if (offset != null) 'offset': offset,
      },
    );
    return (data as List? ?? [])
        .map((e) => StorageObject.fromJson(e as Map<String, dynamic>))
        .toList();
  }

  /// DELETE /storage/v1/object/:bucket with { prefixes: [...] } — bulk delete.
  Future<Map<String, dynamic>> removeByPrefixes(List<String> prefixes) async =>
      await _http.requestJson(
            'DELETE',
            '/storage/v1/object/$_encodedBucket',
            body: {'prefixes': prefixes},
          )
          as Map<String, dynamic>;

  /// Build the no-JWT public URL.
  ///
  /// GET /storage/v1/object/public/:project_id/:bucket/*path.
  /// Only works for buckets with [Bucket.public] = true.
  String getPublicUrl(String path) {
    final project = _projectIdGetter();
    if (project == null) {
      throw const BasinApiError(
        'E_INVALID_REQUEST',
        'public URLs require a project id (pass projectId to BasinClient.create())',
        0,
      );
    }
    final base = _http.baseUrl.replaceAll(RegExp(r'/$'), '');
    return '$base/storage/v1/object/public/$project/$_encodedBucket/${_encodePath(path)}';
  }

  /// POST /storage/v1/object/sign/upload/:bucket/*path — mint signed URL.
  ///
  /// TTL is capped at 7 days server-side; default 3600s.
  Future<SignedUrl> createSignedUrl(
    String path, {
    int expiresIn = 3600,
  }) async {
    final data = await _http.requestJson(
      'POST',
      '/storage/v1/object/sign/upload/$_encodedBucket/${_encodePath(path)}',
      body: {'expires_in': expiresIn},
    ) as Map<String, dynamic>;
    return SignedUrl.fromJson(data, _http.baseUrl);
  }

  /// GET /storage/v1/object/sign/:project/:bucket/*path — download via signed URL.
  ///
  /// No JWT needed — the token in the URL authorises the request.
  Future<DownloadResult> downloadSigned(
    String project,
    String path,
    String token,
  ) async {
    final response = await _http.request(
      'GET',
      '/storage/v1/object/sign/$project/$_encodedBucket/${_encodePath(path)}',
      query: [('token', token)],
      auth: false,
    );
    return DownloadResult(
      data: response.bodyBytes,
      contentType: response.headers['content-type'],
      etag: response.headers['etag'],
    );
  }
}

// Suppress unused import lint for http.dart — it's used via rawBody.
// ignore_for_file: unused_import
