/// Storage client — bindings for /storage/v1/*.
///
/// Route sources (verified, crates/basin-rest/src/server.rs:373-424):
/// - POST   /storage/v1/bucket                                  create bucket
/// - GET    /storage/v1/bucket/:name                            bucket metadata
/// - DELETE /storage/v1/bucket/:name                            delete + purge
/// - POST   /storage/v1/object/:bucket/*path                    upload (JWT)
/// - GET    /storage/v1/object/:bucket/*path                    download (JWT)
/// - DELETE /storage/v1/object/:bucket/*path                    delete (JWT)
/// - POST   /storage/v1/object/list/:bucket                     list (body: prefix/limit/offset)
/// - DELETE /storage/v1/object/:bucket                          bulk delete (body: prefixes[])
/// - GET    /storage/v1/object/public/:project_id/:bucket/*path public download (no JWT)
/// - POST   /storage/v1/object/sign/upload/:bucket/*path        mint signed URL (JWT)
/// - GET    /storage/v1/object/sign/:project/:bucket/*path      signed download (no JWT)

import Foundation

private struct CreateBucketBody: Encodable {
    let name: String
    let `public`: Bool
    let file_size_limit: Int?
    let allowed_mime_types: [String]
}

private struct ListObjectsBody: Encodable {
    let prefix: String?
    let limit: Int?
    let offset: Int?
}

private struct BulkDeleteBody: Encodable {
    let prefixes: [String]
}

private struct SignedURLBody: Encodable {
    let expires_in: Int
}

// MARK: - SignedUrl decode shim

private struct SignedUrlWire: Decodable {
    // The server returns camelCase for this endpoint (storage_sign.rs).
    let signedUrl: String
    let expiresAt: String
}

// MARK: - StorageClient

/// Storage client. Use `fromBucket(_:)` for object-level operations.
public actor StorageClient {
    private let transport: HTTPTransport
    private let projectIDGetter: @Sendable () async -> String?

    init(transport: HTTPTransport, projectIDGetter: @escaping @Sendable () async -> String?) {
        self.transport = transport
        self.projectIDGetter = projectIDGetter
    }

    /// POST /storage/v1/bucket (201).
    public func createBucket(
        _ name: String,
        public isPublic: Bool = false,
        fileSizeLimit: Int? = nil,
        allowedMimeTypes: [String] = []
    ) async throws -> Bucket {
        try await transport.requestJSON(
            "POST", path: "/storage/v1/bucket",
            body: CreateBucketBody(
                name: name,
                public: isPublic,
                file_size_limit: fileSizeLimit,
                allowed_mime_types: allowedMimeTypes
            )
        )
    }

    /// GET /storage/v1/bucket/:name.
    public func getBucket(_ name: String) async throws -> Bucket {
        try await transport.requestJSON("GET", path: "/storage/v1/bucket/\(percentEncode(name))")
    }

    /// DELETE /storage/v1/bucket/:name — also purges object bytes.
    @discardableResult
    public func deleteBucket(_ name: String) async throws -> Data {
        try await transport.requestJSONVoid("DELETE", path: "/storage/v1/bucket/\(percentEncode(name))")
    }

    /// Return an object-level client scoped to one bucket.
    public func fromBucket(_ bucket: String) -> StorageBucketClient {
        StorageBucketClient(transport: transport, bucket: bucket, projectIDGetter: projectIDGetter)
    }
}

// MARK: - StorageBucketClient

/// Object operations scoped to one bucket.
public actor StorageBucketClient {
    private let transport: HTTPTransport
    private let bucket: String
    private let projectIDGetter: @Sendable () async -> String?

    init(transport: HTTPTransport, bucket: String, projectIDGetter: @escaping @Sendable () async -> String?) {
        self.transport = transport
        self.bucket = bucket
        self.projectIDGetter = projectIDGetter
    }

    private func objectPath(_ path: String) -> String {
        "/storage/v1/object/\(percentEncode(bucket))/\(encodeObjectPath(path))"
    }

    /// POST /storage/v1/object/:bucket/*path.
    public func upload(
        _ path: String,
        data: Data,
        contentType: String? = nil
    ) async throws -> StorageObject {
        var headers: [String: String] = [:]
        if let ct = contentType { headers["Content-Type"] = ct }
        let (responseData, _) = try await transport.requestRaw(
            "POST", path: objectPath(path),
            body: data, headers: headers
        )
        return try decodeJSON(responseData)
    }

    /// GET /storage/v1/object/:bucket/*path — authenticated download.
    public func download(_ path: String) async throws -> DownloadResult {
        let (data, response) = try await transport.requestRaw("GET", path: objectPath(path))
        return DownloadResult(
            data: data,
            contentType: response.value(forHTTPHeaderField: "Content-Type"),
            etag: response.value(forHTTPHeaderField: "ETag")
        )
    }

    /// DELETE /storage/v1/object/:bucket/*path — single delete.
    @discardableResult
    public func remove(_ path: String) async throws -> Data {
        let (data, _) = try await transport.requestRaw("DELETE", path: objectPath(path))
        return data
    }

    /// POST /storage/v1/object/list/:bucket.
    public func list(
        prefix: String? = nil,
        limit: Int? = nil,
        offset: Int? = nil
    ) async throws -> [StorageObject] {
        try await transport.requestJSON(
            "POST",
            path: "/storage/v1/object/list/\(percentEncode(bucket))",
            body: ListObjectsBody(prefix: prefix, limit: limit, offset: offset)
        )
    }

    /// DELETE /storage/v1/object/:bucket with { prefixes: [...] }.
    @discardableResult
    public func removeByPrefixes(_ prefixes: [String]) async throws -> Data {
        try await transport.requestJSONVoid(
            "DELETE",
            path: "/storage/v1/object/\(percentEncode(bucket))",
            body: BulkDeleteBody(prefixes: prefixes)
        )
    }

    /// Build the no-JWT public URL.
    ///
    /// GET /storage/v1/object/public/:project_id/:bucket/*path
    /// Only works for buckets with public=true.
    public func getPublicURL(_ path: String) async throws -> String {
        guard let project = await projectIDGetter() else {
            throw BasinApiError(
                rawCode: "E_INVALID_REQUEST",
                message: "public URLs require a project id (pass projectID to BasinClient)",
                status: 0
            )
        }
        let base = transport.baseURL
        return "\(base)/storage/v1/object/public/\(project)/\(percentEncode(bucket))/\(encodeObjectPath(path))"
    }

    /// POST /storage/v1/object/sign/upload/:bucket/*path — mint signed URL.
    ///
    /// TTL is capped at 7 days server-side; default 3600 s. Despite the
    /// 'upload' literal in the path, this mints a download URL.
    public func createSignedURL(_ path: String, expiresIn: Int = 3600) async throws -> SignedUrl {
        let (data, _) = try await transport.requestRaw(
            "POST",
            path: "/storage/v1/object/sign/upload/\(percentEncode(bucket))/\(encodeObjectPath(path))",
            body: try JSONEncoder().encode(SignedURLBody(expires_in: expiresIn)),
            headers: ["Content-Type": "application/json"]
        )
        let wire = try decodeJSON(data) as SignedUrlWire
        let base = transport.baseURL
        return SignedUrl(signedUrl: wire.signedUrl,
                        expiresAt: wire.expiresAt,
                        absoluteUrl: base + wire.signedUrl)
    }
}
