import XCTest
@testable import Basin

final class StorageTests: XCTestCase {

    var client: BasinClient!

    override func setUp() {
        super.setUp()
        MockURLProtocol.reset()
        client = BasinClient(url: "http://localhost:8080", key: "key", projectID: "proj_01J", session: mockSession())
    }

    private let bucketJSON: [String: Any] = [
        "id": "bkt_001", "name": "avatars", "public": true,
        "file_size_limit": NSNull(), "allowed_mime_types": [],
        "created_at": "2024-01-01T00:00:00Z", "updated_at": "2024-01-01T00:00:00Z"
    ]

    private let objectJSON: [String: Any] = [
        "id": "obj_001", "bucket_id": "bkt_001", "path": "users/alice.png",
        "size": 1024, "mime_type": "image/png", "owner": NSNull(), "etag": "abc123",
        "created_at": "2024-01-01T00:00:00Z", "updated_at": "2024-01-01T00:00:00Z",
        "metadata": NSNull()
    ]

    // MARK: - Bucket operations

    func testCreateBucket() async throws {
        MockURLProtocol.push { req in
            XCTAssertEqual(req.httpMethod, "POST")
            XCTAssertEqual(req.url?.path, "/storage/v1/bucket")
            return mockResponse(url: req.url!, status: 201, json: self.bucketJSON)
        }
        let bucket = try await client.storage.createBucket("avatars", public: true)
        XCTAssertEqual(bucket.name, "avatars")
        XCTAssertTrue(bucket.public)
    }

    func testGetBucket() async throws {
        MockURLProtocol.push { req in
            XCTAssertEqual(req.httpMethod, "GET")
            XCTAssert(req.url?.path.contains("/storage/v1/bucket/") == true)
            return mockResponse(url: req.url!, status: 200, json: self.bucketJSON)
        }
        let bucket = try await client.storage.getBucket("avatars")
        XCTAssertEqual(bucket.id, "bkt_001")
    }

    func testDeleteBucket() async throws {
        MockURLProtocol.push { req in
            XCTAssertEqual(req.httpMethod, "DELETE")
            return mockResponse(url: req.url!, status: 200, json: ["ok": true])
        }
        _ = try await client.storage.deleteBucket("avatars")
    }

    // MARK: - Object operations

    func testUpload() async throws {
        MockURLProtocol.push { req in
            XCTAssertEqual(req.httpMethod, "POST")
            XCTAssert(req.url?.path.contains("/storage/v1/object/avatars/") == true)
            return mockResponse(url: req.url!, status: 201, json: self.objectJSON)
        }
        let bucket = await client.storage.fromBucket("avatars")
        let obj = try await bucket.upload("users/alice.png",
                                          data: Data([0x89, 0x50, 0x4E, 0x47]),
                                          contentType: "image/png")
        XCTAssertEqual(obj.path, "users/alice.png")
        XCTAssertEqual(obj.size, 1024)
    }

    func testDownload() async throws {
        let imageData = Data([0x89, 0x50, 0x4E, 0x47])
        MockURLProtocol.push { req in
            XCTAssertEqual(req.httpMethod, "GET")
            let response = HTTPURLResponse(
                url: req.url!, statusCode: 200, httpVersion: nil,
                headerFields: ["Content-Type": "image/png", "ETag": "abc123"]
            )!
            return (imageData, response)
        }
        let bucket = await client.storage.fromBucket("avatars")
        let result = try await bucket.download("users/alice.png")
        XCTAssertEqual(result.data, imageData)
        XCTAssertEqual(result.contentType, "image/png")
        XCTAssertEqual(result.etag, "abc123")
    }

    func testRemoveObject() async throws {
        MockURLProtocol.push { req in
            XCTAssertEqual(req.httpMethod, "DELETE")
            return mockResponse(url: req.url!, status: 200, json: ["ok": true])
        }
        let bucket = await client.storage.fromBucket("avatars")
        _ = try await bucket.remove("users/alice.png")
    }

    func testListObjects() async throws {
        MockURLProtocol.push { req in
            XCTAssertEqual(req.httpMethod, "POST")
            XCTAssert(req.url?.path.contains("/storage/v1/object/list/avatars") == true)
            return mockResponse(url: req.url!, status: 200, json: [self.objectJSON])
        }
        let bucket = await client.storage.fromBucket("avatars")
        let objects = try await bucket.list(prefix: "users/")
        XCTAssertEqual(objects.count, 1)
        XCTAssertEqual(objects[0].etag, "abc123")
    }

    func testRemoveByPrefixes() async throws {
        MockURLProtocol.push { req in
            XCTAssertEqual(req.httpMethod, "DELETE")
            XCTAssert(req.url?.path.hasSuffix("/storage/v1/object/avatars") == true)
            return mockResponse(url: req.url!, status: 200, json: ["ok": true])
        }
        let bucket = await client.storage.fromBucket("avatars")
        _ = try await bucket.removeByPrefixes(["users/alice.png", "users/bob.png"])
    }

    // MARK: - URLs

    func testGetPublicURL() async throws {
        let bucket = await client.storage.fromBucket("avatars")
        let url = try await bucket.getPublicURL("users/alice.png")
        XCTAssert(url.contains("/storage/v1/object/public/proj_01J/avatars/users/alice.png"), "got: \(url)")
    }

    func testGetPublicURL_NoProjectID_Throws() async {
        let clientNoProject = BasinClient(url: "http://localhost:8080", key: "key", session: mockSession())
        let bucket = await clientNoProject.storage.fromBucket("avatars")
        do {
            _ = try await bucket.getPublicURL("test.png")
            XCTFail("Expected BasinApiError")
        } catch let e as BasinApiError {
            XCTAssertEqual(e.rawCode, "E_INVALID_REQUEST")
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
    }

    func testErrorEnvelopeSurfacesSQLState() throws {
        let body = #"{"code":"E_INVALID_REQUEST","message":"duplicate key value violates unique constraint","sqlstate":"23505"}"#
        let err = BasinApiError.fromResponseBody(Data(body.utf8), status: 400)
        XCTAssertEqual(err.rawCode, "E_INVALID_REQUEST")
        XCTAssertEqual(err.sqlState, "23505")
        XCTAssert(err.description.contains("23505"))
    }

    func testErrorEnvelopeSQLStateNilWhenAbsent() throws {
        let body = #"{"code":"E_NOT_FOUND","message":"gone"}"#
        let err = BasinApiError.fromResponseBody(Data(body.utf8), status: 404)
        XCTAssertNil(err.sqlState)
    }

    func testCreateSignedURL() async throws {
        MockURLProtocol.push { req in
            XCTAssertEqual(req.httpMethod, "POST")
            XCTAssert(req.url?.path.contains("/storage/v1/object/sign/upload/avatars/") == true)
            // Server returns camelCase per storage_sign.rs.
            return mockResponse(url: req.url!, status: 200, json: [
                "signedUrl": "/storage/v1/object/sign/proj_01J/avatars/users/alice.png?token=sig_tok",
                "expiresAt": "2099-01-01T00:00:00Z"
            ])
        }
        let bucket = await client.storage.fromBucket("avatars")
        let signed = try await bucket.createSignedURL("users/alice.png", expiresIn: 3600)
        XCTAssert(signed.absoluteUrl.hasPrefix("http://localhost:8080"), "got: \(signed.absoluteUrl)")
        XCTAssert(signed.absoluteUrl.contains("sig_tok"), "got: \(signed.absoluteUrl)")
        XCTAssertFalse(signed.expiresAt.isEmpty)
    }
}
