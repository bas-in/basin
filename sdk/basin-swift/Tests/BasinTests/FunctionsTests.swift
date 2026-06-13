import XCTest
@testable import Basin

final class FunctionsTests: XCTestCase {

    var client: BasinClient!

    override func setUp() {
        super.setUp()
        MockURLProtocol.reset()
        client = BasinClient(url: "http://localhost:8080", key: "key", projectID: "01JTEST", session: mockSession())
    }

    // MARK: rpc

    func testRPC_ScalarResult() async throws {
        MockURLProtocol.push { req in
            XCTAssertEqual(req.httpMethod, "POST")
            XCTAssertEqual(req.url?.path, "/rest/v1/rpc/add")
            return mockResponse(url: req.url!, status: 200, json: 42)
        }
        let result = try await client.rpc("add", args: ["a": 40, "b": 2])
        XCTAssertEqual(result as? Int, 42)
    }

    func testRPC_RowResult() async throws {
        MockURLProtocol.push { req in
            XCTAssertEqual(req.url?.path, "/rest/v1/rpc/top_orders")
            return mockResponse(url: req.url!, status: 200,
                                json: [["id": 1, "total": 500], ["id": 2, "total": 300]])
        }
        let result = try await client.rpc("top_orders", args: ["limit": 2])
        if let rows = result as? [[String: Any]] {
            XCTAssertEqual(rows.count, 2)
        } else {
            XCTFail("Expected array of rows")
        }
    }

    // MARK: invoke

    func testInvoke_Success() async throws {
        let responseBody = #"{"resized":true}"#.data(using: .utf8)!
        MockURLProtocol.push { req in
            XCTAssertEqual(req.httpMethod, "POST")
            XCTAssertEqual(req.url?.path, "/fn/v1/resize")
            let response = HTTPURLResponse(
                url: req.url!, statusCode: 200, httpVersion: nil,
                headerFields: ["Content-Type": "application/json"]
            )!
            return (responseBody, response)
        }
        let result = try await client.functions.invoke("resize",
                                                       body: #"{"width":100}"#.data(using: .utf8))
        XCTAssertEqual(result.status, 200)
    }

    func testInvoke_BasinErrorSurfaced() async throws {
        MockURLProtocol.push { req in
            let body = #"{"code":"E_NOT_FOUND","message":"function not found"}"#.data(using: .utf8)!
            let response = HTTPURLResponse(
                url: req.url!, statusCode: 404, httpVersion: nil,
                headerFields: ["Content-Type": "application/json"]
            )!
            return (body, response)
        }
        do {
            _ = try await client.functions.invoke("nonexistent")
            XCTFail("Expected BasinApiError")
        } catch let e as BasinApiError {
            XCTAssertEqual(e.rawCode, "E_NOT_FOUND")
            XCTAssertEqual(e.status, 404)
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
    }
}
