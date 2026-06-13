import XCTest
@testable import Basin

final class QueryTests: XCTestCase {

    var client: BasinClient!

    override func setUp() {
        super.setUp()
        MockURLProtocol.reset()
        client = BasinClient(url: "http://localhost:8080", key: "key", projectID: "01JTEST", session: mockSession())
    }

    // MARK: - Query building

    private struct Order: Decodable {
        let id: Int
        let total: Int
        let status: String
    }

    func testSelect_BuildsCorrectQuery() async throws {
        MockURLProtocol.push { req in
            XCTAssertEqual(req.url?.path, "/rest/v1/orders")
            XCTAssert(req.url?.query?.contains("select=id%2Ctotal") == true
                      || req.url?.query?.contains("select=id,total") == true)
            return mockResponse(url: req.url!, status: 200,
                                json: [["id": 1, "total": 100, "status": "paid"]])
        }
        let rows: [Order] = try await client.table("orders")
            .select("id,total")
            .run()
        XCTAssertEqual(rows.count, 1)
        XCTAssertEqual(rows[0].id, 1)
    }

    func testEqFilter() async throws {
        MockURLProtocol.push { req in
            let q = req.url?.query ?? ""
            XCTAssert(q.contains("status=eq.paid"), "query was: \(q)")
            return mockResponse(url: req.url!, status: 200,
                                json: [["id": 2, "total": 50, "status": "paid"]])
        }
        let rows: [Order] = try await client.table("orders")
            .eq("status", .string("paid"))
            .run()
        XCTAssertEqual(rows[0].status, "paid")
    }

    func testGteFilter() async throws {
        MockURLProtocol.push { req in
            let q = req.url?.query ?? ""
            XCTAssert(q.contains("total=gte.100"), "query was: \(q)")
            return mockResponse(url: req.url!, status: 200, json: [])
        }
        let _: [Order] = try await client.table("orders").gte("total", .int(100)).run()
    }

    func testInFilter() async throws {
        MockURLProtocol.push { req in
            let q = req.url?.query ?? ""
            // in.(a,b,c) parenthesised
            XCTAssert(q.contains("status=in.") && q.contains("paid"), "query was: \(q)")
            return mockResponse(url: req.url!, status: 200, json: [])
        }
        let _: [Order] = try await client.table("orders")
            .`in`("status", [.string("paid"), .string("pending")])
            .run()
    }

    func testIsNullFilter() async throws {
        MockURLProtocol.push { req in
            let q = req.url?.query ?? ""
            XCTAssert(q.contains("deleted_at=is.null"), "query was: \(q)")
            return mockResponse(url: req.url!, status: 200, json: [])
        }
        let _: [Order] = try await client.table("orders").`is`("deleted_at", "null").run()
    }

    func testOrderAndLimit() async throws {
        MockURLProtocol.push { req in
            let q = req.url?.query ?? ""
            XCTAssert(q.contains("order=total.desc"), "query was: \(q)")
            XCTAssert(q.contains("limit=10"), "query was: \(q)")
            return mockResponse(url: req.url!, status: 200, json: [])
        }
        let _: [Order] = try await client.table("orders")
            .order("total", ascending: false)
            .limit(10)
            .run()
    }

    func testCursorPagination() async throws {
        MockURLProtocol.push { req in
            let q = req.url?.query ?? ""
            XCTAssert(q.contains("cursor=tok_abc"), "query was: \(q)")
            return mockResponse(url: req.url!, status: 200, json: [
                "rows": [["id": 3, "total": 30, "status": "new"]],
                "next_cursor": "tok_def"
            ])
        }
        let page: QueryPage<Order> = try await client.table("orders")
            .cursor("tok_abc")
            .limit(10)
            .page()
        XCTAssertEqual(page.rows.count, 1)
        XCTAssertEqual(page.nextCursor, "tok_def")
    }

    func testPage_NoNextCursor() async throws {
        MockURLProtocol.push { req in
            return mockResponse(url: req.url!, status: 200, json: [
                "rows": [["id": 1, "total": 10, "status": "new"]],
                "next_cursor": NSNull()
            ])
        }
        let page: QueryPage<Order> = try await client.table("orders").limit(10).page()
        XCTAssertNil(page.nextCursor)
    }

    // MARK: - Writes

    func testInsert() async throws {
        MockURLProtocol.push { req in
            XCTAssertEqual(req.httpMethod, "POST")
            XCTAssertEqual(req.url?.path, "/rest/v1/orders")
            return mockResponse(url: req.url!, status: 201, json: ["ok": true, "tag": "INSERT 0 1"])
        }
        struct NewOrder: Encodable { let total: Int; let status: String }
        _ = try await client.table("orders").insert(NewOrder(total: 100, status: "new"))
    }

    func testUpdate() async throws {
        MockURLProtocol.push { req in
            XCTAssertEqual(req.httpMethod, "PATCH")
            let q = req.url?.query ?? ""
            XCTAssert(q.contains("id=eq.1"), "query was: \(q)")
            return mockResponse(url: req.url!, status: 200, json: ["ok": true, "tag": "UPDATE 1"])
        }
        struct Patch: Encodable { let status: String }
        _ = try await client.table("orders").eq("id", .int(1)).update(Patch(status: "paid"))
    }

    func testDelete() async throws {
        MockURLProtocol.push { req in
            XCTAssertEqual(req.httpMethod, "DELETE")
            return mockResponse(url: req.url!, status: 200, json: ["ok": true, "tag": "DELETE 1"])
        }
        _ = try await client.table("orders").eq("id", .int(1)).delete()
    }

    func testDelete_EngineUnsupported() async {
        MockURLProtocol.push { req in
            return mockErrorResponse(url: req.url!, status: 501,
                                     code: "E_ENGINE_UNSUPPORTED", message: "DELETE not supported")
        }
        do {
            _ = try await client.table("orders").delete()
            XCTFail("Expected BasinApiError")
        } catch let e as BasinApiError {
            XCTAssertEqual(e.code, .engineUnsupported)
            XCTAssertEqual(e.status, 501)
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
    }

    // MARK: - Bearer header

    func testBearerHeader_IsSet() async throws {
        MockURLProtocol.push { req in
            XCTAssertEqual(req.value(forHTTPHeaderField: "Authorization"), "Bearer key")
            return mockResponse(url: req.url!, status: 200, json: [])
        }
        let _: [Order] = try await client.table("orders").run()
    }
}
