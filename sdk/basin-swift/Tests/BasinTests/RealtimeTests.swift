import XCTest
@testable import Basin

/// Realtime frame parsing and decode tests.
/// Full WebSocket integration requires a live server — these tests cover the
/// frame decode path (parseFrame) and the error/gap/presence frame structs.
final class RealtimeTests: XCTestCase {

    // MARK: - Frame decode helpers

    private func assertDecodes<T: Decodable>(_ json: String, as _: T.Type) throws -> T {
        try JSONDecoder().decode(T.self, from: json.data(using: .utf8)!)
    }

    // MARK: RealtimeEvent

    func testDecodeEventFrame() throws {
        let json = """
        {
          "type": "event",
          "project": "01JTEST",
          "table": "orders",
          "op": "INSERT",
          "seq": 42,
          "before": null,
          "after": {"id": 1, "total": 100}
        }
        """
        let event = try assertDecodes(json, as: RealtimeEvent.self)
        XCTAssertEqual(event.type, "event")
        XCTAssertEqual(event.table, "orders")
        XCTAssertEqual(event.op, "INSERT")
        XCTAssertEqual(event.seq, 42)
        XCTAssertNil(event.before)
        XCTAssertNotNil(event.after)
    }

    func testDecodeEventFrame_Update() throws {
        let json = """
        {
          "type": "event", "project": "p", "table": "orders",
          "op": "UPDATE", "seq": 99,
          "before": {"id": 1, "status": "new"},
          "after":  {"id": 1, "status": "paid"}
        }
        """
        let event = try assertDecodes(json, as: RealtimeEvent.self)
        XCTAssertEqual(event.op, "UPDATE")
        XCTAssertNotNil(event.before)
        XCTAssertNotNil(event.after)
    }

    // MARK: RealtimeErrorFrame

    func testDecodeErrorFrame() throws {
        let json = """
        {"type":"error","code":"lag","table":"orders","missed":5}
        """
        let frame = try assertDecodes(json, as: RealtimeErrorFrame.self)
        XCTAssertEqual(frame.code, "lag")
        XCTAssertEqual(frame.missed, 5)
    }

    func testDecodeErrorFrame_InvalidFilter() throws {
        let json = """
        {"type":"error","code":"invalid_filter","table":"orders","missed":null}
        """
        let frame = try assertDecodes(json, as: RealtimeErrorFrame.self)
        XCTAssertEqual(frame.code, "invalid_filter")
        XCTAssertNil(frame.missed)
    }

    // MARK: RealtimeGapFrame

    func testDecodeGapFrame() throws {
        let json = """
        {
          "type": "gap",
          "table": "events",
          "last_event_id": 100,
          "oldest_in_ring": 200,
          "newest_in_ring": 300
        }
        """
        let frame = try assertDecodes(json, as: RealtimeGapFrame.self)
        XCTAssertEqual(frame.lastEventId, 100)
        XCTAssertEqual(frame.oldestInRing, 200)
        XCTAssertEqual(frame.newestInRing, 300)
    }

    // MARK: SubscribedFrame / UnsubscribedFrame

    func testDecodeSubscribedFrame() throws {
        let json = """{"type":"subscribed","table":"orders"}"""
        let frame = try assertDecodes(json, as: SubscribedFrame.self)
        XCTAssertEqual(frame.table, "orders")
    }

    func testDecodeUnsubscribedFrame() throws {
        let json = """{"type":"unsubscribed","table":"orders"}"""
        let frame = try assertDecodes(json, as: UnsubscribedFrame.self)
        XCTAssertEqual(frame.table, "orders")
    }

    // MARK: Presence frames

    func testDecodePresenceStateFrame() throws {
        let json = """
        {
          "type": "presence_state",
          "channel": "room:1",
          "presences": [
            {"client_id": "user-abc", "metadata": {"name": "Alice"}, "joined_at": "2024-01-01T00:00:00Z"}
          ]
        }
        """
        let frame = try assertDecodes(json, as: PresenceStateFrame.self)
        XCTAssertEqual(frame.channel, "room:1")
        XCTAssertEqual(frame.presences.count, 1)
        XCTAssertEqual(frame.presences[0].clientId, "user-abc")
    }

    func testDecodePresenceDiffFrame() throws {
        let json = """
        {
          "type": "presence_diff",
          "channel": "room:1",
          "joins": [{"client_id": "u1"}],
          "leaves": [{"client_id": "u2"}]
        }
        """
        let frame = try assertDecodes(json, as: PresenceDiffFrame.self)
        XCTAssertEqual(frame.joins.count, 1)
        XCTAssertEqual(frame.leaves.count, 1)
        XCTAssertEqual(frame.leaves[0].clientId, "u2")
    }

    func testDecodePresenceErrorFrame_NoUnderscore() throws {
        // Wire type is "presenceerror" (no underscore) — rename_all="lowercase" in Rust.
        let json = """
        {
          "type": "presenceerror",
          "code": "identity_mismatch",
          "channel": "room:1",
          "message": "client_id does not match JWT identity"
        }
        """
        let frame = try assertDecodes(json, as: PresenceErrorFrame.self)
        XCTAssertEqual(frame.type, "presenceerror")  // no underscore
        XCTAssertEqual(frame.code, "identity_mismatch")
    }

    // MARK: AnyCodable

    func testAnyCodable_NestedObject() throws {
        let json = """
        {"id": 1, "tags": ["a", "b"], "meta": {"k": true}}
        """
        let dict = try JSONDecoder().decode([String: AnyCodable].self,
                                            from: json.data(using: .utf8)!)
        XCTAssertEqual(dict["id"]?.value as? Int, 1)
        if let tags = dict["tags"]?.value as? [Any] {
            XCTAssertEqual(tags.count, 2)
        } else {
            XCTFail("tags should be array")
        }
    }

    // MARK: - Error decode

    func testErrorCode_Unknown_PassesThrough() throws {
        let data = """{"code":"E_NEW_FUTURE_CODE","message":"from new server"}""".data(using: .utf8)!
        let err = BasinApiError.fromResponseBody(data, status: 422)
        XCTAssertEqual(err.rawCode, "E_NEW_FUTURE_CODE")
        XCTAssertEqual(err.code, .unknown)
    }

    func testErrorCode_MissingBody_Fallback() throws {
        let err = BasinApiError.fromResponseBody(Data(), status: 503)
        XCTAssertEqual(err.rawCode, "E_UNKNOWN")
        XCTAssertEqual(err.status, 503)
    }
}
