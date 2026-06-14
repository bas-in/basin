/// QueryBuilder — fluent REST query builder for /rest/v1/:table.
///
/// Route source (verified): crates/basin-rest/src/server.rs:243-249 —
/// GET|POST|PATCH|DELETE /rest/v1/:table.
///
/// The filter grammar is Basin's PostgREST-style dialect (parser.rs):
/// - select=<cols,csv>
/// - <col>=<op>.<value>  ops: eq|neq|gt|gte|lt|lte|in|is
///   (in.(a,b,c) parenthesised; is.null / is.notnull)
/// - order=<col>.asc|desc, limit=N, offset=N, cursor=<token>
///
/// Response shapes (routes/data.rs):
/// - plain GET → JSON array of rows
/// - GET with limit or cursor → { rows, next_cursor }
/// - GET with ?stream=true → NDJSON, one row per line;
///   final line `{"_basin_next_cursor":"…"}` carries the pagination cursor
/// - POST → 201, { ok, tag }; PATCH/DELETE → { ok, tag }
///
/// Generic `T` must be Decodable (or use [String: AnyCodable] for schemaless).

import Foundation

// MARK: - Scalar helpers

private func literal(_ value: QueryScalar) -> String {
    switch value {
    case .null:         return "null"
    case .bool(let b):  return b ? "true" : "false"
    case .int(let i):   return "\(i)"
    case .double(let d): return "\(d)"
    case .string(let s): return s
    }
}

public enum QueryScalar {
    case null
    case bool(Bool)
    case int(Int)
    case double(Double)
    case string(String)
}

public extension QueryScalar {
    static func from(_ v: Bool)   -> QueryScalar { .bool(v) }
    static func from(_ v: Int)    -> QueryScalar { .int(v) }
    static func from(_ v: Double) -> QueryScalar { .double(v) }
    static func from(_ v: String) -> QueryScalar { .string(v) }
}

// MARK: - Internal page envelope

private struct PageEnvelope<T: Decodable>: Decodable {
    let rows: [T]
    let next_cursor: String?
}

// MARK: - NDJSON streaming helpers

/// Sentinel line the server appends as the last NDJSON line when paginating.
/// Wire field: `{"_basin_next_cursor": "..."}`.
private struct CursorSentinel: Decodable {
    let cursor: String
    enum CodingKeys: String, CodingKey {
        case cursor = "_basin_next_cursor"
    }
}

/// Actor-isolated box that receives the pagination cursor captured during
/// NDJSON streaming.  `StreamPage` holds a reference to this box.
actor CursorBox {
    private(set) var value: String?
    func set(_ cursor: String?) { value = cursor }
}

// MARK: - QueryBuilder

/// Fluent query builder for /rest/v1/:table.
///
/// Usage:
/// ```swift
/// let result = try await client.table("orders")
///     .select("id,total,status")
///     .eq("status", .string("paid"))
///     .gte("total", .int(100))
///     .order("total", ascending: false)
///     .limit(50)
///     .run() as [Order]
/// ```
public final class QueryBuilder: @unchecked Sendable {
    private let transport: HTTPTransport
    private let table: String
    private var params: [(String, String)] = []

    init(transport: HTTPTransport, table: String) {
        self.transport = transport
        self.table = table
    }

    // MARK: Projection / filters (all return self for chaining)

    /// select=<cols> projection; omit or pass "*" for all columns.
    @discardableResult
    public func select(_ columns: String = "*") -> QueryBuilder {
        params.append(("select", columns.isEmpty ? "*" : columns))
        return self
    }

    public func eq(_ column: String, _ value: QueryScalar) -> QueryBuilder {
        params.append((column, "eq.\(literal(value))")); return self
    }

    public func neq(_ column: String, _ value: QueryScalar) -> QueryBuilder {
        params.append((column, "neq.\(literal(value))")); return self
    }

    public func gt(_ column: String, _ value: QueryScalar) -> QueryBuilder {
        params.append((column, "gt.\(literal(value))")); return self
    }

    public func gte(_ column: String, _ value: QueryScalar) -> QueryBuilder {
        params.append((column, "gte.\(literal(value))")); return self
    }

    public func lt(_ column: String, _ value: QueryScalar) -> QueryBuilder {
        params.append((column, "lt.\(literal(value))")); return self
    }

    public func lte(_ column: String, _ value: QueryScalar) -> QueryBuilder {
        params.append((column, "lte.\(literal(value))")); return self
    }

    /// `<col>=in.(a,b,c)` — parenthesised list per parser.rs parse_in_list.
    public func `in`(_ column: String, _ values: [QueryScalar]) -> QueryBuilder {
        let list = values.map { literal($0) }.joined(separator: ",")
        params.append((column, "in.(\(list))")); return self
    }

    /// `<col>=is.null` / `<col>=is.notnull`.
    public func `is`(_ column: String, _ value: String) -> QueryBuilder {
        params.append((column, "is.\(value)")); return self
    }

    /// order=<col>.asc|desc (repeatable).
    public func order(_ column: String, ascending: Bool = true) -> QueryBuilder {
        params.append(("order", "\(column).\(ascending ? "asc" : "desc")")); return self
    }

    /// limit=N — switches GET response to { rows, next_cursor }.
    public func limit(_ n: Int) -> QueryBuilder {
        params.append(("limit", "\(n)")); return self
    }

    public func offset(_ n: Int) -> QueryBuilder {
        params.append(("offset", "\(n)")); return self
    }

    /// Resume keyset pagination from a next_cursor token.
    public func cursor(_ token: String) -> QueryBuilder {
        params.append(("cursor", token)); return self
    }

    // MARK: - Execution

    /// Execute as GET; decode rows as `T`.
    ///
    /// When the server returns `{ rows, next_cursor }` only the rows are
    /// returned here; use `page()` to also get the cursor.
    public func run<T: Decodable>() async throws -> [T] {
        let (data, _) = try await transport.requestRaw("GET", path: "/rest/v1/\(table)", query: params)
        return try normalizeGet(data)
    }

    /// Execute as GET and return rows + pagination cursor.
    public func page<T: Decodable>() async throws -> QueryPage<T> {
        let (data, _) = try await transport.requestRaw("GET", path: "/rest/v1/\(table)", query: params)
        return try normalizeGetPage(data)
    }

    // MARK: - NDJSON streaming (?stream=true)

    /// Execute as a streaming GET (`?stream=true`), decoding each newline-delimited
    /// JSON row as it arrives.  Rows are yielded one-by-one via `AsyncThrowingStream`
    /// without buffering the full response in memory.
    ///
    /// The server's trailing `{"_basin_next_cursor":"…"}` line is NOT yielded;
    /// it is captured and returned as the stream's termination value.
    /// Use `streamPage()` to also retrieve that cursor.
    ///
    /// Route: `GET /rest/v1/:table?stream=true&…`
    /// Server source: `crates/basin-rest/src/routes/data.rs` NDJSON branch.
    ///
    /// ```swift
    /// for try await row in client.table("events").eq("type", .string("click")).stream() as AsyncThrowingStream<Event, Error> {
    ///     process(row)
    /// }
    /// ```
    public func stream<T: Decodable>(_ type: T.Type = T.self) -> AsyncThrowingStream<T, Error> {
        let streamParams = params + [("stream", "true")]
        let transport = self.transport
        let table = self.table
        return AsyncThrowingStream { continuation in
            Task {
                do {
                    let (bytes, _) = try await transport.requestBytes(
                        "GET",
                        path: "/rest/v1/\(table)",
                        query: streamParams
                    )
                    for try await line in bytes.lines {
                        let trimmed = line.trimmingCharacters(in: .whitespaces)
                        guard !trimmed.isEmpty else { continue }
                        guard let lineData = trimmed.data(using: .utf8) else { continue }
                        // Skip the trailing cursor sentinel line — don't yield it.
                        if trimmed.hasPrefix("{\"_basin_next_cursor\"") { continue }
                        let row = try JSONDecoder().decode(T.self, from: lineData)
                        continuation.yield(row)
                    }
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
        }
    }

    /// Like `stream()`, but also captures the trailing pagination cursor emitted
    /// by the server as the final NDJSON line.
    ///
    /// Returns a `StreamPage<T>`: an `AsyncThrowingStream<T, Error>` of row values
    /// plus a `nextCursor` property that is populated once the stream is exhausted.
    ///
    /// ```swift
    /// let page = client.table("events").limit(500).streamPage() as StreamPage<Event>
    /// for try await row in page.rows { process(row) }
    /// let cursor = await page.nextCursor  // available after the loop
    /// ```
    public func streamPage<T: Decodable>(_ type: T.Type = T.self) -> StreamPage<T> {
        let streamParams = params + [("stream", "true")]
        let transport = self.transport
        let table = self.table
        let cursorBox = CursorBox()
        let rows = AsyncThrowingStream<T, Error> { continuation in
            Task {
                do {
                    let (bytes, _) = try await transport.requestBytes(
                        "GET",
                        path: "/rest/v1/\(table)",
                        query: streamParams
                    )
                    for try await line in bytes.lines {
                        let trimmed = line.trimmingCharacters(in: .whitespaces)
                        guard !trimmed.isEmpty else { continue }
                        guard let lineData = trimmed.data(using: .utf8) else { continue }
                        // Intercept the trailing cursor sentinel.
                        if trimmed.hasPrefix("{\"_basin_next_cursor\"") {
                            if let sentinel = try? JSONDecoder().decode(
                                CursorSentinel.self, from: lineData) {
                                await cursorBox.set(sentinel.cursor)
                            }
                            continue
                        }
                        let row = try JSONDecoder().decode(T.self, from: lineData)
                        continuation.yield(row)
                    }
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
        }
        return StreamPage(rows: rows, cursorBox: cursorBox)
    }

    /// POST /rest/v1/:table — insert one object or an array.
    @discardableResult
    public func insert<T: Encodable>(_ value: T) async throws -> Data {
        let body = try JSONEncoder().encode(value)
        let (data, _) = try await transport.requestRaw(
            "POST", path: "/rest/v1/\(table)",
            body: body,
            headers: ["Content-Type": "application/json"]
        )
        return data
    }

    /// PATCH /rest/v1/:table?<filters> — update rows matching the filters.
    @discardableResult
    public func update<T: Encodable>(_ value: T) async throws -> Data {
        let body = try JSONEncoder().encode(value)
        let (data, _) = try await transport.requestRaw(
            "PATCH", path: "/rest/v1/\(table)",
            query: params, body: body,
            headers: ["Content-Type": "application/json"]
        )
        return data
    }

    /// DELETE /rest/v1/:table?<filters>.
    ///
    /// May throw `BasinApiError` with code `.engineUnsupported` (501) on
    /// engines without DELETE support.
    @discardableResult
    public func delete() async throws -> Data {
        let (data, _) = try await transport.requestRaw(
            "DELETE", path: "/rest/v1/\(table)", query: params
        )
        return data
    }
}

// MARK: - Response normalisation

private func normalizeGet<T: Decodable>(_ data: Data) throws -> [T] {
    // Try array first.
    if let rows = try? JSONDecoder().decode([T].self, from: data) { return rows }
    // Try paginated envelope.
    if let page = try? JSONDecoder().decode(PageEnvelope<T>.self, from: data) { return page.rows }
    return []
}

private func normalizeGetPage<T: Decodable>(_ data: Data) throws -> QueryPage<T> {
    if let page = try? JSONDecoder().decode(PageEnvelope<T>.self, from: data) {
        return QueryPage(rows: page.rows, nextCursor: page.next_cursor)
    }
    // Plain array — no cursor.
    let rows = try JSONDecoder().decode([T].self, from: data)
    return QueryPage(rows: rows, nextCursor: nil)
}
