/// Top-level BasinClient.
///
/// `key` is a JWT or an API-key secret — both are accepted as
/// `Authorization: Bearer <key>` (crates/basin-rest/src/server.rs `authorize`,
/// JWT verify first, API-key lookup fallback).
///
/// Usage:
/// ```swift
/// let client = BasinClient(url: "https://project.basin.run", key: "my-api-key")
///
/// // Query builder
/// let orders: [Order] = try await client.table("orders")
///     .select("id,total,status")
///     .eq("status", .string("paid"))
///     .gte("total", .int(100))
///     .limit(50)
///     .run()
///
/// // RPC
/// let result = try await client.rpc("add", args: ["a": 40, "b": 2])
///
/// // Health
/// let status = try await client.health()
/// ```

import Foundation

// MARK: - BasinClient

public final class BasinClient: Sendable {
    public let auth: AuthClient
    public let functions: FunctionsClient
    public let storage: StorageClient
    public let realtime: RealtimeClient

    private let transport: HTTPTransport
    private let explicitProjectID: String?
    private let key: String?

    /// Create a Basin client.
    ///
    /// - Parameter url: Base URL of the Basin server, e.g. `"https://project.basin.run"`.
    /// - Parameter key: JWT or raw API key — passed as `Authorization: Bearer <key>`.
    /// - Parameter projectID: Project ULID. Optional when using a JWT that carries a
    ///   `project_id` claim; required for public storage URLs and realtime.
    /// - Parameter session: Optional URLSession for testing.
    public init(
        url: String,
        key: String? = nil,
        projectID: String? = nil,
        session: URLSession = .shared
    ) {
        let transport = HTTPTransport(baseURL: url, key: key, session: session)
        self.transport = transport
        self.explicitProjectID = projectID
        self.key = key

        let authClient = AuthClient(transport: transport, defaultProjectID: projectID)
        self.auth = authClient
        self.functions = FunctionsClient(transport: transport)

        // Project ID resolver — checks explicit ID first, then JWT claim from session,
        // then from the static key.
        let resolveProjectID: @Sendable () async -> String? = {
            if let p = projectID { return p }
            if let s = await authClient.getSession(),
               let p = projectIDFromJWT(s.accessToken) { return p }
            if let k = key { return projectIDFromJWT(k) }
            return nil
        }

        self.storage = StorageClient(transport: transport, projectIDGetter: resolveProjectID)
        self.realtime = RealtimeClient(
            baseURL: url,
            getToken: { await authClient.accessToken() },
            getProjectID: resolveProjectID
        )

        // Wire the transport's token getter to the auth client so every
        // request uses the freshest session token.
        Task {
            await transport.setTokenGetter({ await authClient.accessToken() })
        }
    }

    // MARK: - Convenience

    /// Return a QueryBuilder for /rest/v1/:table.
    public func table(_ name: String) -> QueryBuilder {
        QueryBuilder(transport: transport, table: name)
    }

    /// Alias matching the JS SDK's `from(table:)` style.
    public func from(table name: String) -> QueryBuilder {
        QueryBuilder(transport: transport, table: name)
    }

    /// POST /rest/v1/rpc/:name — convenience alias for functions.rpc.
    public func rpc(_ name: String, args: [String: Any] = [:]) async throws -> Any {
        try await functions.rpc(name, args: args)
    }

    /// GET /health → "ok".
    public func health() async throws -> String {
        let (data, _) = try await transport.requestRaw(
            "GET", path: "/health", auth: false
        )
        return String(data: data, encoding: .utf8) ?? ""
    }

    /// Escape hatch for routes not yet wrapped (e.g. /admin/v1/*).
    public func request(
        _ method: String,
        path: String,
        query: [(String, String)]? = nil,
        body: Data? = nil,
        headers: [String: String] = [:]
    ) async throws -> (Data, HTTPURLResponse) {
        try await transport.requestRaw(method, path: path, query: query,
                                       body: body, headers: headers)
    }
}
