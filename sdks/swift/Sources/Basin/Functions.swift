/// Functions client — Basin's two function-invocation surfaces.
///
/// Route sources (verified):
/// - ANY  /fn/v1/:name          crates/basin-rest/src/server.rs:238,
///   handler crates/basin-rest/src/routes/fn_handler.rs (any_fn_handler) —
///   HTTP-handler-shape Wasm/JS functions; the guest's response (status,
///   headers, body) is proxied back verbatim.
/// - POST /rest/v1/rpc/:fn_name crates/basin-rest/src/server.rs:236,
///   handler crates/basin-rest/src/routes/rpc.rs (post_rpc) — catalog SQL /
///   Wasm UDFs. Body is a JSON object of named args; scalar results unwrap to
///   the bare value, table results return an array of row objects.

import Foundation

// MARK: - FunctionsClient

public actor FunctionsClient {
    private let transport: HTTPTransport

    init(transport: HTTPTransport) {
        self.transport = transport
    }

    /// ANY /fn/v1/:name — invoke an HTTP-handler function.
    ///
    /// The function's response is proxied verbatim, so non-2xx statuses are
    /// NOT automatically errors — unless the body is Basin's own error envelope
    /// (auth 401, unknown function 404, invocation failure 500), which raises
    /// BasinApiError as usual.
    ///
    /// - Parameter name: Function name registered in the catalog.
    /// - Parameter method: HTTP method (default POST).
    /// - Parameter body: Optional request body (JSON-serialisable or raw Data).
    /// - Parameter headers: Optional extra headers.
    public func invoke(
        _ name: String,
        method: String = "POST",
        body: Data? = nil,
        headers: [String: String] = [:]
    ) async throws -> FunctionInvokeResult {
        let token = await transport.bearer()
        var hdrs = headers
        if let tok = token { hdrs["Authorization"] = "Bearer \(tok)" }

        let url = buildURL(base: transport.baseURL,
                           path: "/fn/v1/\(percentEncode(name))", query: nil)
        var req = URLRequest(url: url)
        req.httpMethod = method
        req.httpBody = body
        for (k, v) in hdrs { req.setValue(v, forHTTPHeaderField: k) }

        do {
            let (data, response) = try await URLSession.shared.data(for: req)
            guard let http = response as? HTTPURLResponse else {
                throw BasinNetworkError(URLError(.badServerResponse))
            }
            let ct = http.value(forHTTPHeaderField: "Content-Type") ?? ""
            var result: Any = data
            if ct.contains("json"), !data.isEmpty,
               let json = try? JSONSerialization.jsonObject(with: data) {
                result = json
                // Surface Basin-layer errors (not the function's own responses).
                if !(200..<300).contains(http.statusCode),
                   let d = result as? [String: Any],
                   let code = d["code"] as? String, code.hasPrefix("E_"),
                   let msg = d["message"] as? String {
                    throw BasinApiError(rawCode: code, message: msg, status: http.statusCode,
                                        sqlState: d["sqlstate"] as? String)
                }
            }
            var allHeaders: [String: String] = [:]
            if let httpResp = response as? HTTPURLResponse {
                for (k, v) in httpResp.allHeaderFields {
                    if let key = k as? String, let val = v as? String {
                        allHeaders[key] = val
                    }
                }
            }
            return FunctionInvokeResult(status: http.statusCode, headers: allHeaders, data: result)
        } catch let e as BasinApiError { throw e }
          catch let e as BasinNetworkError { throw e }
          catch { throw BasinNetworkError(error) }
    }

    /// POST /rest/v1/rpc/:fn_name — call a catalog UDF.
    ///
    /// Args are named; missing declared args become NULL.
    /// Returns the raw decoded JSON object (scalar or array of rows).
    public func rpc(_ name: String, args: [String: Any] = [:]) async throws -> Any {
        let body = try JSONSerialization.data(withJSONObject: args)
        let (data, _) = try await transport.requestRaw(
            "POST",
            path: "/rest/v1/rpc/\(percentEncode(name))",
            body: body,
            headers: ["Content-Type": "application/json"]
        )
        if data.isEmpty { return Optional<Any>.none as Any }
        return try JSONSerialization.jsonObject(with: data, options: [.fragmentsAllowed])
    }
}
