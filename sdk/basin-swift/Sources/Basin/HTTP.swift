/// HTTP transport layer — URLSession-backed, Bearer-token injection,
/// error-envelope decoding → BasinApiError, URL building with typed query pairs.

import Foundation

// MARK: - URL building

func buildURL(base: String, path: String, query: [(String, String)]?) -> URL {
    var s = base.trimmingCharacters(in: CharacterSet(charactersIn: "/"))
    s += path
    if let q = query, !q.isEmpty {
        var comps = URLComponents(string: s)!
        comps.queryItems = q.map { URLQueryItem(name: $0.0, value: $0.1) }
        return comps.url!
    }
    return URL(string: s)!
}

func percentEncode(_ segment: String) -> String {
    segment.addingPercentEncoding(withAllowedCharacters: .urlPathAllowed
        .subtracting(CharacterSet(charactersIn: "/:"))) ?? segment
}

func encodeObjectPath(_ path: String) -> String {
    path.split(separator: "/", omittingEmptySubsequences: false)
        .map { percentEncode(String($0)) }
        .joined(separator: "/")
}

// MARK: - Response helpers

func raiseForResponse(data: Data, status: Int) throws {
    throw BasinApiError.fromResponseBody(data, status: status)
}

func decodeJSON<T: Decodable>(_ data: Data) throws -> T {
    do {
        return try JSONDecoder().decode(T.self, from: data)
    } catch {
        throw error
    }
}

func decodeAnyJSON(_ data: Data) throws -> Any {
    try JSONSerialization.jsonObject(with: data, options: [.fragmentsAllowed])
}

// MARK: - HTTPTransport actor

/// Actor-isolated HTTP transport used by all sub-clients.
///
/// `getToken` is called before every authenticated request.  When it returns
/// `nil` the static `key` is used.  The function may be replaced after
/// construction (auth wires it in after the transport is created).
actor HTTPTransport {
    nonisolated let baseURL: String
    private let key: String?
    private let session: URLSession
    var getToken: (@Sendable () async -> String?)?

    init(baseURL: String, key: String? = nil, session: URLSession = .shared) {
        self.baseURL = baseURL.trimmingCharacters(in: CharacterSet(charactersIn: "/"))
        self.key = key
        self.session = session
    }

    func setTokenGetter(_ fn: @escaping @Sendable () async -> String?) {
        self.getToken = fn
    }

    // MARK: Bearer token

    func bearer() async -> String? {
        if let fn = getToken {
            if let tok = await fn() { return tok }
        }
        return key
    }

    // MARK: Core request

    /// Execute a request and return (Data, HTTPURLResponse).
    func request(
        _ method: String,
        path: String,
        query: [(String, String)]? = nil,
        body: Data? = nil,
        headers: [String: String] = [:],
        auth: Bool = true
    ) async throws -> (Data, HTTPURLResponse) {
        let url = buildURL(base: baseURL, path: path, query: query)
        var req = URLRequest(url: url)
        req.httpMethod = method
        req.httpBody = body

        if auth, let tok = await bearer() {
            req.setValue("Bearer \(tok)", forHTTPHeaderField: "Authorization")
        }
        for (k, v) in headers {
            req.setValue(v, forHTTPHeaderField: k)
        }

        do {
            let (data, response) = try await session.data(for: req)
            guard let http = response as? HTTPURLResponse else {
                throw BasinNetworkError(URLError(.badServerResponse))
            }
            if !(200..<300).contains(http.statusCode) {
                throw BasinApiError.fromResponseBody(data, status: http.statusCode)
            }
            return (data, http)
        } catch let e as BasinApiError {
            throw e
        } catch let e as BasinNetworkError {
            throw e
        } catch {
            throw BasinNetworkError(error)
        }
    }

    // MARK: JSON convenience

    /// Execute a request with a JSON body and return the decoded JSON response.
    func requestJSON<Body: Encodable, Response: Decodable>(
        _ method: String,
        path: String,
        query: [(String, String)]? = nil,
        body: Body? = nil,
        auth: Bool = true
    ) async throws -> Response {
        var bodyData: Data?
        var hdrs: [String: String] = [:]
        if let b = body {
            bodyData = try JSONEncoder().encode(b)
            hdrs["Content-Type"] = "application/json"
        }
        let (data, _) = try await request(method, path: path, query: query,
                                          body: bodyData, headers: hdrs, auth: auth)
        if data.isEmpty { throw BasinApiError(rawCode: "E_UNKNOWN", message: "empty response", status: 0) }
        return try decodeJSON(data)
    }

    /// Overload for requests with no body.
    func requestJSON<Response: Decodable>(
        _ method: String,
        path: String,
        query: [(String, String)]? = nil,
        auth: Bool = true
    ) async throws -> Response {
        let (data, _) = try await request(method, path: path, query: query, auth: auth)
        if data.isEmpty { throw BasinApiError(rawCode: "E_UNKNOWN", message: "empty response", status: 0) }
        return try decodeJSON(data)
    }

    /// Execute returning raw Data (for binary responses).
    func requestRaw(
        _ method: String,
        path: String,
        query: [(String, String)]? = nil,
        body: Data? = nil,
        headers: [String: String] = [:],
        auth: Bool = true
    ) async throws -> (Data, HTTPURLResponse) {
        try await request(method, path: path, query: query,
                          body: body, headers: headers, auth: auth)
    }

    /// Execute returning an `AsyncBytes` stream for incremental line reading (NDJSON).
    ///
    /// Validates the status code eagerly (reads the first bytes to detect an error
    /// envelope), then hands off the byte stream to the caller for line iteration.
    /// Available on macOS 12+ / iOS 15+ (URLSession.bytes).
    func requestBytes(
        _ method: String,
        path: String,
        query: [(String, String)]? = nil,
        headers: [String: String] = [:],
        auth: Bool = true
    ) async throws -> (URLSession.AsyncBytes, HTTPURLResponse) {
        let url = buildURL(base: baseURL, path: path, query: query)
        var req = URLRequest(url: url)
        req.httpMethod = method
        if auth, let tok = await bearer() {
            req.setValue("Bearer \(tok)", forHTTPHeaderField: "Authorization")
        }
        for (k, v) in headers {
            req.setValue(v, forHTTPHeaderField: k)
        }
        do {
            let (bytes, response) = try await session.bytes(for: req)
            guard let http = response as? HTTPURLResponse else {
                throw BasinNetworkError(URLError(.badServerResponse))
            }
            if !(200..<300).contains(http.statusCode) {
                // Materialize the body so we can decode the error envelope.
                var buf = Data()
                for try await byte in bytes { buf.append(byte) }
                throw BasinApiError.fromResponseBody(buf, status: http.statusCode)
            }
            return (bytes, http)
        } catch let e as BasinApiError {
            throw e
        } catch let e as BasinNetworkError {
            throw e
        } catch {
            throw BasinNetworkError(error)
        }
    }

    /// POST/PATCH/DELETE with JSON body, ignore response body (204 or { ok, tag }).
    @discardableResult
    func requestJSONVoid<Body: Encodable>(
        _ method: String,
        path: String,
        query: [(String, String)]? = nil,
        body: Body,
        auth: Bool = true
    ) async throws -> Data {
        let bodyData = try JSONEncoder().encode(body)
        let hdrs: [String: String] = ["Content-Type": "application/json"]
        let (data, _) = try await request(method, path: path, query: query,
                                          body: bodyData, headers: hdrs, auth: auth)
        return data
    }

    /// Overload for DELETE/etc with no request body.
    @discardableResult
    func requestJSONVoid(
        _ method: String,
        path: String,
        query: [(String, String)]? = nil,
        auth: Bool = true
    ) async throws -> Data {
        let (data, _) = try await request(method, path: path, query: query, auth: auth)
        return data
    }
}

// MARK: - Encodable dictionary shim

/// Thin wrapper so we can pass `[String: Any]` as `Encodable` where needed.
struct JSONObject: Encodable {
    let dict: [String: Any]

    func encode(to encoder: Encoder) throws {
        var c = encoder.container(keyedBy: RawKey.self)
        for (key, value) in dict {
            let k = RawKey(stringValue: key)!
            switch value {
            case let v as Bool:   try c.encode(v, forKey: k)
            case let v as Int:    try c.encode(v, forKey: k)
            case let v as Double: try c.encode(v, forKey: k)
            case let v as String: try c.encode(v, forKey: k)
            case let v as [String]:  try c.encode(v, forKey: k)
            case let v as Int?:   try c.encode(v, forKey: k)
            case let v as [String: Any]:
                try c.encode(JSONObject(dict: v), forKey: k)
            default:
                try c.encodeNil(forKey: k)
            }
        }
    }

    struct RawKey: CodingKey {
        var stringValue: String
        var intValue: Int? { nil }
        init?(stringValue: String) { self.stringValue = stringValue }
        init?(intValue: Int) { nil }
    }
}
