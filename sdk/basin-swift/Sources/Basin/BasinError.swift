/// Typed errors mirroring basin-rest's error envelope.
///
/// Every failed request returns {"code": "...", "message": "..."} with an
/// appropriate HTTP status (crates/basin-rest/src/errors.rs).  `code` is the
/// stable contract; `message` is human-readable and must not be matched on.

import Foundation

// Stable error codes emitted by basin-rest (errors.rs ErrorCode::as_str).
public enum BasinErrorCode: String {
    case unauthenticated = "E_UNAUTHENTICATED"
    case forbidden       = "E_FORBIDDEN"
    case notFound        = "E_NOT_FOUND"
    case invalidRequest  = "E_INVALID_REQUEST"
    case rateLimited     = "E_RATE_LIMITED"
    case engineUnsupported = "E_ENGINE_UNSUPPORTED"
    case `internal`      = "E_INTERNAL"
    case emailDisabled   = "E_EMAIL_DISABLED"
    case revokedToken    = "E_REVOKED_TOKEN"
    case unknown         = "E_UNKNOWN"
}

/// Raised for any non-2xx response from a Basin server.
///
/// - `code`: Stable error code from the response body (`E_*`). Unknown codes
///   from a newer server pass through via `rawCode`.
/// - `status`: HTTP status of the response (0 when synthesised client-side).
/// - `message`: Human-readable detail — do **not** match on this; use `code`.
public struct BasinApiError: Error, CustomStringConvertible {
    /// Decoded stable code, or `.unknown` for unrecognised strings.
    public let code: BasinErrorCode
    /// Raw string code from the wire (preserved even for unknown codes).
    public let rawCode: String
    /// HTTP status code.
    public let status: Int
    /// Human-readable message.
    public let message: String

    public init(rawCode: String, message: String, status: Int) {
        self.rawCode = rawCode
        self.code = BasinErrorCode(rawValue: rawCode) ?? .unknown
        self.status = status
        self.message = message
    }

    public var description: String {
        "BasinApiError(code=\(rawCode), status=\(status), message=\(message))"
    }
}

/// Raised when the transport layer fails before a server response is received
/// (connection refused, timeout, etc.).
public struct BasinNetworkError: Error, CustomStringConvertible {
    public let underlying: Error

    public init(_ underlying: Error) {
        self.underlying = underlying
    }

    public var description: String {
        "BasinNetworkError: \(underlying)"
    }
}

// MARK: - Wire envelope decode

struct ErrorEnvelope: Decodable {
    let code: String
    let message: String
}

extension BasinApiError {
    static func fromResponseBody(_ body: Data, status: Int) -> BasinApiError {
        if let envelope = try? JSONDecoder().decode(ErrorEnvelope.self, from: body) {
            return BasinApiError(rawCode: envelope.code, message: envelope.message, status: status)
        }
        let text = String(data: body, encoding: .utf8) ?? "HTTP \(status)"
        return BasinApiError(rawCode: "E_UNKNOWN", message: text, status: status)
    }
}
