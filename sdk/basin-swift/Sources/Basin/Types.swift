/// Shared wire types mirrored from the Rust handlers they bind to.
///
/// JSON field names match the server's serde output exactly via CodingKeys.
/// The server uses snake_case for auth/data types (access_expires_at, etc.)
/// and camelCase for some storage-sign fields (signedUrl, expiresAt).

import Foundation

// MARK: - Auth

/// Token body returned by /auth/v1/signin, /auth/v1/refresh,
/// /auth/v1/magic-link/consume (crates/basin-rest/src/routes/auth.rs token_body).
public struct Session: Codable, Sendable {
    public let accessToken: String
    public let refreshToken: String
    /// RFC 3339 timestamp when the access token expires.
    public let accessExpiresAt: String
    /// RFC 3339 timestamp when the refresh token expires.
    public let refreshExpiresAt: String

    enum CodingKeys: String, CodingKey {
        case accessToken      = "access_token"
        case refreshToken     = "refresh_token"
        case accessExpiresAt  = "access_expires_at"
        case refreshExpiresAt = "refresh_expires_at"
    }
}

/// POST /auth/v1/signup response.
public struct SignUpResult: Decodable, Sendable {
    public let ok: Bool
    public let userId: String

    enum CodingKeys: String, CodingKey {
        case ok
        case userId = "user_id"
    }
}

/// POST /auth/v1/api-keys response (the secret is shown exactly once).
public struct ApiKeyIssued: Decodable, Sendable {
    public let id: Int
    public let name: String
    public let secret: String
    public let createdAt: String

    enum CodingKeys: String, CodingKey {
        case id
        case name
        case secret
        case createdAt = "created_at"
    }
}

/// GET /auth/v1/api-keys array element.
public struct ApiKeyDescriptor: Decodable, Sendable {
    public let id: Int
    public let name: String
    public let createdAt: String
    public let lastUsedAt: String?
    public let revokedAt: String?

    enum CodingKeys: String, CodingKey {
        case id
        case name
        case createdAt  = "created_at"
        case lastUsedAt = "last_used_at"
        case revokedAt  = "revoked_at"
    }
}

// MARK: - OAuth

/// GET /auth/v1/oauth/:provider/authorize response.
public struct OAuthAuthorizeResult: Decodable, Sendable {
    /// Full provider authorize URL — redirect the browser here.
    public let redirectUrl: String
    /// CSRF state value included in redirect_url; echoed back on callback.
    public let state: String

    enum CodingKeys: String, CodingKey {
        case redirectUrl = "redirect_url"
        case state
    }
}

// MARK: - MFA

/// POST /auth/v1/factors response for factor_type="totp".
public struct TotpEnrollResult: Decodable, Sendable {
    public let factorId: String
    public let factorType: String
    /// Base-32 TOTP secret — display in a QR code or give to an auth app.
    public let secretB32: String
    /// otpauth://totp/... URI suitable for QR code display.
    public let otpauthUri: String

    enum CodingKeys: String, CodingKey {
        case factorId    = "factor_id"
        case factorType  = "factor_type"
        case secretB32   = "secret_b32"
        case otpauthUri  = "otpauth_uri"
    }
}

/// POST /auth/v1/factors response for factor_type="webauthn".
public struct WebAuthnEnrollResult: Decodable, Sendable {
    public let factorId: String
    public let factorType: String
    public let challengeId: String
    /// Serialised PublicKeyCredentialCreationOptions JSON.
    public let creationOptionsJson: String

    enum CodingKeys: String, CodingKey {
        case factorId           = "factor_id"
        case factorType         = "factor_type"
        case challengeId        = "challenge_id"
        case creationOptionsJson = "creation_options_json"
    }
}

/// Union result of POST /auth/v1/factors.
public enum MfaEnrollResult: Sendable {
    case totp(TotpEnrollResult)
    case webAuthn(WebAuthnEnrollResult)
}

/// Element of GET /auth/v1/factors array.
public struct FactorDescriptor: Decodable, Sendable {
    public let id: String
    public let factorType: String  // "totp" | "webauthn"
    public let status: String      // "unverified" | "verified"
    public let friendlyName: String
    public let createdAt: String
    public let updatedAt: String

    enum CodingKeys: String, CodingKey {
        case id
        case factorType  = "factor_type"
        case status
        case friendlyName = "friendly_name"
        case createdAt   = "created_at"
        case updatedAt   = "updated_at"
    }
}

/// POST /auth/v1/factors/:id/verify response.
///
/// `recoveryCodes` is only present on the **first** verified factor for a
/// user — store these securely; they are shown exactly once.
public struct VerifyFactorResult: Decodable, Sendable {
    public let ok: Bool
    public let recoveryCodes: [String]?

    enum CodingKeys: String, CodingKey {
        case ok
        case recoveryCodes = "recovery_codes"
    }
}

/// POST /auth/v1/factors/:id/challenge response for TOTP.
public struct TotpChallengeResult: Decodable, Sendable {
    public let challengeId: String

    enum CodingKeys: String, CodingKey {
        case challengeId = "challenge_id"
    }
}

/// POST /auth/v1/factors/:id/challenge response for WebAuthn.
public struct WebAuthnChallengeResult: Decodable, Sendable {
    public let challengeId: String
    /// Serialised PublicKeyCredentialRequestOptions JSON; present for WebAuthn factors.
    public let requestOptionsJson: String?

    enum CodingKeys: String, CodingKey {
        case challengeId      = "challenge_id"
        case requestOptionsJson = "request_options_json"
    }
}

/// Union result of POST /auth/v1/factors/:id/challenge.
public enum MfaChallengeResult: Sendable {
    case totp(TotpChallengeResult)
    case webAuthn(WebAuthnChallengeResult)

    public var challengeId: String {
        switch self {
        case .totp(let r): return r.challengeId
        case .webAuthn(let r): return r.challengeId
        }
    }
}

// MARK: - Data / query

/// Non-row result for writes: { ok: true, tag: "..." }.
public struct ExecTag: Decodable, Sendable {
    public let ok: Bool
    public let tag: String
}

/// Wrapped GET response when limit or cursor is supplied.
public struct QueryPage<T: Decodable & Sendable>: Sendable {
    public let rows: [T]
    public let nextCursor: String?
}

// MARK: - Storage

/// Object bucket metadata.
public struct Bucket: Decodable, Sendable {
    public let id: String
    public let name: String
    public let `public`: Bool
    public let fileSizeLimit: Int?
    public let allowedMimeTypes: [String]
    public let createdAt: String
    public let updatedAt: String

    enum CodingKeys: String, CodingKey {
        case id
        case name
        case `public`        = "public"
        case fileSizeLimit   = "file_size_limit"
        case allowedMimeTypes = "allowed_mime_types"
        case createdAt       = "created_at"
        case updatedAt       = "updated_at"
    }
}

/// Storage object metadata.
public struct StorageObject: Decodable, Sendable {
    public let id: String
    public let bucketId: String
    public let path: String
    public let size: Int
    public let mimeType: String?
    public let owner: String?
    public let etag: String
    public let createdAt: String
    public let updatedAt: String

    enum CodingKeys: String, CodingKey {
        case id
        case bucketId  = "bucket_id"
        case path
        case size
        case mimeType  = "mime_type"
        case owner
        case etag
        case createdAt = "created_at"
        case updatedAt = "updated_at"
    }
}

/// Signed URL returned by POST /storage/v1/object/sign/upload/:bucket/*path.
///
/// The server uses camelCase (`signedUrl`, `expiresAt`) for the storage-sign
/// response — CodingKeys reflect this exactly.
public struct SignedUrl: Decodable, Sendable {
    /// Server-relative signed URL.
    public let signedUrl: String
    /// RFC 3339 expiry timestamp.
    public let expiresAt: String
    /// Absolute URL (base + signedUrl) — computed at decode time.
    public let absoluteUrl: String

    enum CodingKeys: String, CodingKey {
        case signedUrl
        case expiresAt
    }

    // Custom init so we can inject absoluteUrl without it being on the wire.
    public init(signedUrl: String, expiresAt: String, absoluteUrl: String) {
        self.signedUrl = signedUrl
        self.expiresAt = expiresAt
        self.absoluteUrl = absoluteUrl
    }

    public init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        signedUrl = try c.decode(String.self, forKey: .signedUrl)
        expiresAt = try c.decode(String.self, forKey: .expiresAt)
        // absoluteUrl is filled in by SignedUrl.make(from:baseURL:).
        absoluteUrl = ""
    }

    static func make(from decoder: Decoder, baseURL: String) throws -> SignedUrl {
        let v = try SignedUrl(from: decoder)
        return SignedUrl(signedUrl: v.signedUrl,
                        expiresAt: v.expiresAt,
                        absoluteUrl: baseURL + v.signedUrl)
    }
}

/// Downloaded object payload.
public struct DownloadResult: Sendable {
    public let data: Data
    public let contentType: String?
    public let etag: String?
}

// MARK: - Functions

/// Proxied response from ANY /fn/v1/:name.
public struct FunctionInvokeResult: @unchecked Sendable {
    public let status: Int
    public let headers: [String: String]
    /// JSON-decoded body when Content-Type is application/json, else raw UTF-8 string.
    public let data: Any
}

// MARK: - Realtime wire frames

public typealias ChangeOp = String  // "INSERT" | "UPDATE" | "DELETE"

/// {"type":"event",...} server frame.
public struct RealtimeEvent: Decodable, Sendable {
    public let type: String
    public let project: String
    public let table: String
    public let op: ChangeOp
    public let seq: Int
    public let before: [String: AnyCodable]?
    public let after: [String: AnyCodable]?
}

/// {"type":"error",...} protocol error for a table.
public struct RealtimeErrorFrame: Decodable, Sendable {
    public let type: String
    public let code: String
    public let table: String
    public let missed: Int?
}

/// {"type":"gap",...} reconnect-resume gap; client must cold re-sync.
public struct RealtimeGapFrame: Decodable, Sendable {
    public let type: String
    public let table: String
    public let lastEventId: Int
    public let oldestInRing: Int
    public let newestInRing: Int

    enum CodingKeys: String, CodingKey {
        case type
        case table
        case lastEventId   = "last_event_id"
        case oldestInRing  = "oldest_in_ring"
        case newestInRing  = "newest_in_ring"
    }
}

/// {"type":"subscribed","table":...}
public struct SubscribedFrame: Decodable, Sendable {
    public let type: String
    public let table: String
}

/// {"type":"unsubscribed","table":...}
public struct UnsubscribedFrame: Decodable, Sendable {
    public let type: String
    public let table: String
}

// MARK: - Presence frames

public struct PresenceEntry: Decodable, Sendable {
    public let clientId: String
    public let metadata: AnyCodable?
    public let joinedAt: String?

    enum CodingKeys: String, CodingKey {
        case clientId  = "client_id"
        case metadata
        case joinedAt  = "joined_at"
    }
}

/// {"type":"presence_state","channel":...,"presences":[...]}
public struct PresenceStateFrame: Decodable, Sendable {
    public let type: String
    public let channel: String
    public let presences: [PresenceEntry]
}

/// {"type":"presence_diff","channel":...,"joins":[...],"leaves":[...]}
public struct PresenceDiffFrame: Decodable, Sendable {
    public let type: String
    public let channel: String
    public let joins: [PresenceEntry]
    public let leaves: [PresenceEntry]
}

/// {"type":"presenceerror",...}
/// Note: the server serialises with rename_all="lowercase" → wire type is
/// "presenceerror" (no underscore). CodingKeys reflect the exact wire names.
public struct PresenceErrorFrame: Decodable, Sendable {
    public let type: String   // "presenceerror"
    public let code: String
    public let channel: String
    public let message: String
}

/// All possible server→client realtime frame types.
public enum ServerFrame: Sendable {
    case event(RealtimeEvent)
    case error(RealtimeErrorFrame)
    case gap(RealtimeGapFrame)
    case subscribed(SubscribedFrame)
    case unsubscribed(UnsubscribedFrame)
    case presenceState(PresenceStateFrame)
    case presenceDiff(PresenceDiffFrame)
    case presenceError(PresenceErrorFrame)
}

// MARK: - AnyCodable helper

/// Type-erased Codable wrapper for heterogeneous JSON values.
public struct AnyCodable: Codable, @unchecked Sendable {
    public let value: Any

    public init(_ value: Any) {
        self.value = value
    }

    public init(from decoder: Decoder) throws {
        let c = try decoder.singleValueContainer()
        if let v = try? c.decode(Bool.self)   { value = v; return }
        if let v = try? c.decode(Int.self)    { value = v; return }
        if let v = try? c.decode(Double.self) { value = v; return }
        if let v = try? c.decode(String.self) { value = v; return }
        if let v = try? c.decode([String: AnyCodable].self) { value = v.mapValues { $0.value }; return }
        if let v = try? c.decode([AnyCodable].self) { value = v.map { $0.value }; return }
        if c.decodeNil() { value = Optional<Any>.none as Any; return }
        throw DecodingError.typeMismatch(AnyCodable.self,
            .init(codingPath: decoder.codingPath, debugDescription: "Unsupported JSON value"))
    }

    public func encode(to encoder: Encoder) throws {
        var c = encoder.singleValueContainer()
        switch value {
        case let v as Bool:   try c.encode(v)
        case let v as Int:    try c.encode(v)
        case let v as Double: try c.encode(v)
        case let v as String: try c.encode(v)
        case let v as [String: Any]:
            try c.encode(v.mapValues { AnyCodable($0) })
        case let v as [Any]:
            try c.encode(v.map { AnyCodable($0) })
        default:
            try c.encodeNil()
        }
    }
}
