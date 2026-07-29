/// Auth client — bindings for /auth/v1/*.
///
/// Route sources (verified against crates/basin-rest/src/server.rs):
/// - POST /auth/v1/signup                              server.rs:250
/// - POST /auth/v1/signin                              server.rs:251
/// - POST /auth/v1/refresh                             server.rs:252
/// - POST /auth/v1/signout                             server.rs:253
/// - POST /auth/v1/verify-email                        server.rs:254
/// - POST /auth/v1/reset-password                      server.rs:255
/// - POST /auth/v1/request-password-reset              server.rs:256
/// - POST /auth/v1/magic-link                          server.rs:262
/// - POST /auth/v1/magic-link/consume                  server.rs:263
/// - POST|GET /auth/v1/api-keys                        server.rs:267
/// - DELETE /auth/v1/api-keys/:id                      server.rs:271
/// - GET /auth/v1/oauth/:provider/authorize            server.rs:277
/// - POST|GET /auth/v1/factors                         server.rs:286-287
/// - POST /auth/v1/factors/:id/verify                  server.rs:290
/// - POST /auth/v1/factors/:id/challenge               server.rs:294
/// - POST /auth/v1/factors/:id/challenge/verify        server.rs:298
/// - DELETE /auth/v1/factors/:id                       server.rs:302
///
/// Auto-refresh: `accessToken()` checks `access_expires_at` and calls
/// `refreshSession()` if the token expires within 10 seconds. Actor isolation
/// ensures concurrent callers coalesce (only one refresh in flight).

import Foundation

// Refresh this many seconds before the access token's stated expiry.
private let expirySkewSeconds: TimeInterval = 10

// MARK: - JWT project_id extraction

/// Decode the `project_id` claim from a Basin JWT payload segment.
/// Returns nil for non-JWT tokens (e.g. raw API keys).
func projectIDFromJWT(_ token: String) -> String? {
    let parts = token.split(separator: ".", omittingEmptySubsequences: false)
    guard parts.count == 3 else { return nil }
    var b64 = String(parts[1])
        .replacingOccurrences(of: "-", with: "+")
        .replacingOccurrences(of: "_", with: "/")
    // Add padding
    let remainder = b64.count % 4
    if remainder != 0 { b64 += String(repeating: "=", count: 4 - remainder) }
    guard let data = Data(base64Encoded: b64),
          let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
          let pid = json["project_id"] as? String else { return nil }
    return pid
}

// MARK: - AuthClient

/// Actor-isolated auth client for /auth/v1/* routes.
///
/// Obtain via `BasinClient.auth`.
public actor AuthClient {

    private let transport: HTTPTransport
    private let defaultProjectID: String?
    private var _session: Session?

    init(transport: HTTPTransport, defaultProjectID: String?) {
        self.transport = transport
        self.defaultProjectID = defaultProjectID
    }

    // MARK: Session management

    /// Return the current session, or nil when signed out.
    public func getSession() -> Session? { _session }

    /// Adopt an externally obtained token pair (e.g. restored from Keychain).
    public func setSession(_ session: Session?) { _session = session }

    /// Return the project ID for auth calls.
    func resolveProjectID(_ override: String?) async throws -> String {
        if let p = override { return p }
        if let p = defaultProjectID { return p }
        // Try to extract from the static key.
        if let key = await transport.bearer(), let p = projectIDFromJWT(key) { return p }
        throw BasinApiError(rawCode: "E_INVALID_REQUEST",
                            message: "project id required: pass projectID to BasinClient",
                            status: 0)
    }

    /// Return a valid access token, auto-refreshing if near expiry.
    /// Returns nil when there is no session (callers fall back to the static key).
    public func accessToken() async -> String? {
        guard let s = _session else { return nil }
        if let expiry = parseISO8601(s.accessExpiresAt) {
            if expiry.timeIntervalSinceNow <= expirySkewSeconds {
                if let refreshed = try? await refreshSession() {
                    return refreshed.accessToken
                }
            }
        }
        return s.accessToken
    }

    // MARK: - Auth flows

    /// POST /auth/v1/signup → { ok, user_id } (201).
    @discardableResult
    public func signUp(email: String, password: String, projectID: String? = nil) async throws -> SignUpResult {
        let pid = try await resolveProjectID(projectID)
        return try await transport.requestJSON(
            "POST", path: "/auth/v1/signup",
            body: ["project_id": pid, "email": email, "password": password] as [String: String],
            auth: false
        )
    }

    /// POST /auth/v1/signin → token body; stored as the live session.
    @discardableResult
    public func signIn(email: String, password: String, projectID: String? = nil) async throws -> Session {
        let pid = try await resolveProjectID(projectID)
        let session: Session = try await transport.requestJSON(
            "POST", path: "/auth/v1/signin",
            body: ["project_id": pid, "email": email, "password": password] as [String: String],
            auth: false
        )
        _session = session
        return session
    }

    /// Revoke the current refresh token server-side then clear the local session.
    ///
    /// Calls POST /auth/v1/signout. 404 (older servers) and network errors are
    /// tolerated silently — the local session is always cleared regardless.
    public func signOut() async {
        guard let current = _session else { return }
        _session = nil
        do {
            _ = try await transport.requestJSONVoid(
                "POST", path: "/auth/v1/signout",
                body: ["refresh_token": current.refreshToken] as [String: String],
                auth: false
            )
        } catch let e as BasinApiError where e.status == 404 {
            // Older server without the signout route — tolerate silently.
        } catch {
            // Network error or other transient failure — local clear already done.
        }
    }

    /// POST /auth/v1/refresh with the stored refresh token.
    ///
    /// Rotates both tokens. Throws E_REVOKED_TOKEN if the token was already rotated.
    @discardableResult
    public func refreshSession() async throws -> Session {
        guard let current = _session else {
            throw BasinApiError(rawCode: "E_UNAUTHENTICATED", message: "no session to refresh", status: 0)
        }
        let session: Session = try await transport.requestJSON(
            "POST", path: "/auth/v1/refresh",
            body: ["refresh_token": current.refreshToken] as [String: String],
            auth: false
        )
        _session = session
        return session
    }

    /// POST /auth/v1/verify-email → { ok: true }.
    @discardableResult
    public func verifyEmail(token: String, projectID: String? = nil) async throws -> [String: Bool] {
        let pid = try await resolveProjectID(projectID)
        return try await transport.requestJSON(
            "POST", path: "/auth/v1/verify-email",
            body: ["project_id": pid, "token": token] as [String: String],
            auth: false
        )
    }

    /// POST /auth/v1/request-password-reset → { ok: true }.
    @discardableResult
    public func requestPasswordReset(email: String, projectID: String? = nil) async throws -> [String: Bool] {
        let pid = try await resolveProjectID(projectID)
        return try await transport.requestJSON(
            "POST", path: "/auth/v1/request-password-reset",
            body: ["project_id": pid, "email": email] as [String: String],
            auth: false
        )
    }

    /// POST /auth/v1/reset-password → { ok: true }.
    @discardableResult
    public func resetPassword(token: String, newPassword: String, projectID: String? = nil) async throws -> [String: Bool] {
        let pid = try await resolveProjectID(projectID)
        return try await transport.requestJSON(
            "POST", path: "/auth/v1/reset-password",
            body: ["project_id": pid, "token": token, "new_password": newPassword] as [String: String],
            auth: false
        )
    }

    /// POST /auth/v1/magic-link — 204 always (never confirms address).
    public func requestMagicLink(email: String) async throws {
        _ = try await transport.requestJSONVoid(
            "POST", path: "/auth/v1/magic-link",
            body: ["email": email] as [String: String],
            auth: false
        )
    }

    /// POST /auth/v1/magic-link/consume → token body; stored as session.
    @discardableResult
    public func consumeMagicLink(token: String) async throws -> Session {
        let session: Session = try await transport.requestJSON(
            "POST", path: "/auth/v1/magic-link/consume",
            body: ["token": token] as [String: String],
            auth: false
        )
        _session = session
        return session
    }

    // MARK: - API Keys (JWT-gated)

    /// POST /auth/v1/api-keys → issued key incl. one-time secret.
    public func createApiKey(name: String) async throws -> ApiKeyIssued {
        try await transport.requestJSON(
            "POST", path: "/auth/v1/api-keys",
            body: ["name": name] as [String: String]
        )
    }

    /// GET /auth/v1/api-keys.
    public func listApiKeys() async throws -> [ApiKeyDescriptor] {
        try await transport.requestJSON("GET", path: "/auth/v1/api-keys")
    }

    /// DELETE /auth/v1/api-keys/:id → { ok: true }.
    @discardableResult
    public func deleteApiKey(_ id: Int) async throws -> Data {
        try await transport.requestJSONVoid("DELETE", path: "/auth/v1/api-keys/\(id)")
    }

    // MARK: - OAuth

    /// GET /auth/v1/oauth/:provider/authorize → { redirect_url, state }.
    ///
    /// Returns the provider authorize URL. Redirect the user's browser to
    /// `result.redirectUrl`; the server's callback handler exchanges the code
    /// and issues tokens.
    ///
    /// - Parameter provider: Lower-case provider name, e.g. "google" or "github".
    /// - Parameter redirectTo: URL to redirect to after flow completes.
    public func getOAuthAuthorizeURL(
        provider: String,
        redirectTo: String = "",
        projectID: String? = nil
    ) async throws -> OAuthAuthorizeResult {
        let pid = try await resolveProjectID(projectID)
        return try await transport.requestJSON(
            "GET",
            path: "/auth/v1/oauth/\(provider)/authorize",
            query: [("project_id", pid), ("redirect_to", redirectTo)],
            auth: false
        )
    }

    // MARK: - MFA

    private struct EnrollBody: Encodable {
        let factor_type: String
        let friendly_name: String
    }

    /// POST /auth/v1/factors — begin MFA factor enrollment (JWT-gated).
    ///
    /// - Parameter factorType: `"totp"` or `"webauthn"`.
    /// - Parameter friendlyName: Human-readable label.
    public func enrollFactor(factorType: String, friendlyName: String = "") async throws -> MfaEnrollResult {
        let data = try await transport.requestJSONVoid(
            "POST", path: "/auth/v1/factors",
            body: EnrollBody(factor_type: factorType, friendly_name: friendlyName)
        )
        // Discriminate on factor_type field.
        if let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
           json["factor_type"] as? String == "webauthn" {
            let r = try decodeJSON(data) as WebAuthnEnrollResult
            return .webAuthn(r)
        }
        let r = try decodeJSON(data) as TotpEnrollResult
        return .totp(r)
    }

    /// GET /auth/v1/factors — list MFA factors (JWT-gated).
    public func listFactors() async throws -> [FactorDescriptor] {
        try await transport.requestJSON("GET", path: "/auth/v1/factors")
    }

    private struct VerifyFactorBody: Encodable {
        let code: String?
        let attestation: String?
        let challenge_id: String?
    }

    /// POST /auth/v1/factors/:id/verify — confirm enrollment (JWT-gated).
    ///
    /// - Parameter code: TOTP path: 6-digit OTP code.
    /// - Parameter attestation: WebAuthn path: attestation JSON.
    /// - Parameter challengeID: WebAuthn path: challenge_id from enrollFactor.
    public func verifyFactor(
        _ factorID: String,
        code: String? = nil,
        attestation: String? = nil,
        challengeID: String? = nil
    ) async throws -> VerifyFactorResult {
        try await transport.requestJSON(
            "POST", path: "/auth/v1/factors/\(factorID)/verify",
            body: VerifyFactorBody(code: code, attestation: attestation, challenge_id: challengeID)
        )
    }

    /// POST /auth/v1/factors/:id/challenge — begin a step-up challenge (JWT-gated).
    public func challengeFactor(_ factorID: String) async throws -> MfaChallengeResult {
        let data = try await transport.requestJSONVoid(
            "POST", path: "/auth/v1/factors/\(factorID)/challenge",
            body: EmptyBody()
        )
        if let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
           json["request_options_json"] != nil {
            let r = try decodeJSON(data) as WebAuthnChallengeResult
            return .webAuthn(r)
        }
        let r = try decodeJSON(data) as TotpChallengeResult
        return .totp(r)
    }

    private struct VerifyChallengeBody: Encodable {
        let challenge_id: String
        let code: String?
        let assertion: String?
    }

    /// POST /auth/v1/factors/:id/challenge/verify → aal2 Session (JWT-gated).
    @discardableResult
    public func verifyChallenge(
        _ factorID: String,
        challengeID: String,
        code: String? = nil,
        assertion: String? = nil
    ) async throws -> Session {
        let session: Session = try await transport.requestJSON(
            "POST", path: "/auth/v1/factors/\(factorID)/challenge/verify",
            body: VerifyChallengeBody(challenge_id: challengeID, code: code, assertion: assertion)
        )
        _session = session
        return session
    }

    /// DELETE /auth/v1/factors/:id — unenroll a factor (requires aal2 JWT).
    @discardableResult
    public func unenrollFactor(_ factorID: String) async throws -> Data {
        try await transport.requestJSONVoid("DELETE", path: "/auth/v1/factors/\(factorID)")
    }
}

// MARK: - Helpers

private struct EmptyBody: Encodable {}

private func parseISO8601(_ s: String) -> Date? {
    let fmt = ISO8601DateFormatter()
    fmt.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
    if let d = fmt.date(from: s) { return d }
    fmt.formatOptions = [.withInternetDateTime]
    return fmt.date(from: s)
}
