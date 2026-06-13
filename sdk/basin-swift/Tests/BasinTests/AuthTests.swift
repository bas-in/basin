import XCTest
@testable import Basin

final class AuthTests: XCTestCase {

    var client: BasinClient!
    let baseURL = "http://localhost:8080"

    override func setUp() {
        super.setUp()
        MockURLProtocol.reset()
        client = BasinClient(url: baseURL, key: nil, projectID: "01JTEST", session: mockSession())
    }

    // MARK: signUp

    func testSignUp() async throws {
        MockURLProtocol.push { req in
            XCTAssertEqual(req.httpMethod, "POST")
            XCTAssert(req.url?.path.hasSuffix("/auth/v1/signup") == true)
            return mockResponse(url: req.url!, status: 201,
                                json: ["ok": true, "user_id": "usr_001"])
        }
        let result = try await client.auth.signUp(email: "a@b.com", password: "secret")
        XCTAssertTrue(result.ok)
        XCTAssertEqual(result.userId, "usr_001")
    }

    // MARK: signIn

    func testSignIn() async throws {
        MockURLProtocol.push { req in
            XCTAssertEqual(req.url?.path, "/auth/v1/signin")
            return mockResponse(url: req.url!, status: 200, json: [
                "access_token": "acc.tok.123",
                "refresh_token": "ref.tok.456",
                "access_expires_at": "2099-01-01T00:00:00Z",
                "refresh_expires_at": "2099-06-01T00:00:00Z"
            ])
        }
        let session = try await client.auth.signIn(email: "a@b.com", password: "secret")
        XCTAssertEqual(session.accessToken, "acc.tok.123")
        XCTAssertEqual(session.refreshToken, "ref.tok.456")
        // Session stored.
        let stored = await client.auth.getSession()
        XCTAssertNotNil(stored)
    }

    // MARK: signOut

    func testSignOut_NoSession_IsNoOp() async {
        // No mock needed — no HTTP call should be made.
        await client.auth.signOut()
        let session = await client.auth.getSession()
        XCTAssertNil(session)
    }

    func testSignOut_ClearsSessionEvenOn404() async throws {
        // Seed a session.
        await client.auth.setSession(Session(
            accessToken: "a", refreshToken: "r",
            accessExpiresAt: "2099-01-01T00:00:00Z",
            refreshExpiresAt: "2099-06-01T00:00:00Z"
        ))
        MockURLProtocol.push { req in
            XCTAssertEqual(req.url?.path, "/auth/v1/signout")
            return mockResponse(url: req.url!, status: 404, json: ["code": "E_NOT_FOUND", "message": "no route"])
        }
        await client.auth.signOut()
        let session = await client.auth.getSession()
        XCTAssertNil(session)
    }

    // MARK: refreshSession

    func testRefreshSession() async throws {
        await client.auth.setSession(Session(
            accessToken: "old_access", refreshToken: "old_refresh",
            accessExpiresAt: "2099-01-01T00:00:00Z",
            refreshExpiresAt: "2099-06-01T00:00:00Z"
        ))
        MockURLProtocol.push { req in
            XCTAssertEqual(req.url?.path, "/auth/v1/refresh")
            return mockResponse(url: req.url!, status: 200, json: [
                "access_token": "new_access",
                "refresh_token": "new_refresh",
                "access_expires_at": "2099-01-01T00:00:00Z",
                "refresh_expires_at": "2099-06-01T00:00:00Z"
            ])
        }
        let session = try await client.auth.refreshSession()
        XCTAssertEqual(session.accessToken, "new_access")
    }

    func testRefreshSession_NoSession_Throws() async {
        do {
            _ = try await client.auth.refreshSession()
            XCTFail("Expected throw")
        } catch let e as BasinApiError {
            XCTAssertEqual(e.rawCode, "E_UNAUTHENTICATED")
        } catch {
            XCTFail("Unexpected error type: \(error)")
        }
    }

    // MARK: autoRefresh

    func testAutoRefresh_TriggersWhenNearExpiry() async throws {
        // Set an already-expired access token.
        await client.auth.setSession(Session(
            accessToken: "expired_access", refreshToken: "ref",
            accessExpiresAt: "2000-01-01T00:00:00Z",  // in the past
            refreshExpiresAt: "2099-06-01T00:00:00Z"
        ))
        MockURLProtocol.push { req in
            XCTAssertEqual(req.url?.path, "/auth/v1/refresh")
            return mockResponse(url: req.url!, status: 200, json: [
                "access_token": "fresh_access",
                "refresh_token": "fresh_refresh",
                "access_expires_at": "2099-01-01T00:00:00Z",
                "refresh_expires_at": "2099-06-01T00:00:00Z"
            ])
        }
        let tok = await client.auth.accessToken()
        XCTAssertEqual(tok, "fresh_access")
    }

    // MARK: apiKeys

    func testCreateApiKey() async throws {
        // Seed a session so JWT auth works.
        await client.auth.setSession(Session(
            accessToken: "jwt", refreshToken: "r",
            accessExpiresAt: "2099-01-01T00:00:00Z",
            refreshExpiresAt: "2099-06-01T00:00:00Z"
        ))
        MockURLProtocol.push { req in
            XCTAssertEqual(req.url?.path, "/auth/v1/api-keys")
            XCTAssertEqual(req.httpMethod, "POST")
            return mockResponse(url: req.url!, status: 201, json: [
                "id": 1, "name": "ci", "secret": "sk_abc", "created_at": "2024-01-01T00:00:00Z"
            ])
        }
        let key = try await client.auth.createApiKey(name: "ci")
        XCTAssertEqual(key.secret, "sk_abc")
        XCTAssertEqual(key.name, "ci")
    }

    func testListApiKeys() async throws {
        await client.auth.setSession(Session(
            accessToken: "jwt", refreshToken: "r",
            accessExpiresAt: "2099-01-01T00:00:00Z",
            refreshExpiresAt: "2099-06-01T00:00:00Z"
        ))
        MockURLProtocol.push { req in
            XCTAssertEqual(req.httpMethod, "GET")
            return mockResponse(url: req.url!, status: 200, json: [[
                "id": 1, "name": "ci",
                "created_at": "2024-01-01T00:00:00Z",
                "last_used_at": NSNull(),
                "revoked_at": NSNull()
            ]])
        }
        let keys = try await client.auth.listApiKeys()
        XCTAssertEqual(keys.count, 1)
        XCTAssertEqual(keys[0].name, "ci")
    }

    // MARK: magicLink

    func testRequestMagicLink() async throws {
        MockURLProtocol.push { req in
            XCTAssertEqual(req.url?.path, "/auth/v1/magic-link")
            return mockResponse(url: req.url!, status: 204, json: [:])
        }
        // Should not throw.
        try await client.auth.requestMagicLink(email: "a@b.com")
    }

    func testConsumeMagicLink() async throws {
        MockURLProtocol.push { _ in
            return mockResponse(url: URL(string: "http://localhost")!, status: 200, json: [
                "access_token": "magic_access",
                "refresh_token": "magic_refresh",
                "access_expires_at": "2099-01-01T00:00:00Z",
                "refresh_expires_at": "2099-06-01T00:00:00Z"
            ])
        }
        let session = try await client.auth.consumeMagicLink(token: "tok_magic")
        XCTAssertEqual(session.accessToken, "magic_access")
    }

    // MARK: OAuth

    func testGetOAuthAuthorizeURL() async throws {
        MockURLProtocol.push { req in
            XCTAssert(req.url?.path.contains("/auth/v1/oauth/google/authorize") == true)
            return mockResponse(url: req.url!, status: 200, json: [
                "redirect_url": "https://accounts.google.com/authorize?state=abc",
                "state": "abc"
            ])
        }
        let result = try await client.auth.getOAuthAuthorizeURL(provider: "google", redirectTo: "https://myapp.example.com/cb")
        XCTAssertTrue(result.redirectUrl.contains("accounts.google.com"))
        XCTAssertEqual(result.state, "abc")
    }

    // MARK: MFA — TOTP

    func testEnrollFactor_TOTP() async throws {
        await client.auth.setSession(Session(
            accessToken: "jwt", refreshToken: "r",
            accessExpiresAt: "2099-01-01T00:00:00Z",
            refreshExpiresAt: "2099-06-01T00:00:00Z"
        ))
        MockURLProtocol.push { req in
            XCTAssertEqual(req.url?.path, "/auth/v1/factors")
            return mockResponse(url: req.url!, status: 201, json: [
                "factor_id": "fac_001", "factor_type": "totp",
                "secret_b32": "JBSWY3DPEHPK3PXP",
                "otpauth_uri": "otpauth://totp/Basin:alice@example.com?secret=JBSWY3DPEHPK3PXP"
            ])
        }
        let result = try await client.auth.enrollFactor(factorType: "totp", friendlyName: "App")
        if case .totp(let t) = result {
            XCTAssertEqual(t.factorId, "fac_001")
            XCTAssertEqual(t.secretB32, "JBSWY3DPEHPK3PXP")
        } else {
            XCTFail("Expected totp result")
        }
    }

    func testVerifyFactor() async throws {
        await client.auth.setSession(Session(
            accessToken: "jwt", refreshToken: "r",
            accessExpiresAt: "2099-01-01T00:00:00Z",
            refreshExpiresAt: "2099-06-01T00:00:00Z"
        ))
        MockURLProtocol.push { req in
            XCTAssert(req.url?.path.contains("/auth/v1/factors/fac_001/verify") == true)
            return mockResponse(url: req.url!, status: 200, json: [
                "ok": true, "recovery_codes": ["aaa", "bbb"]
            ])
        }
        let result = try await client.auth.verifyFactor("fac_001", code: "123456")
        XCTAssertTrue(result.ok)
        XCTAssertEqual(result.recoveryCodes, ["aaa", "bbb"])
    }

    func testChallengeFactor_TOTP() async throws {
        await client.auth.setSession(Session(
            accessToken: "jwt", refreshToken: "r",
            accessExpiresAt: "2099-01-01T00:00:00Z",
            refreshExpiresAt: "2099-06-01T00:00:00Z"
        ))
        MockURLProtocol.push { req in
            return mockResponse(url: req.url!, status: 200, json: ["challenge_id": "chal_001"])
        }
        let result = try await client.auth.challengeFactor("fac_001")
        XCTAssertEqual(result.challengeId, "chal_001")
    }

    func testVerifyChallenge() async throws {
        await client.auth.setSession(Session(
            accessToken: "jwt", refreshToken: "r",
            accessExpiresAt: "2099-01-01T00:00:00Z",
            refreshExpiresAt: "2099-06-01T00:00:00Z"
        ))
        MockURLProtocol.push { req in
            XCTAssert(req.url?.path.contains("/challenge/verify") == true)
            return mockResponse(url: req.url!, status: 200, json: [
                "access_token": "aal2_token",
                "refresh_token": "ref",
                "access_expires_at": "2099-01-01T00:00:00Z",
                "refresh_expires_at": "2099-06-01T00:00:00Z"
            ])
        }
        let session = try await client.auth.verifyChallenge(
            "fac_001", challengeID: "chal_001", code: "654321"
        )
        XCTAssertEqual(session.accessToken, "aal2_token")
    }

    // MARK: Error decode

    func testApiError_Decoded() async {
        MockURLProtocol.push { req in
            return mockErrorResponse(url: req.url!, status: 401,
                                     code: "E_UNAUTHENTICATED", message: "token expired")
        }
        do {
            _ = try await client.auth.listApiKeys()
            XCTFail("Expected BasinApiError")
        } catch let e as BasinApiError {
            XCTAssertEqual(e.code, .unauthenticated)
            XCTAssertEqual(e.status, 401)
            XCTAssertEqual(e.message, "token expired")
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
    }
}
