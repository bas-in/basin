package io.basin.sdk;

import com.fasterxml.jackson.databind.JsonNode;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Auth client for {@code /auth/v1/*} routes.
 *
 * <p>Route sources (verified against {@code crates/basin-rest/src/server.rs}):
 * <ul>
 *   <li>{@code POST /auth/v1/signup}                           server.rs:302
 *   <li>{@code POST /auth/v1/signin}                           server.rs:303
 *   <li>{@code POST /auth/v1/refresh}                          server.rs:304
 *   <li>{@code POST /auth/v1/signout}                          server.rs:305
 *   <li>{@code POST /auth/v1/verify-email}                     server.rs:306
 *   <li>{@code POST /auth/v1/reset-password}                   server.rs:307
 *   <li>{@code POST /auth/v1/request-password-reset}           server.rs:308
 *   <li>{@code POST /auth/v1/magic-link}                       server.rs:315
 *   <li>{@code POST /auth/v1/magic-link/consume}               server.rs:316-319
 *   <li>{@code POST|GET /auth/v1/api-keys}                     server.rs:322-323
 *   <li>{@code DELETE /auth/v1/api-keys/:id}                   server.rs:325-327
 *   <li>{@code GET /auth/v1/oauth/:provider/authorize}         server.rs:329-332
 *   <li>{@code POST|GET /auth/v1/factors}                      server.rs:339-342
 *   <li>{@code POST /auth/v1/factors/:id/verify}               server.rs:343-345
 *   <li>{@code POST /auth/v1/factors/:id/challenge}            server.rs:347-349
 *   <li>{@code POST /auth/v1/factors/:id/challenge/verify}     server.rs:351-353
 *   <li>{@code DELETE /auth/v1/factors/:id}                    server.rs:354-356
 * </ul>
 *
 * <p>Obtain via {@link BasinClient#auth()}.
 */
public final class AuthClient {

    /** Refresh the access token this many seconds before its stated expiry. */
    private static final int EXPIRY_SKEW_S = 10;

    private final BasinTransport transport;
    private final String defaultProjectId;

    private volatile Session session;
    private final ReentrantLock refreshLock = new ReentrantLock();

    AuthClient(BasinTransport transport, String defaultProjectId) {
        this.transport = transport;
        this.defaultProjectId = defaultProjectId;
    }

    // ------------------------------------------------------------------
    // Session management
    // ------------------------------------------------------------------

    /** Return the current session, or {@code null} when signed out. */
    public Session getSession() {
        return session;
    }

    /** Adopt an externally obtained token pair (e.g. restored from storage). */
    public void setSession(Session session) {
        this.session = session;
    }

    /**
     * Return a valid access token, auto-refreshing when within 10 s of expiry.
     *
     * <p>Returns {@code null} when there is no session; callers fall back to the
     * static API key. Thread-safe: at most one concurrent refresh is issued.
     */
    public String accessToken() {
        Session s = session;
        if (s == null) return null;
        try {
            Instant expiry = Instant.parse(s.accessExpiresAt);
            if (Instant.now().plusSeconds(EXPIRY_SKEW_S).isAfter(expiry)) {
                // Need a refresh — only one thread does it.
                if (refreshLock.tryLock()) {
                    try {
                        // Re-check after acquiring the lock.
                        s = session;
                        if (s != null) {
                            Instant recheck = Instant.parse(s.accessExpiresAt);
                            if (Instant.now().plusSeconds(EXPIRY_SKEW_S).isAfter(recheck)) {
                                s = refreshSession();
                            }
                        }
                    } finally {
                        refreshLock.unlock();
                    }
                }
                // Another thread was already refreshing — return whatever we have.
                s = session;
            }
        } catch (Exception ignored) {
            // Malformed expiry or other parse error — return the token as-is.
        }
        return s != null ? s.accessToken : null;
    }

    // ------------------------------------------------------------------
    // Project-ID resolution
    // ------------------------------------------------------------------

    private String resolveProjectId(String override) {
        String pid = override != null ? override
                : defaultProjectId != null ? defaultProjectId
                : projectIdFromJwt(transport.staticKey);
        if (pid == null || pid.isEmpty()) {
            throw new BasinApiException("E_INVALID_REQUEST",
                    "project id required: pass projectId to BasinClient.Builder or set it on auth flows",
                    0);
        }
        return pid;
    }

    /**
     * Decode the {@code project_id} JWT claim without verifying the signature.
     * Returns {@code null} for non-JWT strings (raw API keys) or on parse error.
     */
    static String projectIdFromJwt(String token) {
        if (token == null || token.isEmpty()) return null;
        String[] parts = token.split("\\.");
        if (parts.length != 3) return null;
        try {
            String padded = parts[1]
                    .replace('-', '+')
                    .replace('_', '/');
            int mod = padded.length() % 4;
            if (mod != 0) padded += "=".repeat(4 - mod);
            byte[] decoded = Base64.getDecoder().decode(padded);
            String json = new String(decoded, StandardCharsets.UTF_8);
            // Minimal extraction — avoid a full parse for this hot path.
            int idx = json.indexOf("\"project_id\"");
            if (idx < 0) return null;
            int colon = json.indexOf(':', idx);
            if (colon < 0) return null;
            int q1 = json.indexOf('"', colon + 1);
            if (q1 < 0) return null;
            int q2 = json.indexOf('"', q1 + 1);
            if (q2 < 0) return null;
            return json.substring(q1 + 1, q2);
        } catch (Exception ignored) {
            return null;
        }
    }

    // ------------------------------------------------------------------
    // Auth flows
    // ------------------------------------------------------------------

    /**
     * {@code POST /auth/v1/signup} → {@link SignUpResult} (201).
     *
     * @param email     user email
     * @param password  plaintext password
     * @param projectId project ULID; falls back to the client default when {@code null}
     */
    public SignUpResult signUp(String email, String password, String projectId) {
        Map<String, Object> body = Map.of(
                "project_id", resolveProjectId(projectId),
                "email", email,
                "password", password);
        JsonNode node = transport.executeJson(transport.jsonPost("/auth/v1/signup", null, body, false));
        return transport.readValue(node, SignUpResult.class);
    }

    /** Async variant of {@link #signUp}. */
    public CompletableFuture<SignUpResult> signUpAsync(String email, String password, String projectId) {
        Map<String, Object> body = Map.of(
                "project_id", resolveProjectId(projectId),
                "email", email,
                "password", password);
        return transport.executeJsonAsync(transport.jsonPost("/auth/v1/signup", null, body, false))
                .thenApply(n -> transport.readValue(n, SignUpResult.class));
    }

    /**
     * {@code POST /auth/v1/signin} → {@link Session}; stored as the live session.
     */
    public Session signIn(String email, String password, String projectId) {
        Map<String, Object> body = Map.of(
                "project_id", resolveProjectId(projectId),
                "email", email,
                "password", password);
        JsonNode node = transport.executeJson(transport.jsonPost("/auth/v1/signin", null, body, false));
        Session s = transport.readValue(node, Session.class);
        this.session = s;
        return s;
    }

    /** Async variant of {@link #signIn}. */
    public CompletableFuture<Session> signInAsync(String email, String password, String projectId) {
        Map<String, Object> body = Map.of(
                "project_id", resolveProjectId(projectId),
                "email", email,
                "password", password);
        return transport.executeJsonAsync(transport.jsonPost("/auth/v1/signin", null, body, false))
                .thenApply(n -> {
                    Session s = transport.readValue(n, Session.class);
                    this.session = s;
                    return s;
                });
    }

    /**
     * {@code POST /auth/v1/signout} — revokes the refresh token server-side then
     * clears the local session.
     *
     * <p>The local session is cleared regardless of the server response.
     * 404 (older server without the signout route) and network errors are
     * tolerated silently. If there is no active session this is a no-op.
     */
    public void signOut() {
        Session current = this.session;
        this.session = null; // clear first — always end up signed-out
        if (current == null) return;
        try {
            transport.executeJson(transport.jsonPost("/auth/v1/signout", null,
                    Map.of("refresh_token", current.refreshToken), false));
        } catch (BasinApiException e) {
            if (e.status == 404) return; // older server, tolerate
            throw e;
        } catch (BasinNetworkException ignored) {
            // local clear already done
        }
    }

    /** Async variant of {@link #signOut}. */
    public CompletableFuture<Void> signOutAsync() {
        Session current = this.session;
        this.session = null;
        if (current == null) return CompletableFuture.completedFuture(null);
        return transport.executeJsonAsync(transport.jsonPost("/auth/v1/signout", null,
                        Map.of("refresh_token", current.refreshToken), false))
                .handle((n, ex) -> null); // tolerate 404 + network errors
    }

    /**
     * {@code POST /auth/v1/refresh} — rotates both tokens.
     *
     * @throws BasinApiException with code {@code E_REVOKED_TOKEN} if the token was
     *                           already rotated.
     */
    public Session refreshSession() {
        Session current = session;
        if (current == null) {
            throw new BasinApiException("E_UNAUTHENTICATED", "no session to refresh", 0);
        }
        JsonNode node = transport.executeJson(transport.jsonPost("/auth/v1/refresh", null,
                Map.of("refresh_token", current.refreshToken), false));
        Session s = transport.readValue(node, Session.class);
        this.session = s;
        return s;
    }

    /** Async variant of {@link #refreshSession}. */
    public CompletableFuture<Session> refreshSessionAsync() {
        Session current = session;
        if (current == null) {
            CompletableFuture<Session> f = new CompletableFuture<>();
            f.completeExceptionally(
                    new BasinApiException("E_UNAUTHENTICATED", "no session to refresh", 0));
            return f;
        }
        return transport.executeJsonAsync(transport.jsonPost("/auth/v1/refresh", null,
                        Map.of("refresh_token", current.refreshToken), false))
                .thenApply(n -> {
                    Session s = transport.readValue(n, Session.class);
                    this.session = s;
                    return s;
                });
    }

    /**
     * {@code POST /auth/v1/verify-email} → {@code {"ok":true}}.
     *
     * @param token     the token from the verification email
     * @param projectId project ULID; falls back to the client default
     */
    public JsonNode verifyEmail(String token, String projectId) {
        return transport.executeJson(transport.jsonPost("/auth/v1/verify-email", null,
                Map.of("project_id", resolveProjectId(projectId), "token", token), false));
    }

    /** Async variant of {@link #verifyEmail}. */
    public CompletableFuture<JsonNode> verifyEmailAsync(String token, String projectId) {
        return transport.executeJsonAsync(transport.jsonPost("/auth/v1/verify-email", null,
                Map.of("project_id", resolveProjectId(projectId), "token", token), false));
    }

    /**
     * {@code POST /auth/v1/request-password-reset} → {@code {"ok":true}}.
     */
    public JsonNode requestPasswordReset(String email, String projectId) {
        return transport.executeJson(transport.jsonPost("/auth/v1/request-password-reset", null,
                Map.of("project_id", resolveProjectId(projectId), "email", email), false));
    }

    /** Async variant of {@link #requestPasswordReset}. */
    public CompletableFuture<JsonNode> requestPasswordResetAsync(String email, String projectId) {
        return transport.executeJsonAsync(transport.jsonPost("/auth/v1/request-password-reset", null,
                Map.of("project_id", resolveProjectId(projectId), "email", email), false));
    }

    /**
     * {@code POST /auth/v1/reset-password} → {@code {"ok":true}}.
     */
    public JsonNode resetPassword(String token, String newPassword, String projectId) {
        return transport.executeJson(transport.jsonPost("/auth/v1/reset-password", null,
                Map.of("project_id", resolveProjectId(projectId),
                        "token", token,
                        "new_password", newPassword), false));
    }

    /** Async variant of {@link #resetPassword}. */
    public CompletableFuture<JsonNode> resetPasswordAsync(String token, String newPassword, String projectId) {
        return transport.executeJsonAsync(transport.jsonPost("/auth/v1/reset-password", null,
                Map.of("project_id", resolveProjectId(projectId),
                        "token", token,
                        "new_password", newPassword), false));
    }

    /**
     * {@code POST /auth/v1/magic-link} — 204 always (never confirms address).
     */
    public void requestMagicLink(String email) {
        transport.executeJson(transport.jsonPost("/auth/v1/magic-link", null,
                Map.of("email", email), false));
    }

    /** Async variant of {@link #requestMagicLink}. */
    public CompletableFuture<Void> requestMagicLinkAsync(String email) {
        return transport.executeJsonAsync(transport.jsonPost("/auth/v1/magic-link", null,
                Map.of("email", email), false)).thenApply(n -> null);
    }

    /**
     * {@code POST /auth/v1/magic-link/consume} → {@link Session}; stored as session.
     */
    public Session consumeMagicLink(String token) {
        JsonNode node = transport.executeJson(transport.jsonPost("/auth/v1/magic-link/consume", null,
                Map.of("token", token), false));
        Session s = transport.readValue(node, Session.class);
        this.session = s;
        return s;
    }

    /** Async variant of {@link #consumeMagicLink}. */
    public CompletableFuture<Session> consumeMagicLinkAsync(String token) {
        return transport.executeJsonAsync(transport.jsonPost("/auth/v1/magic-link/consume", null,
                Map.of("token", token), false))
                .thenApply(n -> {
                    Session s = transport.readValue(n, Session.class);
                    this.session = s;
                    return s;
                });
    }

    // ------------------------------------------------------------------
    // API keys (JWT-gated)
    // ------------------------------------------------------------------

    /** {@code POST /auth/v1/api-keys} → {@link ApiKeyIssued} (secret shown once). */
    public ApiKeyIssued createApiKey(String name) {
        JsonNode node = transport.executeJson(
                transport.jsonPost("/auth/v1/api-keys", null, Map.of("name", name), true));
        return transport.readValue(node, ApiKeyIssued.class);
    }

    /** Async variant of {@link #createApiKey}. */
    public CompletableFuture<ApiKeyIssued> createApiKeyAsync(String name) {
        return transport.executeJsonAsync(
                transport.jsonPost("/auth/v1/api-keys", null, Map.of("name", name), true))
                .thenApply(n -> transport.readValue(n, ApiKeyIssued.class));
    }

    /** {@code GET /auth/v1/api-keys} → list of {@link ApiKeyDescriptor}. */
    public List<ApiKeyDescriptor> listApiKeys() {
        JsonNode node = transport.executeJson(transport.jsonGet("/auth/v1/api-keys", null, true));
        return transport.mapper.convertValue(node,
                transport.mapper.getTypeFactory()
                        .constructCollectionType(List.class, ApiKeyDescriptor.class));
    }

    /** Async variant of {@link #listApiKeys}. */
    public CompletableFuture<List<ApiKeyDescriptor>> listApiKeysAsync() {
        return transport.executeJsonAsync(transport.jsonGet("/auth/v1/api-keys", null, true))
                .thenApply(n -> transport.mapper.convertValue(n,
                        transport.mapper.getTypeFactory()
                                .constructCollectionType(List.class, ApiKeyDescriptor.class)));
    }

    /** {@code DELETE /auth/v1/api-keys/:id} → {@code {"ok":true}}. */
    public JsonNode deleteApiKey(long keyId) {
        return transport.executeJson(
                transport.jsonDelete("/auth/v1/api-keys/" + keyId, null, null, true));
    }

    /** Async variant of {@link #deleteApiKey}. */
    public CompletableFuture<JsonNode> deleteApiKeyAsync(long keyId) {
        return transport.executeJsonAsync(
                transport.jsonDelete("/auth/v1/api-keys/" + keyId, null, null, true));
    }

    // ------------------------------------------------------------------
    // OAuth
    // ------------------------------------------------------------------

    /**
     * {@code GET /auth/v1/oauth/:provider/authorize} → {@link OAuthAuthorizeResult}.
     *
     * <p>Builds the provider authorize URL server-side (with PKCE + signed CSRF state)
     * and returns it. Redirect the user's browser to {@code result.redirectUrl}.
     *
     * <p>Supported preset providers: {@code google}, {@code github}, {@code apple},
     * {@code bitbucket}, {@code discord}, {@code figma}, {@code gitlab},
     * {@code linkedin}, {@code microsoft} (azure_ad), {@code notion}, {@code slack},
     * {@code spotify}, {@code twitch}, {@code twitter_x} (twitter). Custom OIDC
     * providers are registered server-side and passed by name.
     *
     * @param provider   lower-case provider name, e.g. {@code "google"}
     * @param redirectTo URL to redirect to after the flow completes; may be empty
     * @param projectId  project ULID; falls back to client default
     */
    public OAuthAuthorizeResult getOAuthAuthorizeUrl(String provider, String redirectTo,
                                                      String projectId) {
        List<String[]> query = List.of(
                new String[]{"project_id", resolveProjectId(projectId)},
                new String[]{"redirect_to", redirectTo != null ? redirectTo : ""});
        JsonNode node = transport.executeJson(
                transport.jsonGet("/auth/v1/oauth/" + provider + "/authorize", query, false));
        return transport.readValue(node, OAuthAuthorizeResult.class);
    }

    /** Async variant of {@link #getOAuthAuthorizeUrl}. */
    public CompletableFuture<OAuthAuthorizeResult> getOAuthAuthorizeUrlAsync(
            String provider, String redirectTo, String projectId) {
        List<String[]> query = List.of(
                new String[]{"project_id", resolveProjectId(projectId)},
                new String[]{"redirect_to", redirectTo != null ? redirectTo : ""});
        return transport.executeJsonAsync(
                transport.jsonGet("/auth/v1/oauth/" + provider + "/authorize", query, false))
                .thenApply(n -> transport.readValue(n, OAuthAuthorizeResult.class));
    }

    // ------------------------------------------------------------------
    // MFA: Factor enrollment
    // ------------------------------------------------------------------

    /**
     * {@code POST /auth/v1/factors} — begin MFA factor enrollment (JWT-gated).
     *
     * @param factorType   {@code "totp"} or {@code "webauthn"}
     * @param friendlyName human-readable label, e.g. {@code "Authenticator App"}
     * @return {@link TotpEnrollResult} for TOTP or {@link WebAuthnEnrollResult} for WebAuthn
     */
    public Object enrollFactor(String factorType, String friendlyName) {
        Map<String, Object> body = Map.of(
                "factor_type", factorType,
                "friendly_name", friendlyName != null ? friendlyName : "");
        JsonNode node = transport.executeJson(
                transport.jsonPost("/auth/v1/factors", null, body, true));
        return decodeEnrollResult(node);
    }

    /** Async variant of {@link #enrollFactor}. */
    public CompletableFuture<Object> enrollFactorAsync(String factorType, String friendlyName) {
        Map<String, Object> body = Map.of(
                "factor_type", factorType,
                "friendly_name", friendlyName != null ? friendlyName : "");
        return transport.executeJsonAsync(
                transport.jsonPost("/auth/v1/factors", null, body, true))
                .thenApply(this::decodeEnrollResult);
    }

    private Object decodeEnrollResult(JsonNode node) {
        if (node.has("factor_type") && "webauthn".equals(node.get("factor_type").asText())) {
            return transport.readValue(node, WebAuthnEnrollResult.class);
        }
        return transport.readValue(node, TotpEnrollResult.class);
    }

    /** {@code GET /auth/v1/factors} — list MFA factors (JWT-gated). */
    public List<FactorDescriptor> listFactors() {
        JsonNode node = transport.executeJson(transport.jsonGet("/auth/v1/factors", null, true));
        return transport.mapper.convertValue(node,
                transport.mapper.getTypeFactory()
                        .constructCollectionType(List.class, FactorDescriptor.class));
    }

    /** Async variant of {@link #listFactors}. */
    public CompletableFuture<List<FactorDescriptor>> listFactorsAsync() {
        return transport.executeJsonAsync(transport.jsonGet("/auth/v1/factors", null, true))
                .thenApply(n -> transport.mapper.convertValue(n,
                        transport.mapper.getTypeFactory()
                                .constructCollectionType(List.class, FactorDescriptor.class)));
    }

    /**
     * {@code POST /auth/v1/factors/:id/verify} — confirm enrollment (JWT-gated).
     *
     * @param factorId    UUID string returned by {@link #enrollFactor}
     * @param code        TOTP path: 6-digit OTP code; {@code null} for WebAuthn
     * @param attestation WebAuthn path: attestation JSON; {@code null} for TOTP
     * @param challengeId WebAuthn path: {@code challenge_id} from enrollment; {@code null} for TOTP
     */
    public VerifyFactorResult verifyFactor(String factorId, String code,
                                            String attestation, String challengeId) {
        java.util.HashMap<String, Object> body = new java.util.HashMap<>();
        if (code != null) body.put("code", code);
        if (attestation != null) body.put("attestation", attestation);
        if (challengeId != null) body.put("challenge_id", challengeId);
        JsonNode node = transport.executeJson(
                transport.jsonPost("/auth/v1/factors/" + factorId + "/verify", null, body, true));
        return transport.readValue(node, VerifyFactorResult.class);
    }

    /** Async variant of {@link #verifyFactor}. */
    public CompletableFuture<VerifyFactorResult> verifyFactorAsync(String factorId, String code,
                                                                    String attestation,
                                                                    String challengeId) {
        java.util.HashMap<String, Object> body = new java.util.HashMap<>();
        if (code != null) body.put("code", code);
        if (attestation != null) body.put("attestation", attestation);
        if (challengeId != null) body.put("challenge_id", challengeId);
        return transport.executeJsonAsync(
                transport.jsonPost("/auth/v1/factors/" + factorId + "/verify", null, body, true))
                .thenApply(n -> transport.readValue(n, VerifyFactorResult.class));
    }

    /**
     * {@code POST /auth/v1/factors/:id/challenge} — begin a step-up challenge (JWT-gated).
     *
     * @return {@link TotpChallengeResult} for TOTP or {@link WebAuthnChallengeResult} for WebAuthn
     */
    public Object challengeFactor(String factorId) {
        JsonNode node = transport.executeJson(
                transport.jsonPost("/auth/v1/factors/" + factorId + "/challenge", null, Map.of(), true));
        return decodeChallengeResult(node);
    }

    /** Async variant of {@link #challengeFactor}. */
    public CompletableFuture<Object> challengeFactorAsync(String factorId) {
        return transport.executeJsonAsync(
                transport.jsonPost("/auth/v1/factors/" + factorId + "/challenge", null, Map.of(), true))
                .thenApply(this::decodeChallengeResult);
    }

    private Object decodeChallengeResult(JsonNode node) {
        if (node.has("request_options_json") && !node.get("request_options_json").isNull()) {
            return transport.readValue(node, WebAuthnChallengeResult.class);
        }
        return transport.readValue(node, TotpChallengeResult.class);
    }

    /**
     * {@code POST /auth/v1/factors/:id/challenge/verify} — complete step-up → aal2 JWT.
     *
     * <p>Returns a new {@link Session} whose access token carries {@code aal2} in its
     * JWT claims, and stores it as the live session.
     *
     * @param factorId    UUID string of the factor being challenged
     * @param challengeId {@code challenge_id} returned by {@link #challengeFactor}
     * @param code        TOTP path: 6-digit OTP code; {@code null} for WebAuthn
     * @param assertion   WebAuthn path: assertion JSON; {@code null} for TOTP
     */
    public Session verifyChallenge(String factorId, String challengeId,
                                    String code, String assertion) {
        java.util.HashMap<String, Object> body = new java.util.HashMap<>();
        body.put("challenge_id", challengeId);
        if (code != null) body.put("code", code);
        if (assertion != null) body.put("assertion", assertion);
        JsonNode node = transport.executeJson(
                transport.jsonPost("/auth/v1/factors/" + factorId + "/challenge/verify",
                        null, body, true));
        Session s = transport.readValue(node, Session.class);
        this.session = s;
        return s;
    }

    /** Async variant of {@link #verifyChallenge}. */
    public CompletableFuture<Session> verifyChallengeAsync(String factorId, String challengeId,
                                                             String code, String assertion) {
        java.util.HashMap<String, Object> body = new java.util.HashMap<>();
        body.put("challenge_id", challengeId);
        if (code != null) body.put("code", code);
        if (assertion != null) body.put("assertion", assertion);
        return transport.executeJsonAsync(
                transport.jsonPost("/auth/v1/factors/" + factorId + "/challenge/verify",
                        null, body, true))
                .thenApply(n -> {
                    Session s = transport.readValue(n, Session.class);
                    this.session = s;
                    return s;
                });
    }

    /**
     * {@code DELETE /auth/v1/factors/:id} — unenroll a factor (requires aal2 JWT).
     */
    public JsonNode unenrollFactor(String factorId) {
        return transport.executeJson(
                transport.jsonDelete("/auth/v1/factors/" + factorId, null, null, true));
    }

    /** Async variant of {@link #unenrollFactor}. */
    public CompletableFuture<JsonNode> unenrollFactorAsync(String factorId) {
        return transport.executeJsonAsync(
                transport.jsonDelete("/auth/v1/factors/" + factorId, null, null, true));
    }
}
