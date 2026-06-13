package io.basin.sdk;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link BasinClient} using a local {@link HttpServer} as a
 * mock — zero external dependencies, no running Basin instance needed.
 *
 * <p>Each test registers a handler for a specific path and verifies that the SDK:
 * <ol>
 *   <li>Constructs the correct request URL
 *   <li>Sends the expected request body / headers
 *   <li>Decodes the response into the correct type
 *   <li>Propagates error envelopes as {@link BasinApiException}
 * </ol>
 */
class BasinClientTest {

    private HttpServer server;
    private String baseUrl;
    private BasinClient client;
    private final ObjectMapper mapper = new ObjectMapper();

    @BeforeEach
    void setUp() throws IOException {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.start();
        int port = server.getAddress().getPort();
        baseUrl = "http://localhost:" + port;
        client = BasinClient.builder()
                .url(baseUrl)
                .apiKey("test-key")
                .projectId("proj-01")
                .build();
    }

    @AfterEach
    void tearDown() {
        client.close();
        server.stop(0);
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private void registerJson(String path, int status, Object body) {
        server.createContext(path, exchange -> {
            byte[] bytes = mapper.writeValueAsBytes(body);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        });
    }

    private void registerRaw(String path, int status, String contentType, byte[] body) {
        server.createContext(path, exchange -> {
            exchange.getResponseHeaders().set("Content-Type", contentType);
            exchange.sendResponseHeaders(status, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
    }

    private String readBody(HttpExchange exchange) throws IOException {
        return new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
    }

    // ------------------------------------------------------------------
    // Health
    // ------------------------------------------------------------------

    @Test
    void health_returns_ok() throws Exception {
        registerRaw("/health", 200, "text/plain", "ok".getBytes(StandardCharsets.UTF_8));
        String result = client.healthBlocking();
        assertEquals("ok", result);
    }

    // ------------------------------------------------------------------
    // Auth — sign-up
    // ------------------------------------------------------------------

    @Test
    void signUp_parses_result() throws Exception {
        registerJson("/auth/v1/signup", 201,
                Map.of("ok", true, "user_id", "user-abc"));
        SignUpResult r = client.auth.signUp("a@b.com", "pass", "proj-01");
        assertTrue(r.ok);
        assertEquals("user-abc", r.userId);
    }

    // ------------------------------------------------------------------
    // Auth — sign-in stores session
    // ------------------------------------------------------------------

    @Test
    void signIn_stores_session() throws Exception {
        registerJson("/auth/v1/signin", 200, Map.of(
                "access_token", "acc.tok",
                "refresh_token", "ref.tok",
                "access_expires_at", "2099-01-01T00:00:00Z",
                "refresh_expires_at", "2099-01-01T00:00:00Z"));
        Session s = client.auth.signIn("a@b.com", "pass", "proj-01");
        assertEquals("acc.tok", s.accessToken);
        assertSame(s, client.auth.getSession());
    }

    // ------------------------------------------------------------------
    // Auth — sign-out clears session even on 404
    // ------------------------------------------------------------------

    @Test
    void signOut_clears_session_on_404() throws Exception {
        // Put a session in manually
        client.auth.setSession(new Session("acc", "ref",
                "2099-01-01T00:00:00Z", "2099-01-01T00:00:00Z"));

        registerJson("/auth/v1/signout", 404,
                Map.of("code", "E_NOT_FOUND", "message", "no signout route"));

        client.auth.signOut();
        assertNull(client.auth.getSession());
    }

    // ------------------------------------------------------------------
    // Auth — refresh session
    // ------------------------------------------------------------------

    @Test
    void refreshSession_updates_session() throws Exception {
        client.auth.setSession(new Session("old-acc", "old-ref",
                "2000-01-01T00:00:00Z", "2099-01-01T00:00:00Z"));

        registerJson("/auth/v1/refresh", 200, Map.of(
                "access_token", "new-acc",
                "refresh_token", "new-ref",
                "access_expires_at", "2099-01-01T00:00:00Z",
                "refresh_expires_at", "2099-01-01T00:00:00Z"));

        Session refreshed = client.auth.refreshSession();
        assertEquals("new-acc", refreshed.accessToken);
        assertEquals("new-acc", client.auth.getSession().accessToken);
    }

    // ------------------------------------------------------------------
    // Auth — auto-refresh on accessToken()
    // ------------------------------------------------------------------

    @Test
    void accessToken_auto_refreshes_near_expiry() throws Exception {
        // Token expired in the past → should trigger refresh
        client.auth.setSession(new Session("expired-acc", "ref-tok",
                "2000-01-01T00:00:00Z", "2099-01-01T00:00:00Z"));

        registerJson("/auth/v1/refresh", 200, Map.of(
                "access_token", "fresh-acc",
                "refresh_token", "new-ref",
                "access_expires_at", "2099-01-01T00:00:00Z",
                "refresh_expires_at", "2099-01-01T00:00:00Z"));

        String token = client.auth.accessToken();
        assertEquals("fresh-acc", token);
    }

    // ------------------------------------------------------------------
    // Auth — magic link
    // ------------------------------------------------------------------

    @Test
    void requestMagicLink_sends_post() throws Exception {
        final String[] capturedBody = {null};
        server.createContext("/auth/v1/magic-link", exchange -> {
            try {
                capturedBody[0] = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
                byte[] resp = mapper.writeValueAsBytes(Map.of());
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(204, resp.length);
                try (OutputStream os = exchange.getResponseBody()) { os.write(resp); }
            } catch (Exception e) {
                exchange.sendResponseHeaders(500, 0);
            }
        });
        client.auth.requestMagicLink("alice@example.com");
        assertNotNull(capturedBody[0]);
        assertTrue(capturedBody[0].contains("alice@example.com"));
    }

    // ------------------------------------------------------------------
    // Auth — API keys
    // ------------------------------------------------------------------

    @Test
    void createApiKey_returns_issued_key() throws Exception {
        registerJson("/auth/v1/api-keys", 201, Map.of(
                "id", 42, "name", "ci", "secret", "sec_xxx", "created_at", "2024-01-01T00:00:00Z"));
        ApiKeyIssued issued = client.auth.createApiKey("ci");
        assertEquals(42L, issued.id);
        assertEquals("sec_xxx", issued.secret);
    }

    @Test
    void listApiKeys_parses_array() throws Exception {
        registerJson("/auth/v1/api-keys", 200, List.of(
                Map.of("id", 1, "name", "k1", "created_at", "2024-01-01T00:00:00Z"),
                Map.of("id", 2, "name", "k2", "created_at", "2024-01-01T00:00:00Z")));
        List<ApiKeyDescriptor> keys = client.auth.listApiKeys();
        assertEquals(2, keys.size());
        assertEquals("k1", keys.get(0).name);
    }

    // ------------------------------------------------------------------
    // Auth — OAuth
    // ------------------------------------------------------------------

    @Test
    void getOAuthAuthorizeUrl_returns_result() throws Exception {
        registerJson("/auth/v1/oauth/google/authorize", 200, Map.of(
                "redirect_url", "https://accounts.google.com/o/oauth2/v2/auth?state=xyz",
                "state", "xyz"));
        OAuthAuthorizeResult r = client.auth.getOAuthAuthorizeUrl("google", "", "proj-01");
        assertTrue(r.redirectUrl.contains("google.com"));
        assertEquals("xyz", r.state);
    }

    // ------------------------------------------------------------------
    // Auth — MFA TOTP
    // ------------------------------------------------------------------

    @Test
    void enrollFactor_totp_returns_result() throws Exception {
        registerJson("/auth/v1/factors", 201, Map.of(
                "factor_id", "fid-1",
                "factor_type", "totp",
                "secret_b32", "JBSWY3DP",
                "otpauth_uri", "otpauth://totp/Basin:alice?secret=JBSWY3DP"));
        Object result = client.auth.enrollFactor("totp", "My App");
        assertInstanceOf(TotpEnrollResult.class, result);
        TotpEnrollResult r = (TotpEnrollResult) result;
        assertEquals("JBSWY3DP", r.secretB32);
    }

    @Test
    void enrollFactor_webauthn_returns_result() throws Exception {
        registerJson("/auth/v1/factors", 201, Map.of(
                "factor_id", "fid-2",
                "factor_type", "webauthn",
                "challenge_id", "ch-1",
                "creation_options_json", "{\"rp\":{}}"));
        Object result = client.auth.enrollFactor("webauthn", "YubiKey");
        assertInstanceOf(WebAuthnEnrollResult.class, result);
    }

    @Test
    void challengeFactor_totp_returns_challenge_id() throws Exception {
        registerJson("/auth/v1/factors/fid-1/challenge", 200,
                Map.of("challenge_id", "ch-abc"));
        Object result = client.auth.challengeFactor("fid-1");
        assertInstanceOf(TotpChallengeResult.class, result);
        assertEquals("ch-abc", ((TotpChallengeResult) result).challengeId);
    }

    @Test
    void verifyChallenge_stores_aal2_session() throws Exception {
        registerJson("/auth/v1/factors/fid-1/challenge/verify", 200, Map.of(
                "access_token", "aal2-acc",
                "refresh_token", "aal2-ref",
                "access_expires_at", "2099-01-01T00:00:00Z",
                "refresh_expires_at", "2099-01-01T00:00:00Z"));
        Session s = client.auth.verifyChallenge("fid-1", "ch-abc", "654321", null);
        assertEquals("aal2-acc", s.accessToken);
        assertSame(s, client.auth.getSession());
    }

    // ------------------------------------------------------------------
    // Query builder
    // ------------------------------------------------------------------

    @Test
    void queryBuilder_select_eq_limit() throws Exception {
        final String[] capturedPath = {null};
        server.createContext("/rest/v1/orders", exchange -> {
            capturedPath[0] = exchange.getRequestURI().toString();
            byte[] resp = mapper.writeValueAsBytes(
                    List.of(Map.of("id", 1, "total", 200)));
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, resp.length);
            try (OutputStream os = exchange.getResponseBody()) { os.write(resp); }
        });

        QueryResult result = client.table("orders")
                .select("id,total")
                .eq("status", "paid")
                .limit(10)
                .executeBlocking();

        assertEquals(1, result.rows.size());
        assertNotNull(capturedPath[0]);
        assertTrue(capturedPath[0].contains("select=id%2Ctotal")
                || capturedPath[0].contains("select=id,total"), "select param present");
        assertTrue(capturedPath[0].contains("status=eq.paid"), "eq filter present");
        assertTrue(capturedPath[0].contains("limit=10"), "limit present");
    }

    @Test
    void queryBuilder_paginated_response() throws Exception {
        registerJson("/rest/v1/events", 200, Map.of(
                "rows", List.of(Map.of("id", 1), Map.of("id", 2)),
                "next_cursor", "cursor-abc"));
        QueryResult result = client.table("events").limit(2).executeBlocking();
        assertEquals(2, result.rows.size());
        assertEquals("cursor-abc", result.nextCursor);
    }

    @Test
    void queryBuilder_insert() throws Exception {
        final String[] body = {null};
        server.createContext("/rest/v1/orders", exchange -> {
            if (!"POST".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, 0);
                return;
            }
            body[0] = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            byte[] resp = mapper.writeValueAsBytes(Map.of("ok", true, "tag", "INSERT 0 1"));
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(201, resp.length);
            try (OutputStream os = exchange.getResponseBody()) { os.write(resp); }
        });

        client.table("orders").insertBlocking(Map.of("total", 50, "status", "new"));
        assertNotNull(body[0]);
        assertTrue(body[0].contains("total"));
    }

    @Test
    void queryBuilder_in_filter() throws Exception {
        final String[] capturedPath = {null};
        server.createContext("/rest/v1/products", exchange -> {
            capturedPath[0] = exchange.getRequestURI().getRawQuery();
            byte[] resp = mapper.writeValueAsBytes(List.of());
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, resp.length);
            try (OutputStream os = exchange.getResponseBody()) { os.write(resp); }
        });

        client.table("products").in("id", List.of(1, 2, 3)).executeBlocking();
        assertNotNull(capturedPath[0]);
        // Should contain in.(1,2,3) encoded
        assertTrue(capturedPath[0].contains("in."), "in. operator present in query: " + capturedPath[0]);
    }

    // ------------------------------------------------------------------
    // Error handling
    // ------------------------------------------------------------------

    @Test
    void error_envelope_decoded_as_BasinApiException() throws Exception {
        registerJson("/rest/v1/locked", 403, Map.of(
                "code", "E_FORBIDDEN", "message", "access denied"));
        BasinApiException ex = assertThrows(BasinApiException.class,
                () -> client.table("locked").executeBlocking());
        assertEquals("E_FORBIDDEN", ex.code);
        assertEquals(403, ex.status);
    }

    @Test
    void error_with_sqlstate_decoded() throws Exception {
        registerJson("/rest/v1/orders", 422, Map.of(
                "code", "E_INVALID_REQUEST",
                "message", "duplicate key",
                "sqlstate", "23505"));
        // Trigger via insert
        BasinApiException ex = assertThrows(BasinApiException.class,
                () -> client.table("orders").insertBlocking(Map.of("id", 1)));
        assertEquals("23505", ex.sqlState);
    }

    @Test
    void refreshSession_throws_when_no_session() {
        assertNull(client.auth.getSession());
        BasinApiException ex = assertThrows(BasinApiException.class,
                () -> client.auth.refreshSession());
        assertEquals("E_UNAUTHENTICATED", ex.code);
    }

    // ------------------------------------------------------------------
    // RPC
    // ------------------------------------------------------------------

    @Test
    void rpc_posts_to_correct_path() throws Exception {
        final String[] capturedPath = {null};
        server.createContext("/rest/v1/rpc/add_two", exchange -> {
            capturedPath[0] = exchange.getRequestURI().getPath();
            byte[] resp = mapper.writeValueAsBytes(42);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, resp.length);
            try (OutputStream os = exchange.getResponseBody()) { os.write(resp); }
        });

        var result = client.rpcBlocking("add_two", Map.of("a", 1, "b", 41));
        assertEquals("/rest/v1/rpc/add_two", capturedPath[0]);
        assertEquals(42, result.intValue());
    }

    // ------------------------------------------------------------------
    // Storage
    // ------------------------------------------------------------------

    @Test
    void createBucket_returns_bucket() throws Exception {
        registerJson("/storage/v1/bucket", 201, Map.of(
                "id", "bkt-1", "name", "avatars", "public", true,
                "allowed_mime_types", List.of("image/png"),
                "created_at", "2024-01-01T00:00:00Z",
                "updated_at", "2024-01-01T00:00:00Z"));
        StorageBucket b = client.storage.createBucketBlocking("avatars", true, null, null);
        assertEquals("avatars", b.name);
        assertTrue(b.isPublic);
    }

    @Test
    void uploadObject_parses_storage_object() throws Exception {
        server.createContext("/storage/v1/object/avatars/users/alice.png", exchange -> {
            if (!"POST".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, 0);
                return;
            }
            byte[] resp = mapper.writeValueAsBytes(Map.of(
                    "id", "obj-1", "bucket_id", "bkt-1", "path", "users/alice.png",
                    "size", 1024, "etag", "abc123",
                    "created_at", "2024-01-01T00:00:00Z",
                    "updated_at", "2024-01-01T00:00:00Z"));
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, resp.length);
            try (OutputStream os = exchange.getResponseBody()) { os.write(resp); }
        });

        byte[] data = "hello".getBytes(StandardCharsets.UTF_8);
        StorageObject obj = client.storage.fromBucket("avatars")
                .uploadBlocking("users/alice.png", data, "image/png");
        assertEquals("users/alice.png", obj.path);
        assertEquals(1024L, obj.size);
    }

    @Test
    void getPublicUrl_builds_correct_url() {
        String url = client.storage.fromBucket("avatars").getPublicUrl("users/alice.png");
        assertEquals(baseUrl + "/storage/v1/object/public/proj-01/avatars/users/alice.png", url);
    }

    @Test
    void createSignedUrl_returns_absolute_url() throws Exception {
        registerJson("/storage/v1/object/sign/upload/avatars/users/alice.png", 200, Map.of(
                "signedUrl", "/storage/v1/object/sign/proj-01/avatars/users/alice.png?token=xxx",
                "expiresAt", "2024-12-31T00:00:00Z"));

        SignedUrl signed = client.storage.fromBucket("avatars")
                .createSignedUrlBlocking("users/alice.png", 3600);

        assertNotNull(signed.signedUrl);
        assertTrue(signed.absoluteUrl.startsWith(baseUrl));
    }

    @Test
    void listObjects_returns_list() throws Exception {
        registerJson("/storage/v1/object/list/avatars", 200, List.of(
                Map.of("id", "o1", "bucket_id", "b1", "path", "a.png",
                        "size", 100, "etag", "e1",
                        "created_at", "2024-01-01T00:00:00Z",
                        "updated_at", "2024-01-01T00:00:00Z"),
                Map.of("id", "o2", "bucket_id", "b1", "path", "b.png",
                        "size", 200, "etag", "e2",
                        "created_at", "2024-01-01T00:00:00Z",
                        "updated_at", "2024-01-01T00:00:00Z")));
        List<StorageObject> objects = client.storage.fromBucket("avatars")
                .listBlocking(null, null, null);
        assertEquals(2, objects.size());
        assertEquals("a.png", objects.get(0).path);
    }

    // ------------------------------------------------------------------
    // Functions
    // ------------------------------------------------------------------

    @Test
    void invoke_proxies_function_response() throws Exception {
        server.createContext("/fn/v1/resize", exchange -> {
            byte[] resp = "{\"width\":100}".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, resp.length);
            try (OutputStream os = exchange.getResponseBody()) { os.write(resp); }
        });

        FunctionInvokeResult r = client.functions.invokeBlocking(
                "resize", "POST", Map.of("width", 100), null);
        assertEquals(200, r.status);
        assertNotNull(r.data);
    }

    @Test
    void invoke_raises_BasinApiException_on_error_envelope() throws Exception {
        server.createContext("/fn/v1/badone", exchange -> {
            byte[] resp = mapper.writeValueAsBytes(
                    Map.of("code", "E_NOT_FOUND", "message", "function not found"));
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(404, resp.length);
            try (OutputStream os = exchange.getResponseBody()) { os.write(resp); }
        });

        BasinApiException ex = assertThrows(BasinApiException.class,
                () -> client.functions.invokeBlocking("badone", "POST", null, null));
        assertEquals("E_NOT_FOUND", ex.code);
    }

    // ------------------------------------------------------------------
    // Builder validation
    // ------------------------------------------------------------------

    @Test
    void builder_throws_without_url() {
        assertThrows(IllegalStateException.class,
                () -> BasinClient.builder().apiKey("k").build());
    }

    // ------------------------------------------------------------------
    // JWT project_id extraction
    // ------------------------------------------------------------------

    @Test
    void projectIdFromJwt_extracts_claim() {
        // Build a minimal JWT with project_id in payload (unsigned — SDK only reads, never verifies).
        // Payload: {"project_id":"proj-abc","exp":9999999999}
        String payload = java.util.Base64.getUrlEncoder().withoutPadding()
                .encodeToString("{\"project_id\":\"proj-abc\",\"exp\":9999999999}".getBytes());
        String token = "header." + payload + ".sig";
        assertEquals("proj-abc", AuthClient.projectIdFromJwt(token));
    }

    @Test
    void projectIdFromJwt_returns_null_for_raw_key() {
        assertNull(AuthClient.projectIdFromJwt("raw-api-key-not-a-jwt"));
    }
}
