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
    // NDJSON streaming
    // ------------------------------------------------------------------

    @Test
    void stream_decodes_ndjson_rows() throws Exception {
        // Three data rows followed by a cursor sentinel line.
        String ndjson =
                "{\"id\":1,\"name\":\"alice\"}\n"
                + "{\"id\":2,\"name\":\"bob\"}\n"
                + "{\"id\":3,\"name\":\"carol\"}\n"
                + "{\"_basin_next_cursor\":\"cur-xyz\"}\n";

        server.createContext("/rest/v1/users", exchange -> {
            // The server should see ?stream=true in the query string.
            String qs = exchange.getRequestURI().getRawQuery();
            assertTrue(qs != null && qs.contains("stream=true"),
                    "stream=true must be present in query: " + qs);
            byte[] body = ndjson.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/x-ndjson");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) { os.write(body); }
        });

        QueryBuilder.StreamResult sr = client.table("users").stream();
        List<Map<String, Object>> rows = sr.rows();

        assertEquals(3, rows.size(), "three data rows expected");
        assertEquals(1, ((Number) rows.get(0).get("id")).intValue());
        assertEquals("alice", rows.get(0).get("name"));
        assertEquals(2, ((Number) rows.get(1).get("id")).intValue());
        assertEquals(3, ((Number) rows.get(2).get("id")).intValue());
        assertEquals("cur-xyz", sr.nextCursor(), "cursor extracted from sentinel line");
    }

    @Test
    void stream_no_cursor_when_sentinel_absent() throws Exception {
        String ndjson = "{\"id\":10}\n{\"id\":20}\n";

        server.createContext("/rest/v1/items", exchange -> {
            byte[] body = ndjson.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/x-ndjson");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) { os.write(body); }
        });

        QueryBuilder.StreamResult sr = client.table("items").stream();
        assertEquals(2, sr.rows().size());
        assertNull(sr.nextCursor(), "no cursor when sentinel line absent");
    }

    @Test
    void stream_empty_response() throws Exception {
        server.createContext("/rest/v1/empty_table", exchange -> {
            exchange.getResponseHeaders().set("Content-Type", "application/x-ndjson");
            exchange.sendResponseHeaders(200, 0);
            exchange.getResponseBody().close();
        });

        QueryBuilder.StreamResult sr = client.table("empty_table").stream();
        assertTrue(sr.rows().isEmpty());
        assertNull(sr.nextCursor());
    }

    @Test
    void stream_propagates_server_error() throws Exception {
        server.createContext("/rest/v1/forbidden_stream", exchange -> {
            byte[] body = mapper.writeValueAsBytes(
                    Map.of("code", "E_FORBIDDEN", "message", "not allowed"));
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(403, body.length);
            try (OutputStream os = exchange.getResponseBody()) { os.write(body); }
        });

        BasinApiException ex = assertThrows(BasinApiException.class,
                () -> client.table("forbidden_stream").stream());
        assertEquals("E_FORBIDDEN", ex.code);
        assertEquals(403, ex.status);
    }

    @Test
    void stream_iterable_in_foreach() throws Exception {
        String ndjson = "{\"v\":1}\n{\"v\":2}\n{\"v\":3}\n";
        server.createContext("/rest/v1/nums", exchange -> {
            byte[] body = ndjson.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/x-ndjson");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) { os.write(body); }
        });

        int sum = 0;
        for (Map<String, Object> row : client.table("nums").stream()) {
            sum += ((Number) row.get("v")).intValue();
        }
        assertEquals(6, sum, "1+2+3=6");
    }

    // ------------------------------------------------------------------
    // Arrow IPC transport
    // ------------------------------------------------------------------

    /**
     * Encode a minimal Arrow IPC stream with two rows and two columns
     * (id: Int64, name: Utf8) for use in Arrow tests.
     */
    private byte[] buildArrowIpcBytes() throws IOException {
        org.apache.arrow.memory.RootAllocator allocator =
                new org.apache.arrow.memory.RootAllocator(Long.MAX_VALUE);

        List<org.apache.arrow.vector.types.pojo.Field> fields = List.of(
                org.apache.arrow.vector.types.pojo.Field.nullable("id",
                        new org.apache.arrow.vector.types.pojo.ArrowType.Int(64, true)),
                org.apache.arrow.vector.types.pojo.Field.nullable("name",
                        new org.apache.arrow.vector.types.pojo.ArrowType.Utf8()));
        org.apache.arrow.vector.types.pojo.Schema schema =
                new org.apache.arrow.vector.types.pojo.Schema(fields);

        try (org.apache.arrow.vector.VectorSchemaRoot root =
                     org.apache.arrow.vector.VectorSchemaRoot.create(schema, allocator)) {
            root.allocateNew();

            org.apache.arrow.vector.BigIntVector ids =
                    (org.apache.arrow.vector.BigIntVector) root.getVector("id");
            org.apache.arrow.vector.VarCharVector names =
                    (org.apache.arrow.vector.VarCharVector) root.getVector("name");

            ids.set(0, 101L);
            ids.set(1, 202L);
            names.set(0, "alice".getBytes(StandardCharsets.UTF_8));
            names.set(1, "bob".getBytes(StandardCharsets.UTF_8));
            ids.setValueCount(2);
            names.setValueCount(2);
            root.setRowCount(2);

            java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
            try (org.apache.arrow.vector.ipc.ArrowStreamWriter writer =
                         new org.apache.arrow.vector.ipc.ArrowStreamWriter(root, null, baos)) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }
            return baos.toByteArray();
        }
    }

    @Test
    void toArrow_native_ipc_path() throws Exception {
        byte[] ipcBytes = buildArrowIpcBytes();

        server.createContext("/rest/v1/arrow_users", exchange -> {
            // Verify the client sent the Arrow Accept header.
            String accept = exchange.getRequestHeaders().getFirst("Accept");
            assertEquals("application/vnd.apache.arrow.stream", accept,
                    "client must send Arrow Accept header");

            exchange.getResponseHeaders().set("Content-Type", "application/vnd.apache.arrow.stream");
            exchange.getResponseHeaders().set("X-Basin-Next-Cursor", "ipc-cursor-1");
            exchange.getResponseHeaders().set("X-Basin-Row-Count", "2");
            exchange.sendResponseHeaders(200, ipcBytes.length);
            try (OutputStream os = exchange.getResponseBody()) { os.write(ipcBytes); }
        });

        try (ArrowResult ar = client.table("arrow_users").toArrow()) {
            assertEquals(2, ar.root.getRowCount(), "two rows decoded");
            assertEquals("ipc-cursor-1", ar.nextCursor, "cursor from header");
            assertEquals(2L, ar.rowCount, "row count from header");

            org.apache.arrow.vector.BigIntVector idVec =
                    (org.apache.arrow.vector.BigIntVector) ar.root.getVector("id");
            assertEquals(101L, idVec.get(0));
            assertEquals(202L, idVec.get(1));

            org.apache.arrow.vector.VarCharVector nameVec =
                    (org.apache.arrow.vector.VarCharVector) ar.root.getVector("name");
            assertEquals("alice", nameVec.getObject(0).toString());
            assertEquals("bob", nameVec.getObject(1).toString());
        }
    }

    @Test
    void toArrow_json_fallback() throws Exception {
        // Server returns plain JSON — SDK must convert client-side.
        List<Map<String, Object>> jsonRows = List.of(
                Map.of("id", 1, "score", 9.5, "active", true),
                Map.of("id", 2, "score", 7.0, "active", false));

        server.createContext("/rest/v1/scores", exchange -> {
            // Accept header from client should be the Arrow MIME type.
            String accept = exchange.getRequestHeaders().getFirst("Accept");
            assertEquals("application/vnd.apache.arrow.stream", accept);

            // But server replies with JSON (older server / fallback scenario).
            byte[] body = mapper.writeValueAsBytes(jsonRows);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) { os.write(body); }
        });

        try (ArrowResult ar = client.table("scores").toArrow()) {
            assertEquals(2, ar.root.getRowCount(), "two rows in fallback Arrow table");
            assertNull(ar.nextCursor, "no cursor when header absent");
            // id column inferred as BigInt (Integer → long).
            org.apache.arrow.vector.BigIntVector idVec =
                    (org.apache.arrow.vector.BigIntVector) ar.root.getVector("id");
            assertNotNull(idVec, "id column present");
            assertEquals(1L, idVec.get(0));
            assertEquals(2L, idVec.get(1));
        }
    }

    @Test
    void toArrow_propagates_server_error() throws Exception {
        server.createContext("/rest/v1/arrow_locked", exchange -> {
            byte[] body = mapper.writeValueAsBytes(
                    Map.of("code", "E_FORBIDDEN", "message", "no arrow for you"));
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(403, body.length);
            try (OutputStream os = exchange.getResponseBody()) { os.write(body); }
        });

        BasinApiException ex = assertThrows(BasinApiException.class,
                () -> client.table("arrow_locked").toArrow());
        assertEquals("E_FORBIDDEN", ex.code);
        assertEquals(403, ex.status);
    }

    @Test
    void toArrow_no_cursor_when_header_absent() throws Exception {
        byte[] ipcBytes = buildArrowIpcBytes();

        server.createContext("/rest/v1/arrow_no_cursor", exchange -> {
            exchange.getResponseHeaders().set("Content-Type", "application/vnd.apache.arrow.stream");
            // No X-Basin-Next-Cursor header.
            exchange.sendResponseHeaders(200, ipcBytes.length);
            try (OutputStream os = exchange.getResponseBody()) { os.write(ipcBytes); }
        });

        try (ArrowResult ar = client.table("arrow_no_cursor").toArrow()) {
            assertNull(ar.nextCursor, "null cursor when header absent");
        }
    }

    @Test
    void toArrow_empty_cursor_header_treated_as_null() throws Exception {
        byte[] ipcBytes = buildArrowIpcBytes();

        server.createContext("/rest/v1/arrow_empty_cursor", exchange -> {
            exchange.getResponseHeaders().set("Content-Type", "application/vnd.apache.arrow.stream");
            exchange.getResponseHeaders().set("X-Basin-Next-Cursor", "");
            exchange.sendResponseHeaders(200, ipcBytes.length);
            try (OutputStream os = exchange.getResponseBody()) { os.write(ipcBytes); }
        });

        try (ArrowResult ar = client.table("arrow_empty_cursor").toArrow()) {
            assertNull(ar.nextCursor, "empty cursor header normalised to null");
        }
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
