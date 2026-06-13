package io.basin.sdk;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Top-level Basin SDK client.
 *
 * <p>Thread-safe; safe for concurrent use from multiple threads.
 *
 * <h2>Quickstart (async)</h2>
 * <pre>{@code
 * BasinClient client = BasinClient.builder()
 *     .url("https://your-project.basin.run")
 *     .apiKey("your-api-key")
 *     .projectId("01JWXXX...")
 *     .build();
 *
 * // Query
 * QueryResult result = client.table("orders")
 *     .select("id,total,status")
 *     .eq("status", "paid")
 *     .gte("total", "100")
 *     .limit(50)
 *     .execute()
 *     .join();
 *
 * // Insert
 * client.table("orders")
 *     .insert(Map.of("total", 50, "status", "new"))
 *     .join();
 *
 * // RPC
 * JsonNode result = client.rpc("add_two", Map.of("a", 1, "b", 2)).join();
 *
 * client.close();
 * }</pre>
 *
 * <h2>Quickstart (blocking)</h2>
 * <pre>{@code
 * QueryResult result = client.table("orders").limit(100).executeBlocking();
 * }</pre>
 *
 * <p>The client holds an {@link HttpClient} instance and should be closed when
 * done to release its thread pool. Use try-with-resources:
 * <pre>{@code
 * try (BasinClient client = BasinClient.builder()...build()) {
 *     // …
 * }
 * }</pre>
 */
public final class BasinClient implements AutoCloseable {

    /** Auth client for {@code /auth/v1/*} routes. */
    public final AuthClient auth;

    /** Storage client for {@code /storage/v1/*} routes. */
    public final StorageClient storage;

    /** Functions client for {@code /fn/v1/*} and {@code /rest/v1/rpc/*}. */
    public final FunctionsClient functions;

    private final BasinTransport transport;
    private final String explicitProjectId;
    private final String staticKey;

    private volatile RealtimeClient realtimeClient;
    private final Object realtimeLock = new Object();

    private BasinClient(Builder builder) {
        ObjectMapper mapper = builder.objectMapper != null
                ? builder.objectMapper
                : defaultMapper();

        HttpClient httpClient = builder.httpClient != null
                ? builder.httpClient
                : HttpClient.newBuilder()
                        .connectTimeout(builder.timeout)
                        .build();

        this.staticKey = builder.apiKey;
        this.explicitProjectId = builder.projectId;

        this.transport = new BasinTransport(builder.url, builder.apiKey, httpClient, mapper);

        this.auth = new AuthClient(transport, builder.projectId);

        // Wire the transport's token supplier to the auth client, so every
        // authed request automatically uses the freshest session access token.
        transport.setTokenSupplier(auth::accessToken);

        this.storage = new StorageClient(transport, this::resolveProjectId);
        this.functions = new FunctionsClient(transport);
    }

    // ------------------------------------------------------------------
    // Project ID resolution
    // ------------------------------------------------------------------

    String resolveProjectId() {
        if (explicitProjectId != null && !explicitProjectId.isEmpty()) {
            return explicitProjectId;
        }
        Session s = auth.getSession();
        if (s != null) {
            String fromSession = AuthClient.projectIdFromJwt(s.accessToken);
            if (fromSession != null && !fromSession.isEmpty()) return fromSession;
        }
        if (staticKey != null) {
            String fromKey = AuthClient.projectIdFromJwt(staticKey);
            if (fromKey != null && !fromKey.isEmpty()) return fromKey;
        }
        return null;
    }

    // ------------------------------------------------------------------
    // Query API
    // ------------------------------------------------------------------

    /**
     * Return a {@link QueryBuilder} for {@code /rest/v1/:table}.
     *
     * @param name table name
     */
    public QueryBuilder table(String name) {
        return new QueryBuilder(transport, name);
    }

    /** Alias for {@link #table(String)}, matching JS SDK {@code from()} style. */
    public QueryBuilder from(String name) {
        return table(name);
    }

    // ------------------------------------------------------------------
    // RPC convenience
    // ------------------------------------------------------------------

    /**
     * {@code POST /rest/v1/rpc/:fn_name} — convenience alias for {@code functions.rpc(…)}.
     *
     * @param fnName function name
     * @param args   named arguments; {@code null} for no args
     */
    public CompletableFuture<com.fasterxml.jackson.databind.JsonNode> rpc(
            String fnName, java.util.Map<String, Object> args) {
        return functions.rpc(fnName, args);
    }

    /** Blocking variant of {@link #rpc}. */
    public com.fasterxml.jackson.databind.JsonNode rpcBlocking(
            String fnName, java.util.Map<String, Object> args) {
        return rpc(fnName, args).join();
    }

    // ------------------------------------------------------------------
    // Health
    // ------------------------------------------------------------------

    /**
     * {@code GET /health} → {@code "ok"}.
     */
    public CompletableFuture<String> health() {
        return transport.executeRawAsync(
                transport.jsonGet("/health", null, false))
                .thenApply(resp -> new String(resp.body(), java.nio.charset.StandardCharsets.UTF_8).strip());
    }

    /** Blocking variant of {@link #health()}. */
    public String healthBlocking() {
        return health().join();
    }

    // ------------------------------------------------------------------
    // Realtime (lazy)
    // ------------------------------------------------------------------

    /**
     * Lazy {@link RealtimeClient} for {@code GET /realtime/v1/ws/:project}.
     *
     * <p>The WebSocket connection is not opened until the first call to
     * {@link RealtimeClient#subscribe} or {@link RealtimeClient#connect}.
     */
    public RealtimeClient realtime() {
        if (realtimeClient == null) {
            synchronized (realtimeLock) {
                if (realtimeClient == null) {
                    realtimeClient = new RealtimeClient(
                            transport.baseUrl,
                            auth::accessToken,
                            this::resolveProjectId,
                            transport.httpClient,
                            transport.mapper);
                }
            }
        }
        return realtimeClient;
    }

    // ------------------------------------------------------------------
    // Escape hatch
    // ------------------------------------------------------------------

    /**
     * Execute an arbitrary request against the Basin API.
     *
     * <p>Use for routes not yet wrapped by the SDK (e.g. {@code /admin/v1/*}).
     *
     * @param method HTTP method
     * @param path   server-relative path, e.g. {@code "/admin/v1/projects"}
     * @param body   request body; serialised as JSON when non-null
     * @return parsed JSON response node
     */
    public CompletableFuture<com.fasterxml.jackson.databind.JsonNode> request(
            String method, String path, Object body) {
        java.net.http.HttpRequest req = switch (method.toUpperCase()) {
            case "GET"    -> transport.jsonGet(path, null, true);
            case "POST"   -> transport.jsonPost(path, null, body, true);
            case "PATCH"  -> transport.jsonPatch(path, null, body, true);
            case "DELETE" -> transport.jsonDelete(path, null, body, true);
            default       -> transport.anyMethod(method.toUpperCase(), path,
                    body != null ? transport.toBytes(body) : null, null, true);
        };
        return transport.executeJsonAsync(req);
    }

    // ------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------

    /** Release the underlying HTTP client thread pool and close any realtime connection. */
    @Override
    public void close() {
        if (realtimeClient != null) {
            realtimeClient.close();
        }
        // java.net.http.HttpClient has no public close() before Java 21.
        // For Java 21+ it implements AutoCloseable; we guard with instanceof.
        if (transport.httpClient instanceof AutoCloseable ac) {
            try {
                ac.close();
            } catch (Exception ignored) {
            }
        }
    }

    // ------------------------------------------------------------------
    // Builder
    // ------------------------------------------------------------------

    /** Create a new {@link Builder}. */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Fluent builder for {@link BasinClient}.
     *
     * <pre>{@code
     * BasinClient client = BasinClient.builder()
     *     .url("https://your-project.basin.run")
     *     .apiKey("your-api-key")
     *     .projectId("01JWXXX...")
     *     .build();
     * }</pre>
     */
    public static final class Builder {
        private String url;
        private String apiKey;
        private String projectId;
        private Duration timeout = Duration.ofSeconds(30);
        private HttpClient httpClient;
        private ObjectMapper objectMapper;

        private Builder() {}

        /** Basin server base URL, e.g. {@code "https://your-project.basin.run"}. */
        public Builder url(String url) {
            this.url = url;
            return this;
        }

        /**
         * JWT or raw API key — sent as {@code Authorization: Bearer <key>}.
         * Pass {@code null} if you will authenticate later via {@link AuthClient#signIn}.
         */
        public Builder apiKey(String apiKey) {
            this.apiKey = apiKey;
            return this;
        }

        /**
         * Default project ULID. Optional when the API key is a JWT that carries
         * a {@code project_id} claim, or when using auth session tokens.
         * Required for public storage URLs and realtime.
         */
        public Builder projectId(String projectId) {
            this.projectId = projectId;
            return this;
        }

        /** Default per-request timeout. Default: 30 seconds. */
        public Builder timeout(Duration timeout) {
            this.timeout = timeout;
            return this;
        }

        /** Custom {@link HttpClient} for testing or advanced transport config. */
        public Builder httpClient(HttpClient httpClient) {
            this.httpClient = httpClient;
            return this;
        }

        /** Custom Jackson {@link ObjectMapper}. Defaults to a standard instance. */
        public Builder objectMapper(ObjectMapper objectMapper) {
            this.objectMapper = objectMapper;
            return this;
        }

        /** Build the {@link BasinClient}. Throws {@link IllegalStateException} if url is null. */
        public BasinClient build() {
            if (url == null || url.isEmpty()) {
                throw new IllegalStateException("BasinClient.Builder: url must be set");
            }
            return new BasinClient(this);
        }
    }

    // ------------------------------------------------------------------
    // Internal
    // ------------------------------------------------------------------

    private static ObjectMapper defaultMapper() {
        return new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }
}
