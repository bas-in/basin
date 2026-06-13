package io.basin.sdk;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Internal HTTP transport for Basin SDK.
 *
 * <p>All public {@link BasinClient} methods delegate here. Handles:
 * <ul>
 *   <li>Bearer auth (static key or session token from the auth client)
 *   <li>Error envelope decoding into {@link BasinApiException}
 *   <li>JSON serialisation via Jackson
 * </ul>
 *
 * <p>This class is package-private; callers use it through {@link BasinClient}.
 */
final class BasinTransport {

    final String baseUrl;
    private final String staticKey;
    private volatile Supplier<String> tokenSupplier; // returns null → fall back to staticKey
    final HttpClient httpClient;
    final ObjectMapper mapper;

    BasinTransport(String baseUrl, String staticKey, HttpClient httpClient, ObjectMapper mapper) {
        this.baseUrl = baseUrl.replaceAll("/+$", "");
        this.staticKey = staticKey;
        this.httpClient = httpClient;
        this.mapper = mapper;
    }

    /** Wire the auth client's token getter so every request uses the freshest token. */
    void setTokenSupplier(Supplier<String> supplier) {
        this.tokenSupplier = supplier;
    }

    // ------------------------------------------------------------------
    // Bearer token resolution
    // ------------------------------------------------------------------

    String resolveBearer() {
        Supplier<String> sup = tokenSupplier;
        if (sup != null) {
            String tok = sup.get();
            if (tok != null && !tok.isEmpty()) {
                return tok;
            }
        }
        return staticKey;
    }

    // ------------------------------------------------------------------
    // URL construction
    // ------------------------------------------------------------------

    URI buildUri(String path, List<String[]> queryParams) {
        StringBuilder sb = new StringBuilder(baseUrl).append(path);
        if (queryParams != null && !queryParams.isEmpty()) {
            sb.append('?');
            boolean first = true;
            for (String[] kv : queryParams) {
                if (!first) sb.append('&');
                sb.append(urlEncode(kv[0])).append('=').append(urlEncode(kv[1]));
                first = false;
            }
        }
        return URI.create(sb.toString());
    }

    private static String urlEncode(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8).replace("+", "%20");
    }

    // ------------------------------------------------------------------
    // Request builder helpers
    // ------------------------------------------------------------------

    HttpRequest.Builder baseRequest(String path, List<String[]> query, boolean auth) {
        HttpRequest.Builder builder = HttpRequest.newBuilder(buildUri(path, query));
        if (auth) {
            String bearer = resolveBearer();
            if (bearer != null && !bearer.isEmpty()) {
                builder.header("Authorization", "Bearer " + bearer);
            }
        }
        return builder;
    }

    // ------------------------------------------------------------------
    // Synchronous execute + decode
    // ------------------------------------------------------------------

    /** Execute a request and return the raw response (no error decoding). */
    HttpResponse<byte[]> executeRaw(HttpRequest request) {
        try {
            return httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
        } catch (IOException e) {
            throw new BasinNetworkException("Network error: " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BasinNetworkException("Request interrupted", e);
        }
    }

    /** Execute a request and return the parsed JSON body, throwing on error. */
    JsonNode executeJson(HttpRequest request) {
        HttpResponse<byte[]> resp = executeRaw(request);
        return decodeResponse(resp);
    }

    private JsonNode decodeResponse(HttpResponse<byte[]> resp) {
        int status = resp.statusCode();
        byte[] body = resp.body();

        JsonNode node = null;
        if (body != null && body.length > 0) {
            try {
                node = mapper.readTree(body);
            } catch (IOException e) {
                // non-JSON body — treat as opaque
            }
        }

        if (status < 200 || status >= 300) {
            throw BasinApiException.fromBody(node, status);
        }
        return node;
    }

    // ------------------------------------------------------------------
    // Async execute + decode
    // ------------------------------------------------------------------

    CompletableFuture<JsonNode> executeJsonAsync(HttpRequest request) {
        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray())
                .thenApply(this::decodeResponse);
    }

    CompletableFuture<HttpResponse<byte[]>> executeRawAsync(HttpRequest request) {
        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray());
    }

    // ------------------------------------------------------------------
    // JSON serialisation helpers
    // ------------------------------------------------------------------

    byte[] toBytes(Object body) {
        try {
            return mapper.writeValueAsBytes(body);
        } catch (IOException e) {
            throw new BasinException("Failed to serialize request body", e);
        }
    }

    <T> T readValue(JsonNode node, Class<T> type) {
        try {
            return mapper.treeToValue(node, type);
        } catch (IOException e) {
            throw new BasinException("Failed to decode response as " + type.getName(), e);
        }
    }

    // ------------------------------------------------------------------
    // Standard request factories
    // ------------------------------------------------------------------

    HttpRequest jsonGet(String path, List<String[]> query, boolean auth) {
        return baseRequest(path, query, auth)
                .GET()
                .header("Accept", "application/json")
                .build();
    }

    HttpRequest jsonPost(String path, List<String[]> query, Object body, boolean auth) {
        byte[] bytes = body != null ? toBytes(body) : new byte[0];
        HttpRequest.Builder b = baseRequest(path, query, auth)
                .POST(HttpRequest.BodyPublishers.ofByteArray(bytes))
                .header("Accept", "application/json");
        if (body != null) {
            b.header("Content-Type", "application/json");
        }
        return b.build();
    }

    HttpRequest rawPost(String path, byte[] bytes, Map<String, String> extraHeaders, boolean auth) {
        HttpRequest.Builder b = baseRequest(path, null, auth)
                .POST(HttpRequest.BodyPublishers.ofByteArray(bytes))
                .header("Accept", "application/json");
        if (extraHeaders != null) {
            extraHeaders.forEach(b::header);
        }
        return b.build();
    }

    HttpRequest jsonPatch(String path, List<String[]> query, Object body, boolean auth) {
        byte[] bytes = body != null ? toBytes(body) : new byte[0];
        HttpRequest.Builder b = baseRequest(path, query, auth)
                .method("PATCH", HttpRequest.BodyPublishers.ofByteArray(bytes))
                .header("Accept", "application/json");
        if (body != null) {
            b.header("Content-Type", "application/json");
        }
        return b.build();
    }

    HttpRequest jsonDelete(String path, List<String[]> query, Object body, boolean auth) {
        if (body != null) {
            byte[] bytes = toBytes(body);
            return baseRequest(path, query, auth)
                    .method("DELETE", HttpRequest.BodyPublishers.ofByteArray(bytes))
                    .header("Content-Type", "application/json")
                    .header("Accept", "application/json")
                    .build();
        }
        return baseRequest(path, query, auth)
                .DELETE()
                .header("Accept", "application/json")
                .build();
    }

    HttpRequest anyMethod(String method, String path, byte[] body,
                          Map<String, String> extraHeaders, boolean auth) {
        HttpRequest.Builder b = baseRequest(path, null, auth)
                .method(method, body != null
                        ? HttpRequest.BodyPublishers.ofByteArray(body)
                        : HttpRequest.BodyPublishers.noBody())
                .header("Accept", "application/json");
        if (extraHeaders != null) {
            extraHeaders.forEach(b::header);
        }
        return b.build();
    }
}
