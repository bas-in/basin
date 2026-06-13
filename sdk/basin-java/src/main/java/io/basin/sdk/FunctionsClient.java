package io.basin.sdk;

import com.fasterxml.jackson.databind.JsonNode;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Functions client for Basin's two function invocation surfaces.
 *
 * <p>Route sources (verified):
 * <ul>
 *   <li>{@code ANY /fn/v1/:name} — HTTP-handler-shape Wasm/JS functions;
 *     the guest's response (status, headers, body) is proxied verbatim.
 *     {@code crates/basin-rest/src/server.rs:290},
 *     handler {@code crates/basin-rest/src/routes/fn_handler.rs any_fn_handler}.
 *   <li>{@code POST /rest/v1/rpc/:fn_name} — catalog SQL / Wasm UDFs; body is a
 *     JSON object of named args; scalar results unwrap to the bare value, table
 *     results return an array of row objects.
 *     {@code crates/basin-rest/src/server.rs:288},
 *     handler {@code crates/basin-rest/src/routes/rpc.rs post_rpc}.
 * </ul>
 *
 * <p>Both are bearer-authenticated. Obtain via {@link BasinClient#functions()}.
 */
public final class FunctionsClient {

    private final BasinTransport transport;

    FunctionsClient(BasinTransport transport) {
        this.transport = transport;
    }

    // ------------------------------------------------------------------
    // invoke — ANY /fn/v1/:name
    // ------------------------------------------------------------------

    /**
     * Invoke an HTTP-handler function at {@code ANY /fn/v1/:name}.
     *
     * <p>The function's own response (status, headers, body) is proxied verbatim,
     * so non-2xx statuses are NOT automatically errors — unless the body is Basin's
     * own error envelope (auth 401, unknown function 404, invocation failure 500),
     * which surfaces as {@link BasinApiException}.
     *
     * @param name    function name
     * @param method  HTTP method, e.g. {@code "POST"} (default)
     * @param body    request body — serialised as JSON when non-null and not bytes/string;
     *                pass {@code null} for no body
     * @param headers extra HTTP headers to send; merged with auth header
     */
    public CompletableFuture<FunctionInvokeResult> invoke(
            String name, String method, Object body, Map<String, String> headers) {
        String path = "/fn/v1/" + encodeSegment(name);
        byte[] bytes = null;
        Map<String, String> hdrs = new HashMap<>(headers != null ? headers : Map.of());

        if (body instanceof byte[]) {
            bytes = (byte[]) body;
        } else if (body instanceof String) {
            bytes = ((String) body).getBytes(StandardCharsets.UTF_8);
        } else if (body != null) {
            bytes = transport.toBytes(body);
            hdrs.putIfAbsent("Content-Type", "application/json");
        }

        String effectiveMethod = method != null ? method.toUpperCase() : "POST";
        var req = transport.anyMethod(effectiveMethod, path, bytes, hdrs, true);

        return transport.executeRawAsync(req).thenApply(resp -> {
            int status = resp.statusCode();
            byte[] rawBody = resp.body();

            Map<String, String> respHeaders = new HashMap<>();
            resp.headers().map().forEach((k, vs) -> {
                if (!vs.isEmpty()) respHeaders.put(k, vs.get(0));
            });

            String ct = respHeaders.getOrDefault("content-type", "");
            Object data;
            if (rawBody != null && rawBody.length > 0 && ct.contains("json")) {
                try {
                    data = transport.mapper.readTree(rawBody);
                } catch (Exception e) {
                    data = new String(rawBody, StandardCharsets.UTF_8);
                }
            } else if (rawBody != null && rawBody.length > 0) {
                data = new String(rawBody, StandardCharsets.UTF_8);
            } else {
                data = null;
            }

            // Basin-layer errors (not the function's own responses) surface as typed exceptions.
            if (status >= 400 && data instanceof JsonNode jsonNode) {
                if (isErrorEnvelope(jsonNode)) {
                    throw BasinApiException.fromBody(jsonNode, status);
                }
            }

            return new FunctionInvokeResult(status, respHeaders, data);
        });
    }

    /** Blocking variant of {@link #invoke}. */
    public FunctionInvokeResult invokeBlocking(String name, String method, Object body,
                                                Map<String, String> headers) {
        return invoke(name, method, body, headers).join();
    }

    // ------------------------------------------------------------------
    // rpc — POST /rest/v1/rpc/:fn_name
    // ------------------------------------------------------------------

    /**
     * Call a catalog UDF at {@code POST /rest/v1/rpc/:fn_name}.
     *
     * <p>Args are named; missing declared args become NULL. Returns the bare scalar
     * for single-value results, or an array of rows for RETURNS TABLE.
     *
     * @param fnName function name registered in the catalog
     * @param args   named arguments; pass {@code null} or an empty map for no args
     */
    public CompletableFuture<JsonNode> rpc(String fnName, Map<String, Object> args) {
        return transport.executeJsonAsync(
                transport.jsonPost("/rest/v1/rpc/" + encodeSegment(fnName),
                        null, args != null ? args : Map.of(), true));
    }

    /** Blocking variant of {@link #rpc}. */
    public JsonNode rpcBlocking(String fnName, Map<String, Object> args) {
        return rpc(fnName, args).join();
    }

    // ------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------

    private static String encodeSegment(String segment) {
        return URLEncoder.encode(segment, StandardCharsets.UTF_8).replace("+", "%20");
    }

    private static boolean isErrorEnvelope(JsonNode node) {
        if (!node.isObject()) return false;
        JsonNode code = node.get("code");
        JsonNode msg = node.get("message");
        return code != null && code.isTextual() && code.asText().startsWith("E_")
                && msg != null && msg.isTextual();
    }
}
