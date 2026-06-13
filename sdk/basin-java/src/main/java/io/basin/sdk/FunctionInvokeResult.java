package io.basin.sdk;

import java.util.Map;

/**
 * Proxied response from {@code ANY /fn/v1/:name}.
 *
 * <p>The function's own status, headers, and body are returned verbatim.
 * Non-2xx statuses are NOT automatically errors — unless the body is Basin's
 * error envelope (auth 401, unknown function 404, invocation failure 500).
 */
public final class FunctionInvokeResult {

    /** HTTP status code returned by the function. */
    public final int status;

    /** Response headers from the function. */
    public final Map<String, String> headers;

    /**
     * Response body — a parsed JSON object/array when Content-Type is JSON,
     * or the raw {@link String} otherwise.
     */
    public final Object data;

    public FunctionInvokeResult(int status, Map<String, String> headers, Object data) {
        this.status = status;
        this.headers = headers;
        this.data = data;
    }
}
