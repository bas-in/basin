package io.basin.sdk;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Raised for any non-2xx response from a Basin server.
 *
 * <p>The server always returns a JSON error envelope:
 * {@code {"code":"E_*","message":"...","sqlstate":"23505"}} (sqlstate optional).
 * Match on {@link #code}, never on {@link #getMessage()} — the message is
 * human-readable and may change across server versions.
 *
 * <p>Known stable error codes (from {@code crates/basin-rest/src/errors.rs}):
 * <ul>
 *   <li>{@code E_UNAUTHENTICATED} — missing or invalid token
 *   <li>{@code E_FORBIDDEN} — token valid but lacks permission
 *   <li>{@code E_NOT_FOUND} — resource not found
 *   <li>{@code E_INVALID_REQUEST} — bad request body or query
 *   <li>{@code E_RATE_LIMITED} — per-project rate limit hit
 *   <li>{@code E_ENGINE_UNSUPPORTED} — 501, engine can't execute this operation
 *   <li>{@code E_INTERNAL} — server-side error
 *   <li>{@code E_EMAIL_DISABLED} — email auth disabled for the project
 *   <li>{@code E_REVOKED_TOKEN} — refresh token already rotated
 * </ul>
 */
public class BasinApiException extends BasinException {

    /** Stable {@code E_*} error code from the response body. */
    public final String code;

    /** HTTP status code of the response. 0 when synthesised client-side. */
    public final int status;

    /**
     * Optional 5-character SQLSTATE code from the SQL engine layer.
     * {@code null} when the server did not include one.
     */
    public final String sqlState;

    public BasinApiException(String code, String message, int status, String sqlState) {
        super(message);
        this.code = code;
        this.status = status;
        this.sqlState = sqlState;
    }

    public BasinApiException(String code, String message, int status) {
        this(code, message, status, null);
    }

    /**
     * Decode a Basin error envelope JSON node into a {@code BasinApiException}.
     *
     * @param body   parsed response body (may be null or non-object)
     * @param status HTTP response status
     */
    public static BasinApiException fromBody(JsonNode body, int status) {
        String code = "E_UNKNOWN";
        String message = "HTTP " + status;
        String sqlState = null;

        if (body != null && body.isObject()) {
            if (body.has("code") && body.get("code").isTextual()) {
                code = body.get("code").asText();
            }
            if (body.has("message") && body.get("message").isTextual()) {
                message = body.get("message").asText();
            }
            if (body.has("sqlstate") && body.get("sqlstate").isTextual()) {
                sqlState = body.get("sqlstate").asText();
            }
        }

        return new BasinApiException(code, message, status, sqlState);
    }

    @Override
    public String toString() {
        if (sqlState != null && !sqlState.isEmpty()) {
            return String.format("BasinApiException[%s, HTTP %d, sqlstate=%s]: %s",
                    code, status, sqlState, getMessage());
        }
        return String.format("BasinApiException[%s, HTTP %d]: %s", code, status, getMessage());
    }
}
