package io.basin.sdk;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Response from {@code GET /auth/v1/oauth/:provider/authorize}.
 *
 * <p>Redirect the user's browser to {@link #redirectUrl}. The server's callback
 * handler ({@code GET /auth/v1/oauth/:provider/callback}) exchanges the code and
 * issues a Basin token pair. The SDK cannot perform the browser redirect itself.
 */
public final class OAuthAuthorizeResult {

    /** Full provider authorize URL — redirect the browser here. */
    @JsonProperty("redirect_url")
    public final String redirectUrl;

    /** CSRF state value included in {@link #redirectUrl}; echoed on callback. */
    @JsonProperty("state")
    public final String state;

    public OAuthAuthorizeResult(
            @JsonProperty("redirect_url") String redirectUrl,
            @JsonProperty("state") String state) {
        this.redirectUrl = redirectUrl;
        this.state = state;
    }
}
