package io.basin.sdk;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Token pair returned by {@code POST /auth/v1/signin}, {@code /auth/v1/refresh},
 * and {@code /auth/v1/magic-link/consume}.
 *
 * <p>JSON field names match the Rust serde structs in
 * {@code crates/basin-rest/src/routes/auth.rs token_body} exactly.
 */
public final class Session {

    @JsonProperty("access_token")
    public final String accessToken;

    @JsonProperty("refresh_token")
    public final String refreshToken;

    /** RFC 3339 expiry timestamp for the access token. */
    @JsonProperty("access_expires_at")
    public final String accessExpiresAt;

    /** RFC 3339 expiry timestamp for the refresh token. */
    @JsonProperty("refresh_expires_at")
    public final String refreshExpiresAt;

    public Session(
            @JsonProperty("access_token") String accessToken,
            @JsonProperty("refresh_token") String refreshToken,
            @JsonProperty("access_expires_at") String accessExpiresAt,
            @JsonProperty("refresh_expires_at") String refreshExpiresAt) {
        this.accessToken = accessToken;
        this.refreshToken = refreshToken;
        this.accessExpiresAt = accessExpiresAt;
        this.refreshExpiresAt = refreshExpiresAt;
    }
}
