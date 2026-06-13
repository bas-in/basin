package io.basin.sdk;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Response from {@code POST /auth/v1/api-keys}.
 *
 * <p>The {@link #secret} field is shown exactly once — store it immediately.
 */
public final class ApiKeyIssued {

    @JsonProperty("id")
    public final long id;

    @JsonProperty("name")
    public final String name;

    /** Raw API key secret — present only in this response, never retrievable again. */
    @JsonProperty("secret")
    public final String secret;

    @JsonProperty("created_at")
    public final String createdAt;

    public ApiKeyIssued(
            @JsonProperty("id") long id,
            @JsonProperty("name") String name,
            @JsonProperty("secret") String secret,
            @JsonProperty("created_at") String createdAt) {
        this.id = id;
        this.name = name;
        this.secret = secret;
        this.createdAt = createdAt;
    }
}
