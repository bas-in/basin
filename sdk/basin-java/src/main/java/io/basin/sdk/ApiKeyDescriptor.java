package io.basin.sdk;

import com.fasterxml.jackson.annotation.JsonProperty;

/** One element of the {@code GET /auth/v1/api-keys} array. */
public final class ApiKeyDescriptor {

    @JsonProperty("id")
    public final long id;

    @JsonProperty("name")
    public final String name;

    @JsonProperty("created_at")
    public final String createdAt;

    /** {@code null} when the key has never been used. */
    @JsonProperty("last_used_at")
    public final String lastUsedAt;

    /** {@code null} when the key is still active. */
    @JsonProperty("revoked_at")
    public final String revokedAt;

    public ApiKeyDescriptor(
            @JsonProperty("id") long id,
            @JsonProperty("name") String name,
            @JsonProperty("created_at") String createdAt,
            @JsonProperty("last_used_at") String lastUsedAt,
            @JsonProperty("revoked_at") String revokedAt) {
        this.id = id;
        this.name = name;
        this.createdAt = createdAt;
        this.lastUsedAt = lastUsedAt;
        this.revokedAt = revokedAt;
    }
}
