package io.basin.sdk;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * One element of the {@code GET /auth/v1/factors} array.
 *
 * <p>Mirrors {@code FactorDescriptor} in {@code crates/basin-auth/src/mfa.rs}.
 * Secret or credential bytes are never included.
 */
public final class FactorDescriptor {

    @JsonProperty("id")
    public final String id;

    /** {@code "totp"} or {@code "webauthn"}. */
    @JsonProperty("factor_type")
    public final String factorType;

    /** {@code "unverified"} or {@code "verified"}. */
    @JsonProperty("status")
    public final String status;

    @JsonProperty("friendly_name")
    public final String friendlyName;

    @JsonProperty("created_at")
    public final String createdAt;

    @JsonProperty("updated_at")
    public final String updatedAt;

    public FactorDescriptor(
            @JsonProperty("id") String id,
            @JsonProperty("factor_type") String factorType,
            @JsonProperty("status") String status,
            @JsonProperty("friendly_name") String friendlyName,
            @JsonProperty("created_at") String createdAt,
            @JsonProperty("updated_at") String updatedAt) {
        this.id = id;
        this.factorType = factorType;
        this.status = status;
        this.friendlyName = friendlyName;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }
}
