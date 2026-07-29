package io.basin.sdk;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Response from {@code POST /auth/v1/factors} for {@code factor_type="webauthn"}.
 *
 * <p>Pass {@link #creationOptionsJson} to {@code navigator.credentials.create()} in
 * your browser JS bridge. Then call {@code verifyFactor} with the attestation
 * response and the returned {@link #challengeId}.
 */
public final class WebAuthnEnrollResult {

    @JsonProperty("factor_id")
    public final String factorId;

    @JsonProperty("factor_type")
    public final String factorType; // "webauthn"

    @JsonProperty("challenge_id")
    public final String challengeId;

    /** Serialised {@code PublicKeyCredentialCreationOptions} JSON. */
    @JsonProperty("creation_options_json")
    public final String creationOptionsJson;

    public WebAuthnEnrollResult(
            @JsonProperty("factor_id") String factorId,
            @JsonProperty("factor_type") String factorType,
            @JsonProperty("challenge_id") String challengeId,
            @JsonProperty("creation_options_json") String creationOptionsJson) {
        this.factorId = factorId;
        this.factorType = factorType;
        this.challengeId = challengeId;
        this.creationOptionsJson = creationOptionsJson;
    }
}
