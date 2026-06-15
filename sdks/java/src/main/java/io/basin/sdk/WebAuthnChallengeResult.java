package io.basin.sdk;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Response from {@code POST /auth/v1/factors/:id/challenge} for WebAuthn factors.
 *
 * <p>Pass {@link #requestOptionsJson} to {@code navigator.credentials.get()} to obtain
 * the assertion, then call {@code verifyChallenge} with the result.
 */
public final class WebAuthnChallengeResult {

    @JsonProperty("challenge_id")
    public final String challengeId;

    /** Serialised {@code PublicKeyCredentialRequestOptions} JSON; present for WebAuthn factors. */
    @JsonProperty("request_options_json")
    public final String requestOptionsJson;

    public WebAuthnChallengeResult(
            @JsonProperty("challenge_id") String challengeId,
            @JsonProperty("request_options_json") String requestOptionsJson) {
        this.challengeId = challengeId;
        this.requestOptionsJson = requestOptionsJson;
    }
}
