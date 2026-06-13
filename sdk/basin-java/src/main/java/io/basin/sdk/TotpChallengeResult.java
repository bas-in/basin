package io.basin.sdk;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Response from {@code POST /auth/v1/factors/:id/challenge} for TOTP factors. */
public final class TotpChallengeResult {

    @JsonProperty("challenge_id")
    public final String challengeId;

    public TotpChallengeResult(@JsonProperty("challenge_id") String challengeId) {
        this.challengeId = challengeId;
    }
}
