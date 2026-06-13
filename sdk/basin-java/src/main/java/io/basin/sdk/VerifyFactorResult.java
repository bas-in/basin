package io.basin.sdk;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * Response from {@code POST /auth/v1/factors/:id/verify}.
 *
 * <p>{@link #recoveryCodes} is only present on the <em>first</em> verified factor for
 * a user — it will be {@code null} for all subsequent factor verifications.
 * Store these codes securely; they are shown exactly once.
 */
public final class VerifyFactorResult {

    @JsonProperty("ok")
    public final boolean ok;

    /**
     * Recovery codes (present only on first verified factor, {@code null} thereafter).
     * Each code is a hex string.
     */
    @JsonProperty("recovery_codes")
    public final List<String> recoveryCodes;

    public VerifyFactorResult(
            @JsonProperty("ok") boolean ok,
            @JsonProperty("recovery_codes") List<String> recoveryCodes) {
        this.ok = ok;
        this.recoveryCodes = recoveryCodes;
    }
}
