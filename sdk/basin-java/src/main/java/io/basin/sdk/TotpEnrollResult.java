package io.basin.sdk;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Response from {@code POST /auth/v1/factors} for {@code factor_type="totp"}.
 *
 * <p>The factor is {@code unverified} until
 * {@code POST /auth/v1/factors/:id/verify} succeeds with the first valid OTP.
 */
public final class TotpEnrollResult {

    @JsonProperty("factor_id")
    public final String factorId;

    @JsonProperty("factor_type")
    public final String factorType; // "totp"

    /** Base-32 TOTP secret — display as a QR code via {@link #otpauthUri}. */
    @JsonProperty("secret_b32")
    public final String secretB32;

    /** {@code otpauth://totp/...} URI suitable for QR code display. */
    @JsonProperty("otpauth_uri")
    public final String otpauthUri;

    public TotpEnrollResult(
            @JsonProperty("factor_id") String factorId,
            @JsonProperty("factor_type") String factorType,
            @JsonProperty("secret_b32") String secretB32,
            @JsonProperty("otpauth_uri") String otpauthUri) {
        this.factorId = factorId;
        this.factorType = factorType;
        this.secretB32 = secretB32;
        this.otpauthUri = otpauthUri;
    }
}
