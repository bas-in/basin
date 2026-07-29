package io.basin.sdk;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Response from {@code POST /storage/v1/object/sign/upload/:bucket/*path}.
 *
 * <p>JSON field names use camelCase ({@code signedUrl}, {@code expiresAt}) to
 * match the Rust handler output exactly.
 */
public final class SignedUrl {

    /** Server-relative signed URL path. */
    @JsonProperty("signedUrl")
    public final String signedUrl;

    /** RFC 3339 expiry time. */
    @JsonProperty("expiresAt")
    public final String expiresAt;

    /**
     * Full absolute URL (base URL + {@link #signedUrl}).
     * Computed by the SDK; not present in the server response.
     */
    public final String absoluteUrl;

    public SignedUrl(String signedUrl, String expiresAt, String absoluteUrl) {
        this.signedUrl = signedUrl;
        this.expiresAt = expiresAt;
        this.absoluteUrl = absoluteUrl;
    }
}
