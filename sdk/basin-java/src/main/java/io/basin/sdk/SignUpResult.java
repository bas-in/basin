package io.basin.sdk;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Response from {@code POST /auth/v1/signup}. */
public final class SignUpResult {

    @JsonProperty("ok")
    public final boolean ok;

    @JsonProperty("user_id")
    public final String userId;

    public SignUpResult(
            @JsonProperty("ok") boolean ok,
            @JsonProperty("user_id") String userId) {
        this.ok = ok;
        this.userId = userId;
    }
}
