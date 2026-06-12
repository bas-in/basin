import { describe, expect, it } from "vitest";

import { createClient } from "../src/index.js";
import { FetchMock } from "./helpers.js";

const URL_BASE = "http://basin.test";
const PROJECT = "01JTESTPROJECT0000000000";

function tokens(overrides: Partial<Record<string, string>> = {}) {
  return {
    access_token: "acc-1",
    refresh_token: "ref-1",
    access_expires_at: new Date(Date.now() + 3600_000).toISOString(),
    refresh_expires_at: new Date(Date.now() + 86400_000).toISOString(),
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// OAuth
// ---------------------------------------------------------------------------

describe("auth.getOAuthAuthorizeUrl", () => {
  it("hits GET /auth/v1/oauth/:provider/authorize with project_id and redirect_to", async () => {
    const fetch = new FetchMock().queueJson({
      redirect_url: "https://accounts.google.com/o/oauth2/v2/auth?...",
      state: "csrf-state-abc",
    });
    const client = createClient(URL_BASE, undefined, {
      projectId: PROJECT,
      fetch: fetch.impl,
    });

    const result = await client.auth.getOAuthAuthorizeUrl("google", {
      redirectTo: "https://app.example.com/callback",
    });

    expect(result.redirect_url).toBe(
      "https://accounts.google.com/o/oauth2/v2/auth?...",
    );
    expect(result.state).toBe("csrf-state-abc");

    expect(fetch.last.method).toBe("GET");
    expect(fetch.last.url).toContain("/auth/v1/oauth/google/authorize");
    expect(fetch.last.url).toContain(`project_id=${PROJECT}`);
    expect(fetch.last.url).toContain(
      "redirect_to=https%3A%2F%2Fapp.example.com%2Fcallback",
    );
    // Must be unauthenticated (no Authorization header)
    expect(fetch.last.headers["authorization"]).toBeUndefined();
  });

  it("defaults redirect_to to empty string when not supplied", async () => {
    const fetch = new FetchMock().queueJson({
      redirect_url: "https://github.com/login/oauth/authorize?...",
      state: "s",
    });
    const client = createClient(URL_BASE, undefined, {
      projectId: PROJECT,
      fetch: fetch.impl,
    });

    await client.auth.getOAuthAuthorizeUrl("github");

    expect(fetch.last.url).toContain("redirect_to=");
  });

  it("encodes the provider name in the path", async () => {
    const fetch = new FetchMock().queueJson({ redirect_url: "u", state: "s" });
    const client = createClient(URL_BASE, undefined, {
      projectId: PROJECT,
      fetch: fetch.impl,
    });

    await client.auth.getOAuthAuthorizeUrl("twitter_x");

    expect(fetch.last.url).toContain("/auth/v1/oauth/twitter_x/authorize");
  });
});

// ---------------------------------------------------------------------------
// MFA — enrollFactor
// ---------------------------------------------------------------------------

describe("auth.enrollFactor", () => {
  it("enrolls a TOTP factor and returns a TotpEnrollResult", async () => {
    const fetch = new FetchMock().queueJson(
      {
        factor_id: "fac-totp-1",
        factor_type: "totp",
        secret_b32: "JBSWY3DPEHPK3PXP",
        otpauth_uri: "otpauth://totp/Basin:user@example.com?secret=JBSWY3DPEHPK3PXP",
      },
      201,
    );
    const client = createClient(URL_BASE, "jwt-tok", {
      projectId: PROJECT,
      fetch: fetch.impl,
    });

    const result = await client.auth.enrollFactor("totp", {
      friendlyName: "Authenticator App",
    });

    expect(result.factor_id).toBe("fac-totp-1");
    expect(result.factor_type).toBe("totp");
    expect("secret_b32" in result).toBe(true);
    if (result.factor_type === "totp") {
      expect(result.secret_b32).toBe("JBSWY3DPEHPK3PXP");
      expect(result.otpauth_uri).toContain("otpauth://totp/");
    }

    expect(fetch.last.url).toBe(`${URL_BASE}/auth/v1/factors`);
    expect(fetch.last.method).toBe("POST");
    expect(JSON.parse(fetch.last.body!)).toEqual({
      factor_type: "totp",
      friendly_name: "Authenticator App",
    });
  });

  it("enrolls a WebAuthn factor and parses creation_options_json", async () => {
    const creationOptions = {
      rp: { name: "Basin", id: "basin.run" },
      user: { id: "dXNlci0x", name: "user@example.com", displayName: "User" },
      challenge: "Y2hhbGxlbmdl",
      pubKeyCredParams: [{ type: "public-key", alg: -7 }],
    };
    const fetch = new FetchMock().queueJson(
      {
        factor_id: "fac-wa-1",
        factor_type: "webauthn",
        challenge_id: "chall-uuid-1",
        creation_options_json: JSON.stringify(creationOptions),
      },
      201,
    );
    const client = createClient(URL_BASE, "jwt-tok", {
      projectId: PROJECT,
      fetch: fetch.impl,
    });

    const result = await client.auth.enrollFactor("webauthn");

    expect(result.factor_id).toBe("fac-wa-1");
    expect(result.factor_type).toBe("webauthn");
    if (result.factor_type === "webauthn") {
      expect(result.challenge_id).toBe("chall-uuid-1");
      // creation_options must be the parsed object, not a JSON string
      expect(result.creation_options).toEqual(creationOptions);
    }
  });

  it("sends empty friendly_name when not provided", async () => {
    const fetch = new FetchMock().queueJson(
      {
        factor_id: "fac-totp-2",
        factor_type: "totp",
        secret_b32: "ABCDEF",
        otpauth_uri: "otpauth://totp/...",
      },
      201,
    );
    const client = createClient(URL_BASE, "jwt-tok", { fetch: fetch.impl });
    await client.auth.enrollFactor("totp");
    expect(JSON.parse(fetch.last.body!)).toMatchObject({ friendly_name: "" });
  });
});

// ---------------------------------------------------------------------------
// MFA — listFactors
// ---------------------------------------------------------------------------

describe("auth.listFactors", () => {
  it("returns an array of FactorDescriptors", async () => {
    const fetch = new FetchMock().queueJson([
      {
        id: "fac-1",
        factor_type: "totp",
        status: "verified",
        friendly_name: "Authenticator",
        created_at: "2024-01-01T00:00:00Z",
        updated_at: "2024-01-01T00:00:00Z",
      },
    ]);
    const client = createClient(URL_BASE, "jwt-tok", { fetch: fetch.impl });

    const factors = await client.auth.listFactors();

    expect(factors).toHaveLength(1);
    expect(factors[0]!.id).toBe("fac-1");
    expect(factors[0]!.factor_type).toBe("totp");
    expect(factors[0]!.status).toBe("verified");
    expect(fetch.last.method).toBe("GET");
    expect(fetch.last.url).toBe(`${URL_BASE}/auth/v1/factors`);
  });

  it("returns an empty array when the server returns null or empty", async () => {
    const fetch = new FetchMock().queueJson([]);
    const client = createClient(URL_BASE, "jwt-tok", { fetch: fetch.impl });
    const factors = await client.auth.listFactors();
    expect(factors).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// MFA — verifyFactor
// ---------------------------------------------------------------------------

describe("auth.verifyFactor", () => {
  it("sends code for TOTP path and returns VerifyFactorResult", async () => {
    const fetch = new FetchMock().queueJson({ ok: true });
    const client = createClient(URL_BASE, "jwt-tok", { fetch: fetch.impl });

    const result = await client.auth.verifyFactor("fac-totp-1", { code: "123456" });

    expect(result.ok).toBe(true);
    expect(result.recovery_codes).toBeUndefined();
    expect(fetch.last.url).toBe(`${URL_BASE}/auth/v1/factors/fac-totp-1/verify`);
    expect(fetch.last.method).toBe("POST");
    expect(JSON.parse(fetch.last.body!)).toEqual({ code: "123456" });
  });

  it("returns recovery_codes on the first verified factor", async () => {
    const codes = ["aabb", "ccdd", "eeff", "0011", "2233", "4455", "6677", "8899"];
    const fetch = new FetchMock().queueJson({ ok: true, recovery_codes: codes });
    const client = createClient(URL_BASE, "jwt-tok", { fetch: fetch.impl });

    const result = await client.auth.verifyFactor("fac-1", { code: "000000" });

    expect(result.recovery_codes).toEqual(codes);
  });

  it("sends attestation and challenge_id for WebAuthn path", async () => {
    const fetch = new FetchMock().queueJson({ ok: true });
    const client = createClient(URL_BASE, "jwt-tok", { fetch: fetch.impl });

    await client.auth.verifyFactor("fac-wa-1", {
      attestation: '{"id":"cred-id"}',
      challengeId: "chall-uuid-1",
    });

    expect(JSON.parse(fetch.last.body!)).toEqual({
      attestation: '{"id":"cred-id"}',
      challenge_id: "chall-uuid-1",
    });
  });
});

// ---------------------------------------------------------------------------
// MFA — challengeFactor
// ---------------------------------------------------------------------------

describe("auth.challengeFactor", () => {
  it("returns TotpChallengeResult for a TOTP factor", async () => {
    const fetch = new FetchMock().queueJson({ challenge_id: "chall-uuid-2" });
    const client = createClient(URL_BASE, "jwt-tok", { fetch: fetch.impl });

    const result = await client.auth.challengeFactor("fac-totp-1");

    expect(result.challenge_id).toBe("chall-uuid-2");
    expect("request_options" in result).toBe(false);
    expect(fetch.last.url).toBe(
      `${URL_BASE}/auth/v1/factors/fac-totp-1/challenge`,
    );
    expect(fetch.last.method).toBe("POST");
  });

  it("returns WebAuthnChallengeResult with parsed request_options for WebAuthn", async () => {
    const requestOptions = {
      challenge: "Y2hhbGxlbmdl",
      rpId: "basin.run",
      allowCredentials: [{ type: "public-key", id: "Y3JlZC1pZA" }],
    };
    const fetch = new FetchMock().queueJson({
      challenge_id: "chall-uuid-3",
      request_options_json: JSON.stringify(requestOptions),
    });
    const client = createClient(URL_BASE, "jwt-tok", { fetch: fetch.impl });

    const result = await client.auth.challengeFactor("fac-wa-1");

    expect(result.challenge_id).toBe("chall-uuid-3");
    expect("request_options" in result).toBe(true);
    if ("request_options" in result) {
      expect(result.request_options).toEqual(requestOptions);
    }
  });
});

// ---------------------------------------------------------------------------
// MFA — verifyChallenge
// ---------------------------------------------------------------------------

describe("auth.verifyChallenge", () => {
  it("sends challenge_id + code for TOTP, stores the returned aal2 session", async () => {
    const aal2Session = tokens({ access_token: "aal2-acc" });
    const fetch = new FetchMock()
      .queueJson(tokens()) // signIn
      .queueJson(aal2Session); // verifyChallenge
    const client = createClient(URL_BASE, undefined, {
      projectId: PROJECT,
      fetch: fetch.impl,
    });

    await client.auth.signIn({ email: "a@b.c", password: "pw" });
    const session = await client.auth.verifyChallenge(
      "fac-totp-1",
      "chall-uuid-2",
      { code: "654321" },
    );

    expect(session.access_token).toBe("aal2-acc");
    // Session stored: subsequent requests should use the new token
    expect(client.auth.getSession()?.access_token).toBe("aal2-acc");

    const body = JSON.parse(fetch.calls[1]!.body!);
    expect(body).toEqual({ challenge_id: "chall-uuid-2", code: "654321" });
    expect(fetch.calls[1]!.url).toBe(
      `${URL_BASE}/auth/v1/factors/fac-totp-1/challenge/verify`,
    );
    expect(fetch.calls[1]!.method).toBe("POST");
  });

  it("sends challenge_id + assertion for WebAuthn path", async () => {
    const fetch = new FetchMock().queueJson(tokens({ access_token: "aal2-wa" }));
    const client = createClient(URL_BASE, undefined, {
      projectId: PROJECT,
      fetch: fetch.impl,
    });

    await client.auth.verifyChallenge("fac-wa-1", "chall-uuid-3", {
      assertion: '{"id":"cred-id","response":{}}',
    });

    const body = JSON.parse(fetch.last.body!);
    expect(body).toEqual({
      challenge_id: "chall-uuid-3",
      assertion: '{"id":"cred-id","response":{}}',
    });
  });
});

// ---------------------------------------------------------------------------
// MFA — unenrollFactor
// ---------------------------------------------------------------------------

describe("auth.unenrollFactor", () => {
  it("sends DELETE to /auth/v1/factors/:id and returns { ok: true }", async () => {
    const fetch = new FetchMock().queueJson({ ok: true });
    const client = createClient(URL_BASE, "aal2-jwt", { fetch: fetch.impl });

    const result = await client.auth.unenrollFactor("fac-totp-1");

    expect(result.ok).toBe(true);
    expect(fetch.last.url).toBe(`${URL_BASE}/auth/v1/factors/fac-totp-1`);
    expect(fetch.last.method).toBe("DELETE");
    // Must carry the bearer token (aal2 required by server)
    expect(fetch.last.headers["authorization"]).toBe("Bearer aal2-jwt");
  });
});
