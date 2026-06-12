/**
 * `.auth` — bindings for `/auth/v1/*`.
 *
 * Route sources (verified):
 * - `POST /auth/v1/signup`                 crates/basin-rest/src/server.rs:250
 * - `POST /auth/v1/signin`                 crates/basin-rest/src/server.rs:251
 * - `POST /auth/v1/refresh`                crates/basin-rest/src/server.rs:252
 * - `POST /auth/v1/signout`                crates/basin-rest/src/server.rs:253
 * - `POST /auth/v1/verify-email`           crates/basin-rest/src/server.rs:254
 * - `POST /auth/v1/reset-password`         crates/basin-rest/src/server.rs:255
 * - `POST /auth/v1/request-password-reset` crates/basin-rest/src/server.rs:256
 * - `POST /auth/v1/magic-link`             crates/basin-rest/src/server.rs:262
 * - `POST /auth/v1/magic-link/consume`     crates/basin-rest/src/server.rs:263
 * - `POST|GET /auth/v1/api-keys`           crates/basin-rest/src/server.rs:267
 * - `DELETE /auth/v1/api-keys/:id`         crates/basin-rest/src/server.rs:271
 * - `GET /auth/v1/oauth/:provider/authorize` crates/basin-rest/src/server.rs:277
 * - `GET /auth/v1/oauth/:provider/callback`  crates/basin-rest/src/server.rs:281 (server-side only)
 * - `POST|GET /auth/v1/factors`            crates/basin-rest/src/server.rs:286-287
 * - `POST /auth/v1/factors/:id/verify`     crates/basin-rest/src/server.rs:290
 * - `POST /auth/v1/factors/:id/challenge`  crates/basin-rest/src/server.rs:294
 * - `POST /auth/v1/factors/:id/challenge/verify` crates/basin-rest/src/server.rs:298
 * - `DELETE /auth/v1/factors/:id`          crates/basin-rest/src/server.rs:302
 *
 * Flow notes (from crates/basin-rest/src/routes/auth.rs + basin-auth):
 * - signup/signin take a `project_id` in the body — auth is per-project.
 * - refresh rotates the refresh token; reusing a rotated token surfaces as
 *   `E_REVOKED_TOKEN` (401).
 * - signOut() calls `POST /auth/v1/signout` with the current refresh token,
 *   writing a server-side revocation row, then clears the local session.
 *   When no session is active the call is a no-op (already signed out).
 * - verifyChallenge() stores the returned AAL2 session so subsequent requests
 *   carry the elevated access token automatically.
 */

import { BasinApiError } from "./errors.js";
import type { ClientContext } from "./http.js";
import { requestJson } from "./http.js";
import type {
  ApiKeyDescriptor,
  ApiKeyIssued,
  FactorDescriptor,
  MfaChallengeResult,
  MfaEnrollResult,
  OAuthAuthorizeResult,
  Session,
  SignUpResult,
  TotpChallengeResult,
  TotpEnrollResult,
  VerifyFactorResult,
  WebAuthnChallengeResult,
  WebAuthnEnrollResult,
} from "./types.js";

/** Refresh this many ms before the access token's stated expiry. */
const EXPIRY_SKEW_MS = 10_000;

export class AuthClient {
  #ctx: ClientContext;
  #defaultProjectId: string | undefined;
  #session: Session | null = null;
  #refreshing: Promise<Session> | null = null;

  constructor(ctx: ClientContext, defaultProjectId?: string) {
    this.#ctx = ctx;
    this.#defaultProjectId = defaultProjectId;
  }

  /** The current session, or null when signed out. */
  getSession(): Session | null {
    return this.#session;
  }

  /** Adopt an externally obtained token pair (e.g. restored from storage). */
  setSession(session: Session | null): void {
    this.#session = session;
  }

  #projectId(projectId?: string): string {
    const id = projectId ?? this.#defaultProjectId ?? this.#ctx.projectId();
    if (id === undefined) {
      throw new BasinApiError(
        "E_INVALID_REQUEST",
        "project id required: pass { projectId } or set it in createClient options",
        0,
      );
    }
    return id;
  }

  /** `POST /auth/v1/signup` → `{ ok, user_id }` (201). */
  async signUp(params: {
    email: string;
    password: string;
    projectId?: string;
  }): Promise<SignUpResult> {
    return requestJson<SignUpResult>(this.#ctx, "POST", "/auth/v1/signup", {
      auth: false,
      body: {
        project_id: this.#projectId(params.projectId),
        email: params.email,
        password: params.password,
      },
    });
  }

  /** `POST /auth/v1/signin` → token body; stored as the live session. */
  async signIn(params: {
    email: string;
    password: string;
    projectId?: string;
  }): Promise<Session> {
    const session = await requestJson<Session>(
      this.#ctx,
      "POST",
      "/auth/v1/signin",
      {
        auth: false,
        body: {
          project_id: this.#projectId(params.projectId),
          email: params.email,
          password: params.password,
        },
      },
    );
    this.#session = session;
    return session;
  }

  /**
   * Revoke the current refresh token server-side, then clear the local
   * session. `POST /auth/v1/signout` writes a revocation row so that any
   * subsequent `refreshSession()` call with the old token returns 401.
   *
   * - If there is no active session this is a no-op (already signed out).
   * - Server-side errors are surfaced as thrown `BasinApiError`. The local
   *   session is cleared regardless so the client ends up signed out.
   */
  async signOut(): Promise<void> {
    const current = this.#session;
    // Always clear local state first — even if the server call fails the
    // client should be considered signed out.
    this.#session = null;
    if (current === null) return;
    await requestJson(this.#ctx, "POST", "/auth/v1/signout", {
      auth: false,
      body: { refresh_token: current.refresh_token },
    });
  }

  /**
   * `POST /auth/v1/refresh` with the stored refresh token. Rotates both
   * tokens. Throws `E_REVOKED_TOKEN` if the token was already rotated.
   */
  async refreshSession(): Promise<Session> {
    const current = this.#session;
    if (current === null) {
      throw new BasinApiError("E_UNAUTHENTICATED", "no session to refresh", 0);
    }
    // Single-flight: concurrent callers share one refresh round-trip.
    this.#refreshing ??= requestJson<Session>(
      this.#ctx,
      "POST",
      "/auth/v1/refresh",
      { auth: false, body: { refresh_token: current.refresh_token } },
    )
      .then((s) => {
        this.#session = s;
        return s;
      })
      .finally(() => {
        this.#refreshing = null;
      });
    return this.#refreshing;
  }

  /**
   * Access token for the next request, auto-refreshing when the stored
   * `access_expires_at` has passed (with a small skew). Returns undefined
   * when there is no session (callers fall back to the static key).
   */
  async accessToken(): Promise<string | undefined> {
    const s = this.#session;
    if (s === null) return undefined;
    const expiresAt = Date.parse(s.access_expires_at);
    if (Number.isFinite(expiresAt) && expiresAt - EXPIRY_SKEW_MS <= Date.now()) {
      const refreshed = await this.refreshSession();
      return refreshed.access_token;
    }
    return s.access_token;
  }

  /** `POST /auth/v1/verify-email` → `{ ok: true }`. */
  async verifyEmail(params: {
    token: string;
    projectId?: string;
  }): Promise<{ ok: boolean }> {
    return requestJson(this.#ctx, "POST", "/auth/v1/verify-email", {
      auth: false,
      body: { project_id: this.#projectId(params.projectId), token: params.token },
    });
  }

  /** `POST /auth/v1/request-password-reset` → `{ ok: true }`. */
  async requestPasswordReset(params: {
    email: string;
    projectId?: string;
  }): Promise<{ ok: boolean }> {
    return requestJson(this.#ctx, "POST", "/auth/v1/request-password-reset", {
      auth: false,
      body: { project_id: this.#projectId(params.projectId), email: params.email },
    });
  }

  /** `POST /auth/v1/reset-password` → `{ ok: true }`. */
  async resetPassword(params: {
    token: string;
    newPassword: string;
    projectId?: string;
  }): Promise<{ ok: boolean }> {
    return requestJson(this.#ctx, "POST", "/auth/v1/reset-password", {
      auth: false,
      body: {
        project_id: this.#projectId(params.projectId),
        token: params.token,
        new_password: params.newPassword,
      },
    });
  }

  /**
   * `POST /auth/v1/magic-link` — project-agnostic email-link login request.
   * 204 always (never confirms whether the email exists); 503
   * `E_EMAIL_DISABLED` when outbound mail isn't configured.
   */
  async requestMagicLink(email: string): Promise<void> {
    await requestJson(this.#ctx, "POST", "/auth/v1/magic-link", {
      auth: false,
      body: { email },
    });
  }

  /** `POST /auth/v1/magic-link/consume` → token body; stored as the session. */
  async consumeMagicLink(token: string): Promise<Session> {
    const session = await requestJson<Session>(
      this.#ctx,
      "POST",
      "/auth/v1/magic-link/consume",
      { auth: false, body: { token } },
    );
    this.#session = session;
    return session;
  }

  /** `POST /auth/v1/api-keys` (JWT-gated) → issued key incl. one-time secret. */
  async createApiKey(name: string): Promise<ApiKeyIssued> {
    return requestJson(this.#ctx, "POST", "/auth/v1/api-keys", {
      body: { name },
    });
  }

  /** `GET /auth/v1/api-keys` (JWT-gated). */
  async listApiKeys(): Promise<ApiKeyDescriptor[]> {
    return requestJson(this.#ctx, "GET", "/auth/v1/api-keys");
  }

  /** `DELETE /auth/v1/api-keys/:id` (JWT-gated) → `{ ok: true }`. */
  async deleteApiKey(id: number): Promise<{ ok: boolean }> {
    return requestJson(this.#ctx, "DELETE", `/auth/v1/api-keys/${id}`);
  }

  // -------------------------------------------------------------------------
  // OAuth (crates/basin-rest/src/routes/auth.rs oauth_authorize)
  // -------------------------------------------------------------------------

  /**
   * `GET /auth/v1/oauth/:provider/authorize?project_id=&redirect_to=`
   * → `{ redirect_url, state }`.
   *
   * Builds the provider authorize URL server-side (with PKCE + signed CSRF
   * state) and returns it. **The SDK cannot perform the browser redirect
   * itself** — redirect the user's browser to `result.redirect_url`. After
   * the OAuth flow completes, the server's callback handler
   * (`GET /auth/v1/oauth/:provider/callback`) exchanges the code and issues
   * Basin JWT + refresh tokens via the normal token body.
   *
   * Supported providers (preset): google, github, apple, bitbucket, discord,
   * figma, gitlab, linkedin, microsoft (azure_ad), notion, slack, spotify,
   * twitch, twitter_x. Custom OIDC providers can be registered server-side;
   * pass their name here.
   *
   * @param provider  Lower-case provider name, e.g. `"google"` or `"github"`.
   * @param opts.redirectTo  URL to redirect to after the flow completes.
   *   Validated server-side against the per-project redirect_uri allowlist.
   * @param opts.projectId  Defaults to the client's configured project id.
   */
  async getOAuthAuthorizeUrl(
    provider: string,
    opts: { redirectTo?: string; projectId?: string } = {},
  ): Promise<OAuthAuthorizeResult> {
    return requestJson<OAuthAuthorizeResult>(
      this.#ctx,
      "GET",
      `/auth/v1/oauth/${encodeURIComponent(provider)}/authorize`,
      {
        auth: false,
        query: [
          ["project_id", this.#projectId(opts.projectId)],
          ["redirect_to", opts.redirectTo ?? ""],
        ],
      },
    );
  }

  // -------------------------------------------------------------------------
  // MFA (crates/basin-rest/src/routes/auth.rs enroll_factor / factors*)
  // -------------------------------------------------------------------------

  /**
   * `POST /auth/v1/factors` — begin MFA factor enrollment (JWT-gated).
   *
   * Returns a `TotpEnrollResult` for `factor_type = "totp"` or a
   * `WebAuthnEnrollResult` for `factor_type = "webauthn"`. The discriminant
   * field `factor_type` distinguishes the two shapes.
   *
   * For WebAuthn: `result.creation_options` is already parsed from the
   * server's `creation_options_json` field and is ready for
   * `navigator.credentials.create({ publicKey: result.creation_options })`.
   *
   * The factor is `unverified` until `verifyFactor()` succeeds.
   *
   * @param factorType  `"totp"` or `"webauthn"`.
   * @param opts.friendlyName  Human-readable label, e.g. `"Authenticator App"`.
   */
  async enrollFactor(
    factorType: "totp" | "webauthn",
    opts: { friendlyName?: string } = {},
  ): Promise<MfaEnrollResult> {
    const raw = await requestJson<{
      factor_id: string;
      factor_type: string;
      secret_b32?: string;
      otpauth_uri?: string;
      challenge_id?: string;
      creation_options_json?: string;
    }>(this.#ctx, "POST", "/auth/v1/factors", {
      body: {
        factor_type: factorType,
        friendly_name: opts.friendlyName ?? "",
      },
    });

    if (raw.factor_type === "webauthn") {
      const result: WebAuthnEnrollResult = {
        factor_id: raw.factor_id,
        factor_type: "webauthn",
        challenge_id: raw.challenge_id!,
        creation_options: JSON.parse(raw.creation_options_json!),
      };
      return result;
    }

    const result: TotpEnrollResult = {
      factor_id: raw.factor_id,
      factor_type: "totp",
      secret_b32: raw.secret_b32!,
      otpauth_uri: raw.otpauth_uri!,
    };
    return result;
  }

  /**
   * `GET /auth/v1/factors` — list MFA factors for the current user (JWT-gated).
   *
   * Returns metadata descriptors only; the secret / credential bytes are
   * never included.
   */
  async listFactors(): Promise<FactorDescriptor[]> {
    const data = await requestJson<FactorDescriptor[] | null>(
      this.#ctx,
      "GET",
      "/auth/v1/factors",
    );
    return data ?? [];
  }

  /**
   * `POST /auth/v1/factors/:id/verify` — confirm enrollment (JWT-gated).
   *
   * Call after `enrollFactor` to promote the factor from `unverified` to
   * `verified`.
   *
   * - TOTP path: supply `code` (6-digit OTP from the authenticator app).
   * - WebAuthn path: supply `attestation` (JSON from
   *   `navigator.credentials.create()`) and `challengeId`.
   *
   * `recovery_codes` (8 hex strings) is present only on the **first** verified
   * factor for a user — store these securely, they are shown exactly once.
   *
   * @param factorId   UUID returned by `enrollFactor`.
   * @param opts.code        TOTP 6-digit code.
   * @param opts.attestation WebAuthn attestation JSON string.
   * @param opts.challengeId WebAuthn challenge id from `enrollFactor`.
   */
  async verifyFactor(
    factorId: string,
    opts: { code?: string; attestation?: string; challengeId?: string } = {},
  ): Promise<VerifyFactorResult> {
    const body: Record<string, string> = {};
    if (opts.code !== undefined) body["code"] = opts.code;
    if (opts.attestation !== undefined) body["attestation"] = opts.attestation;
    if (opts.challengeId !== undefined) body["challenge_id"] = opts.challengeId;
    return requestJson<VerifyFactorResult>(
      this.#ctx,
      "POST",
      `/auth/v1/factors/${factorId}/verify`,
      { body },
    );
  }

  /**
   * `POST /auth/v1/factors/:id/challenge` — begin a step-up challenge (JWT-gated).
   *
   * Returns a `TotpChallengeResult` (only `challenge_id`) or a
   * `WebAuthnChallengeResult` (`challenge_id` + `request_options`). The server
   * auto-detects the factor type.
   *
   * For WebAuthn: `result.request_options` is already parsed from the
   * server's `request_options_json` field and is ready for
   * `navigator.credentials.get({ publicKey: result.request_options })`.
   *
   * @param factorId  UUID of a `verified` factor.
   */
  async challengeFactor(factorId: string): Promise<MfaChallengeResult> {
    const raw = await requestJson<{
      challenge_id: string;
      request_options_json?: string | null;
    }>(this.#ctx, "POST", `/auth/v1/factors/${factorId}/challenge`, {
      body: {},
    });

    if (raw.request_options_json != null) {
      const result: WebAuthnChallengeResult = {
        challenge_id: raw.challenge_id,
        request_options: JSON.parse(raw.request_options_json),
      };
      return result;
    }

    const result: TotpChallengeResult = { challenge_id: raw.challenge_id };
    return result;
  }

  /**
   * `POST /auth/v1/factors/:id/challenge/verify` — complete step-up → AAL2 JWT.
   *
   * On success returns a new `Session` whose access token carries `aal2` in
   * its JWT claims. The client's session is updated automatically, so
   * subsequent requests will carry the elevated access token.
   *
   * - TOTP path: supply `code` (6-digit OTP).
   * - WebAuthn path: supply `assertion` (JSON string from
   *   `navigator.credentials.get()`).
   *
   * @param factorId    UUID of the factor being challenged.
   * @param challengeId `challenge_id` returned by `challengeFactor`.
   * @param opts.code       TOTP 6-digit code.
   * @param opts.assertion  WebAuthn assertion JSON string.
   */
  async verifyChallenge(
    factorId: string,
    challengeId: string,
    opts: { code?: string; assertion?: string } = {},
  ): Promise<Session> {
    const body: Record<string, string> = { challenge_id: challengeId };
    if (opts.code !== undefined) body["code"] = opts.code;
    if (opts.assertion !== undefined) body["assertion"] = opts.assertion;
    const session = await requestJson<Session>(
      this.#ctx,
      "POST",
      `/auth/v1/factors/${factorId}/challenge/verify`,
      { body },
    );
    this.#session = session;
    return session;
  }

  /**
   * `DELETE /auth/v1/factors/:id` — unenroll an MFA factor (JWT-gated).
   *
   * Requires an `aal2` access token (obtained via `verifyChallenge`). The
   * server enforces this and will return `E_FORBIDDEN` if the session is
   * `aal1` only.
   *
   * @param factorId  UUID of the factor to remove.
   */
  async unenrollFactor(factorId: string): Promise<{ ok: boolean }> {
    return requestJson<{ ok: boolean }>(
      this.#ctx,
      "DELETE",
      `/auth/v1/factors/${factorId}`,
    );
  }
}
