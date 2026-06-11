/**
 * `.auth` — bindings for `/auth/v1/*`.
 *
 * Route sources (verified):
 * - `POST /auth/v1/signup`                 crates/basin-rest/src/server.rs:250
 * - `POST /auth/v1/signin`                 crates/basin-rest/src/server.rs:251
 * - `POST /auth/v1/refresh`                crates/basin-rest/src/server.rs:252
 * - `POST /auth/v1/verify-email`           crates/basin-rest/src/server.rs:253
 * - `POST /auth/v1/reset-password`         crates/basin-rest/src/server.rs:254
 * - `POST /auth/v1/request-password-reset` crates/basin-rest/src/server.rs:255
 * - `POST /auth/v1/magic-link`             crates/basin-rest/src/server.rs:262
 * - `POST /auth/v1/magic-link/consume`     crates/basin-rest/src/server.rs:263
 * - `POST|GET /auth/v1/api-keys`           crates/basin-rest/src/server.rs:267
 * - `DELETE /auth/v1/api-keys/:id`         crates/basin-rest/src/server.rs:271
 *
 * Flow notes (from crates/basin-rest/src/routes/auth.rs + basin-auth):
 * - signup/signin take a `project_id` in the body — auth is per-project.
 * - refresh rotates the refresh token; reusing a rotated token surfaces as
 *   `E_REVOKED_TOKEN` (401).
 * - There is NO server-side sign-out route; `signOut()` only clears the local
 *   session. Revocation happens via refresh-token rotation on the server.
 */

import { BasinApiError } from "./errors.js";
import type { ClientContext } from "./http.js";
import { requestJson } from "./http.js";
import type {
  ApiKeyDescriptor,
  ApiKeyIssued,
  Session,
  SignUpResult,
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
   * Clear the local session. The server exposes no sign-out / token-revoke
   * route (verified against crates/basin-rest/src/server.rs); the refresh
   * token simply expires or is invalidated by rotation.
   */
  signOut(): void {
    this.#session = null;
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
}
