/**
 * AuthMfaClient — TOTP + WebAuthn enrollment, verification, challenge,
 * challenge-verify, and unenroll. Exposed at `basin.auth.mfa`.
 *
 * Wire contract (ADR 0020, engine tasks 5.10.M — routes are FINAL):
 *  - enroll  → POST /auth/v1/factors
 *  - verify  → POST /auth/v1/factors/:id/verify
 *  - challenge       → POST /auth/v1/factors/:id/challenge
 *  - challengeVerify → POST /auth/v1/factors/:id/challenge/verify
 *  - unenroll        → DELETE /auth/v1/factors/:id
 *
 * All session-walled routes attach `Authorization: Bearer <access_token>`
 * from the parent AuthClient's active session. On challenge-verify success
 * the engine returns an aal2 JWT; the SDK adopts it via `adoptSession`.
 */

import { BasinError } from "../errors.js";
import type {
  AuthSession,
  MFAChallengeInput,
  MFAChallengeResult,
  MFAChallengeVerifyInput,
  MFAEnrollInput,
  MFAEnrollResult,
  MFAUnenrollInput,
  MFAVerifyInput,
  MFAVerifyResult,
} from "./types.js";

/**
 * Adapter the parent AuthClient hands to the MFA sub-namespace. Kept
 * intact — `adoptSession` splices a fresh aal2 session back into the
 * parent on `challengeVerify` success.
 */
export interface AuthMfaDeps {
  url: string;
  headers: Record<string, string>;
  fetch: typeof fetch;
  /** Current session, or null. Used to read `access_token` for bearer auth. */
  getSession: () => AuthSession | null;
  /**
   * Hand a fresh session back to the parent AuthClient — used after
   * `challengeVerify()` succeeds. The AuthClient persists to storage +
   * emits `MFA_CHALLENGE_VERIFIED`.
   */
  adoptSession: (session: AuthSession) => Promise<void>;
}

export class AuthMfaClient {
  readonly #deps: AuthMfaDeps;

  constructor(deps: AuthMfaDeps) {
    this.#deps = deps;
  }

  // ── private helpers ──────────────────────────────────────────────────

  /** Build headers with bearer auth when a session is available. */
  #headers(): Record<string, string> {
    const session = this.#deps.getSession();
    if (!session?.access_token) return this.#deps.headers;
    return { ...this.#deps.headers, Authorization: `Bearer ${session.access_token}` };
  }

  /** Shared fetch wrapper — returns the parsed JSON body or a BasinError. */
  async #post(
    path: string,
    body: unknown,
  ): Promise<{ data: unknown; error: BasinError | null }> {
    let res: Response;
    try {
      res = await this.#deps.fetch(`${this.#deps.url}${path}`, {
        method: "POST",
        headers: this.#headers(),
        body: JSON.stringify(body),
      });
    } catch (e) {
      return {
        data: null,
        error: new BasinError("network", networkErrorMessage(e)),
      };
    }
    return parseResponse(res);
  }

  async #delete(path: string): Promise<{ data: unknown; error: BasinError | null }> {
    let res: Response;
    try {
      res = await this.#deps.fetch(`${this.#deps.url}${path}`, {
        method: "DELETE",
        headers: this.#headers(),
      });
    } catch (e) {
      return {
        data: null,
        error: new BasinError("network", networkErrorMessage(e)),
      };
    }
    return parseResponse(res);
  }

  // ── Public API ───────────────────────────────────────────────────────

  /**
   * Begin enrolling a new MFA factor.
   *
   * POSTs `POST /auth/v1/factors` with `{factor_type}`.
   *
   * TOTP: engine returns `{id, secret, qr_code, recovery_codes}`.
   * WebAuthn: engine returns `{id, options}` — a PublicKeyCredentialCreationOptions
   * challenge to pass to `navigator.credentials.create`.
   *
   * @example
   * const { data, error } = await basin.auth.mfa.enroll({ factor: 'totp' });
   * if (error) return handleError(error);
   * renderQrCode(data.qr_url); // show to user
   */
  async enroll(
    input: MFAEnrollInput,
  ): Promise<{ data: MFAEnrollResult | null; error: BasinError | null }> {
    const { data, error } = await this.#post("/factors", {
      factor_type: input.factor,
    });
    if (error) return { data: null, error };

    const raw = data as Record<string, unknown>;
    if (input.factor === "totp") {
      return {
        data: {
          factor: "totp",
          secret: (raw.secret as string) ?? "",
          qr_url: (raw.qr_code as string) ?? "",
          recovery_codes: (raw.recovery_codes as string[]) ?? [],
        },
        error: null,
      };
    }
    // webauthn
    return {
      data: {
        factor: "webauthn",
        options: raw.options,
      },
      error: null,
    };
  }

  /**
   * Verify a fresh TOTP enrollment (confirm the code) or verify a
   * WebAuthn assertion.
   *
   * - TOTP enroll confirm: POSTs `POST /auth/v1/factors/:id/verify` with
   *   `{code}`. On success the factor is activated.
   * - TOTP mid-login challenge (deprecated path — prefer `challengeVerify`):
   *   POSTs `POST /auth/v1/factors/:id/challenge/verify` using the
   *   `partial_token` as the bearer.
   * - WebAuthn: POSTs `POST /auth/v1/factors/:id/verify` with `{assertion}`.
   *
   * @example
   * // Confirm a fresh TOTP enrollment:
   * await basin.auth.mfa.verify({ factor: 'totp', factorId: id, code: '123456' });
   */
  async verify(
    input: MFAVerifyInput,
  ): Promise<{ data: MFAVerifyResult | null; error: BasinError | null }> {
    if (input.factor === "totp") {
      // Confirm a TOTP enrollment — factorId must be on the input.
      // The existing MFAVerifyInput for "totp" carries secret + code from
      // the enrollment flow. The engine's verify endpoint needs the factor id.
      // The input carries {factor, code, secret, recovery_codes?} — the SDK
      // sends code; the engine validates against the stored secret.
      // Since the existing type doesn't carry factorId on the "totp" branch
      // (it's an enrollment-confirm shape), we POST to /factors/<id>/verify.
      // The factorId was returned by enroll(); callers pass it via the type's
      // optional extension. We accept it if present, else error gracefully.
      const extended = input as typeof input & { factorId?: string };
      if (!extended.factorId) {
        return {
          data: null,
          error: new BasinError(
            "invalid_request",
            "auth.mfa.verify for totp requires factorId",
          ),
        };
      }
      const { data, error } = await this.#post(`/factors/${extended.factorId}/verify`, {
        code: input.code,
      });
      if (error) return { data: null, error };
      void data;
      return { data: { factor: "totp", enabled: true }, error: null };
    }

    if (input.factor === "totp_challenge") {
      // Mid-login MFA challenge redemption. The engine issued a partial_token
      // after password auth; the caller now submits the TOTP code along with
      // the factorId. The partial_token is used as the bearer.
      const extended = input as typeof input & { factorId?: string };
      if (!extended.factorId) {
        return {
          data: null,
          error: new BasinError(
            "invalid_request",
            "auth.mfa.verify for totp_challenge requires factorId",
          ),
        };
      }
      let res: Response;
      try {
        res = await this.#deps.fetch(
          `${this.#deps.url}/factors/${extended.factorId}/challenge/verify`,
          {
            method: "POST",
            headers: {
              ...this.#deps.headers,
              Authorization: `Bearer ${input.partial_token}`,
            },
            body: JSON.stringify({ code: input.code }),
          },
        );
      } catch (e) {
        return {
          data: null,
          error: new BasinError("network", networkErrorMessage(e)),
        };
      }
      const { data, error } = await parseResponse(res);
      if (error) return { data: null, error };
      const session = mapToSession(data as Record<string, unknown>);
      if (!session) {
        return {
          data: null,
          error: new BasinError(
            "invalid_response",
            "MFA challenge verify succeeded but response is missing session fields",
            res.status,
          ),
        };
      }
      await this.#deps.adoptSession(session);
      return { data: { factor: "totp_challenge", session }, error: null };
    }

    // webauthn
    {
      const extended = input as typeof input & { factorId?: string };
      if (!extended.factorId) {
        return {
          data: null,
          error: new BasinError(
            "invalid_request",
            "auth.mfa.verify for webauthn requires factorId",
          ),
        };
      }
      const { data, error } = await this.#post(`/factors/${extended.factorId}/verify`, {
        assertion: input.assertion,
      });
      if (error) return { data: null, error };
      void data;
      return { data: { factor: "webauthn", verified: true }, error: null };
    }
  }

  /**
   * Initiate a step-up challenge for an already-enrolled factor.
   *
   * POSTs `POST /auth/v1/factors/:id/challenge`. The engine issues a
   * challenge object; the caller passes the returned `id` (challenge ID)
   * to `challengeVerify` together with the user's code.
   *
   * @example
   * const { data } = await basin.auth.mfa.challenge({ factorId: 'factor-id' });
   * const code = await promptUser('Enter 6-digit code');
   * await basin.auth.mfa.challengeVerify({
   *   factorId: 'factor-id', challengeId: data.id, code,
   * });
   */
  async challenge(
    input: MFAChallengeInput,
  ): Promise<{ data: MFAChallengeResult | null; error: BasinError | null }> {
    const { data, error } = await this.#post(
      `/factors/${input.factorId}/challenge`,
      {},
    );
    if (error) return { data: null, error };
    const raw = data as Record<string, unknown>;
    return {
      data: {
        id: (raw.id as string) ?? "",
        type: (raw.type as "totp" | "webauthn") ?? "totp",
        expires_at:
          typeof raw.expires_at === "number"
            ? raw.expires_at
            : Math.floor(new Date((raw.expires_at as string) ?? 0).getTime() / 1000),
      },
      error: null,
    };
  }

  /**
   * Complete a step-up challenge and re-issue an aal2 session.
   *
   * POSTs `POST /auth/v1/factors/:id/challenge/verify` with either
   * `{challenge_id, code}` (TOTP) or `{challenge_id, assertion}` (WebAuthn).
   * On success, the SDK adopts the new aal2 session and fires
   * `MFA_CHALLENGE_VERIFIED`.
   *
   * @example
   * const { data, error } = await basin.auth.mfa.challengeVerify({
   *   factorId: 'factor-id', challengeId: 'challenge-id', code: '123456',
   * });
   * if (!error) console.log('aal2 session active:', data.session.aal);
   */
  async challengeVerify(
    input: MFAChallengeVerifyInput,
  ): Promise<{ data: { session: AuthSession } | null; error: BasinError | null }> {
    const body =
      "code" in input
        ? { challenge_id: input.challengeId, code: input.code }
        : { challenge_id: input.challengeId, assertion: input.assertion };

    let res: Response;
    try {
      res = await this.#deps.fetch(
        `${this.#deps.url}/factors/${input.factorId}/challenge/verify`,
        {
          method: "POST",
          headers: this.#headers(),
          body: JSON.stringify(body),
        },
      );
    } catch (e) {
      return {
        data: null,
        error: new BasinError("network", networkErrorMessage(e)),
      };
    }
    const { data, error } = await parseResponse(res);
    if (error) return { data: null, error };
    const session = mapToSession(data as Record<string, unknown>);
    if (!session) {
      return {
        data: null,
        error: new BasinError(
          "invalid_response",
          "Challenge verify succeeded but response is missing session fields",
          res.status,
        ),
      };
    }
    await this.#deps.adoptSession(session);
    return { data: { session }, error: null };
  }

  /**
   * Remove an enrolled MFA factor. Requires an aal2 session.
   *
   * Sends `DELETE /auth/v1/factors/:id`.
   *
   * @example
   * const { error } = await basin.auth.mfa.unenroll({ factorId: 'factor-id' });
   */
  async unenroll(
    input: MFAUnenrollInput,
  ): Promise<{ data: { disabled: true } | null; error: BasinError | null }> {
    const { error } = await this.#delete(`/factors/${input.factorId}`);
    if (error) return { data: null, error };
    return { data: { disabled: true }, error: null };
  }
}

// ── module helpers ───────────────────────────────────────────────────────────

async function parseResponse(
  res: Response,
): Promise<{ data: unknown; error: BasinError | null }> {
  let raw: unknown;
  try {
    raw = await res.json();
  } catch {
    return {
      data: null,
      error: new BasinError(
        "invalid_response",
        `MFA response was not JSON (HTTP ${res.status})`,
        res.status,
      ),
    };
  }
  const body = (raw ?? {}) as Record<string, unknown>;
  const wrapped = "data" in body || "error" in body;
  const payload = wrapped ? body.data : body;
  const envelopeError = wrapped
    ? (body.error as { code?: string; message?: string } | null | undefined)
    : null;

  if (!res.ok) {
    return {
      data: null,
      error: new BasinError(
        envelopeError?.code ?? errorCodeForStatus(res.status),
        envelopeError?.message ?? `MFA request failed (HTTP ${res.status})`,
        res.status,
        envelopeError,
      ),
    };
  }
  return { data: payload, error: null };
}

/**
 * Map an engine session payload to AuthSession. Handles both the flat
 * `{access_token, refresh_token, expires_at, user}` shape and the nested
 * `{user, session}` shape that some MFA endpoints return.
 */
function mapToSession(payload: Record<string, unknown>): AuthSession | null {
  // Nested shape: {user, session}
  if (payload.session && payload.user) {
    const s = payload.session as Record<string, unknown>;
    const u = payload.user as Record<string, unknown>;
    if (!s.access_token || !s.refresh_token || !s.expires_at || !u.id) return null;
    return buildSession(s, u);
  }
  // Flat shape: {access_token, refresh_token, expires_at, user}
  if (payload.access_token && payload.user) {
    const u = payload.user as Record<string, unknown>;
    if (!u.id) return null;
    return buildSession(payload, u);
  }
  // Flat shape without nested user: {access_token, refresh_token, expires_at}
  // plus the current session's user — caller handles this
  return null;
}

function buildSession(
  s: Record<string, unknown>,
  u: Record<string, unknown>,
): AuthSession {
  const expiresAt =
    typeof s.expires_at === "number"
      ? s.expires_at
      : Math.floor(new Date(s.expires_at as string).getTime() / 1000);
  return {
    access_token: s.access_token as string,
    refresh_token: s.refresh_token as string,
    token_type: "bearer",
    expires_at: expiresAt,
    aal: (s.aal as "aal1" | "aal2") ?? "aal2",
    amr: (s.amr as string[]) ?? [],
    user: {
      id: u.id as string,
      email: (u.email as string | null) ?? null,
      email_confirmed_at: u.email_verified ? ((u.updated_at as string) ?? null) : null,
      phone: null,
      created_at: (u.created_at as string) ?? "",
      updated_at: (u.updated_at as string) ?? "",
      app_metadata: {},
      user_metadata: {},
    },
  };
}

function errorCodeForStatus(status: number): string {
  if (status === 401) return "unauthorized";
  if (status === 403) return "forbidden";
  if (status === 404) return "not_found";
  if (status === 409) return "conflict";
  if (status === 429) return "rate_limited";
  if (status >= 500) return "internal";
  return "invalid_request";
}

function networkErrorMessage(e: unknown): string {
  if (e instanceof Error && e.message) return e.message;
  return "network error reaching MFA endpoint";
}
