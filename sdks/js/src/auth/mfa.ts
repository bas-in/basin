/**
 * AuthMfaClient — TOTP enrollment + verification, plus a mid-login
 * `totp_challenge` redemption path. Exposed at `basin.auth.mfa`.
 *
 * Returns `BasinError("not_implemented")` from every method today —
 * basin-engine v0.1 has no MFA surface (verified against
 * `basin/crates/basin-rest/src/server.rs`: no `/auth/v1/mfa/*` routes,
 * no `/v1/auth/totp/challenge`). The types in `./types.ts` stay intact
 * so app code can compile against the future shape; the bodies land
 * when basin-rest grows the `/auth/v1/mfa/*` surface in v0.2+.
 *
 * Future wire contract (when implemented):
 *  - `POST /auth/v1/mfa/totp/enable`  → `{secret, qr_url, recovery_codes}` (session-walled)
 *  - `POST /auth/v1/mfa/totp/confirm` → `{enabled: true}`                    (session-walled)
 *  - `POST /auth/v1/mfa/totp/disable` → `{disabled: true}`                   (session-walled)
 *  - `POST /auth/v1/mfa/totp/challenge` → loginResponse (`{user, session}`)  (public)
 *  - `POST /auth/v1/mfa/webauthn/{register,login}/{begin,finish}` ceremonies
 */

import { BasinError } from "../errors.js";
import type {
  AuthSession,
  MFAEnrollInput,
  MFAEnrollResult,
  MFAVerifyInput,
  MFAVerifyResult,
} from "./types.js";

/**
 * Adapter the parent AuthClient hands to the MFA sub-namespace. Kept
 * intact for forward compatibility — when the engine's MFA surface
 * lands the SDK will use `adoptSession` to splice a fresh session
 * back into the parent on `totp_challenge` success.
 */
export interface AuthMfaDeps {
  url: string;
  headers: Record<string, string>;
  fetch: typeof fetch;
  /** Current session, or null. Used to read `access_token` for bearer auth. */
  getSession: () => AuthSession | null;
  /**
   * Hand a fresh session back to the parent AuthClient — used after
   * `verify({factor:"totp_challenge"})` succeeds. The AuthClient
   * persists to storage + emits `SIGNED_IN` and `MFA_CHALLENGE_VERIFIED`.
   */
  adoptSession: (session: AuthSession) => Promise<void>;
}

export class AuthMfaClient {
  // Deps captured for forward compatibility — unused today. The
  // underscore-prefixed retention silences `noUnusedParameters` while
  // keeping the constructor signature stable for the v0.2 swap.
  readonly #deps: AuthMfaDeps;

  constructor(deps: AuthMfaDeps) {
    this.#deps = deps;
  }

  /**
   * Begin enrolling a new MFA factor.
   *
   * Returns `BasinError("not_implemented")` today — basin-engine has
   * no `/auth/v1/mfa/*` routes. The signature stays so app code can
   * compile against the future shape; lands when basin-rest grows the
   * MFA surface in v0.2+.
   *
   * @example
   * // Returns { data: null, error: BasinError('not_implemented', ...) }
   * const { error } = await basin.auth.mfa.enroll({ factor: 'totp' });
   */
  async enroll(
    _input: MFAEnrollInput,
  ): Promise<{ data: MFAEnrollResult | null; error: BasinError | null }> {
    void this.#deps;
    return {
      data: null,
      error: new BasinError(
        "not_implemented",
        "auth.mfa.enroll (MFA) ships when the engine route lands — tracked in ROADMAP 0.3",
      ),
    };
  }

  /**
   * Complete a TOTP enrollment OR redeem a mid-login MFA challenge.
   *
   * Returns `BasinError("not_implemented")` today — basin-engine has
   * no MFA surface. Lands in basin v0.2+ when the engine grows the
   * `/auth/v1/mfa/totp/{confirm,challenge}` + WebAuthn ceremonies.
   *
   * @example
   * // Returns { data: null, error: BasinError('not_implemented', ...) }
   * const { error } = await basin.auth.mfa.verify({
   *   factor: 'totp', code: '123456', secret: '...',
   * });
   */
  async verify(
    _input: MFAVerifyInput,
  ): Promise<{ data: MFAVerifyResult | null; error: BasinError | null }> {
    return {
      data: null,
      error: new BasinError(
        "not_implemented",
        "auth.mfa.verify (MFA) ships when the engine route lands — tracked in ROADMAP 0.3",
      ),
    };
  }

  /**
   * Disable TOTP for the current user.
   *
   * Returns `BasinError("not_implemented")` today — basin-engine has
   * no `/auth/v1/mfa/totp/disable` route. Lands in basin v0.2+.
   *
   * @example
   * // Returns { data: null, error: BasinError('not_implemented', ...) }
   * const { error } = await basin.auth.mfa.unenroll({ code: '123456' });
   */
  async unenroll(
    _input: { code: string },
  ): Promise<{ data: { disabled: true } | null; error: BasinError | null }> {
    return {
      data: null,
      error: new BasinError(
        "not_implemented",
        "auth.mfa.unenroll (MFA) ships when the engine route lands — tracked in ROADMAP 0.3",
      ),
    };
  }
}
