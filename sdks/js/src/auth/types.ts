/**
 * Auth types — mirror basin-auth's `/auth/v1/*` envelopes (served by the
 * basin engine itself; as of 2026-05-11 basin-auth's catalog lives on
 * the engine via loopback pgwire, not on basin-cloud or an external
 * Postgres).
 *
 * ── SQL session functions ──────────────────────────────────────────────
 *
 * basin-auth injects three SQL functions into every authenticated
 * connection. Use them in RLS policies or plain SELECT queries after a
 * user is signed in:
 *
 *   auth.uid()   → uuid     — UUID of the currently authenticated user
 *   auth.role()  → text     — 'authenticated' | 'anon'
 *   auth.jwt()   → jsonb    — full JWT claims (sub, email, role, exp, …)
 *
 * RLS example (run once during schema setup):
 *
 * ```sql
 * ALTER TABLE items ENABLE ROW LEVEL SECURITY;
 * CREATE POLICY "users see own rows" ON items
 *   FOR ALL USING (owner_id = auth.uid());
 * ```
 *
 * After `basin.auth.signInWithPassword(...)`, PostgREST picks up the
 * session's `access_token` (attached automatically as `Authorization:
 * Bearer <at>` by the query builder) and the engine evaluates
 * `auth.uid()` as the signed-in user's UUID. Anonymous requests — those
 * using only the anon `apikey` — get `auth.role() = 'anon'` and
 * `auth.uid() = null`.
 *
 * ── pgwire connection strings ──────────────────────────────────────────
 *
 * If you connect to the engine's pgwire listener directly (port 5433
 * by default on self-hosted), the username format is:
 *
 *   {tenant_id}_{hex}   — for API-key auth (pass the API key as password)
 *   <access_token>      — for JWT / session auth (pass the JWT as username,
 *                         password is ignored)
 *
 * The SDK itself never constructs pgwire connection strings — the engine
 * returns them from the API and they are passed through opaquely.
 *
 * ── API key format ─────────────────────────────────────────────────────
 *
 * basin API keys have the format `basin_{tenant_id}_{base64}`. The SDK
 * treats them as opaque strings and forwards them verbatim in the
 * `apikey` header. No parsing or validation is performed client-side.
 */

/** One of the 14 BYO-OAuth providers basin-cloud supports. */
export type OAuthProvider =
  | "google"
  | "github"
  | "microsoft"
  | "gitlab"
  | "slack"
  | "discord"
  | "apple"
  | "x"
  | "bitbucket"
  | "notion"
  | "spotify"
  | "twitch"
  | "linkedin"
  | "figma";

export interface AuthUser {
  id: string;
  email: string | null;
  email_confirmed_at: string | null;
  phone: string | null;
  created_at: string;
  updated_at: string;
  app_metadata: Record<string, unknown>;
  user_metadata: Record<string, unknown>;
}

export interface AuthSession {
  access_token: string;
  refresh_token: string;
  token_type: "bearer";
  /** Seconds-since-epoch when the access token expires. */
  expires_at: number;
  user: AuthUser;
}

export type AuthChangeEvent =
  | "INITIAL_SESSION"
  | "SIGNED_IN"
  | "SIGNED_OUT"
  | "TOKEN_REFRESHED"
  | "USER_UPDATED"
  | "PASSWORD_RECOVERY"
  | "MFA_CHALLENGE_VERIFIED";

/** Factor families understood by `basin.auth.mfa.enroll`. */
export type MFAFactorType = "totp" | "webauthn";

export interface MFAEnrollTOTPInput {
  factor: "totp";
}

export interface MFAEnrollWebAuthnInput {
  factor: "webauthn";
}

export type MFAEnrollInput = MFAEnrollTOTPInput | MFAEnrollWebAuthnInput;

/**
 * TOTP enrollment payload returned by `basin.auth.mfa.enroll({factor:"totp"})`.
 * The caller renders `qr_url` (an `otpauth://...` URI) as a QR code,
 * the user adds the secret to their authenticator, and then confirms
 * by passing the displayed `code` plus the same `secret` + `recovery_codes`
 * back to `basin.auth.mfa.verify({factor:"totp", code, secret, recovery_codes})`.
 */
export interface MFAEnrollTOTPResult {
  factor: "totp";
  /** Base32-encoded TOTP secret — pass back to `verify` unchanged. */
  secret: string;
  /** `otpauth://totp/...` URI. Encode as a QR code for the user's app. */
  qr_url: string;
  /** Plaintext recovery codes — shown ONCE. Caller surfaces these. */
  recovery_codes: string[];
}

export interface MFAEnrollWebAuthnResult {
  factor: "webauthn";
  /** Server-issued PublicKeyCredentialCreationOptions to feed to `navigator.credentials.create`. */
  options: unknown;
}

export type MFAEnrollResult = MFAEnrollTOTPResult | MFAEnrollWebAuthnResult;

/**
 * Verification input. Three shapes:
 *  - `{factor:"totp", code, secret, recovery_codes}` — confirms a fresh enrollment.
 *  - `{factor:"totp_challenge", code, partial_token}` — completes a mid-login MFA challenge
 *     (the cloud emitted `requires_totp` from `/auth/v1/login`).
 *  - `{factor:"webauthn", assertion}` — completes a passkey ceremony.
 */
export type MFAVerifyInput =
  | {
      factor: "totp";
      code: string;
      secret: string;
      recovery_codes?: string[];
    }
  | {
      factor: "totp_challenge";
      code: string;
      partial_token: string;
    }
  | {
      factor: "webauthn";
      assertion: unknown;
    };

export type MFAVerifyResult =
  | { factor: "totp"; enabled: true }
  | { factor: "totp_challenge"; session: AuthSession }
  | { factor: "webauthn"; verified: true };

export interface SignUpInput {
  email: string;
  password: string;
  /** Optional metadata stored on `users.user_metadata`. */
  data?: Record<string, unknown>;
  /** URL the user lands on after email confirmation. */
  emailRedirectTo?: string;
}

export interface SignInWithPasswordInput {
  email: string;
  password: string;
}

export interface SignInWithOAuthInput {
  provider: OAuthProvider;
  /** URL the OAuth provider redirects back to after consent. */
  redirectTo?: string;
  /** Provider-specific scopes — e.g. `repo,read:user` for GitHub. */
  scopes?: string;
  /** Optional extra query params forwarded to the provider. */
  queryParams?: Record<string, string>;
}

export interface SignInWithMagicLinkInput {
  email: string;
  /** URL the magic-link callback lands on. */
  emailRedirectTo?: string;
  /** When true, only sign existing users in; reject unknown emails. */
  shouldCreateUser?: boolean;
}

export interface ConsumeMagicLinkInput {
  /** The one-time token from the magic-link URL (query param `token`). */
  token: string;
}

export interface VerifyEmailInput {
  /** The one-time token from the email-verification link (`token` query param). */
  token: string;
}

export interface RequestPasswordResetInput {
  email: string;
  /** URL the reset-link email points back to. */
  emailRedirectTo?: string;
}

export interface ResetPasswordInput {
  /** The one-time token from the reset-password email (`token` query param). */
  token: string;
  /** The new password to set. */
  newPassword: string;
}

/**
 * Information about how to authenticate against the engine's pgwire
 * listener (port 5433 by default) and what session helpers are available
 * in SQL after authentication.
 *
 * For most SDK users this is informational — `basin.auth.*` and
 * `basin.from(...)` handle auth automatically. Direct pgwire connections
 * are an advanced use case (e.g. running raw migrations, connecting via
 * psql / DBeaver, or server-side connection pools).
 *
 * pgwire username formats:
 *   - API-key auth:  username = `{tenant_id}_{hex}`, password = API key
 *   - JWT/session:   username = access_token JWT, password is ignored
 *
 * After any authenticated connection, these SQL session functions
 * are available:
 *   - `auth.uid()`  → uuid   — UUID of the authenticated user
 *   - `auth.role()` → text   — 'authenticated' | 'anon'
 *   - `auth.jwt()`  → jsonb  — full JWT claims as JSONB
 */
export interface PgwireAuthInfo {
  /** The access token from `AuthSession.access_token` — pass as the pgwire username. */
  accessToken: string;
  /** The engine pgwire host (default port 5433). */
  host: string;
  /** The database name (default `basin`). */
  database?: string;
}
