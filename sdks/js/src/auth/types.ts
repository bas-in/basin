/**
 * Auth types — mirror the dashboard's `/auth/v1/*` envelopes.
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
