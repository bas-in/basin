/**
 * Auth namespace public exports. `AuthClient` is constructed by
 * `createClient` — consumers don't instantiate it directly. They reach
 * it via `basin.auth`.
 */

export { AuthClient } from "./client.js";
export { AuthMfaClient } from "./mfa.js";
export type {
  AuthSession,
  AuthUser,
  AuthChangeEvent,
  SignUpInput,
  SignInWithPasswordInput,
  SignInWithOAuthInput,
  SignInWithMagicLinkInput,
  ConsumeMagicLinkInput,
  VerifyEmailInput,
  RequestPasswordResetInput,
  ResetPasswordInput,
  PgwireAuthInfo,
  OAuthProvider,
  MFAFactorType,
  MFAEnrollInput,
  MFAEnrollResult,
  MFAEnrollTOTPResult,
  MFAEnrollWebAuthnResult,
  MFAVerifyInput,
  MFAVerifyResult,
} from "./types.js";
