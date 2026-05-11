/**
 * @bas-in/basin-js — isomorphic client for the Basin platform.
 *
 * Engine-direct: the SDK talks DIRECTLY to basin-engine (the OSS Rust
 * data plane) — never through basin-cloud. basin-cloud is the control
 * plane (orgs / projects / billing / dashboard); the engine is the
 * data plane. Point `createClient` at your engine URL (e.g.
 * `https://basin-engine.fly.dev`, or `http://localhost:5434` for
 * `cargo run -p basin-server`).
 *
 * Single entry point:
 *
 *   import { createClient } from '@bas-in/basin-js'
 *   const basin = createClient(BASIN_ENGINE_URL, BASIN_ANON_KEY)
 *
 * Namespaces exposed on the client: `auth`, `from()` (PostgREST-shaped
 * table builder), `storage`, `realtime` + `channel()`, and `functions`.
 * `auth.signInWithOAuth`, the full `auth.mfa.*` surface, the full
 * `storage` surface, realtime, and functions all return
 * `BasinError("not_implemented")` today — they land when basin-engine
 * grows the corresponding routes in v0.2+.
 */

export { createClient } from "./client.js";
export type { BasinClient, BasinClientOptions } from "./client.js";

export { BasinError } from "./errors.js";
export type { BasinErrorCode } from "./errors.js";

// Auth — re-export the AuthClient class + every Auth* type so consumers
// can write typed callbacks against `onAuthStateChange` etc. without
// reaching into `/auth`.
export { AuthClient, AuthMfaClient } from "./auth/index.js";
export type {
  AuthSession,
  AuthUser,
  AuthChangeEvent,
  OAuthProvider,
  SignUpInput,
  SignInWithPasswordInput,
  SignInWithOAuthInput,
  SignInWithMagicLinkInput,
  ConsumeMagicLinkInput,
  VerifyEmailInput,
  RequestPasswordResetInput,
  ResetPasswordInput,
  PgwireAuthInfo,
  MFAFactorType,
  MFAEnrollInput,
  MFAEnrollResult,
  MFAEnrollTOTPResult,
  MFAEnrollWebAuthnResult,
  MFAVerifyInput,
  MFAVerifyResult,
} from "./auth/index.js";

// Realtime + Functions — Tier 4 / Tier 5 placeholders.
export { RealtimeClient, RealtimeChannel } from "./realtime/index.js";
export type {
  RealtimeEvent,
  PostgresChangesPayload,
  PostgresChangesFilter,
  RealtimeListener,
} from "./realtime/index.js";

export { FunctionsClient } from "./functions/index.js";
export type { InvokeOptions, InvokeResult } from "./functions/index.js";
