/** Shared wire types, mirrored from the Rust handlers they bind to. */

/**
 * Token body returned by `/auth/v1/signin`, `/auth/v1/refresh`,
 * `/auth/v1/magic-link/consume` (crates/basin-rest/src/routes/auth.rs
 * `token_body`).
 */
export interface Session {
  access_token: string;
  refresh_token: string;
  /** RFC 3339 */
  access_expires_at: string;
  /** RFC 3339 */
  refresh_expires_at: string;
}

/** `POST /auth/v1/signup` response. */
export interface SignUpResult {
  ok: boolean;
  user_id: string;
}

/** `POST /auth/v1/api-keys` response (the secret is shown exactly once). */
export interface ApiKeyIssued {
  id: number;
  name: string;
  secret: string;
  created_at: string;
}

/** `GET /auth/v1/api-keys` element. */
export interface ApiKeyDescriptor {
  id: number;
  name: string;
  created_at: string;
  last_used_at: string | null;
  revoked_at: string | null;
}

/** Non-row result for writes: `{ ok: true, tag: "..." }`. */
export interface ExecTag {
  ok: boolean;
  tag: string;
}

/** Wrapped GET response when `limit` or `cursor` is supplied. */
export interface Page<T> {
  rows: T[];
  next_cursor: string | null;
}

// ---------------------------------------------------------------------------
// Realtime wire frames (crates/basin-realtime/src/ws.rs ClientMsg/ServerMsg)
// ---------------------------------------------------------------------------

export type ChangeOp = "INSERT" | "UPDATE" | "DELETE";

/** `{"type":"event",...}` server frame. */
export interface RealtimeEvent {
  type: "event";
  project: string;
  table: string;
  op: ChangeOp;
  before?: Record<string, unknown>;
  after?: Record<string, unknown>;
  seq: number;
}

export interface RealtimeErrorFrame {
  type: "error";
  code: string;
  table: string;
  missed?: number;
}

/** Reconnect-resume gap: missed events were evicted; cold re-sync needed. */
export interface RealtimeGapFrame {
  type: "gap";
  table: string;
  last_event_id: number;
  oldest_in_ring: number;
  newest_in_ring: number;
}

export interface PresenceStateFrame {
  type: "presence_state";
  channel: string;
  presences: unknown[];
}

export interface PresenceDiffFrame {
  type: "presence_diff";
  channel: string;
  joins: unknown[];
  leaves: unknown[];
}

export type RealtimeServerFrame =
  | RealtimeEvent
  | RealtimeErrorFrame
  | RealtimeGapFrame
  | PresenceStateFrame
  | PresenceDiffFrame
  | { type: "subscribed"; table: string }
  | { type: "unsubscribed"; table: string }
  | { type: "presenceerror"; code: string; channel: string; message: string };

// ---------------------------------------------------------------------------
// Storage (crates/basin-rest/src/routes/storage.rs, storage_sign.rs)
// ---------------------------------------------------------------------------

export interface Bucket {
  id: string;
  name: string;
  public: boolean;
  file_size_limit: number | null;
  allowed_mime_types: string[];
  created_at: string;
  updated_at: string;
}

export interface StorageObject {
  id: string;
  bucket_id: string;
  path: string;
  size: number;
  mime_type: string | null;
  metadata: unknown;
  owner: string | null;
  etag: string;
  created_at: string;
  updated_at: string;
}

/** `POST /storage/v1/object/sign/upload/:bucket/*path` response. */
export interface SignedUrl {
  /** Server-relative URL: `/storage/v1/object/sign/:project/:bucket/*path?token=…&expires=…` */
  signedUrl: string;
  /** RFC 3339 expiry. */
  expiresAt: string;
}

// ---------------------------------------------------------------------------
// Functions (`ANY /fn/v1/:name`, crates/basin-rest/src/routes/fn_handler.rs)
// ---------------------------------------------------------------------------

export interface FunctionInvokeResult<T = unknown> {
  /** Status returned by the function (proxied verbatim). */
  status: number;
  headers: Headers;
  /** JSON-parsed body when the content-type is JSON, else the raw text. */
  data: T;
}

// ---------------------------------------------------------------------------
// OAuth (crates/basin-rest/src/routes/auth.rs oauth_authorize / oauth_callback)
// ---------------------------------------------------------------------------

/**
 * `GET /auth/v1/oauth/:provider/authorize` response.
 *
 * The SDK cannot perform the browser redirect dance itself. Redirect the
 * user's browser to `redirect_url`; after the provider redirects back to
 * Basin's callback endpoint the server exchanges the code and issues tokens.
 * The server-side callback is `GET /auth/v1/oauth/:provider/callback` and it
 * returns a standard token body (Session).
 */
export interface OAuthAuthorizeResult {
  /** Full provider authorize URL — redirect the browser here. */
  redirect_url: string;
  /** CSRF state value included in redirect_url; echoed back on callback. */
  state: string;
}

// ---------------------------------------------------------------------------
// MFA (crates/basin-rest/src/routes/auth.rs enroll_factor / factors*)
// ---------------------------------------------------------------------------

/**
 * `POST /auth/v1/factors` response for `factor_type = "totp"`.
 *
 * The factor is `unverified` until `POST /auth/v1/factors/:id/verify` succeeds
 * with the first valid OTP code.
 */
export interface TotpEnrollResult {
  factor_id: string;
  /** Discriminant — always `"totp"`. */
  factor_type: "totp";
  /** Base-32 TOTP secret — display in the QR code or give to an auth app. */
  secret_b32: string;
  /** `otpauth://totp/...` URI suitable for QR code display. */
  otpauth_uri: string;
}

/**
 * `POST /auth/v1/factors` response for `factor_type = "webauthn"`.
 *
 * Pass `creation_options` to `navigator.credentials.create()` and then call
 * `verifyFactor` with the attestation response and `challenge_id`.
 *
 * `creation_options` is the JSON-parsed form of the server's
 * `creation_options_json` field — ready for direct use with the WebAuthn
 * browser API. Do NOT re-stringify it before passing to
 * `navigator.credentials.create()`.
 */
export interface WebAuthnEnrollResult {
  factor_id: string;
  /** Discriminant — always `"webauthn"`. */
  factor_type: "webauthn";
  challenge_id: string;
  /** Parsed `PublicKeyCredentialCreationOptions` — pass directly to
   * `navigator.credentials.create({ publicKey: result.creation_options })`. */
  creation_options: unknown;
}

/** Discriminated union returned by `enrollFactor`. */
export type MfaEnrollResult = TotpEnrollResult | WebAuthnEnrollResult;

/**
 * Element of `GET /auth/v1/factors` array.
 *
 * Mirrors `FactorDescriptor` in `crates/basin-auth/src/mfa.rs`.
 * Secret / credential bytes are never included.
 */
export interface FactorDescriptor {
  id: string;
  /** `"totp"` or `"webauthn"`. */
  factor_type: string;
  /** `"unverified"` or `"verified"`. */
  status: string;
  friendly_name: string;
  /** RFC 3339 */
  created_at: string;
  /** RFC 3339 */
  updated_at: string;
}

/**
 * `POST /auth/v1/factors/:id/verify` response.
 *
 * `recovery_codes` is only present on the **first** verified factor for a
 * user — it will be `undefined` for all subsequent factor verifications.
 * Store these codes securely; they are shown exactly once.
 */
export interface VerifyFactorResult {
  ok: boolean;
  /** 8 hex recovery codes. Present only on the first verified factor. */
  recovery_codes?: string[];
}

/** `POST /auth/v1/factors/:id/challenge` response for a TOTP factor. */
export interface TotpChallengeResult {
  challenge_id: string;
}

/**
 * `POST /auth/v1/factors/:id/challenge` response for a WebAuthn factor.
 *
 * `request_options` is the JSON-parsed form of the server's
 * `request_options_json` field — ready for use with the WebAuthn browser API.
 * Pass it directly to `navigator.credentials.get({ publicKey: result.request_options })`.
 */
export interface WebAuthnChallengeResult {
  challenge_id: string;
  /** Parsed `PublicKeyCredentialRequestOptions`. */
  request_options: unknown;
}

/**
 * Discriminated union returned by `challengeFactor`.
 *
 * Inspect `"request_options" in result` (or cast) to distinguish WebAuthn from TOTP:
 * WebAuthn results carry `request_options`; TOTP results do not.
 */
export type MfaChallengeResult = TotpChallengeResult | WebAuthnChallengeResult;
