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
