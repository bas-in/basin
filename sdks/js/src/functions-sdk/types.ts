/**
 * @basin/functions — core types.
 *
 * These mirror the W1 host ABI types (`basin:fn/{query,http,log,secret}`)
 * so authored functions are fully typed against the contract that the
 * deployed Wasm component exposes at runtime.
 */

// ---------------------------------------------------------------------------
// Query host binding
// ---------------------------------------------------------------------------

/**
 * A row returned by `query()`.  Column values are the canonical JS
 * representations of the engine's Arrow types.
 */
export type QueryRow = Record<string, unknown>;

/** Result of a `query()` call to the host. */
export interface QueryResult {
  /** Rows returned by the SQL statement. */
  rows: QueryRow[];
  /**
   * Number of rows affected (INSERT / UPDATE / DELETE).
   * `undefined` for SELECT statements.
   */
  rowsAffected?: number;
}

// ---------------------------------------------------------------------------
// HTTP host binding
// ---------------------------------------------------------------------------

/** Outbound HTTP request shape passed to `http()`. */
export interface HttpRequest {
  /** HTTP method (GET, POST, PUT, PATCH, DELETE, HEAD, OPTIONS). */
  method: string;
  /** Fully-qualified URL. Must pass the project's allowlist. */
  url: string;
  /** Request headers. */
  headers?: Record<string, string>;
  /**
   * Request body as a string. Pass `undefined` for body-less methods.
   * The host will enforce the project's `BASIN_NET_BODY_LIMIT_BYTES`.
   */
  body?: string;
}

/** Response from an outbound `http()` call. */
export interface HttpResponse {
  /** HTTP status code. */
  status: number;
  /** Response headers. */
  headers: Record<string, string>;
  /** Response body as a string. */
  body: string;
}

// ---------------------------------------------------------------------------
// Log host binding
// ---------------------------------------------------------------------------

/** Log level accepted by `log()`. Maps to `tracing` log levels in the host. */
export type LogLevel = "debug" | "info" | "warn" | "error";

// ---------------------------------------------------------------------------
// Handler shape
// ---------------------------------------------------------------------------

/** An inbound HTTP request received by the function handler. */
export interface BasinRequest {
  /** HTTP method (uppercase: GET, POST, …). */
  method: string;
  /** Request path (e.g. `/fn/v1/my-handler`). */
  path: string;
  /** Query-string parameters parsed into a map. */
  query: Record<string, string>;
  /** Inbound headers (lower-cased keys). */
  headers: Record<string, string>;
  /**
   * Request body as a string.
   * For JSON payloads call `JSON.parse(req.body)`.
   */
  body: string;
}

/** Response produced by the function handler. */
export interface BasinResponse {
  /**
   * HTTP status code to return.
   * @default 200
   */
  status?: number;
  /** Response headers. */
  headers?: Record<string, string>;
  /**
   * Response body as a string.
   * For JSON payloads pass `JSON.stringify(...)` and set
   * `Content-Type: application/json` in headers.
   */
  body: string;
}

// ---------------------------------------------------------------------------
// Handler function type
// ---------------------------------------------------------------------------

/**
 * The default-export shape every `@basin/functions` handler must satisfy.
 *
 * The function receives a `BasinRequest` and returns (or resolves to) a
 * `BasinResponse`.  On the Wasm side this corresponds to the
 * `basin:fn/handler` WIT export `handle(request) -> response`.
 *
 * @example
 * ```ts
 * import type { BasinRequest, BasinResponse, BasinHandler } from '@basin/functions';
 *
 * const handler: BasinHandler = async (req) => ({
 *   status: 200,
 *   headers: { 'Content-Type': 'application/json' },
 *   body: JSON.stringify({ ok: true }),
 * });
 *
 * export default handler;
 * ```
 */
export type BasinHandler = (
  req: BasinRequest,
) => BasinResponse | Promise<BasinResponse>;
