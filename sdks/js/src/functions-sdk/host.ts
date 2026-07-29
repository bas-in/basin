/**
 * @basin/functions — host binding stubs.
 *
 * In a deployed Wasm component these symbols resolve to actual host imports
 * (`basin:fn/query`, `basin:fn/http`, `basin:fn/log`, `basin:fn/secret`).
 * In the local test harness (`runWithMockHost`) the active host context is
 * injected via `__setHostContext` before each handler call so the stubs
 * delegate to the in-memory mocks — zero deploy needed.
 *
 * AUTHORS: import and call these functions directly.  Do NOT import
 * `__setHostContext` / `__clearHostContext` in production code — those are
 * test-harness internals.
 */

import type {
  HttpRequest,
  HttpResponse,
  LogLevel,
  QueryResult,
} from "./types.js";

// ---------------------------------------------------------------------------
// Host context (injected by the test harness; resolved by the Wasm linker in
// production)
// ---------------------------------------------------------------------------

export interface HostContext {
  query(sql: string, params?: unknown[]): Promise<QueryResult>;
  http(request: HttpRequest): Promise<HttpResponse>;
  log(level: LogLevel, message: string): void;
  secret(name: string): Promise<string>;
}

let _ctx: HostContext | null = null;

/** @internal — test harness only. */
export function __setHostContext(ctx: HostContext): void {
  _ctx = ctx;
}

/** @internal — test harness only. */
export function __clearHostContext(): void {
  _ctx = null;
}

function requireContext(fn: string): HostContext {
  if (_ctx === null) {
    throw new Error(
      `basin:fn/${fn} called outside of a host context. ` +
        `In tests, wrap your handler with runWithMockHost(). ` +
        `In production, this resolves to the Wasm host import automatically.`,
    );
  }
  return _ctx;
}

// ---------------------------------------------------------------------------
// Public host bindings — the API function authors call
// ---------------------------------------------------------------------------

/**
 * Run a SQL statement under the caller's identity.
 *
 * RLS policies apply; any RLS-restricted rows are invisible.  Matches the
 * `basin:fn/query` host import in the W1 ABI.
 *
 * @param sql   Parameterised SQL, e.g. `SELECT * FROM orders WHERE user_id = $1`
 * @param params Optional positional parameters ($1, $2, …)
 *
 * @example
 * ```ts
 * const { rows } = await query(
 *   'SELECT id, total FROM orders WHERE user_id = $1',
 *   [userId],
 * );
 * ```
 */
export async function query(
  sql: string,
  params?: unknown[],
): Promise<QueryResult> {
  return requireContext("query").query(sql, params);
}

/**
 * Make an outbound HTTP request via `basin-net`.
 *
 * The request is subject to the project's URL allowlist, per-project rate
 * limits, body cap, and timeout — identical to `SUBSCRIBE WEBHOOK` and
 * `basin-net`'s other callers.  Matches the `basin:fn/http` host import.
 *
 * @example
 * ```ts
 * const resp = await http({
 *   method: 'POST',
 *   url: 'https://api.stripe.com/v1/charges',
 *   headers: { Authorization: `Bearer ${await secret('STRIPE_KEY')}` },
 *   body: new URLSearchParams({ amount: '1000' }).toString(),
 * });
 * ```
 */
export async function http(request: HttpRequest): Promise<HttpResponse> {
  return requireContext("http").http(request);
}

/**
 * Emit a log line via the host's `tracing` subscriber.
 *
 * Log lines appear in `basin functions logs <name>` once W3 ships.
 * Matches the `basin:fn/log` host import.
 *
 * @example
 * ```ts
 * log('info', `Processing order ${orderId}`);
 * ```
 */
export function log(level: LogLevel, message: string): void {
  requireContext("log").log(level, message);
}

/**
 * Read a project secret decrypted via the host's `EncryptionProvider`.
 *
 * Secrets are project-scoped; name is the key registered in the secrets
 * vault.  Matches the `basin:fn/secret` host import.
 *
 * @example
 * ```ts
 * const apiKey = await secret('STRIPE_API_KEY');
 * ```
 */
export async function secret(name: string): Promise<string> {
  return requireContext("secret").secret(name);
}
