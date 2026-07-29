/**
 * @basin/functions — authoring SDK.
 *
 * Import host bindings and types here when writing a Basin function handler.
 *
 * In the deployed Wasm component the host imports (`query`, `http`, `log`,
 * `secret`) resolve to the engine's `basin:fn/*` imports defined by the W1
 * ABI.  Locally they resolve via the mock-host context injected by
 * `runWithMockHost`.
 *
 * @example
 * ```ts
 * // fn.ts — a Basin function handler
 * import { query, log, secret } from '@basin/functions';
 * import type { BasinHandler } from '@basin/functions';
 *
 * const handler: BasinHandler = async (req) => {
 *   log('info', 'hello from basin');
 *   const { rows } = await query('SELECT 1 AS n');
 *   return {
 *     status: 200,
 *     headers: { 'Content-Type': 'application/json' },
 *     body: JSON.stringify({ rows }),
 *   };
 * };
 *
 * export default handler;
 * ```
 */

// Host bindings
export { http, log, query, secret } from "./host.js";

// Types
export type {
  BasinHandler,
  BasinRequest,
  BasinResponse,
  HttpRequest,
  HttpResponse,
  LogLevel,
  QueryResult,
  QueryRow,
} from "./types.js";

// Test harness (re-exported so consumers can do:
//   import { runWithMockHost } from '@basin/functions')
export { runWithMockHost } from "./harness.js";
export type {
  HttpCall,
  LogCall,
  MockHostOptions,
  MockHostResult,
  MockHttpFn,
  MockLogFn,
  MockQueryFn,
  MockSecretFn,
  QueryCall,
  SecretCall,
} from "./harness.js";
