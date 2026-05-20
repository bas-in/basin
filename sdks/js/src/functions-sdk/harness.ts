/**
 * @basin/functions — local mock-host test harness.
 *
 * `runWithMockHost` lets you exercise a Basin function handler against
 * in-memory mocks for every host import — no deploy, no running engine.
 *
 * @example
 * ```ts
 * import { describe, it, expect } from 'vitest';
 * import { runWithMockHost } from '@basin/functions/harness'; // or relative path in tests
 * import handler from './fn.js';
 *
 * describe('my handler', () => {
 *   it('returns 200 with rows', async () => {
 *     const resp = await runWithMockHost(handler, {
 *       method: 'GET',
 *       path: '/fn/v1/my-handler',
 *       query: {},
 *       headers: {},
 *       body: '',
 *     }, {
 *       mockQuery: async () => ({ rows: [{ id: 1 }] }),
 *     });
 *     expect(resp.status).toBe(200);
 *   });
 * });
 * ```
 */

import {
  __clearHostContext,
  __setHostContext,
  type HostContext,
} from "./host.js";
import type {
  BasinHandler,
  BasinRequest,
  BasinResponse,
  HttpRequest,
  HttpResponse,
  LogLevel,
  QueryResult,
} from "./types.js";

// ---------------------------------------------------------------------------
// Mock option types
// ---------------------------------------------------------------------------

/** Per-call signature for the mock query implementation. */
export type MockQueryFn = (
  sql: string,
  params?: unknown[],
) => QueryResult | Promise<QueryResult>;

/** Per-call signature for the mock http implementation. */
export type MockHttpFn = (
  request: HttpRequest,
) => HttpResponse | Promise<HttpResponse>;

/** Per-call signature for the mock secret implementation. */
export type MockSecretFn = (name: string) => string | Promise<string>;

/** Per-call signature for the mock log implementation. */
export type MockLogFn = (level: LogLevel, message: string) => void;

/**
 * Mock options for `runWithMockHost`.
 *
 * All options are optional.  Omitting a mock causes the corresponding host
 * call to throw with a descriptive error so accidental host calls surface
 * immediately rather than hanging silently.
 */
export interface MockHostOptions {
  /**
   * Mock implementation for `query(sql, params?)`.
   *
   * If omitted, any `query()` call inside the handler will throw.
   */
  mockQuery?: MockQueryFn;
  /**
   * Mock implementation for `http(request)`.
   *
   * If omitted, any `http()` call inside the handler will throw.
   */
  mockHttp?: MockHttpFn;
  /**
   * Mock implementation for `secret(name)`.
   *
   * If omitted, any `secret()` call inside the handler will throw.
   */
  mockSecret?: MockSecretFn;
  /**
   * Mock implementation for `log(level, message)`.
   *
   * Defaults to a no-op so handler logs don't pollute test output unless
   * you explicitly care about them.
   */
  mockLog?: MockLogFn;
}

// ---------------------------------------------------------------------------
// Captured call record — useful for asserting mock invocations
// ---------------------------------------------------------------------------

export interface QueryCall {
  sql: string;
  params?: unknown[];
}

export interface HttpCall {
  request: HttpRequest;
}

export interface SecretCall {
  name: string;
}

export interface LogCall {
  level: LogLevel;
  message: string;
}

export interface MockHostResult {
  /** The handler's response. */
  response: BasinResponse;
  /** All `query()` calls made during the handler invocation, in order. */
  queryCalls: QueryCall[];
  /** All `http()` calls made during the handler invocation, in order. */
  httpCalls: HttpCall[];
  /** All `secret()` calls made during the handler invocation, in order. */
  secretCalls: SecretCall[];
  /** All `log()` calls made during the handler invocation, in order. */
  logCalls: LogCall[];
}

// ---------------------------------------------------------------------------
// runWithMockHost
// ---------------------------------------------------------------------------

/**
 * Invoke a Basin handler against an in-memory mock host.
 *
 * Sets up the mock host context, calls the handler, tears down the context,
 * and returns the response together with a record of every host call made.
 *
 * The host context is always cleaned up — even when the handler throws —
 * so tests that call `runWithMockHost` in sequence don't bleed state.
 *
 * @param handler    The default-exported handler function to test.
 * @param request    The `BasinRequest` to pass to the handler.
 * @param mocks      Optional per-host-import mocks.
 *
 * @returns A `MockHostResult` with the response and call records.
 */
export async function runWithMockHost(
  handler: BasinHandler,
  request: BasinRequest,
  mocks: MockHostOptions = {},
): Promise<MockHostResult> {
  const queryCalls: QueryCall[] = [];
  const httpCalls: HttpCall[] = [];
  const secretCalls: SecretCall[] = [];
  const logCalls: LogCall[] = [];

  const ctx: HostContext = {
    async query(sql: string, params?: unknown[]): Promise<QueryResult> {
      const call: QueryCall = params !== undefined ? { sql, params } : { sql };
      queryCalls.push(call);
      if (!mocks.mockQuery) {
        throw new Error(
          `runWithMockHost: query('${sql}') called but no mockQuery was provided.`,
        );
      }
      return mocks.mockQuery(sql, params);
    },

    async http(req: HttpRequest): Promise<HttpResponse> {
      httpCalls.push({ request: req });
      if (!mocks.mockHttp) {
        throw new Error(
          `runWithMockHost: http('${req.url}') called but no mockHttp was provided.`,
        );
      }
      return mocks.mockHttp(req);
    },

    log(level: LogLevel, message: string): void {
      logCalls.push({ level, message });
      const fn = mocks.mockLog;
      if (fn) {
        fn(level, message);
      }
      // Default: no-op — logs don't need to be visible in test output.
    },

    async secret(name: string): Promise<string> {
      secretCalls.push({ name });
      if (!mocks.mockSecret) {
        throw new Error(
          `runWithMockHost: secret('${name}') called but no mockSecret was provided.`,
        );
      }
      return mocks.mockSecret(name);
    },
  };

  __setHostContext(ctx);
  try {
    const response = await handler(request);
    return { response, queryCalls, httpCalls, secretCalls, logCalls };
  } finally {
    __clearHostContext();
  }
}
