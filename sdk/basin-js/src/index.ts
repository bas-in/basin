/**
 * basin-js — TypeScript client for Basin's HTTP surfaces.
 *
 * Zero runtime dependencies: built on the WHATWG `fetch` and `WebSocket`
 * standards (injectable for tests and non-standard runtimes).
 *
 * ```ts
 * import { createClient } from "basin-js";
 *
 * const basin = createClient("http://localhost:8080", apiKeyOrJwt);
 * const { rows } = await basin.from("orders").select("id,total").gte("total", 100);
 * ```
 */

import { AuthClient } from "./auth.js";
import type { ClientContext, FetchLike } from "./http.js";
import { requestJson } from "./http.js";
import { FunctionsClient } from "./functions.js";
import { QueryBuilder, type Row } from "./query.js";
import { RealtimeClient, type WebSocketCtor } from "./realtime.js";
import { StorageClient } from "./storage.js";

export { AuthClient } from "./auth.js";
export { BasinApiError, ERROR_CODES } from "./errors.js";
export type { BasinErrorCode, KnownErrorCode } from "./errors.js";
export { FunctionsClient } from "./functions.js";
export type { InvokeOptions } from "./functions.js";
export { QueryBuilder } from "./query.js";
export type { QueryResult, Row } from "./query.js";
export { RealtimeClient } from "./realtime.js";
export type {
  ChannelHandlers,
  RealtimeChannel,
  SubscribeOptions,
  WebSocketCtor,
  WebSocketLike,
} from "./realtime.js";
export { StorageBucketClient, StorageClient } from "./storage.js";
export type { CreateBucketOptions, DownloadResult } from "./storage.js";
export type * from "./types.js";

export interface BasinClientOptions {
  /**
   * Project id (ULID-style string). Needed for realtime, public storage
   * URLs, and auth flows. When omitted, it is decoded from the JWT `key` or
   * the signed-in session's access token (`project_id` claim — see
   * crates/basin-auth/src/jwt.rs `WireClaims`). Raw API keys carry no
   * project id, so set this explicitly when using one with realtime.
   */
  projectId?: string;
  /** Custom fetch (defaults to `globalThis.fetch`). */
  fetch?: FetchLike;
  /** Custom WebSocket constructor (defaults to `globalThis.WebSocket`). */
  WebSocket?: WebSocketCtor;
}

/** Decode the `project_id` claim from a Basin JWT. Returns undefined for
 * non-JWT tokens (e.g. raw API keys). */
export function projectIdFromJwt(token: string): string | undefined {
  const parts = token.split(".");
  if (parts.length !== 3) return undefined;
  try {
    const b64 = parts[1]!.replace(/-/g, "+").replace(/_/g, "/");
    const padded = b64 + "=".repeat((4 - (b64.length % 4)) % 4);
    const json =
      typeof atob === "function"
        ? atob(padded)
        : // Node < 16 fallback; Buffer exists wherever atob doesn't.
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          (globalThis as any).Buffer.from(padded, "base64").toString("utf8");
    const claims = JSON.parse(json) as { project_id?: unknown };
    return typeof claims.project_id === "string" ? claims.project_id : undefined;
  } catch {
    return undefined;
  }
}

export class BasinClient {
  readonly auth: AuthClient;
  readonly realtime: RealtimeClient;
  readonly functions: FunctionsClient;
  readonly storage: StorageClient;

  #ctx: ClientContext;

  constructor(url: string, key?: string, options: BasinClientOptions = {}) {
    const baseUrl = url.replace(/\/+$/, "");
    const fetchImpl: FetchLike =
      options.fetch ?? ((input, init) => globalThis.fetch(input, init));

    // Two-phase init: the context closes over the AuthClient so requests
    // always use the freshest token (session beats the static key).
    let auth: AuthClient | undefined;
    const ctx: ClientContext = {
      baseUrl,
      fetchImpl,
      getToken: async () => (await auth?.accessToken()) ?? key,
      projectId: () => {
        if (options.projectId !== undefined) return options.projectId;
        const session = auth?.getSession();
        if (session !== null && session !== undefined) {
          const fromSession = projectIdFromJwt(session.access_token);
          if (fromSession !== undefined) return fromSession;
        }
        return key !== undefined ? projectIdFromJwt(key) : undefined;
      },
    };
    this.#ctx = ctx;

    auth = new AuthClient(ctx, options.projectId);
    this.auth = auth;
    this.functions = new FunctionsClient(ctx);
    this.storage = new StorageClient(ctx);
    this.realtime = new RealtimeClient({
      baseUrl,
      getToken: ctx.getToken,
      projectId: ctx.projectId,
      webSocketImpl:
        options.WebSocket ??
        ((globalThis as { WebSocket?: WebSocketCtor }).WebSocket as WebSocketCtor),
    });
  }

  /**
   * REST query builder for `/rest/v1/:table`
   * (crates/basin-rest/src/server.rs:243-249).
   */
  from<T extends Row = Row>(table: string): QueryBuilder<T> {
    return new QueryBuilder<T>(this.#ctx, table);
  }

  /**
   * `POST /rest/v1/rpc/:fn_name` — convenience alias for
   * `client.functions.rpc` (crates/basin-rest/src/server.rs:236).
   */
  rpc<T = unknown>(fnName: string, args: Record<string, unknown> = {}): Promise<T> {
    return this.functions.rpc<T>(fnName, args);
  }

  /** `GET /health` (crates/basin-rest/src/server.rs:368) → `"ok"`. */
  async health(): Promise<string> {
    const res = await this.#ctx.fetchImpl(`${this.#ctx.baseUrl}/health`);
    return res.text();
  }

  /** Escape hatch for routes not yet wrapped (e.g. `/admin/v1/*`). */
  request<T>(
    method: string,
    path: string,
    opts: { query?: Array<[string, string]>; body?: unknown } = {},
  ): Promise<T> {
    return requestJson<T>(this.#ctx, method, path, opts);
  }
}

/** Create a Basin client. `key` is a JWT or an API-key secret — both are
 * accepted as `Authorization: Bearer <key>` (crates/basin-rest/src/server.rs
 * `authorize`, lines 498-538: JWT verify first, API-key lookup fallback). */
export function createClient(
  url: string,
  key?: string,
  options: BasinClientOptions = {},
): BasinClient {
  return new BasinClient(url, key, options);
}
