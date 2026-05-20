/**
 * BasinClient — the single object returned by `createClient`. Holds
 * the engine URL, the anon key, the active session (when signed in),
 * and exposes the `auth`, `from()`, and `storage` namespaces.
 *
 * Engine-direct architecture: the SDK talks DIRECTLY to basin-engine
 * (the OSS Rust data plane), not through basin-cloud. basin-cloud is
 * the control plane (orgs / projects / billing / dashboard); the
 * engine is the data plane. Every method here targets basin-engine's
 * route surface (`/auth/v1/*`, `/rest/v1/:table`, etc.).
 *
 * This file is the only place that owns the cross-namespace `fetch`
 * helper. Each namespace receives a thin adapter rather than reaching
 * back into the client — keeps the dependency graph one-directional.
 */

import { AdminClient } from "./admin/client.js";
import { AuthClient } from "./auth/client.js";
import { FunctionsClient } from "./functions/client.js";
import { PostgrestQueryBuilder } from "./postgrest/builder.js";
import { RealtimeChannel, RealtimeClient } from "./realtime/client.js";
import { StorageClient } from "./storage/client.js";
import { withRetry, type RetryOptions } from "./internal/retry.js";

export interface BasinClientOptions {
  /**
   * Custom `fetch` impl. Defaults to the global `fetch` (Node 18+,
   * browsers, Bun, Deno). Set this when running in environments that
   * need a polyfill (older Node, react-native) or to inject test stubs.
   */
  fetch?: typeof fetch;

  /**
   * Optional headers merged into every request. Auth `Authorization`
   * still wins on conflict.
   */
  headers?: Record<string, string>;

  /**
   * Override the auth session-storage adapter. Defaults to
   * `localStorage` in the browser, `null` in Node. Pass a custom
   * adapter for SSR / RN / Deno KV.
   */
  authStorage?: {
    getItem: (key: string) => string | null | Promise<string | null>;
    setItem: (key: string, value: string) => void | Promise<void>;
    removeItem: (key: string) => void | Promise<void>;
  };

  /** Storage-namespace bucket route prefix. Default `${url}/storage/v1`. */
  storageUrl?: string;

  /**
   * Project ref (`p_01H...`) used to build edge-function invoke URLs.
   * Optional — callers can also pass `{projectRef}` per-`invoke`.
   * Required only when calling `basin.functions.invoke(slug, {body})`
   * without a per-call ref; the call returns `not_implemented` either
   * way until basin v0.2.
   */
  projectRef?: string;

  /**
   * Retry configuration for all network requests. Pass `false` to
   * disable retries entirely. Defaults to 3 attempts with exponential
   * backoff (250ms base, 5000ms cap), retrying on network errors, 5xx,
   * and 429 (honouring `Retry-After`).
   */
  retry?: RetryOptions | false;
}

export type { RetryOptions };

export interface BasinClient {
  /** Authentication surface — signUp, signIn*, signOut, MFA. */
  readonly auth: AuthClient;

  /** Admin namespace — project provisioning and credential management. */
  readonly admin: AdminClient;

  /**
   * PostgREST-shaped table query builder.
   *
   * ```ts
   * const { data, error } = await basin
   *   .from('products')
   *   .select('id, name, price')
   *   .eq('active', true)
   *   .order('price', { ascending: true })
   *   .limit(10);
   * ```
   */
  from<T = unknown>(table: string): PostgrestQueryBuilder<T>;

  /** Object-storage surface — buckets + signed URLs. */
  readonly storage: StorageClient;

  /**
   * Realtime channels (Tier 4 placeholder). `subscribe()` throws
   * `not_implemented` until basin engine v0.2 ships logical-
   * replication-driven broadcast.
   */
  readonly realtime: RealtimeClient;

  /**
   * Shorthand for `realtime.channel(topic)`. Matches the Supabase
   * shape `basin.channel('room').on(...).subscribe()`.
   */
  channel(topic: string): RealtimeChannel;

  /**
   * Edge functions (Tier 5 placeholder). `invoke()` returns
   * `not_implemented` until basin engine v0.2 ships functions.
   */
  readonly functions: FunctionsClient;
}

/**
 * Construct a Basin client.
 *
 * The SDK speaks DIRECTLY to basin-engine — basin-cloud is the
 * control plane (dashboard / billing / org-management) and never
 * appears in the SDK's request path.
 *
 * @param engineURL — basin engine HTTP base URL. Could be the
 *                    cloud-managed regional endpoint
 *                    `https://<region>.basin.run`, or a self-hosted
 *                    engine such as `http://localhost:5434`
 *                    (`cargo run -p basin-server`). NOT a basin-cloud
 *                    control-plane URL. Self-hosters don't need a
 *                    separate Postgres — basin-auth's catalog lives on
 *                    the engine itself over loopback pgwire as of
 *                    2026-05-11.
 * @param anonKey   — public anon API key. Format: `basin_{tenant_id}_{base64}`.
 *                    Minted by basin-cloud's dashboard at
 *                    `/app/project/:ref/api-keys`, signed with a key
 *                    basin-engine trusts. Rotated via the dashboard; safe
 *                    to ship to browsers. The SDK forwards this opaquely
 *                    in the `apikey` header — no client-side parsing is
 *                    performed.
 * @param options   — optional fetch / headers / storage overrides.
 */
export function createClient(
  engineURL: string,
  anonKey: string,
  options: BasinClientOptions = {},
): BasinClient {
  const url = engineURL;
  const base = url.replace(/\/$/, "");
  const rawFetcher = options.fetch ?? globalThis.fetch;
  if (typeof rawFetcher !== "function") {
    throw new Error(
      "@bas-in/basin-js: `fetch` is not available in this environment. " +
        "Pass `options.fetch` to createClient().",
    );
  }
  const fetcher =
    options.retry === false
      ? rawFetcher
      : withRetry(rawFetcher, options.retry ?? {});

  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    Accept: "application/json",
    apikey: anonKey,
    ...options.headers,
  };

  const auth = new AuthClient({
    url: base,
    headers,
    fetch: fetcher,
    storage: options.authStorage ?? defaultAuthStorage(),
  });

  const storage = new StorageClient({
    url: options.storageUrl ?? `${base}/storage/v1`,
    headers,
    fetch: fetcher,
    auth,
  });

  const realtime = new RealtimeClient({
    url: base,
    headers,
    fetchFn: fetcher,
  });

  const functions = new FunctionsClient({
    url: base,
    headers,
    fetch: fetcher,
  });

  const admin = new AdminClient({
    url: base,
    headers,
    fetch: fetcher,
    auth,
  });

  return {
    auth,
    admin,
    storage,
    realtime,
    functions,
    from<T = unknown>(table: string): PostgrestQueryBuilder<T> {
      return new PostgrestQueryBuilder<T>({
        url: `${base}/rest/v1/${encodeURIComponent(table)}`,
        headers,
        fetch: fetcher,
        auth,
      });
    },
    channel(topic: string): RealtimeChannel {
      return realtime.channel(topic);
    },
  };
}

function defaultAuthStorage(): BasinClientOptions["authStorage"] {
  if (typeof globalThis !== "undefined" && "localStorage" in globalThis) {
    return globalThis.localStorage as unknown as NonNullable<
      BasinClientOptions["authStorage"]
    >;
  }
  return undefined;
}
