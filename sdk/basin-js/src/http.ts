/** Minimal fetch wrapper shared by all sub-clients. Zero dependencies. */

import { errorFromResponse } from "./errors.js";

export type FetchLike = (
  input: string | URL,
  init?: RequestInit,
) => Promise<Response>;

/** Shared per-client context threaded through every sub-client. */
export interface ClientContext {
  /** Base URL without a trailing slash, e.g. `http://localhost:8080`. */
  baseUrl: string;
  fetchImpl: FetchLike;
  /**
   * Resolve the bearer token for the next request: the live session's
   * access token (auto-refreshed) or the static key handed to createClient.
   */
  getToken(): Promise<string | undefined>;
  /** Project id, if known (explicit option or decoded from a JWT). */
  projectId(): string | undefined;
}

export interface RequestOptions {
  /** `[key, value]` query pairs, appended in order (server parses pairs). */
  query?: Array<[string, string]>;
  /** JSON body. Mutually exclusive with `rawBody`. */
  body?: unknown;
  /** Pre-encoded body (storage uploads). */
  rawBody?: BodyInit;
  headers?: Record<string, string>;
  /** Attach `Authorization: Bearer <token>` (default true). */
  auth?: boolean;
}

function buildUrl(
  baseUrl: string,
  path: string,
  query?: Array<[string, string]>,
): string {
  let url = baseUrl + path;
  if (query && query.length > 0) {
    const params = new URLSearchParams();
    for (const [k, v] of query) params.append(k, v);
    url += `?${params.toString()}`;
  }
  return url;
}

/**
 * Issue a request and return the raw `Response` after the error gate:
 * any non-2xx response is parsed into a thrown `BasinApiError`
 * (`{code, message}` envelope per crates/basin-rest/src/errors.rs).
 */
export async function requestRaw(
  ctx: ClientContext,
  method: string,
  path: string,
  opts: RequestOptions = {},
): Promise<Response> {
  const headers: Record<string, string> = { ...opts.headers };
  if (opts.auth !== false) {
    const token = await ctx.getToken();
    if (token !== undefined) headers["authorization"] = `Bearer ${token}`;
  }
  let body: BodyInit | undefined;
  if (opts.rawBody !== undefined) {
    body = opts.rawBody;
  } else if (opts.body !== undefined) {
    headers["content-type"] ??= "application/json";
    body = JSON.stringify(opts.body);
  }
  const res = await ctx.fetchImpl(buildUrl(ctx.baseUrl, path, opts.query), {
    method,
    headers,
    body,
  });
  if (!res.ok) throw await errorFromResponse(res);
  return res;
}

/** `requestRaw` + JSON decode. A 204 / empty body resolves to `null`. */
export async function requestJson<T>(
  ctx: ClientContext,
  method: string,
  path: string,
  opts: RequestOptions = {},
): Promise<T> {
  const res = await requestRaw(ctx, method, path, opts);
  if (res.status === 204) return null as T;
  const text = await res.text();
  if (text.length === 0) return null as T;
  return JSON.parse(text) as T;
}

/** Percent-encode each segment of an object path, preserving `/` separators. */
export function encodeObjectPath(path: string): string {
  return path
    .split("/")
    .map((seg) => encodeURIComponent(seg))
    .join("/");
}
