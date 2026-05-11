/**
 * FunctionsClient — Tier 5 placeholder.
 *
 * Edge functions also land in basin engine v0.2. The HTTP shim
 * `basin.functions.invoke('slug', { body, headers? })` is wired
 * today against the cloud's intended route shape
 * (`POST /v1/projects/:ref/functions/:slug/invoke`), but the
 * project ref isn't carried on the client; callers pass it via
 * `options.projectRef` for now, or pre-set it on the FunctionsClient
 * once we add `createClient({projectRef})`.
 *
 * When the engine ships functions in v0.2 the body of `invoke()`
 * stays — we just drop the `not_implemented` guard.
 */

import { BasinError } from "../errors.js";

export interface FunctionsClientDeps {
  url: string;
  headers: Record<string, string>;
  fetch: typeof fetch;
  /** Optional default project ref used when `invoke` callers omit it. */
  projectRef?: string;
}

export interface InvokeOptions {
  /** Request body. JSON-serialized when an object; passed through when a string/ArrayBuffer. */
  body?: unknown;
  /** Per-call headers merged on top of the client default headers. */
  headers?: Record<string, string>;
  /** Project ref override. Required when the client wasn't constructed with one. */
  projectRef?: string;
  /** Custom Authorization. Defaults to whatever the client's `auth` adapter supplies. */
  authToken?: string;
}

export interface InvokeResult<T = unknown> {
  data: T | null;
  error: BasinError | null;
  /** Raw response status when the round-trip completed. */
  status?: number;
}

export class FunctionsClient {
  #url: string;
  #headers: Record<string, string>;
  #fetch: typeof fetch;
  #projectRef: string | undefined;
  /** Whether the engine-side functions surface is live yet. */
  readonly enabled = false;

  constructor(deps: FunctionsClientDeps) {
    this.#url = deps.url;
    this.#headers = deps.headers;
    this.#fetch = deps.fetch;
    this.#projectRef = deps.projectRef;
  }

  /**
   * Invoke an edge function. Throws `not_implemented` until basin v0.2.
   *
   * @example
   * const { data, error } = await basin.functions.invoke('send-welcome-email', {
   *   projectRef: 'p_01H...',
   *   body: { user_id: 'u_01H...' },
   * });
   */
  async invoke<T = unknown>(
    slug: string,
    options: InvokeOptions = {},
  ): Promise<InvokeResult<T>> {
    if (!slug) {
      return {
        data: null,
        error: new BasinError("invalid_request", "functions.invoke requires a slug"),
      };
    }
    if (!this.enabled) {
      return {
        data: null,
        error: new BasinError(
          "not_implemented",
          "basin.functions.invoke is not implemented yet — " +
            "lands when the engine ships edge functions (basin v0.2). " +
            "Surface is stable so app code written today keeps working.",
        ),
      };
    }
    // ─── unreachable until v0.2 ────────────────────────────────────
    /* c8 ignore start */
    const projectRef = options.projectRef ?? this.#projectRef;
    if (!projectRef) {
      return {
        data: null,
        error: new BasinError(
          "invalid_request",
          "functions.invoke requires options.projectRef (or set on createClient)",
        ),
      };
    }
    const url = `${this.#url}/v1/projects/${encodeURIComponent(projectRef)}/functions/${encodeURIComponent(slug)}/invoke`;
    const headers: Record<string, string> = { ...this.#headers, ...options.headers };
    if (options.authToken) headers.Authorization = `Bearer ${options.authToken}`;
    let res: Response;
    try {
      res = await this.#fetch(url, {
        method: "POST",
        headers,
        body:
          options.body === undefined
            ? undefined
            : typeof options.body === "string" || options.body instanceof ArrayBuffer
              ? (options.body as BodyInit)
              : JSON.stringify(options.body),
      });
    } catch (e) {
      return {
        data: null,
        error: new BasinError(
          "network",
          e instanceof Error ? e.message : "network error reaching functions endpoint",
        ),
      };
    }
    let body: unknown;
    try {
      body = await res.json();
    } catch {
      body = null;
    }
    if (!res.ok) {
      return {
        data: null,
        error: new BasinError(
          "invalid_request",
          `functions.invoke('${slug}') failed (HTTP ${res.status})`,
          res.status,
          body,
        ),
        status: res.status,
      };
    }
    return { data: body as T, error: null, status: res.status };
    /* c8 ignore stop */
  }
}
