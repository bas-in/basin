/**
 * `.functions` — bindings for Basin's two function-invocation surfaces.
 *
 * Route sources (verified):
 * - `ANY  /fn/v1/:name`          crates/basin-rest/src/server.rs:238,
 *   handler crates/basin-rest/src/routes/fn_handler.rs (`any_fn_handler`) —
 *   HTTP-handler-shape Wasm functions; the guest's response (status, headers,
 *   body) is proxied back verbatim. Querystrings are NOT forwarded (W2).
 * - `POST /rest/v1/rpc/:fn_name` crates/basin-rest/src/server.rs:236,
 *   handler crates/basin-rest/src/routes/rpc.rs (`post_rpc`) — catalog SQL /
 *   Wasm UDFs. Body is a JSON object of named args; scalar results unwrap to
 *   the bare value, table results return an array of row objects.
 *
 * Both are bearer-authenticated like every other route.
 */

import { errorFromResponse, isErrorEnvelope } from "./errors.js";
import type { ClientContext } from "./http.js";
import { requestJson } from "./http.js";
import type { FunctionInvokeResult } from "./types.js";

export interface InvokeOptions {
  /** HTTP method forwarded into the function (route is method-agnostic). */
  method?: string;
  /** JSON-serialized when an object; strings/bytes pass through raw. */
  body?: unknown;
  headers?: Record<string, string>;
}

export class FunctionsClient {
  #ctx: ClientContext;

  constructor(ctx: ClientContext) {
    this.#ctx = ctx;
  }

  /**
   * `ANY /fn/v1/:name` — invoke an HTTP-handler function.
   *
   * The function's response is proxied verbatim, so non-2xx statuses are NOT
   * automatically errors — unless the body is Basin's own error envelope
   * (auth 401, unknown function 404, invocation failure 500), which throws
   * `BasinApiError` as usual.
   */
  async invoke<T = unknown>(
    name: string,
    opts: InvokeOptions = {},
  ): Promise<FunctionInvokeResult<T>> {
    const headers: Record<string, string> = { ...opts.headers };
    const token = await this.#ctx.getToken();
    if (token !== undefined) headers["authorization"] = `Bearer ${token}`;

    let body: BodyInit | undefined;
    if (typeof opts.body === "string" || opts.body instanceof Uint8Array) {
      body = opts.body as BodyInit;
    } else if (opts.body !== undefined) {
      headers["content-type"] ??= "application/json";
      body = JSON.stringify(opts.body);
    }

    const res = await this.#ctx.fetchImpl(
      `${this.#ctx.baseUrl}/fn/v1/${encodeURIComponent(name)}`,
      { method: opts.method ?? "POST", headers, body },
    );

    const contentType = res.headers.get("content-type") ?? "";
    const text = await res.text();
    let data: unknown = text;
    if (contentType.includes("json") && text.length > 0) {
      try {
        data = JSON.parse(text);
      } catch {
        // Function declared JSON but sent something else — keep the text.
      }
    }
    // Basin-layer failures (not the function's own responses) use the
    // standard envelope; surface those as typed errors.
    if (!res.ok && isErrorEnvelope(data)) {
      throw await errorFromResponse(
        new Response(text, { status: res.status, headers: res.headers }),
      );
    }
    return { status: res.status, headers: res.headers, data: data as T };
  }

  /**
   * `POST /rest/v1/rpc/:fn_name` — call a catalog UDF. Args are named;
   * missing declared args become NULL. Returns the bare scalar for
   * single-value results, or an array of rows for `RETURNS TABLE`.
   */
  async rpc<T = unknown>(
    fnName: string,
    args: Record<string, unknown> = {},
  ): Promise<T> {
    return requestJson<T>(
      this.#ctx,
      "POST",
      `/rest/v1/rpc/${encodeURIComponent(fnName)}`,
      { body: args },
    );
  }
}
