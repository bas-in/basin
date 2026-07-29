import { BasinError } from "../errors.js";

export interface FunctionsClientDeps {
  url: string;
  headers: Record<string, string>;
  fetch: typeof fetch;
}

export interface InvokeOptions {
  /** Named arguments passed as the JSON request body. */
  body?: Record<string, unknown>;
  /** Per-call headers merged on top of the client default headers. */
  headers?: Record<string, string>;
}

export interface InvokeResult<T = unknown> {
  data: T | null;
  error: BasinError | null;
}

export class FunctionsClient {
  #url: string;
  #headers: Record<string, string>;
  #fetch: typeof fetch;
  readonly enabled = true;

  constructor(deps: FunctionsClientDeps) {
    this.#url = deps.url;
    this.#headers = deps.headers;
    this.#fetch = deps.fetch;
  }

  async invoke<T = unknown>(
    fnName: string,
    options: InvokeOptions = {},
  ): Promise<InvokeResult<T>> {
    if (!fnName) {
      return {
        data: null,
        error: new BasinError("invalid_request", "functions.invoke requires a function name"),
      };
    }

    const url = `${this.#url}/rest/v1/rpc/${encodeURIComponent(fnName)}`;
    const headers: Record<string, string> = {
      ...this.#headers,
      "Content-Type": "application/json",
      ...options.headers,
    };

    let res: Response;
    try {
      res = await this.#fetch(url, {
        method: "POST",
        headers,
        body: options.body !== undefined ? JSON.stringify(options.body) : "{}",
      });
    } catch (e) {
      return {
        data: null,
        error: new BasinError(
          "network",
          e instanceof Error ? e.message : "network error reaching rpc endpoint",
        ),
      };
    }

    if (res.status === 401 || res.status === 403) {
      return {
        data: null,
        error: new BasinError("unauthorized", "unauthorized", res.status),
      };
    }

    let body: unknown;
    try {
      body = await res.json();
    } catch {
      return {
        data: null,
        error: new BasinError(
          "invalid_response",
          `functions.invoke('${fnName}') response was not JSON (HTTP ${res.status})`,
          res.status,
        ),
      };
    }

    if (!res.ok) {
      return {
        data: null,
        error: new BasinError(
          "internal",
          `functions.invoke('${fnName}') failed (HTTP ${res.status})`,
          res.status,
          body,
        ),
      };
    }

    return { data: body as T, error: null };
  }
}
