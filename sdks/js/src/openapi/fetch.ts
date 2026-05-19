import { BasinError } from "../errors.js";
import type { OpenAPIDocument } from "./types.js";

export async function fetchOpenAPI(
  url: string,
  anonKey: string,
  opts?: {
    fetch?: typeof globalThis.fetch;
    headers?: Record<string, string>;
    signal?: AbortSignal;
  },
): Promise<OpenAPIDocument> {
  const base = url.replace(/\/$/, "");
  const endpoint = `${base}/rest/v1/_openapi.json`;
  const fetcher = opts?.fetch ?? globalThis.fetch;

  const headers: Record<string, string> = {
    Accept: "application/json",
    apikey: anonKey,
    ...opts?.headers,
  };

  let res: Response;
  try {
    res = await fetcher(endpoint, {
      method: "GET",
      headers,
      signal: opts?.signal ?? null,
    });
  } catch (cause) {
    throw new BasinError(
      "network",
      "Failed to fetch OpenAPI document",
      undefined,
      { cause },
    );
  }

  if (res.status === 404) {
    throw new BasinError(
      "not_found",
      `OpenAPI document not found at ${endpoint}`,
      404,
    );
  }

  if (!res.ok) {
    throw new BasinError(
      "invalid_response",
      `OpenAPI fetch returned ${res.status}`,
      res.status,
    );
  }

  let doc: OpenAPIDocument;
  try {
    doc = (await res.json()) as OpenAPIDocument;
  } catch (cause) {
    throw new BasinError(
      "invalid_response",
      "OpenAPI document is not valid JSON",
      undefined,
      { cause },
    );
  }

  return doc;
}
