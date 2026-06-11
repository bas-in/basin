import { describe, expect, it } from "vitest";

import { BasinApiError, createClient } from "../src/index.js";
import { errorFromResponse, isErrorEnvelope } from "../src/errors.js";
import { FetchMock, jsonResponse } from "./helpers.js";

const URL_BASE = "http://basin.test";

describe("error mapping", () => {
  it("parses the {code, message} envelope into BasinApiError", async () => {
    const err = await errorFromResponse(
      jsonResponse({ code: "E_FORBIDDEN", message: "nope" }, 403),
    );
    expect(err).toBeInstanceOf(BasinApiError);
    expect(err.code).toBe("E_FORBIDDEN");
    expect(err.message).toBe("nope");
    expect(err.status).toBe(403);
  });

  it("falls back to E_UNKNOWN for non-envelope bodies", async () => {
    const err = await errorFromResponse(
      new Response("Bad Gateway", { status: 502 }),
    );
    expect(err.code).toBe("E_UNKNOWN");
    expect(err.message).toBe("Bad Gateway");
    expect(err.status).toBe(502);
  });

  it("unknown future codes pass through unchanged", async () => {
    const err = await errorFromResponse(
      jsonResponse({ code: "E_SOMETHING_NEW", message: "m" }, 400),
    );
    expect(err.code).toBe("E_SOMETHING_NEW");
  });

  it("isErrorEnvelope only matches E_*-coded objects", () => {
    expect(isErrorEnvelope({ code: "E_NOT_FOUND", message: "x" })).toBe(true);
    expect(isErrorEnvelope({ code: "ok", message: "x" })).toBe(false);
    expect(isErrorEnvelope({ resized: true })).toBe(false);
    expect(isErrorEnvelope("E_NOT_FOUND")).toBe(false);
  });

  it("client surfaces a 401 envelope from any route as a typed error", async () => {
    const fetch = new FetchMock().queueJson(
      { code: "E_UNAUTHENTICATED", message: "missing Authorization header" },
      401,
    );
    const client = createClient(URL_BASE, undefined, { fetch: fetch.impl });
    const err = await client
      .from("users")
      .rows()
      .catch((e) => e);
    expect(err).toBeInstanceOf(BasinApiError);
    expect((err as BasinApiError).code).toBe("E_UNAUTHENTICATED");
    expect((err as BasinApiError).status).toBe(401);
  });

  it("rate limiting surfaces as E_RATE_LIMITED with status 429", async () => {
    const fetch = new FetchMock().queueJson(
      { code: "E_RATE_LIMITED", message: "per-project rate limit exceeded" },
      429,
    );
    const client = createClient(URL_BASE, "key", { fetch: fetch.impl });
    await expect(client.from("users").rows()).rejects.toMatchObject({
      code: "E_RATE_LIMITED",
      status: 429,
    });
  });
});
