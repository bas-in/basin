import { describe, expect, it } from "vitest";

import { BasinApiError, createClient } from "../src/index.js";
import { FetchMock, jsonResponse } from "./helpers.js";

const URL_BASE = "http://basin.test";

describe("functions.invoke (/fn/v1/:name)", () => {
  it("POSTs JSON bodies with auth and parses JSON responses", async () => {
    const fetch = new FetchMock().queue(jsonResponse({ resized: true }));
    const client = createClient(URL_BASE, "key", { fetch: fetch.impl });
    const res = await client.functions.invoke<{ resized: boolean }>("resize", {
      body: { width: 100 },
    });
    expect(res.status).toBe(200);
    expect(res.data).toEqual({ resized: true });
    expect(fetch.last.url).toBe(`${URL_BASE}/fn/v1/resize`);
    expect(fetch.last.method).toBe("POST");
    expect(fetch.last.headers["authorization"]).toBe("Bearer key");
    expect(JSON.parse(fetch.last.body!)).toEqual({ width: 100 });
  });

  it("forwards arbitrary methods (route is method-agnostic)", async () => {
    const fetch = new FetchMock().queue(new Response("pong", { status: 200 }));
    const client = createClient(URL_BASE, "key", { fetch: fetch.impl });
    const res = await client.functions.invoke<string>("ping", { method: "GET" });
    expect(fetch.last.method).toBe("GET");
    expect(res.data).toBe("pong");
  });

  it("proxies the function's own non-2xx responses without throwing", async () => {
    // A guest function returning 418 is NOT a Basin error envelope.
    const fetch = new FetchMock().queue(
      new Response("teapot", { status: 418 }),
    );
    const client = createClient(URL_BASE, "key", { fetch: fetch.impl });
    const res = await client.functions.invoke("teapot");
    expect(res.status).toBe(418);
    expect(res.data).toBe("teapot");
  });

  it("throws a typed error for Basin-layer envelopes (404 unknown function)", async () => {
    const fetch = new FetchMock().queueJson(
      { code: "E_NOT_FOUND", message: 'function "nope" not found for project' },
      404,
    );
    const client = createClient(URL_BASE, "key", { fetch: fetch.impl });
    const err = await client.functions.invoke("nope").catch((e) => e);
    expect(err).toBeInstanceOf(BasinApiError);
    expect((err as BasinApiError).code).toBe("E_NOT_FOUND");
  });
});
