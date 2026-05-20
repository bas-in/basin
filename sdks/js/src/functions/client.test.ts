import { describe, expect, it } from "vitest";
import { createClient } from "../client.js";
import { BasinError } from "../errors.js";

const BASE_URL = "https://api.basin.run";
const ANON_KEY = "anon-key";

function stubFetch(
  resp: { status?: number; body: unknown },
  captured?: { url?: string; method?: string; headers?: Record<string, string>; body?: unknown },
): typeof fetch {
  return async (input: Request | string | URL, init?: RequestInit): Promise<Response> => {
    if (captured) {
      captured.url =
        typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input.url;
      captured.method = init?.method ?? "GET";
      const rawHeaders = init?.headers ?? {};
      captured.headers = Object.fromEntries(
        rawHeaders instanceof Headers
          ? rawHeaders.entries()
          : Object.entries(rawHeaders as Record<string, string>),
      );
      if (init?.body) {
        try {
          captured.body = JSON.parse(init.body as string);
        } catch {
          captured.body = init.body;
        }
      }
    }
    const status = resp.status ?? 200;
    return new Response(JSON.stringify(resp.body), {
      status,
      headers: { "Content-Type": "application/json" },
    });
  };
}

function rejectFetch(err: Error): typeof fetch {
  return async () => {
    throw err;
  };
}

describe("functions.invoke", () => {
  it("scalar function: add(x,y) returns data === 7", async () => {
    const captured: { url?: string; method?: string; body?: unknown } = {};
    const basin = createClient(BASE_URL, ANON_KEY, {
      fetch: stubFetch({ body: 7 }, captured),
    });

    const { data, error } = await basin.functions.invoke<number>("add", {
      body: { x: 3, y: 4 },
    });

    expect(error).toBeNull();
    expect(data).toBe(7);
    expect(captured.url).toBe(`${BASE_URL}/rest/v1/rpc/add`);
    expect(captured.method).toBe("POST");
    expect(captured.body).toEqual({ x: 3, y: 4 });
  });

  it("RETURNS TABLE function returns array of row objects", async () => {
    const rows = [
      { id: 1, name: "alice" },
      { id: 2, name: "bob" },
    ];
    const basin = createClient(BASE_URL, ANON_KEY, {
      fetch: stubFetch({ body: rows }),
    });

    const { data, error } = await basin.functions.invoke<{ id: number; name: string }[]>(
      "list_users",
      { body: {} },
    );

    expect(error).toBeNull();
    expect(Array.isArray(data)).toBe(true);
    expect(data).toEqual(rows);
    expect(data).toHaveLength(2);
  });

  it("no auth token → engine 401 propagates as BasinError('unauthorized')", async () => {
    const basin = createClient(BASE_URL, ANON_KEY, {
      fetch: stubFetch({ status: 401, body: { error: "unauthorized" } }),
    });

    const { data, error } = await basin.functions.invoke("secret_fn", { body: {} });

    expect(data).toBeNull();
    expect(error).toBeInstanceOf(BasinError);
    expect(error?.code).toBe("unauthorized");
    expect(error?.status).toBe(401);
  });

  it("5xx → BasinError with internal code", async () => {
    const basin = createClient(BASE_URL, ANON_KEY, {
      fetch: stubFetch({ status: 500, body: { error: "internal server error" } }),
    });

    const { data, error } = await basin.functions.invoke("broken_fn", { body: {} });

    expect(data).toBeNull();
    expect(error).toBeInstanceOf(BasinError);
    expect(error?.code).toBe("internal");
    expect(error?.status).toBe(500);
  });

  it("network error → BasinError('network')", async () => {
    const basin = createClient(BASE_URL, ANON_KEY, {
      fetch: rejectFetch(new Error("connection refused")),
    });

    const { data, error } = await basin.functions.invoke("add", { body: { x: 1, y: 2 } });

    expect(data).toBeNull();
    expect(error).toBeInstanceOf(BasinError);
    expect(error?.code).toBe("network");
    expect(error?.message).toBe("connection refused");
  });

  it("empty function name → BasinError('invalid_request')", async () => {
    const basin = createClient(BASE_URL, ANON_KEY, {
      fetch: async () => new Response("{}", { status: 200 }),
    });

    const { error } = await basin.functions.invoke("");

    expect(error?.code).toBe("invalid_request");
  });

  it("bearer token from client headers is forwarded", async () => {
    const captured: { headers?: Record<string, string> } = {};
    const basin = createClient(BASE_URL, ANON_KEY, {
      headers: { Authorization: "Bearer test-jwt" },
      fetch: stubFetch({ body: 1 }, captured),
    });

    await basin.functions.invoke("fn", { body: {} });

    expect(captured.headers?.["Authorization"]).toBe("Bearer test-jwt");
  });

  it("per-call headers override client defaults", async () => {
    const captured: { headers?: Record<string, string> } = {};
    const basin = createClient(BASE_URL, ANON_KEY, {
      fetch: stubFetch({ body: 42 }, captured),
    });

    await basin.functions.invoke("fn", {
      body: {},
      headers: { "X-Custom": "yes" },
    });

    expect(captured.headers?.["X-Custom"]).toBe("yes");
  });

  it("enabled === true", () => {
    const basin = createClient(BASE_URL, ANON_KEY, {
      fetch: async () => new Response("{}", { status: 200 }),
    });
    expect(basin.functions.enabled).toBe(true);
  });
});
