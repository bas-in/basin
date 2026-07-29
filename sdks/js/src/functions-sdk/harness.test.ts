/**
 * Tests for the @basin/functions authoring SDK.
 *
 * All tests run against the local mock-host harness — no running engine
 * required.  Gates from the task spec:
 *   - `query`, `http`, `log`, `secret` are callable (type-check + runtime)
 *   - The template handler run against the mock host returns the expected response.
 *   - query / http / secret mocks are invoked and their results flow through.
 *   - `runWithMockHost` records every host-import call.
 */

import { describe, expect, it } from "vitest";

// Import from the SDK source directly (within the same package the
// sub-path export `./functions-sdk` resolves here at runtime via tsconfig
// paths; in production consumers use `@bas-in/basin-js/functions-sdk`).
import {
  http,
  log,
  query,
  runWithMockHost,
  secret,
} from "./index.js";
import type {
  BasinHandler,
  BasinRequest,
  BasinResponse,
  HttpRequest,
  HttpResponse,
  QueryResult,
} from "./index.js";
import templateHandler from "./template.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeRequest(overrides: Partial<BasinRequest> = {}): BasinRequest {
  return {
    method: "GET",
    path: "/fn/v1/test",
    query: {},
    headers: { "content-type": "application/json" },
    body: "",
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// Type-check smoke test: imported symbols are callable
// ---------------------------------------------------------------------------

describe("host binding exports", () => {
  it("query / http / log / secret are functions", () => {
    expect(typeof query).toBe("function");
    expect(typeof http).toBe("function");
    expect(typeof log).toBe("function");
    expect(typeof secret).toBe("function");
  });

  it("runWithMockHost is a function", () => {
    expect(typeof runWithMockHost).toBe("function");
  });

  it("calling a host binding outside of a context throws a descriptive error", () => {
    expect(() => log("info", "test")).toThrow("outside of a host context");
  });
});

// ---------------------------------------------------------------------------
// query() mock flows through
// ---------------------------------------------------------------------------

describe("runWithMockHost — query", () => {
  it("mockQuery result flows into the handler", async () => {
    const expectedRows: QueryResult["rows"] = [{ id: 1, name: "alice" }];

    const handler: BasinHandler = async () => {
      const result = await query("SELECT id, name FROM users WHERE id = $1", [1]);
      return {
        status: 200,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(result.rows),
      };
    };

    const { response, queryCalls } = await runWithMockHost(
      handler,
      makeRequest(),
      { mockQuery: async () => ({ rows: expectedRows }) },
    );

    expect(response.status).toBe(200);
    const parsed = JSON.parse(response.body) as unknown[];
    expect(parsed).toEqual(expectedRows);
    expect(queryCalls).toHaveLength(1);
    expect(queryCalls[0]?.sql).toBe("SELECT id, name FROM users WHERE id = $1");
    expect(queryCalls[0]?.params).toEqual([1]);
  });

  it("rowsAffected is propagated from the mock", async () => {
    const handler: BasinHandler = async () => {
      const result = await query("DELETE FROM stale WHERE created_at < $1", ["2025-01-01"]);
      return { status: 200, body: String(result.rowsAffected ?? 0) };
    };

    const { response } = await runWithMockHost(handler, makeRequest(), {
      mockQuery: async () => ({ rows: [], rowsAffected: 42 }),
    });

    expect(response.body).toBe("42");
  });

  it("omitting mockQuery and calling query() throws", async () => {
    const handler: BasinHandler = async () => {
      await query("SELECT 1");
      return { status: 200, body: "" };
    };

    await expect(
      runWithMockHost(handler, makeRequest()),
    ).rejects.toThrow("no mockQuery was provided");
  });

  it("multiple query() calls are all recorded", async () => {
    const handler: BasinHandler = async () => {
      await query("SELECT 1");
      await query("SELECT 2");
      return { status: 200, body: "" };
    };

    const { queryCalls } = await runWithMockHost(handler, makeRequest(), {
      mockQuery: async () => ({ rows: [] }),
    });

    expect(queryCalls).toHaveLength(2);
    expect(queryCalls[0]?.sql).toBe("SELECT 1");
    expect(queryCalls[1]?.sql).toBe("SELECT 2");
  });
});

// ---------------------------------------------------------------------------
// http() mock flows through
// ---------------------------------------------------------------------------

describe("runWithMockHost — http", () => {
  it("mockHttp result flows into the handler", async () => {
    const mockResponse: HttpResponse = {
      status: 200,
      headers: { "content-type": "application/json" },
      body: '{"stripe":"ok"}',
    };

    const handler: BasinHandler = async () => {
      const resp = await http({
        method: "POST",
        url: "https://api.stripe.com/v1/charges",
        headers: { Authorization: "Bearer sk_test" },
        body: "amount=1000",
      });
      return { status: resp.status, body: resp.body };
    };

    const { response, httpCalls } = await runWithMockHost(
      handler,
      makeRequest(),
      { mockHttp: async () => mockResponse },
    );

    expect(response.status).toBe(200);
    expect(response.body).toBe('{"stripe":"ok"}');
    expect(httpCalls).toHaveLength(1);
    expect(httpCalls[0]?.request.url).toBe("https://api.stripe.com/v1/charges");
    expect(httpCalls[0]?.request.method).toBe("POST");
  });

  it("omitting mockHttp and calling http() throws", async () => {
    const handler: BasinHandler = async () => {
      await http({ method: "GET", url: "https://example.com" });
      return { status: 200, body: "" };
    };

    await expect(
      runWithMockHost(handler, makeRequest()),
    ).rejects.toThrow("no mockHttp was provided");
  });
});

// ---------------------------------------------------------------------------
// secret() mock flows through
// ---------------------------------------------------------------------------

describe("runWithMockHost — secret", () => {
  it("mockSecret result flows into the handler", async () => {
    const handler: BasinHandler = async () => {
      const key = await secret("STRIPE_API_KEY");
      return { status: 200, body: key };
    };

    const { response, secretCalls } = await runWithMockHost(
      handler,
      makeRequest(),
      { mockSecret: async (name) => `mock-secret-for-${name}` },
    );

    expect(response.status).toBe(200);
    expect(response.body).toBe("mock-secret-for-STRIPE_API_KEY");
    expect(secretCalls).toHaveLength(1);
    expect(secretCalls[0]?.name).toBe("STRIPE_API_KEY");
  });

  it("omitting mockSecret and calling secret() throws", async () => {
    const handler: BasinHandler = async () => {
      await secret("MISSING");
      return { status: 200, body: "" };
    };

    await expect(
      runWithMockHost(handler, makeRequest()),
    ).rejects.toThrow("no mockSecret was provided");
  });
});

// ---------------------------------------------------------------------------
// log() — default no-op; optional override
// ---------------------------------------------------------------------------

describe("runWithMockHost — log", () => {
  it("log calls are recorded even without mockLog", async () => {
    const handler: BasinHandler = async () => {
      log("info", "starting");
      log("warn", "almost done");
      return { status: 200, body: "" };
    };

    const { logCalls } = await runWithMockHost(handler, makeRequest());

    expect(logCalls).toHaveLength(2);
    expect(logCalls[0]).toEqual({ level: "info", message: "starting" });
    expect(logCalls[1]).toEqual({ level: "warn", message: "almost done" });
  });

  it("mockLog override is called", async () => {
    const seen: string[] = [];

    const handler: BasinHandler = async () => {
      log("debug", "detail");
      return { status: 200, body: "" };
    };

    await runWithMockHost(handler, makeRequest(), {
      mockLog: (_level, msg) => { seen.push(msg); },
    });

    expect(seen).toEqual(["detail"]);
  });
});

// ---------------------------------------------------------------------------
// BasinRequest fields are forwarded to the handler
// ---------------------------------------------------------------------------

describe("runWithMockHost — request passthrough", () => {
  it("handler sees the full BasinRequest", async () => {
    let captured: BasinRequest | null = null;

    const handler: BasinHandler = async (req: BasinRequest): Promise<BasinResponse> => {
      captured = req;
      return { status: 200, body: "" };
    };

    const req = makeRequest({
      method: "POST",
      path: "/fn/v1/orders",
      query: { limit: "10" },
      headers: { authorization: "Bearer jwt" },
      body: '{"user_id":42}',
    });

    await runWithMockHost(handler, req);

    expect(captured).not.toBeNull();
    expect(captured!.method).toBe("POST");
    expect(captured!.query["limit"]).toBe("10");
    expect(captured!.headers["authorization"]).toBe("Bearer jwt");
    expect(JSON.parse(captured!.body)).toEqual({ user_id: 42 });
  });
});

// ---------------------------------------------------------------------------
// Context isolation — sequential runs don't bleed state
// ---------------------------------------------------------------------------

describe("runWithMockHost — context isolation", () => {
  it("host context is cleared after each run", async () => {
    const handler: BasinHandler = async () => {
      await query("SELECT 1");
      return { status: 200, body: "" };
    };

    // First call works fine.
    await runWithMockHost(handler, makeRequest(), {
      mockQuery: async () => ({ rows: [] }),
    });

    // After the run the context must be cleared; a raw host call throws.
    expect(() => log("info", "leak test")).toThrow("outside of a host context");
  });

  it("context is cleared even when the handler throws", async () => {
    const handler: BasinHandler = async () => {
      throw new Error("handler error");
    };

    await expect(
      runWithMockHost(handler, makeRequest()),
    ).rejects.toThrow("handler error");

    // Context still cleared.
    expect(() => log("info", "after throw")).toThrow("outside of a host context");
  });
});

// ---------------------------------------------------------------------------
// Template handler (end-to-end: query + secret + log)
// ---------------------------------------------------------------------------

describe("template handler", () => {
  it("returns 200 with orders array for a valid user_id", async () => {
    const mockOrders: QueryResult["rows"] = [
      { id: 1, total_cents: 5000, status: "paid", created_at: "2026-01-01" },
      { id: 2, total_cents: 1200, status: "pending", created_at: "2026-01-02" },
    ];

    const { response, queryCalls, secretCalls, logCalls } = await runWithMockHost(
      templateHandler,
      makeRequest({
        method: "POST",
        body: JSON.stringify({ user_id: 42 }),
      }),
      {
        mockQuery: async () => ({ rows: mockOrders }),
        mockSecret: async () => "mock-internal-key",
      },
    );

    expect(response.status).toBe(200);
    expect(response.headers?.["Content-Type"]).toBe("application/json");

    const parsed = JSON.parse(response.body) as { orders: unknown[] };
    expect(parsed.orders).toEqual(mockOrders);

    // query was called with the user_id param
    expect(queryCalls).toHaveLength(1);
    expect(queryCalls[0]?.params).toEqual([42]);

    // secret was fetched
    expect(secretCalls).toHaveLength(1);
    expect(secretCalls[0]?.name).toBe("INTERNAL_API_KEY");

    // log was emitted
    expect(logCalls.some((c) => c.message.includes("42"))).toBe(true);
  });

  it("returns 400 when user_id is missing", async () => {
    const { response } = await runWithMockHost(
      templateHandler,
      makeRequest({
        method: "POST",
        body: JSON.stringify({ other: "field" }),
      }),
      { mockSecret: async () => "key" },
    );

    expect(response.status).toBe(400);
    const body = JSON.parse(response.body) as { error: string };
    expect(body.error).toMatch(/user_id/);
  });

  it("returns 400 on invalid JSON body", async () => {
    const { response } = await runWithMockHost(
      templateHandler,
      makeRequest({ method: "POST", body: "not-json" }),
      { mockSecret: async () => "key" },
    );

    expect(response.status).toBe(400);
    const body = JSON.parse(response.body) as { error: string };
    expect(body.error).toMatch(/JSON/);
  });
});

// ---------------------------------------------------------------------------
// Inline custom handler (verifies query + http + secret compose together)
// ---------------------------------------------------------------------------

describe("composed handler — query + http + secret", () => {
  it("all three mocks are invoked and their results flow through", async () => {
    const handler: BasinHandler = async (req: BasinRequest): Promise<BasinResponse> => {
      const { rows } = await query("SELECT email FROM users WHERE id = $1", [7]);
      const apiKey = await secret("NOTIFY_API_KEY");
      const notifyResp = await http({
        method: "POST",
        url: "https://notify.example.com/v1/send",
        headers: { "X-Api-Key": apiKey },
        body: JSON.stringify({ to: rows[0]?.["email"], event: req.query["event"] }),
      });
      return {
        status: notifyResp.status,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ sent: notifyResp.status === 200 }),
      };
    };

    const req = makeRequest({
      method: "POST",
      query: { event: "signup" },
      body: JSON.stringify({ user_id: 7 }),
    });

    const httpResp: HttpResponse = {
      status: 200,
      headers: {},
      body: '{"ok":true}',
    };

    const { response, queryCalls, httpCalls, secretCalls } = await runWithMockHost(
      handler,
      req,
      {
        mockQuery: async () => ({ rows: [{ email: "alice@example.com" }] }),
        mockSecret: async (name) => `secret-${name}`,
        mockHttp: async (r: HttpRequest) => {
          // Verify the secret was threaded through to the HTTP call.
          expect(r.headers?.["X-Api-Key"]).toBe("secret-NOTIFY_API_KEY");
          return httpResp;
        },
      },
    );

    expect(response.status).toBe(200);
    const parsed = JSON.parse(response.body) as { sent: boolean };
    expect(parsed.sent).toBe(true);

    expect(queryCalls).toHaveLength(1);
    expect(queryCalls[0]?.params).toEqual([7]);

    expect(secretCalls).toHaveLength(1);
    expect(secretCalls[0]?.name).toBe("NOTIFY_API_KEY");

    expect(httpCalls).toHaveLength(1);
    expect(httpCalls[0]?.request.url).toBe("https://notify.example.com/v1/send");
  });
});
