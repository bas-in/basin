import { describe, expect, it } from "vitest";
import { fetchOpenAPI } from "./fetch.js";
import { BasinError } from "../errors.js";
import type { OpenAPIDocument } from "./types.js";

const MINIMAL_DOC: OpenAPIDocument = {
  openapi: "3.0.3",
  info: { title: "Basin REST API", version: "0.1.0" },
  paths: {
    "/rest/v1/users": {
      get: {
        operationId: "getUsers",
        responses: {
          "200": { description: "OK" },
        },
      },
    },
  },
  components: {
    schemas: {
      users: {
        type: "object",
        properties: {
          id: { type: "integer" },
          email: { type: "string" },
        },
        required: ["id", "email"],
      },
    },
  },
};

function stubFetch(
  resp: { status?: number; body?: unknown; bodyText?: string },
  captured?: { url?: string; headers?: Record<string, string> },
): typeof globalThis.fetch {
  return async (
    input: Request | string | URL,
    init?: RequestInit,
  ): Promise<Response> => {
    if (captured) {
      captured.url = typeof input === "string" ? input : input.toString();
      captured.headers = Object.fromEntries(
        new Headers(init?.headers as HeadersInit).entries(),
      );
    }
    const status = resp.status ?? 200;
    const bodyStr =
      resp.bodyText !== undefined
        ? resp.bodyText
        : JSON.stringify(resp.body ?? null);
    return new Response(bodyStr, {
      status,
      headers: { "Content-Type": "application/json" },
    });
  };
}

function networkErrorFetch(): typeof globalThis.fetch {
  return async (): Promise<Response> => {
    throw new TypeError("Failed to fetch");
  };
}

describe("fetchOpenAPI — happy path", () => {
  it("fetches and parses a valid OpenAPI document", async () => {
    const captured: { url?: string; headers?: Record<string, string> } = {};
    const doc = await fetchOpenAPI(
      "https://api.basin.run",
      "test-anon-key",
      { fetch: stubFetch({ body: MINIMAL_DOC }, captured) },
    );

    expect(doc.openapi).toBe("3.0.3");
    expect(doc.info.title).toBe("Basin REST API");
    expect(doc.paths["/rest/v1/users"]).toBeDefined();
    expect(doc.components?.schemas?.["users"]?.type).toBe("object");

    expect(captured.url).toBe(
      "https://api.basin.run/rest/v1/_openapi.json",
    );
    expect(captured.headers?.["apikey"]).toBe("test-anon-key");
  });

  it("strips trailing slash from the URL before appending the path", async () => {
    const captured: { url?: string } = {};
    await fetchOpenAPI("https://api.basin.run/", "key", {
      fetch: stubFetch({ body: MINIMAL_DOC }, captured),
    });
    expect(captured.url).toBe("https://api.basin.run/rest/v1/_openapi.json");
  });

  it("merges caller-provided headers without clobbering apikey", async () => {
    const captured: { headers?: Record<string, string> } = {};
    await fetchOpenAPI("https://api.basin.run", "key", {
      fetch: stubFetch({ body: MINIMAL_DOC }, captured),
      headers: { "X-Custom": "value" },
    });
    expect(captured.headers?.["x-custom"]).toBe("value");
    expect(captured.headers?.["apikey"]).toBe("key");
  });
});

describe("fetchOpenAPI — 404", () => {
  it("throws BasinError('not_found') on HTTP 404", async () => {
    await expect(
      fetchOpenAPI("https://api.basin.run", "key", {
        fetch: stubFetch({ status: 404, body: { message: "not found" } }),
      }),
    ).rejects.toSatisfy(
      (e: unknown) =>
        e instanceof BasinError &&
        e.code === "not_found" &&
        e.status === 404 &&
        e.message.includes("/rest/v1/_openapi.json"),
    );
  });
});

describe("fetchOpenAPI — 500", () => {
  it("throws BasinError('invalid_response') on HTTP 500 with status preserved", async () => {
    await expect(
      fetchOpenAPI("https://api.basin.run", "key", {
        fetch: stubFetch({ status: 500, body: { message: "internal error" } }),
      }),
    ).rejects.toSatisfy(
      (e: unknown) =>
        e instanceof BasinError &&
        e.code === "invalid_response" &&
        e.status === 500,
    );
  });
});

describe("fetchOpenAPI — malformed JSON", () => {
  it("throws BasinError('invalid_response') when body is not valid JSON", async () => {
    await expect(
      fetchOpenAPI("https://api.basin.run", "key", {
        fetch: stubFetch({ bodyText: "not-json{{" }),
      }),
    ).rejects.toSatisfy(
      (e: unknown) =>
        e instanceof BasinError &&
        e.code === "invalid_response" &&
        e.message === "OpenAPI document is not valid JSON",
    );
  });
});

describe("fetchOpenAPI — network failure", () => {
  it("throws BasinError('network') when fetch rejects", async () => {
    await expect(
      fetchOpenAPI("https://api.basin.run", "key", {
        fetch: networkErrorFetch(),
      }),
    ).rejects.toSatisfy(
      (e: unknown) =>
        e instanceof BasinError &&
        e.code === "network" &&
        e.message === "Failed to fetch OpenAPI document",
    );
  });
});
