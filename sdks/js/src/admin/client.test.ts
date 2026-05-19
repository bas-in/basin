import { describe, expect, it } from "vitest";
import { createClient } from "../client.js";
import { BasinError } from "../errors.js";
import type { Credential } from "./types.js";

const BASE_URL = "https://api.basin.run";
const ANON_KEY = "anon-key";

function stubFetch(
  resp: { status?: number; body: unknown },
  captured?: { url?: string; method?: string; headers?: Record<string, string>; body?: unknown },
): typeof fetch {
  return async (input: Request | string | URL, init?: RequestInit): Promise<Response> => {
    if (captured) {
      captured.url = typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
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

const SAMPLE_CONNECTION_STRING = "postgres://user:pass@host:5433/db";

const SAMPLE_CREDENTIALS: Credential[] = [
  {
    id: "cred-1",
    project_id: "proj_x",
    pgwire_user: "pgwire_user_1",
    created_at: "2026-05-01T00:00:00Z",
    last_used_at: "2026-05-10T00:00:00Z",
    revoked_at: null,
  },
  {
    id: "cred-2",
    project_id: "proj_x",
    pgwire_user: "pgwire_user_2",
    created_at: "2026-05-02T00:00:00Z",
    last_used_at: null,
    revoked_at: null,
  },
];

// ── provision ────────────────────────────────────────────────────────

describe("admin.projects.provision", () => {
  it("happy path — POSTs correct URL/body and returns connectionString", async () => {
    const captured: { url?: string; method?: string; body?: unknown } = {};
    const basin = createClient(BASE_URL, ANON_KEY, {
      fetch: stubFetch(
        { body: { [SAMPLE_CONNECTION_STRING]: null } },
        captured,
      ),
    });

    const { data, error } = await basin.admin.projects.provision({ projectId: "proj_x" });

    expect(error).toBeNull();
    expect(data).toEqual({ connectionString: SAMPLE_CONNECTION_STRING });
    expect(captured.url).toBe(`${BASE_URL}/admin/v1/projects`);
    expect(captured.method).toBe("POST");
    expect(captured.body).toEqual({ project_id: "proj_x" });
  });

  it("401 → BasinError('unauthorized') with status preserved", async () => {
    const basin = createClient(BASE_URL, ANON_KEY, {
      fetch: stubFetch({ status: 401, body: { error: "not an admin" } }),
    });

    const { data, error } = await basin.admin.projects.provision({ projectId: "proj_x" });

    expect(data).toBeNull();
    expect(error).toBeInstanceOf(BasinError);
    expect(error?.code).toBe("unauthorized");
    expect(error?.status).toBe(401);
    expect(error?.message).toContain("is_admin");
  });

  it("403 → BasinError('unauthorized') with status preserved", async () => {
    const basin = createClient(BASE_URL, ANON_KEY, {
      fetch: stubFetch({ status: 403, body: { error: "forbidden" } }),
    });

    const { data, error } = await basin.admin.projects.provision({ projectId: "proj_x" });

    expect(data).toBeNull();
    expect(error?.code).toBe("unauthorized");
    expect(error?.status).toBe(403);
  });

  it("network reject → BasinError('network')", async () => {
    const basin = createClient(BASE_URL, ANON_KEY, {
      fetch: rejectFetch(new Error("connection refused")),
    });

    const { data, error } = await basin.admin.projects.provision({ projectId: "proj_x" });

    expect(data).toBeNull();
    expect(error).toBeInstanceOf(BasinError);
    expect(error?.code).toBe("network");
    expect(error?.message).toBe("connection refused");
  });
});

// ── rotateCredentials ─────────────────────────────────────────────────

describe("admin.projects.rotateCredentials", () => {
  it("happy path — POSTs correct URL and returns connectionString", async () => {
    const captured: { url?: string; method?: string } = {};
    const basin = createClient(BASE_URL, ANON_KEY, {
      fetch: stubFetch(
        { body: { [SAMPLE_CONNECTION_STRING]: null } },
        captured,
      ),
    });

    const { data, error } = await basin.admin.projects.rotateCredentials("pgwire_user_1");

    expect(error).toBeNull();
    expect(data).toEqual({ connectionString: SAMPLE_CONNECTION_STRING });
    expect(captured.url).toBe(`${BASE_URL}/admin/v1/projects/pgwire_user_1/rotate`);
    expect(captured.method).toBe("POST");
  });

  it("encodes special characters in pgwireUser", async () => {
    const captured: { url?: string } = {};
    const basin = createClient(BASE_URL, ANON_KEY, {
      fetch: stubFetch({ body: { [SAMPLE_CONNECTION_STRING]: null } }, captured),
    });

    await basin.admin.projects.rotateCredentials("user/with spaces");

    expect(captured.url).toBe(`${BASE_URL}/admin/v1/projects/user%2Fwith%20spaces/rotate`);
  });

  it("401 → BasinError('unauthorized') with status preserved", async () => {
    const basin = createClient(BASE_URL, ANON_KEY, {
      fetch: stubFetch({ status: 401, body: {} }),
    });

    const { data, error } = await basin.admin.projects.rotateCredentials("pgwire_user_1");

    expect(data).toBeNull();
    expect(error?.code).toBe("unauthorized");
    expect(error?.status).toBe(401);
  });

  it("404 → BasinError('not_found') with pgwireUser in message", async () => {
    const basin = createClient(BASE_URL, ANON_KEY, {
      fetch: stubFetch({ status: 404, body: {} }),
    });

    const { data, error } = await basin.admin.projects.rotateCredentials("pgwire_user_1");

    expect(data).toBeNull();
    expect(error).toBeInstanceOf(BasinError);
    expect(error?.code).toBe("not_found");
    expect(error?.status).toBe(404);
    expect(error?.message).toContain("pgwire_user_1");
  });

  it("network reject → BasinError('network')", async () => {
    const basin = createClient(BASE_URL, ANON_KEY, {
      fetch: rejectFetch(new Error("timeout")),
    });

    const { data, error } = await basin.admin.projects.rotateCredentials("pgwire_user_1");

    expect(data).toBeNull();
    expect(error?.code).toBe("network");
    expect(error?.message).toBe("timeout");
  });
});

// ── listCredentials ───────────────────────────────────────────────────

describe("admin.projects.listCredentials", () => {
  it("happy path — GETs correct URL and returns credential array", async () => {
    const captured: { url?: string; method?: string } = {};
    const basin = createClient(BASE_URL, ANON_KEY, {
      fetch: stubFetch({ body: SAMPLE_CREDENTIALS }, captured),
    });

    const { data, error } = await basin.admin.projects.listCredentials("proj_x");

    expect(error).toBeNull();
    expect(data).toEqual(SAMPLE_CREDENTIALS);
    expect(data).toHaveLength(2);
    expect(captured.url).toBe(`${BASE_URL}/admin/v1/projects/proj_x/credentials`);
    expect(captured.method).toBe("GET");
  });

  it("empty list response → {data: [], error: null}", async () => {
    const basin = createClient(BASE_URL, ANON_KEY, {
      fetch: stubFetch({ body: [] }),
    });

    const { data, error } = await basin.admin.projects.listCredentials("proj_x");

    expect(error).toBeNull();
    expect(data).toEqual([]);
    expect(Array.isArray(data)).toBe(true);
  });

  it("401 → BasinError('unauthorized') with status preserved", async () => {
    const basin = createClient(BASE_URL, ANON_KEY, {
      fetch: stubFetch({ status: 401, body: {} }),
    });

    const { data, error } = await basin.admin.projects.listCredentials("proj_x");

    expect(data).toBeNull();
    expect(error?.code).toBe("unauthorized");
    expect(error?.status).toBe(401);
  });

  it("network reject → BasinError('network')", async () => {
    const basin = createClient(BASE_URL, ANON_KEY, {
      fetch: rejectFetch(new Error("ECONNREFUSED")),
    });

    const { data, error } = await basin.admin.projects.listCredentials("proj_x");

    expect(data).toBeNull();
    expect(error?.code).toBe("network");
    expect(error?.message).toBe("ECONNREFUSED");
  });

  it("encodes special characters in projectId", async () => {
    const captured: { url?: string } = {};
    const basin = createClient(BASE_URL, ANON_KEY, {
      fetch: stubFetch({ body: [] }, captured),
    });

    await basin.admin.projects.listCredentials("proj/special id");

    expect(captured.url).toBe(`${BASE_URL}/admin/v1/projects/proj%2Fspecial%20id/credentials`);
  });
});
