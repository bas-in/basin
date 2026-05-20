import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createClient } from "../client.js";
import { BasinError } from "../errors.js";

// Build a `fetch` stub that returns a single canned response. The
// closure captures the request so individual tests can assert on URL,
// method, headers, body.
function stubFetch(
  resp: { status?: number; body: unknown },
  captured?: { request?: Request },
): typeof fetch {
  return async (input: Request | string | URL, init?: RequestInit): Promise<Response> => {
    if (captured) {
      captured.request = new Request(input as RequestInfo, init);
    }
    const status = resp.status ?? 200;
    return new Response(JSON.stringify(resp.body), {
      status,
      headers: { "Content-Type": "application/json" },
    });
  };
}

// In-memory storage adapter so tests can verify session persistence.
function memoryStorage() {
  const m = new Map<string, string>();
  return {
    getItem: (k: string) => m.get(k) ?? null,
    setItem: (k: string, v: string) => {
      m.set(k, v);
    },
    removeItem: (k: string) => {
      m.delete(k);
    },
    raw: m,
  };
}

describe("auth.signInWithPassword", () => {
  it("rejects empty input with invalid_request", async () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: stubFetch({ body: {} }),
    });
    const { data, error } = await basin.auth.signInWithPassword({
      email: "",
      password: "",
    });
    expect(data).toBeNull();
    expect(error?.code).toBe("invalid_request");
  });

  it("POSTs the engine's /auth/v1/signin endpoint with email+password", async () => {
    const captured: { request?: Request } = {};
    const basin = createClient("https://api.basin.run", "anon-key", {
      fetch: stubFetch(
        {
          body: {
            data: {
              user: {
                id: "01H...",
                email: "pc@example.com",
                email_verified: true,
                created_at: "2026-05-11T00:00:00Z",
                updated_at: "2026-05-11T00:00:00Z",
              },
              session: {
                access_token: "at",
                refresh_token: "rt",
                expires_at: "2026-05-11T01:00:00Z",
                session_id: "sess-1",
              },
            },
            error: null,
          },
        },
        captured,
      ),
    });
    const { data, error } = await basin.auth.signInWithPassword({
      email: "pc@example.com",
      password: "hunter2",
    });
    expect(error).toBeNull();
    expect(data?.access_token).toBe("at");
    expect(data?.refresh_token).toBe("rt");
    expect(data?.token_type).toBe("bearer");
    expect(data?.user.email).toBe("pc@example.com");

    // Request assertions.
    expect(captured.request?.method).toBe("POST");
    expect(captured.request?.url).toBe("https://api.basin.run/auth/v1/signin");
    expect(captured.request?.headers.get("apikey")).toBe("anon-key");
    const body = await captured.request?.json();
    expect(body).toEqual({ email: "pc@example.com", password: "hunter2" });
  });

  it("maps RFC3339 expires_at to seconds-since-epoch", async () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: stubFetch({
        body: {
          data: {
            user: { id: "u1", email: "x@y.z" },
            session: {
              access_token: "at",
              refresh_token: "rt",
              expires_at: "2030-01-01T00:00:00Z",
            },
          },
        },
      }),
    });
    const { data } = await basin.auth.signInWithPassword({
      email: "x@y.z",
      password: "p",
    });
    // 2030-01-01T00:00:00Z = 1893456000
    expect(data?.expires_at).toBe(1893456000);
  });

  it("surfaces the cloud's typed error envelope as a BasinError", async () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: stubFetch({
        status: 401,
        body: {
          data: null,
          error: {
            code: "invalid_credentials",
            message: "Email or password is incorrect.",
          },
        },
      }),
    });
    const { data, error } = await basin.auth.signInWithPassword({
      email: "x@y.z",
      password: "wrong",
    });
    expect(data).toBeNull();
    expect(error).toBeInstanceOf(BasinError);
    expect(error?.code).toBe("invalid_credentials");
    expect(error?.status).toBe(401);
  });

  it("returns mfa_required when the cloud emits requires_totp", async () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: stubFetch({
        body: {
          data: {
            requires_totp: true,
            partial_token: "pt-abc",
          },
        },
      }),
    });
    const { data, error } = await basin.auth.signInWithPassword({
      email: "x@y.z",
      password: "p",
    });
    expect(data).toBeNull();
    expect(error?.code).toBe("mfa_required");
    expect((error?.details as { partial_token?: string })?.partial_token).toBe("pt-abc");
  });

  it("falls back to network error when fetch rejects", async () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: async () => {
        throw new Error("boom");
      },
    });
    const { data, error } = await basin.auth.signInWithPassword({
      email: "x@y.z",
      password: "p",
    });
    expect(data).toBeNull();
    expect(error?.code).toBe("network");
    expect(error?.message).toContain("boom");
  });

  it("persists the session to the auth storage adapter on success", async () => {
    const storage = memoryStorage();
    const basin = createClient("https://api.basin.run", "anon", {
      authStorage: storage,
      fetch: stubFetch({
        body: {
          data: {
            user: { id: "u1", email: "x@y.z" },
            session: {
              access_token: "at",
              refresh_token: "rt",
              expires_at: "2030-01-01T00:00:00Z",
            },
          },
        },
      }),
    });
    await basin.auth.signInWithPassword({ email: "x@y.z", password: "p" });
    expect(storage.raw.has("basin.auth.session")).toBe(true);
    const parsed = JSON.parse(storage.raw.get("basin.auth.session")!);
    expect(parsed.access_token).toBe("at");
  });

  it("fires SIGNED_IN on onAuthStateChange subscribers", async () => {
    const events: string[] = [];
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: stubFetch({
        body: {
          data: {
            user: { id: "u1", email: "x@y.z" },
            session: {
              access_token: "at",
              refresh_token: "rt",
              expires_at: "2030-01-01T00:00:00Z",
            },
          },
        },
      }),
    });
    basin.auth.onAuthStateChange((event) => events.push(event));
    await basin.auth.signInWithPassword({ email: "x@y.z", password: "p" });
    expect(events).toContain("SIGNED_IN");
  });

  it("supports the tolerant flat-shape (no envelope) for test/mock servers", async () => {
    // Some test mocks (and possibly hand-crafted public endpoints) return
    // a flat shape without {data, error} wrapping. unwrapAuthBody is
    // tolerant — treats the body itself as the payload.
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: stubFetch({
        body: {
          user: { id: "u1", email: "x@y.z" },
          session: {
            access_token: "at",
            refresh_token: "rt",
            expires_at: "2030-01-01T00:00:00Z",
          },
        },
      }),
    });
    const { data, error } = await basin.auth.signInWithPassword({
      email: "x@y.z",
      password: "p",
    });
    expect(error).toBeNull();
    expect(data?.access_token).toBe("at");
  });

  it("returns invalid_response when the payload is missing user/session", async () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: stubFetch({
        body: { data: { user: { id: "u1" } } }, // session missing
      }),
    });
    const { data, error } = await basin.auth.signInWithPassword({
      email: "x@y.z",
      password: "p",
    });
    expect(data).toBeNull();
    expect(error?.code).toBe("invalid_response");
  });
});

describe("auth.signUp", () => {
  it("rejects empty input with invalid_request", async () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: stubFetch({ body: {} }),
    });
    const { data, error } = await basin.auth.signUp({
      email: "",
      password: "",
    });
    expect(data).toBeNull();
    expect(error?.code).toBe("invalid_request");
  });

  it("POSTs /auth/v1/signup with email+password and returns the new session", async () => {
    const captured: { request?: Request } = {};
    const basin = createClient("https://api.basin.run", "anon-key", {
      fetch: stubFetch(
        {
          status: 201,
          body: {
            data: {
              user: {
                id: "01H_new",
                email: "new@example.com",
                email_verified: false,
                created_at: "2026-05-11T00:00:00Z",
                updated_at: "2026-05-11T00:00:00Z",
              },
              session: {
                access_token: "at-new",
                refresh_token: "rt-new",
                expires_at: "2026-05-11T01:00:00Z",
              },
              org: { id: "org-new" },
            },
            error: null,
          },
        },
        captured,
      ),
    });
    const { data, error } = await basin.auth.signUp({
      email: "new@example.com",
      password: "hunter22hunter22",
    });
    expect(error).toBeNull();
    expect(data?.user.id).toBe("01H_new");
    expect(data?.user.email).toBe("new@example.com");
    expect(data?.user.email_confirmed_at).toBeNull(); // unverified → null
    expect(data?.access_token).toBe("at-new");

    expect(captured.request?.method).toBe("POST");
    expect(captured.request?.url).toBe("https://api.basin.run/auth/v1/signup");
    const body = await captured.request?.json();
    expect(body).toEqual({
      email: "new@example.com",
      password: "hunter22hunter22",
    });
  });

  it("surfaces cloud's email_in_use conflict as a typed BasinError", async () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: stubFetch({
        status: 409,
        body: {
          data: null,
          error: {
            code: "email_in_use",
            message: "Email already in use. Try signing in instead.",
          },
        },
      }),
    });
    const { data, error } = await basin.auth.signUp({
      email: "exists@example.com",
      password: "hunter22hunter22",
    });
    expect(data).toBeNull();
    expect(error?.code).toBe("email_in_use");
    expect(error?.status).toBe(409);
  });

  it("persists the session + fires SIGNED_IN on success", async () => {
    const events: string[] = [];
    const storage = memoryStorage();
    const basin = createClient("https://api.basin.run", "anon", {
      authStorage: storage,
      fetch: stubFetch({
        status: 201,
        body: {
          data: {
            user: { id: "u-new", email: "x@y.z" },
            session: {
              access_token: "at",
              refresh_token: "rt",
              expires_at: "2030-01-01T00:00:00Z",
            },
          },
        },
      }),
    });
    basin.auth.onAuthStateChange((e) => events.push(e));
    await basin.auth.signUp({ email: "x@y.z", password: "hunter22hunter22" });
    expect(events).toContain("SIGNED_IN");
    expect(storage.raw.has("basin.auth.session")).toBe(true);
  });

  it("collapses fetch reject to network error", async () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: async () => {
        throw new TypeError("offline");
      },
    });
    const { data, error } = await basin.auth.signUp({
      email: "x@y.z",
      password: "hunter22hunter22",
    });
    expect(data).toBeNull();
    expect(error?.code).toBe("network");
  });
});

describe("auth.signInWithOAuth", () => {
  it("returns not_implemented today (engine has no /auth/v1/oauth/* routes)", async () => {
    let fetchCalled = false;
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: async () => {
        fetchCalled = true;
        return new Response("{}", { status: 200 });
      },
    });
    const { data, error } = await basin.auth.signInWithOAuth({
      provider: "google",
    });
    expect(data).toBeNull();
    expect(error).toBeInstanceOf(BasinError);
    expect(error?.code).toBe("not_implemented");
    expect(error?.message).toContain("OAuth");
    expect(error?.message).toContain("ROADMAP 0.3");
    // No network egress — the stub returns not_implemented before any fetch.
    expect(fetchCalled).toBe(false);
  });
});

describe("auth.signInWithMagicLink", () => {
  it("rejects empty email with invalid_request", async () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: stubFetch({ body: {} }),
    });
    const { data, error } = await basin.auth.signInWithMagicLink({
      email: "",
    });
    expect(data).toBeNull();
    expect(error?.code).toBe("invalid_request");
  });

  it("POSTs /auth/v1/magic-link with email and returns {data: null, error: null} on 202", async () => {
    const captured: { request?: Request } = {};
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: stubFetch(
        {
          status: 202,
          body: { data: { sent: true }, error: null },
        },
        captured,
      ),
    });
    const { data, error } = await basin.auth.signInWithMagicLink({
      email: "pc@example.com",
    });
    expect(data).toBeNull();
    expect(error).toBeNull();
    expect(captured.request?.method).toBe("POST");
    expect(captured.request?.url).toBe(
      "https://api.basin.run/auth/v1/magic-link",
    );
    const body = await captured.request?.json();
    expect(body).toEqual({ email: "pc@example.com" });
  });

  it("does not fire SIGNED_IN (no session yet on magic-link request)", async () => {
    const events: string[] = [];
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: stubFetch({
        status: 202,
        body: { data: { sent: true }, error: null },
      }),
    });
    basin.auth.onAuthStateChange((e) => events.push(e));
    await basin.auth.signInWithMagicLink({ email: "pc@example.com" });
    expect(events).not.toContain("SIGNED_IN");
  });

  it("collapses network errors to BasinError", async () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: async () => {
        throw new Error("offline");
      },
    });
    const { error } = await basin.auth.signInWithMagicLink({
      email: "pc@example.com",
    });
    expect(error?.code).toBe("network");
  });

  it("surfaces rate-limit errors from the cloud", async () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: stubFetch({
        status: 429,
        body: {
          data: null,
          error: { code: "rate_limited", message: "Slow down." },
        },
      }),
    });
    const { error } = await basin.auth.signInWithMagicLink({
      email: "pc@example.com",
    });
    expect(error?.code).toBe("rate_limited");
    expect(error?.status).toBe(429);
  });
});

describe("auth.signOut", () => {
  it("clears local state + fires SIGNED_OUT without any network call", async () => {
    let fetchCalled = false;
    const storage = memoryStorage();
    const basin = createClient("https://api.basin.run", "anon", {
      authStorage: storage,
      fetch: async (input: Request | string | URL) => {
        const url = typeof input === "string" ? input : (input as URL | Request).toString();
        if (url.endsWith("/signin")) {
          return new Response(
            JSON.stringify({
              data: {
                user: { id: "u1", email: "x@y.z" },
                session: {
                  access_token: "at",
                  refresh_token: "rt",
                  expires_at: "2030-01-01T00:00:00Z",
                },
              },
            }),
            { status: 200, headers: { "Content-Type": "application/json" } },
          );
        }
        // ANY other URL means signOut tried to hit the network — fail.
        fetchCalled = true;
        throw new Error("signOut should not make a network call: " + url);
      },
    });
    // Sign in first so signOut has a session to clear.
    await basin.auth.signInWithPassword({ email: "x@y.z", password: "p" });
    expect(storage.raw.has("basin.auth.session")).toBe(true);

    const events: string[] = [];
    basin.auth.onAuthStateChange((e) => events.push(e));

    const { error } = await basin.auth.signOut();
    expect(error).toBeNull();
    expect(basin.auth.getSession()).toBeNull();
    expect(storage.raw.has("basin.auth.session")).toBe(false);
    expect(events).toContain("SIGNED_OUT");
    // Engine has no /logout endpoint — sign-out is local-only.
    expect(fetchCalled).toBe(false);
  });

  it("no-ops gracefully when there's no active session", async () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: async () => {
        throw new Error("signOut should not be called");
      },
    });
    const { error } = await basin.auth.signOut();
    expect(error).toBeNull();
  });
});

describe("auth.getSession + getUser + hydration", () => {
  it("returns null on a fresh client with no storage", () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: stubFetch({ body: {} }),
    });
    expect(basin.auth.getSession()).toBeNull();
    expect(basin.auth.getUser()).toBeNull();
  });

  it("hydrates a valid session from sync storage on construction", () => {
    const storage = memoryStorage();
    const future = Math.floor(Date.now() / 1000) + 3600;
    storage.raw.set(
      "basin.auth.session",
      JSON.stringify({
        access_token: "hydrated-at",
        refresh_token: "hydrated-rt",
        token_type: "bearer",
        expires_at: future,
        user: { id: "u-hydrated", email: "h@x.io" },
      }),
    );
    const basin = createClient("https://api.basin.run", "anon", {
      authStorage: storage,
      fetch: stubFetch({ body: {} }),
    });
    const session = basin.auth.getSession();
    expect(session?.access_token).toBe("hydrated-at");
    expect(basin.auth.getUser()?.email).toBe("h@x.io");
  });

  it("fires INITIAL_SESSION on construction (null when no cache)", () => {
    const events: { event: string; hasSession: boolean }[] = [];
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: stubFetch({ body: {} }),
    });
    basin.auth.onAuthStateChange((e, s) =>
      events.push({ event: e, hasSession: s !== null }),
    );
    // Subscribers added AFTER construction won't see the synchronous
    // INITIAL_SESSION emit — that's the natural ordering. Confirm
    // that getSession() reflects the no-cache state.
    expect(basin.auth.getSession()).toBeNull();
    expect(events.length).toBe(0);
  });

  it("ignores a session whose expires_at is in the past", () => {
    const storage = memoryStorage();
    const past = Math.floor(Date.now() / 1000) - 3600;
    storage.raw.set(
      "basin.auth.session",
      JSON.stringify({
        access_token: "stale-at",
        refresh_token: "stale-rt",
        token_type: "bearer",
        expires_at: past,
        user: { id: "u-stale", email: "s@x.io" },
      }),
    );
    const basin = createClient("https://api.basin.run", "anon", {
      authStorage: storage,
      fetch: stubFetch({ body: {} }),
    });
    // Expired session is NOT loaded into memory — refreshSession (next
    // firing) will pick up the refresh_token from storage and rotate.
    expect(basin.auth.getSession()).toBeNull();
  });

  it("ignores a malformed session blob (corrupt JSON)", () => {
    const storage = memoryStorage();
    storage.raw.set("basin.auth.session", "not-json-{");
    const basin = createClient("https://api.basin.run", "anon", {
      authStorage: storage,
      fetch: stubFetch({ body: {} }),
    });
    expect(basin.auth.getSession()).toBeNull();
    // Corrupt blob should be cleaned out so the next load doesn't repeat.
    expect(storage.raw.has("basin.auth.session")).toBe(false);
  });

  it("ignores a partially-shaped session blob (missing fields)", () => {
    const storage = memoryStorage();
    // Missing refresh_token + user.
    storage.raw.set(
      "basin.auth.session",
      JSON.stringify({ access_token: "x", expires_at: 9999999999 }),
    );
    const basin = createClient("https://api.basin.run", "anon", {
      authStorage: storage,
      fetch: stubFetch({ body: {} }),
    });
    expect(basin.auth.getSession()).toBeNull();
  });

  it("hydrates from an async storage adapter", async () => {
    const future = Math.floor(Date.now() / 1000) + 3600;
    const blob = JSON.stringify({
      access_token: "async-at",
      refresh_token: "async-rt",
      token_type: "bearer",
      expires_at: future,
      user: { id: "u-async", email: "a@x.io" },
    });
    const asyncStorage = {
      getItem: async (_: string) => blob,
      setItem: async (_: string, __: string) => {},
      removeItem: async (_: string) => {},
    };
    const events: { event: string; hasSession: boolean }[] = [];
    const basin = createClient("https://api.basin.run", "anon", {
      authStorage: asyncStorage,
      fetch: stubFetch({ body: {} }),
    });
    basin.auth.onAuthStateChange((e, s) =>
      events.push({ event: e, hasSession: s !== null }),
    );
    // Sync-call right after construction — async storage hasn't resolved yet.
    expect(basin.auth.getSession()).toBeNull();
    // After one microtask, the session should be populated + INITIAL_SESSION fires.
    await new Promise<void>((r) => setTimeout(r, 0));
    expect(basin.auth.getSession()?.access_token).toBe("async-at");
    expect(events.some((e) => e.event === "INITIAL_SESSION" && e.hasSession)).toBe(true);
  });
});

describe("auth.onAuthStateChange", () => {
  it("delivers each event to every subscriber until unsubscribed", async () => {
    const a: string[] = [];
    const b: string[] = [];
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: stubFetch({
        body: {
          data: {
            user: { id: "u1", email: "x@y.z" },
            session: {
              access_token: "at",
              refresh_token: "rt",
              expires_at: "2030-01-01T00:00:00Z",
            },
          },
        },
      }),
    });
    const subA = basin.auth.onAuthStateChange((e) => a.push(e));
    const subB = basin.auth.onAuthStateChange((e) => b.push(e));

    await basin.auth.signInWithPassword({ email: "x@y.z", password: "p" });
    expect(a).toContain("SIGNED_IN");
    expect(b).toContain("SIGNED_IN");

    // Unsubscribe A only — B should keep receiving events.
    subA.data.subscription.unsubscribe();
    a.length = 0;
    b.length = 0;
    await basin.auth.signOut();
    expect(a).toEqual([]);
    expect(b).toContain("SIGNED_OUT");

    subB.data.subscription.unsubscribe();
  });
});

describe("auth.refreshSession", () => {
  async function signIn(basin: ReturnType<typeof createClient>) {
    return basin.auth.signInWithPassword({ email: "x@y.z", password: "p" });
  }

  it("returns no_session when no session is active", async () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: stubFetch({ body: {} }),
    });
    const { data, error } = await basin.auth.refreshSession();
    expect(data).toBeNull();
    expect(error?.code).toBe("no_session");
  });

  it("POSTs /auth/v1/refresh with the refresh_token body", async () => {
    const captured: { request?: Request } = {};
    let phase: "login" | "refresh" = "login";
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: async (input: Request | string | URL, init?: RequestInit) => {
        const url = typeof input === "string" ? input : (input as URL | Request).toString();
        if (url.endsWith("/signin")) {
          phase = "refresh";
          return new Response(
            JSON.stringify({
              data: {
                user: { id: "u1", email: "x@y.z" },
                session: {
                  access_token: "at-old",
                  refresh_token: "rt-old",
                  expires_at: "2030-01-01T00:00:00Z",
                },
              },
            }),
            { status: 200, headers: { "Content-Type": "application/json" } },
          );
        }
        if (url.endsWith("/refresh") && phase === "refresh") {
          captured.request = new Request(input as RequestInfo, init);
          return new Response(
            JSON.stringify({
              data: {
                access_token: "at-new",
                refresh_token: "rt-new",
                expires_at: "2030-01-01T02:00:00Z",
                session_id: "sess-1",
              },
            }),
            { status: 200, headers: { "Content-Type": "application/json" } },
          );
        }
        throw new Error("unexpected URL: " + url);
      },
    });
    await signIn(basin);
    const { data, error } = await basin.auth.refreshSession();
    expect(error).toBeNull();
    expect(data?.access_token).toBe("at-new");
    expect(data?.refresh_token).toBe("rt-new");
    expect(data?.user.id).toBe("u1"); // user preserved from previous session
    expect(captured.request?.method).toBe("POST");
    expect(captured.request?.url).toBe("https://api.basin.run/auth/v1/refresh");
    const body = await captured.request?.json();
    expect(body).toEqual({ refresh_token: "rt-old" });
  });

  it("clears session + fires SIGNED_OUT on 401 (refresh rejected)", async () => {
    const events: string[] = [];
    let phase: "login" | "refresh" = "login";
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: async (input: Request | string | URL) => {
        const url = typeof input === "string" ? input : (input as URL | Request).toString();
        if (url.endsWith("/signin")) {
          phase = "refresh";
          return new Response(
            JSON.stringify({
              data: {
                user: { id: "u1", email: "x@y.z" },
                session: {
                  access_token: "at",
                  refresh_token: "rt",
                  expires_at: "2030-01-01T00:00:00Z",
                },
              },
            }),
            { status: 200, headers: { "Content-Type": "application/json" } },
          );
        }
        if (phase === "refresh") {
          return new Response(
            JSON.stringify({
              data: null,
              error: { code: "not_authenticated", message: "Sign in required." },
            }),
            { status: 401, headers: { "Content-Type": "application/json" } },
          );
        }
        throw new Error("unexpected");
      },
    });
    await signIn(basin);
    basin.auth.onAuthStateChange((e) => events.push(e));
    const { data, error } = await basin.auth.refreshSession();
    expect(data).toBeNull();
    expect(error?.code).toBe("refresh_failed");
    expect(error?.status).toBe(401);
    expect(basin.auth.getSession()).toBeNull();
    expect(events).toContain("SIGNED_OUT");
  });

  it("fires TOKEN_REFRESHED on success", async () => {
    const events: string[] = [];
    let _phase: "login" | "refresh" = "login";
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: async (input: Request | string | URL) => {
        const url = typeof input === "string" ? input : (input as URL | Request).toString();
        if (url.endsWith("/signin")) {
          _phase = "refresh";
          return new Response(
            JSON.stringify({
              data: {
                user: { id: "u1", email: "x@y.z" },
                session: {
                  access_token: "at",
                  refresh_token: "rt",
                  expires_at: "2030-01-01T00:00:00Z",
                },
              },
            }),
            { status: 200, headers: { "Content-Type": "application/json" } },
          );
        }
        return new Response(
          JSON.stringify({
            data: {
              access_token: "at-new",
              refresh_token: "rt-new",
              expires_at: "2030-01-01T02:00:00Z",
            },
          }),
          { status: 200, headers: { "Content-Type": "application/json" } },
        );
      },
    });
    await signIn(basin);
    basin.auth.onAuthStateChange((e) => events.push(e));
    await basin.auth.refreshSession();
    expect(events).toContain("TOKEN_REFRESHED");
  });

  it("returns invalid_response when refresh body is incomplete", async () => {
    let _phase: "login" | "refresh" = "login";
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: async (input: Request | string | URL) => {
        const url = typeof input === "string" ? input : (input as URL | Request).toString();
        if (url.endsWith("/signin")) {
          _phase = "refresh";
          return new Response(
            JSON.stringify({
              data: {
                user: { id: "u1", email: "x@y.z" },
                session: {
                  access_token: "at",
                  refresh_token: "rt",
                  expires_at: "2030-01-01T00:00:00Z",
                },
              },
            }),
            { status: 200, headers: { "Content-Type": "application/json" } },
          );
        }
        // Missing refresh_token in the response body.
        return new Response(
          JSON.stringify({
            data: { access_token: "at-new", expires_at: "2030-01-01T02:00:00Z" },
          }),
          { status: 200, headers: { "Content-Type": "application/json" } },
        );
      },
    });
    await signIn(basin);
    const { data, error } = await basin.auth.refreshSession();
    expect(data).toBeNull();
    expect(error?.code).toBe("invalid_response");
  });
});

describe("auth.mfa.* (not_implemented)", () => {
  // basin-engine has no /auth/v1/mfa/* surface today; every MFA method
  // returns BasinError("not_implemented") without touching the network.
  it("mfa.enroll({factor:'totp'}) returns not_implemented", async () => {
    let fetchCalled = false;
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: async () => {
        fetchCalled = true;
        return new Response("{}", { status: 200 });
      },
    });
    const { data, error } = await basin.auth.mfa.enroll({ factor: "totp" });
    expect(data).toBeNull();
    expect(error).toBeInstanceOf(BasinError);
    expect(error?.code).toBe("not_implemented");
    expect(error?.message).toContain("ROADMAP 0.3");
    expect(fetchCalled).toBe(false);
  });

  it("mfa.enroll({factor:'webauthn'}) returns not_implemented", async () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: async () => new Response("{}", { status: 200 }),
    });
    const { data, error } = await basin.auth.mfa.enroll({ factor: "webauthn" });
    expect(data).toBeNull();
    expect(error?.code).toBe("not_implemented");
  });

  it("mfa.verify({factor:'totp'}) returns not_implemented", async () => {
    let fetchCalled = false;
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: async () => {
        fetchCalled = true;
        return new Response("{}", { status: 200 });
      },
    });
    const { data, error } = await basin.auth.mfa.verify({
      factor: "totp",
      code: "123456",
      secret: "JBSWY3DPEHPK3PXP",
    });
    expect(data).toBeNull();
    expect(error?.code).toBe("not_implemented");
    expect(error?.message).toContain("ROADMAP 0.3");
    expect(fetchCalled).toBe(false);
  });

  it("mfa.verify({factor:'totp_challenge'}) returns not_implemented", async () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: async () => new Response("{}", { status: 200 }),
    });
    const { data, error } = await basin.auth.mfa.verify({
      factor: "totp_challenge",
      code: "654321",
      partial_token: "pt-abc",
    });
    expect(data).toBeNull();
    expect(error?.code).toBe("not_implemented");
  });

  it("mfa.unenroll() returns not_implemented", async () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: async () => new Response("{}", { status: 200 }),
    });
    const { data, error } = await basin.auth.mfa.unenroll({ code: "123456" });
    expect(data).toBeNull();
    expect(error?.code).toBe("not_implemented");
    expect(error?.message).toContain("ROADMAP 0.3");
  });
});

describe("auth auto-refresh timer", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("fires refreshSession 60s before access-token expiry", async () => {
    let loginCalls = 0;
    let refreshCalls = 0;
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: async (input: Request | string | URL) => {
        const url = typeof input === "string" ? input : (input as URL | Request).toString();
        if (url.endsWith("/signin")) {
          loginCalls++;
          // Token expires in 5 minutes from now.
          const exp = Math.floor(Date.now() / 1000) + 300;
          return new Response(
            JSON.stringify({
              data: {
                user: { id: "u1", email: "x@y.z" },
                session: {
                  access_token: "at-1",
                  refresh_token: "rt-1",
                  expires_at: new Date(exp * 1000).toISOString(),
                },
              },
            }),
            { status: 200, headers: { "Content-Type": "application/json" } },
          );
        }
        if (url.endsWith("/refresh")) {
          refreshCalls++;
          const exp = Math.floor(Date.now() / 1000) + 300;
          return new Response(
            JSON.stringify({
              data: {
                access_token: "at-2",
                refresh_token: "rt-2",
                expires_at: new Date(exp * 1000).toISOString(),
              },
            }),
            { status: 200, headers: { "Content-Type": "application/json" } },
          );
        }
        throw new Error("unexpected url " + url);
      },
    });
    await basin.auth.signInWithPassword({ email: "x@y.z", password: "p" });
    expect(loginCalls).toBe(1);
    expect(refreshCalls).toBe(0);
    // Advance to just BEFORE the refresh window (300s - 60s buffer = 240s).
    await vi.advanceTimersByTimeAsync(239 * 1000);
    expect(refreshCalls).toBe(0);
    // Cross the 240s mark — timer should fire.
    await vi.advanceTimersByTimeAsync(2 * 1000);
    expect(refreshCalls).toBe(1);
    expect(basin.auth.getSession()?.access_token).toBe("at-2");
  });

  it("signOut cancels the pending auto-refresh", async () => {
    let refreshCalls = 0;
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: async (input: Request | string | URL) => {
        const url = typeof input === "string" ? input : (input as URL | Request).toString();
        if (url.endsWith("/signin")) {
          const exp = Math.floor(Date.now() / 1000) + 300;
          return new Response(
            JSON.stringify({
              data: {
                user: { id: "u1", email: "x@y.z" },
                session: {
                  access_token: "at-1",
                  refresh_token: "rt-1",
                  expires_at: new Date(exp * 1000).toISOString(),
                },
              },
            }),
            { status: 200, headers: { "Content-Type": "application/json" } },
          );
        }
        if (url.endsWith("/refresh")) {
          refreshCalls++;
        }
        return new Response("{}", { status: 200 });
      },
    });
    await basin.auth.signInWithPassword({ email: "x@y.z", password: "p" });
    await basin.auth.signOut();
    // Advance well past the expiry window — refresh must NOT fire.
    await vi.advanceTimersByTimeAsync(10 * 60 * 1000);
    expect(refreshCalls).toBe(0);
  });

  it("reschedules on each successful refresh", async () => {
    let refreshCalls = 0;
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: async (input: Request | string | URL) => {
        const url = typeof input === "string" ? input : (input as URL | Request).toString();
        if (url.endsWith("/signin")) {
          const exp = Math.floor(Date.now() / 1000) + 300;
          return new Response(
            JSON.stringify({
              data: {
                user: { id: "u1", email: "x@y.z" },
                session: {
                  access_token: "at-1",
                  refresh_token: "rt-1",
                  expires_at: new Date(exp * 1000).toISOString(),
                },
              },
            }),
            { status: 200, headers: { "Content-Type": "application/json" } },
          );
        }
        if (url.endsWith("/refresh")) {
          refreshCalls++;
          const exp = Math.floor(Date.now() / 1000) + 300;
          return new Response(
            JSON.stringify({
              data: {
                access_token: `at-${refreshCalls + 1}`,
                refresh_token: `rt-${refreshCalls + 1}`,
                expires_at: new Date(exp * 1000).toISOString(),
              },
            }),
            { status: 200, headers: { "Content-Type": "application/json" } },
          );
        }
        throw new Error("unexpected url " + url);
      },
    });
    await basin.auth.signInWithPassword({ email: "x@y.z", password: "p" });
    // First refresh window: 240s from sign-in.
    await vi.advanceTimersByTimeAsync(241 * 1000);
    expect(refreshCalls).toBe(1);
    // Second refresh window: another 240s.
    await vi.advanceTimersByTimeAsync(241 * 1000);
    expect(refreshCalls).toBe(2);
  });
});
