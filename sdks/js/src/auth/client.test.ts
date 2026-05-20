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
  it("builds the /auth/v1/authorize URL with provider param and no fetch", async () => {
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
    expect(error).toBeNull();
    expect(data?.provider).toBe("google");
    expect(data?.url).toBe("https://api.basin.run/auth/v1/authorize?provider=google");
    // No network egress — pure URL construction.
    expect(fetchCalled).toBe(false);
  });

  it("includes redirect_to when provided", async () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: async () => new Response("{}", { status: 200 }),
    });
    const { data, error } = await basin.auth.signInWithOAuth({
      provider: "github",
      redirectTo: "https://myapp.com/auth/callback",
    });
    expect(error).toBeNull();
    expect(data?.url).toContain("redirect_to=https%3A%2F%2Fmyapp.com%2Fauth%2Fcallback");
    expect(data?.url).toContain("provider=github");
  });

  it("includes scopes and extra queryParams when provided", async () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: async () => new Response("{}", { status: 200 }),
    });
    const { data } = await basin.auth.signInWithOAuth({
      provider: "github",
      scopes: "repo,read:user",
      queryParams: { login_hint: "pc@example.com" },
    });
    expect(data?.url).toContain("scopes=repo%2Cread%3Auser");
    expect(data?.url).toContain("login_hint=pc%40example.com");
  });

  it("supports all enumerated providers including oidc", async () => {
    const providers = [
      "google", "github", "microsoft", "gitlab", "slack", "discord",
      "apple", "x", "bitbucket", "notion", "spotify", "twitch",
      "linkedin", "figma", "oidc",
    ] as const;
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: async () => new Response("{}", { status: 200 }),
    });
    for (const provider of providers) {
      const { data, error } = await basin.auth.signInWithOAuth({ provider });
      expect(error).toBeNull();
      expect(data?.provider).toBe(provider);
      expect(data?.url).toContain(`provider=${provider}`);
    }
  });

  it("returns invalid_request when provider is missing", async () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: async () => new Response("{}", { status: 200 }),
    });
    // Bypass TS to test runtime guard.
    const { data, error } = await basin.auth.signInWithOAuth(
      { provider: undefined as unknown as "google" },
    );
    expect(data).toBeNull();
    expect(error?.code).toBe("invalid_request");
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

describe("auth.mfa.enroll (TOTP)", () => {
  it("happy path: POSTs /auth/v1/factors and returns totp secret + qr_url", async () => {
    const captured: { request?: Request } = {};
    const basin = createClient("https://api.basin.run", "anon-key", {
      fetch: stubFetch(
        {
          body: {
            data: {
              id: "factor-totp-1",
              secret: "JBSWY3DPEHPK3PXP",
              qr_code: "otpauth://totp/Basin:pc%40example.com?secret=JBSWY3DPEHPK3PXP",
              recovery_codes: ["aaa-bbb", "ccc-ddd"],
            },
            error: null,
          },
        },
        captured,
      ),
    });
    const { data, error } = await basin.auth.mfa.enroll({ factor: "totp" });
    expect(error).toBeNull();
    expect(data?.factor).toBe("totp");
    if (data?.factor !== "totp") throw new Error("expected totp factor");
    expect(data.secret).toBe("JBSWY3DPEHPK3PXP");
    expect(data.qr_url).toContain("otpauth://");
    expect(data.recovery_codes).toHaveLength(2);
    expect(captured.request?.method).toBe("POST");
    expect(captured.request?.url).toBe("https://api.basin.run/auth/v1/factors");
    const body = await captured.request?.json();
    expect(body).toEqual({ factor_type: "totp" });
  });

  it("surfaces engine error on enroll failure", async () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: stubFetch({
        status: 401,
        body: { data: null, error: { code: "unauthorized", message: "Sign in first." } },
      }),
    });
    const { data, error } = await basin.auth.mfa.enroll({ factor: "totp" });
    expect(data).toBeNull();
    expect(error?.code).toBe("unauthorized");
    expect(error?.status).toBe(401);
  });

  it("surfaces network error on fetch reject", async () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: async () => { throw new Error("offline"); },
    });
    const { data, error } = await basin.auth.mfa.enroll({ factor: "totp" });
    expect(data).toBeNull();
    expect(error?.code).toBe("network");
  });
});

describe("auth.mfa.enroll (WebAuthn)", () => {
  it("happy path: POSTs /auth/v1/factors and returns creation challenge", async () => {
    const captured: { request?: Request } = {};
    const creationOptions = { challenge: "abc123", rp: { name: "Basin" } };
    const basin = createClient("https://api.basin.run", "anon-key", {
      fetch: stubFetch(
        {
          body: {
            data: {
              id: "factor-webauthn-1",
              options: creationOptions,
            },
            error: null,
          },
        },
        captured,
      ),
    });
    const { data, error } = await basin.auth.mfa.enroll({ factor: "webauthn" });
    expect(error).toBeNull();
    expect(data?.factor).toBe("webauthn");
    if (data?.factor !== "webauthn") throw new Error("expected webauthn factor");
    expect(data.options).toEqual(creationOptions);
    const body = await captured.request?.json();
    expect(body).toEqual({ factor_type: "webauthn" });
  });

  it("surfaces engine error on WebAuthn enroll failure", async () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: stubFetch({
        status: 403,
        body: { data: null, error: { code: "forbidden", message: "WebAuthn not enabled." } },
      }),
    });
    const { data, error } = await basin.auth.mfa.enroll({ factor: "webauthn" });
    expect(data).toBeNull();
    expect(error?.code).toBe("forbidden");
  });
});

describe("auth.mfa.verify (TOTP enrollment confirm)", () => {
  it("happy path: POSTs /auth/v1/factors/:id/verify and returns enabled:true", async () => {
    const captured: { request?: Request } = {};
    const basin = createClient("https://api.basin.run", "anon-key", {
      fetch: stubFetch(
        {
          body: { data: { verified: true }, error: null },
        },
        captured,
      ),
    });
    const input = Object.assign(
      { factor: "totp" as const, code: "123456", secret: "JBSWY3DPEHPK3PXP" },
      { factorId: "factor-totp-1" },
    );
    const { data, error } = await basin.auth.mfa.verify(input);
    expect(error).toBeNull();
    expect(data?.factor).toBe("totp");
    if (data?.factor !== "totp") throw new Error("expected totp factor");
    expect(data.enabled).toBe(true);
    expect(captured.request?.method).toBe("POST");
    expect(captured.request?.url).toBe(
      "https://api.basin.run/auth/v1/factors/factor-totp-1/verify",
    );
    const body = await captured.request?.json();
    expect(body).toEqual({ code: "123456" });
  });

  it("returns invalid_request when factorId is missing", async () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: async () => new Response("{}", { status: 200 }),
    });
    const { data, error } = await basin.auth.mfa.verify({
      factor: "totp",
      code: "123456",
      secret: "JBSWY3DPEHPK3PXP",
    });
    expect(data).toBeNull();
    expect(error?.code).toBe("invalid_request");
  });

  it("surfaces engine error on verify failure", async () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: stubFetch({
        status: 422,
        body: { data: null, error: { code: "invalid_code", message: "Code incorrect." } },
      }),
    });
    const input = Object.assign(
      { factor: "totp" as const, code: "000000", secret: "S" },
      { factorId: "factor-totp-1" },
    );
    const { data, error } = await basin.auth.mfa.verify(input);
    expect(data).toBeNull();
    expect(error?.code).toBe("invalid_code");
  });
});

describe("auth.mfa.verify (totp_challenge — mid-login)", () => {
  it("happy path: POSTs /factors/:id/challenge/verify with partial_token bearer and adopts aal2 session", async () => {
    const captured: { request?: Request } = {};
    const events: string[] = [];
    const basin = createClient("https://api.basin.run", "anon-key", {
      fetch: stubFetch(
        {
          body: {
            data: {
              user: {
                id: "u-mfa",
                email: "pc@example.com",
                email_verified: true,
                created_at: "2026-01-01T00:00:00Z",
                updated_at: "2026-01-01T00:00:00Z",
              },
              session: {
                access_token: "aal2-at",
                refresh_token: "aal2-rt",
                expires_at: "2030-01-01T00:00:00Z",
                aal: "aal2",
                amr: ["pwd", "totp"],
              },
            },
            error: null,
          },
        },
        captured,
      ),
    });
    basin.auth.onAuthStateChange((e) => events.push(e));
    const input = Object.assign(
      { factor: "totp_challenge" as const, code: "654321", partial_token: "pt-abc" },
      { factorId: "factor-totp-1" },
    );
    const { data, error } = await basin.auth.mfa.verify(input);
    expect(error).toBeNull();
    expect(data?.factor).toBe("totp_challenge");
    if (data?.factor !== "totp_challenge") throw new Error("expected totp_challenge");
    expect(data.session.access_token).toBe("aal2-at");
    expect(data.session.aal).toBe("aal2");
    // Check the request used partial_token as bearer.
    expect(captured.request?.headers.get("authorization")).toBe("Bearer pt-abc");
    expect(captured.request?.url).toBe(
      "https://api.basin.run/auth/v1/factors/factor-totp-1/challenge/verify",
    );
    // MFA_CHALLENGE_VERIFIED fires via adoptSession.
    expect(events).toContain("MFA_CHALLENGE_VERIFIED");
  });

  it("surfaces engine error on challenge verify failure", async () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: stubFetch({
        status: 422,
        body: { data: null, error: { code: "invalid_code", message: "Wrong code." } },
      }),
    });
    const input = Object.assign(
      { factor: "totp_challenge" as const, code: "000000", partial_token: "pt-abc" },
      { factorId: "factor-totp-1" },
    );
    const { data, error } = await basin.auth.mfa.verify(input);
    expect(data).toBeNull();
    expect(error?.code).toBe("invalid_code");
  });
});

describe("auth.mfa.challenge", () => {
  it("happy path: POSTs /auth/v1/factors/:id/challenge and returns challenge object", async () => {
    const captured: { request?: Request } = {};
    const basin = createClient("https://api.basin.run", "anon-key", {
      fetch: stubFetch(
        {
          body: {
            data: {
              id: "challenge-1",
              type: "totp",
              expires_at: 1893456000,
            },
            error: null,
          },
        },
        captured,
      ),
    });
    const { data, error } = await basin.auth.mfa.challenge({ factorId: "factor-totp-1" });
    expect(error).toBeNull();
    expect(data?.id).toBe("challenge-1");
    expect(data?.type).toBe("totp");
    expect(data?.expires_at).toBe(1893456000);
    expect(captured.request?.method).toBe("POST");
    expect(captured.request?.url).toBe(
      "https://api.basin.run/auth/v1/factors/factor-totp-1/challenge",
    );
  });

  it("surfaces engine error on challenge failure", async () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: stubFetch({
        status: 404,
        body: { data: null, error: { code: "not_found", message: "Factor not found." } },
      }),
    });
    const { data, error } = await basin.auth.mfa.challenge({ factorId: "no-such-factor" });
    expect(data).toBeNull();
    expect(error?.code).toBe("not_found");
  });

  it("surfaces network error on fetch reject", async () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: async () => { throw new Error("offline"); },
    });
    const { data, error } = await basin.auth.mfa.challenge({ factorId: "f1" });
    expect(data).toBeNull();
    expect(error?.code).toBe("network");
  });
});

describe("auth.mfa.challengeVerify", () => {
  it("happy path (TOTP): POSTs /factors/:id/challenge/verify and adopts aal2 session", async () => {
    const captured: { request?: Request } = {};
    const events: string[] = [];
    const basin = createClient("https://api.basin.run", "anon-key", {
      fetch: stubFetch(
        {
          body: {
            data: {
              user: {
                id: "u-mfa2",
                email: "mfa@example.com",
                email_verified: true,
                created_at: "2026-01-01T00:00:00Z",
                updated_at: "2026-01-01T00:00:00Z",
              },
              session: {
                access_token: "aal2-at2",
                refresh_token: "aal2-rt2",
                expires_at: "2030-06-01T00:00:00Z",
                aal: "aal2",
                amr: ["pwd", "totp"],
              },
            },
            error: null,
          },
        },
        captured,
      ),
    });
    basin.auth.onAuthStateChange((e) => events.push(e));
    const { data, error } = await basin.auth.mfa.challengeVerify({
      factorId: "factor-totp-1",
      challengeId: "challenge-1",
      code: "654321",
    });
    expect(error).toBeNull();
    expect(data?.session.access_token).toBe("aal2-at2");
    expect(data?.session.aal).toBe("aal2");
    const body = await captured.request?.json();
    expect(body).toEqual({ challenge_id: "challenge-1", code: "654321" });
    expect(captured.request?.url).toBe(
      "https://api.basin.run/auth/v1/factors/factor-totp-1/challenge/verify",
    );
    expect(events).toContain("MFA_CHALLENGE_VERIFIED");
  });

  it("happy path (WebAuthn): sends assertion body", async () => {
    const captured: { request?: Request } = {};
    const basin = createClient("https://api.basin.run", "anon-key", {
      fetch: stubFetch(
        {
          body: {
            data: {
              user: { id: "u-wa", email: "wa@example.com" },
              session: {
                access_token: "wa-at",
                refresh_token: "wa-rt",
                expires_at: "2030-01-01T00:00:00Z",
                aal: "aal2",
              },
            },
            error: null,
          },
        },
        captured,
      ),
    });
    const { data, error } = await basin.auth.mfa.challengeVerify({
      factorId: "factor-wa-1",
      challengeId: "challenge-wa-1",
      assertion: { type: "public-key", response: {} },
    });
    expect(error).toBeNull();
    expect(data?.session.aal).toBe("aal2");
    const body = await captured.request?.json();
    expect(body.challenge_id).toBe("challenge-wa-1");
    expect(body.assertion).toBeDefined();
  });

  it("surfaces engine error on challenge verify failure", async () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: stubFetch({
        status: 422,
        body: { data: null, error: { code: "invalid_code", message: "Code expired." } },
      }),
    });
    const { data, error } = await basin.auth.mfa.challengeVerify({
      factorId: "f1",
      challengeId: "c1",
      code: "000000",
    });
    expect(data).toBeNull();
    expect(error?.code).toBe("invalid_code");
  });
});

describe("auth.mfa.unenroll", () => {
  it("happy path: DELETEs /auth/v1/factors/:id and returns {disabled:true}", async () => {
    const captured: { request?: Request } = {};
    const basin = createClient("https://api.basin.run", "anon-key", {
      fetch: async (input: Request | string | URL, init?: RequestInit): Promise<Response> => {
        captured.request = new Request(input as RequestInfo, init);
        return new Response(
          JSON.stringify({ data: { deleted: true }, error: null }),
          { status: 200, headers: { "Content-Type": "application/json" } },
        );
      },
    });
    const { data, error } = await basin.auth.mfa.unenroll({ factorId: "factor-totp-1" });
    expect(error).toBeNull();
    expect(data?.disabled).toBe(true);
    expect(captured.request?.method).toBe("DELETE");
    expect(captured.request?.url).toBe(
      "https://api.basin.run/auth/v1/factors/factor-totp-1",
    );
  });

  it("surfaces engine error on unenroll failure (e.g. aal1 session)", async () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: stubFetch({
        status: 403,
        body: { data: null, error: { code: "forbidden", message: "aal2 required to unenroll." } },
      }),
    });
    const { data, error } = await basin.auth.mfa.unenroll({ factorId: "factor-totp-1" });
    expect(data).toBeNull();
    expect(error?.code).toBe("forbidden");
    expect(error?.status).toBe(403);
  });

  it("surfaces network error on fetch reject", async () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: async () => { throw new Error("offline"); },
    });
    const { data, error } = await basin.auth.mfa.unenroll({ factorId: "f1" });
    expect(data).toBeNull();
    expect(error?.code).toBe("network");
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
