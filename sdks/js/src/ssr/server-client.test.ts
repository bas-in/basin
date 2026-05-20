import { describe, expect, it } from "vitest";
import { createServerClient } from "./server-client.js";
import type { CookieAdapter, CookieSetOptions } from "./server-client.js";

const SESSION_COOKIE = "basin.auth.session";

function mockCookieStore(): CookieAdapter & { raw: Map<string, string> } {
  const raw = new Map<string, string>();
  return {
    raw,
    get(name: string) {
      return raw.get(name) ?? null;
    },
    set(name: string, value: string, _opts?: CookieSetOptions) {
      raw.set(name, value);
    },
    remove(name: string, _opts?: CookieSetOptions) {
      raw.delete(name);
    },
  };
}

const SESSION_PAYLOAD = {
  access_token: "at_test",
  refresh_token: "rt_test",
  token_type: "bearer" as const,
  expires_at: Math.floor(Date.now() / 1000) + 3600,
  user: {
    id: "user-1",
    email: "user@example.com",
    email_confirmed_at: null,
    phone: null,
    created_at: "2026-01-01T00:00:00Z",
    updated_at: "2026-01-01T00:00:00Z",
    app_metadata: {},
    user_metadata: {},
  },
};

function stubFetch(status: number, body: unknown): typeof fetch {
  return async () =>
    new Response(JSON.stringify(body), {
      status,
      headers: { "Content-Type": "application/json" },
    });
}

describe("createServerClient — getSession", () => {
  it("returns null when no session cookie is set", () => {
    const cookies = mockCookieStore();
    const client = createServerClient("https://db.example.com", "anon", {
      cookies,
      fetch: stubFetch(200, {}),
    });
    expect(client.auth.getSession()).toBeNull();
  });

  it("returns the parsed session from the cookie", () => {
    const cookies = mockCookieStore();
    cookies.set(SESSION_COOKIE, JSON.stringify(SESSION_PAYLOAD));
    const client = createServerClient("https://db.example.com", "anon", {
      cookies,
      fetch: stubFetch(200, {}),
    });
    const session = client.auth.getSession();
    expect(session?.access_token).toBe("at_test");
    expect(session?.user.id).toBe("user-1");
  });

  it("returns null for a malformed cookie value", () => {
    const cookies = mockCookieStore();
    cookies.set(SESSION_COOKIE, "not-valid-json{{{");
    const client = createServerClient("https://db.example.com", "anon", {
      cookies,
      fetch: stubFetch(200, {}),
    });
    expect(client.auth.getSession()).toBeNull();
  });
});

describe("createServerClient — signInWithPassword sets cookie", () => {
  it("writes the session cookie on successful sign-in", async () => {
    const cookies = mockCookieStore();
    const engineBody = {
      data: {
        user: {
          id: "user-2",
          email: "x@example.com",
          email_verified: true,
          created_at: "2026-01-01T00:00:00Z",
          updated_at: "2026-01-01T00:00:00Z",
        },
        session: {
          access_token: "new_at",
          refresh_token: "new_rt",
          expires_at: new Date(Date.now() + 3600_000).toISOString(),
        },
      },
      error: null,
    };
    const client = createServerClient("https://db.example.com", "anon", {
      cookies,
      fetch: stubFetch(200, engineBody),
    });
    const { data, error } = await client.auth.signInWithPassword({
      email: "x@example.com",
      password: "secret",
    });
    expect(error).toBeNull();
    expect(data?.access_token).toBe("new_at");
    expect(cookies.raw.has(SESSION_COOKIE)).toBe(true);
    const stored = JSON.parse(cookies.raw.get(SESSION_COOKIE)!);
    expect(stored.access_token).toBe("new_at");
  });

  it("does not write a cookie on failed sign-in", async () => {
    const cookies = mockCookieStore();
    const engineBody = {
      data: null,
      error: { code: "invalid_credentials", message: "bad creds" },
    };
    const client = createServerClient("https://db.example.com", "anon", {
      cookies,
      fetch: stubFetch(401, engineBody),
    });
    const { data, error } = await client.auth.signInWithPassword({
      email: "x@example.com",
      password: "wrong",
    });
    expect(data).toBeNull();
    expect(error).not.toBeNull();
    expect(cookies.raw.has(SESSION_COOKIE)).toBe(false);
  });
});

describe("createServerClient — signOut clears cookie", () => {
  it("removes the session cookie on signOut", async () => {
    const cookies = mockCookieStore();
    cookies.set(SESSION_COOKIE, JSON.stringify(SESSION_PAYLOAD));
    const client = createServerClient("https://db.example.com", "anon", {
      cookies,
      fetch: stubFetch(200, {}),
    });
    await client.auth.signOut();
    expect(cookies.raw.has(SESSION_COOKIE)).toBe(false);
  });
});

describe("createServerClient — getUser", () => {
  it("returns null when no session cookie", () => {
    const cookies = mockCookieStore();
    const client = createServerClient("https://db.example.com", "anon", {
      cookies,
      fetch: stubFetch(200, {}),
    });
    expect(client.auth.getUser()).toBeNull();
  });

  it("returns user from session cookie", () => {
    const cookies = mockCookieStore();
    cookies.set(SESSION_COOKIE, JSON.stringify(SESSION_PAYLOAD));
    const client = createServerClient("https://db.example.com", "anon", {
      cookies,
      fetch: stubFetch(200, {}),
    });
    expect(client.auth.getUser()?.id).toBe("user-1");
  });
});

describe("createServerClient — refreshSession", () => {
  it("returns no_session error when cookie is absent", async () => {
    const cookies = mockCookieStore();
    const client = createServerClient("https://db.example.com", "anon", {
      cookies,
      fetch: stubFetch(200, {}),
    });
    const { data, error } = await client.auth.refreshSession();
    expect(data).toBeNull();
    expect(error?.code).toBe("no_session");
  });
});
