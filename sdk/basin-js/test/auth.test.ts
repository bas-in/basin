import { describe, expect, it } from "vitest";

import { BasinApiError, createClient } from "../src/index.js";
import { FetchMock } from "./helpers.js";

const URL_BASE = "http://basin.test";
const PROJECT = "01JTESTPROJECT0000000000";

function tokens(overrides: Partial<Record<string, string>> = {}) {
  return {
    access_token: "acc-1",
    refresh_token: "ref-1",
    access_expires_at: new Date(Date.now() + 3600_000).toISOString(),
    refresh_expires_at: new Date(Date.now() + 86400_000).toISOString(),
    ...overrides,
  };
}

describe("auth", () => {
  it("signUp posts project_id/email/password to /auth/v1/signup", async () => {
    const fetch = new FetchMock().queueJson({ ok: true, user_id: "u1" }, 201);
    const client = createClient(URL_BASE, undefined, {
      projectId: PROJECT,
      fetch: fetch.impl,
    });
    const res = await client.auth.signUp({ email: "a@b.c", password: "pw" });
    expect(res.user_id).toBe("u1");
    expect(fetch.last.url).toBe(`${URL_BASE}/auth/v1/signup`);
    expect(fetch.last.method).toBe("POST");
    expect(JSON.parse(fetch.last.body!)).toEqual({
      project_id: PROJECT,
      email: "a@b.c",
      password: "pw",
    });
    // signup is unauthenticated.
    expect(fetch.last.headers["authorization"]).toBeUndefined();
  });

  it("signIn stores the session and later requests use its access token", async () => {
    const fetch = new FetchMock()
      .queueJson(tokens())
      .queueJson([{ id: 1 }]);
    const client = createClient(URL_BASE, "static-key", {
      projectId: PROJECT,
      fetch: fetch.impl,
    });
    const session = await client.auth.signIn({ email: "a@b.c", password: "pw" });
    expect(session.access_token).toBe("acc-1");
    expect(client.auth.getSession()).toEqual(session);

    await client.from("users").rows();
    expect(fetch.last.headers["authorization"]).toBe("Bearer acc-1");
  });

  it("refreshSession posts the refresh token and rotates the session", async () => {
    const fetch = new FetchMock()
      .queueJson(tokens())
      .queueJson(tokens({ access_token: "acc-2", refresh_token: "ref-2" }));
    const client = createClient(URL_BASE, undefined, {
      projectId: PROJECT,
      fetch: fetch.impl,
    });
    await client.auth.signIn({ email: "a@b.c", password: "pw" });
    const rotated = await client.auth.refreshSession();
    expect(fetch.last.url).toBe(`${URL_BASE}/auth/v1/refresh`);
    expect(JSON.parse(fetch.last.body!)).toEqual({ refresh_token: "ref-1" });
    expect(rotated.access_token).toBe("acc-2");
    expect(client.auth.getSession()?.refresh_token).toBe("ref-2");
  });

  it("auto-refreshes an expired access token before an authed request", async () => {
    const fetch = new FetchMock()
      .queueJson(
        tokens({ access_expires_at: new Date(Date.now() - 1000).toISOString() }),
      )
      .queueJson(tokens({ access_token: "acc-fresh" }))
      .queueJson([]);
    const client = createClient(URL_BASE, undefined, {
      projectId: PROJECT,
      fetch: fetch.impl,
    });
    await client.auth.signIn({ email: "a@b.c", password: "pw" });
    await client.from("users").rows();

    expect(fetch.calls[1]!.url).toBe(`${URL_BASE}/auth/v1/refresh`);
    expect(fetch.calls[2]!.headers["authorization"]).toBe("Bearer acc-fresh");
  });

  it("surfaces E_REVOKED_TOKEN from refresh as a typed error", async () => {
    const fetch = new FetchMock()
      .queueJson(tokens())
      .queueJson({ code: "E_REVOKED_TOKEN", message: "refresh token revoked" }, 401);
    const client = createClient(URL_BASE, undefined, {
      projectId: PROJECT,
      fetch: fetch.impl,
    });
    await client.auth.signIn({ email: "a@b.c", password: "pw" });
    const err = await client.auth.refreshSession().catch((e) => e);
    expect(err).toBeInstanceOf(BasinApiError);
    expect((err as BasinApiError).code).toBe("E_REVOKED_TOKEN");
    expect((err as BasinApiError).status).toBe(401);
  });

  it("signOut calls POST /auth/v1/signout with the refresh token, then clears session", async () => {
    const fetch = new FetchMock()
      .queueJson(tokens())
      .queueJson({ ok: true })  // signout response
      .queueJson([]);            // subsequent GET
    const client = createClient(URL_BASE, "static-key", {
      projectId: PROJECT,
      fetch: fetch.impl,
    });
    await client.auth.signIn({ email: "a@b.c", password: "pw" });
    await client.auth.signOut();
    expect(client.auth.getSession()).toBeNull();

    // The sign-out call must target the new route and carry the refresh token.
    expect(fetch.calls[1]!.url).toBe(`${URL_BASE}/auth/v1/signout`);
    expect(fetch.calls[1]!.method).toBe("POST");
    expect(JSON.parse(fetch.calls[1]!.body!)).toEqual({ refresh_token: "ref-1" });
    // Sign-out must NOT carry an Authorization header (auth: false).
    expect(fetch.calls[1]!.headers["authorization"]).toBeUndefined();

    // After sign-out, further requests fall back to the static key.
    await client.from("users").rows();
    expect(fetch.last.headers["authorization"]).toBe("Bearer static-key");
  });

  it("signOut is a no-op when there is no active session", async () => {
    const fetch = new FetchMock();
    const client = createClient(URL_BASE, undefined, {
      projectId: PROJECT,
      fetch: fetch.impl,
    });
    // No signIn call — session is null.
    await client.auth.signOut();
    expect(client.auth.getSession()).toBeNull();
    // No HTTP calls should have been made.
    expect(fetch.calls).toHaveLength(0);
  });

  it("magic link request hits /auth/v1/magic-link and consume stores tokens", async () => {
    const fetch = new FetchMock()
      .queue(new Response(null, { status: 204 }))
      .queueJson(tokens({ access_token: "acc-magic" }));
    const client = createClient(URL_BASE, undefined, { fetch: fetch.impl });
    await client.auth.requestMagicLink("a@b.c");
    expect(fetch.last.url).toBe(`${URL_BASE}/auth/v1/magic-link`);
    expect(JSON.parse(fetch.last.body!)).toEqual({ email: "a@b.c" });

    const session = await client.auth.consumeMagicLink("tok123");
    expect(fetch.last.url).toBe(`${URL_BASE}/auth/v1/magic-link/consume`);
    expect(JSON.parse(fetch.last.body!)).toEqual({ token: "tok123" });
    expect(session.access_token).toBe("acc-magic");
  });

  it("api key lifecycle binds /auth/v1/api-keys routes", async () => {
    const fetch = new FetchMock()
      .queueJson({ id: 7, name: "ci", secret: "s3cr3t", created_at: "now" }, 201)
      .queueJson([{ id: 7, name: "ci", created_at: "now", last_used_at: null, revoked_at: null }])
      .queueJson({ ok: true });
    const client = createClient(URL_BASE, "jwt-token", { fetch: fetch.impl });

    const issued = await client.auth.createApiKey("ci");
    expect(issued.secret).toBe("s3cr3t");
    expect(fetch.calls[0]!.url).toBe(`${URL_BASE}/auth/v1/api-keys`);
    expect(fetch.calls[0]!.method).toBe("POST");

    const listed = await client.auth.listApiKeys();
    expect(listed).toHaveLength(1);
    expect(fetch.calls[1]!.method).toBe("GET");

    await client.auth.deleteApiKey(7);
    expect(fetch.calls[2]!.url).toBe(`${URL_BASE}/auth/v1/api-keys/7`);
    expect(fetch.calls[2]!.method).toBe("DELETE");
  });
});
