/**
 * Integration: auth — live basin engine.
 *
 * These tests fire HTTP at the basin engine directly — no basin-cloud
 * proxy is in the request path. basin-auth (the Rust OSS auth service
 * exposing `/auth/v1/*`) is served by the engine itself, and as of
 * 2026-05-11 its catalog lives on the engine over loopback pgwire, so a
 * single `BASIN_TEST_ENGINE_URL` is the only host the suite needs to
 * reach (no separate auth URL, no external Postgres).
 *
 * Skip-gated. See `tests/integration/README.md` for the required env vars
 * and project state. With the vars unset every `it` reports as skipped;
 * no network traffic, no fork-PR breakage.
 */

import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createClient, type BasinClient } from "../../src/index.js";

const PROJECT_REF = process.env.BASIN_TEST_PROJECT_REF;
const ENGINE_URL = process.env.BASIN_TEST_ENGINE_URL;
const ANON_KEY = process.env.BASIN_TEST_ANON_KEY;
const USER_EMAIL = process.env.BASIN_TEST_USER_EMAIL;
const USER_PASSWORD = process.env.BASIN_TEST_USER_PASSWORD;

const gated = !(PROJECT_REF && ENGINE_URL && ANON_KEY);

describe.skipIf(gated)("integration · auth (live basin-engine)", () => {
  let basin: BasinClient;

  beforeAll(() => {
    basin = createClient(ENGINE_URL!, ANON_KEY!);
  });

  afterAll(async () => {
    // Defensive: signOut clears local state regardless of network reply.
    await basin.auth.signOut();
  });

  it("getSession is null on a fresh client", () => {
    const { data, error } = basin.auth.getSession();
    expect(error).toBeNull();
    expect(data.session).toBeNull();
  });

  it.skipIf(!USER_EMAIL || !USER_PASSWORD)(
    "signInWithPassword roundtrips against /auth/v1/signin",
    async () => {
      const { data, error } = await basin.auth.signInWithPassword({
        email: USER_EMAIL!,
        password: USER_PASSWORD!,
      });

      expect(error).toBeNull();
      expect(data.session).not.toBeNull();
      expect(data.session?.access_token).toMatch(/^ey/);
      expect(data.user?.email).toBe(USER_EMAIL);

      // Session is now visible synchronously.
      const peek = basin.auth.getSession();
      expect(peek.data.session?.access_token).toBe(data.session?.access_token);
    },
  );

  it.skipIf(!USER_EMAIL || !USER_PASSWORD)(
    "signOut clears local state",
    async () => {
      await basin.auth.signInWithPassword({
        email: USER_EMAIL!,
        password: USER_PASSWORD!,
      });
      const { error } = await basin.auth.signOut();
      expect(error).toBeNull();
      expect(basin.auth.getSession().data.session).toBeNull();
    },
  );

  it("signInWithPassword surfaces invalid_credentials on bad password", async () => {
    const { data, error } = await basin.auth.signInWithPassword({
      email: "definitely-not-a-real-user@example.test",
      password: "definitely-not-the-password",
    });
    expect(data.session).toBeNull();
    expect(error).not.toBeNull();
    // Engine emits the typed code; SDK passes it through.
    expect(error?.code).toMatch(/invalid_credentials|unauthorized|invalid_request/);
  });

  it("signInWithOAuth returns not_implemented (engine has no /auth/v1/oauth/* yet)", async () => {
    const { data, error } = await basin.auth.signInWithOAuth({
      provider: "google",
    });
    expect(data).toBeNull();
    expect(error?.code).toBe("not_implemented");
  });

  it("mfa.enroll returns not_implemented (engine has no /auth/v1/mfa/* yet)", async () => {
    const { data, error } = await basin.auth.mfa.enroll({ factor: "totp" });
    expect(data).toBeNull();
    expect(error?.code).toBe("not_implemented");
  });
});
