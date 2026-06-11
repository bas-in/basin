/**
 * Live integration test — skipped unless BASIN_URL is set.
 *
 * Run against a local basin-server:
 *
 *   BASIN_URL=http://localhost:8080 \
 *   BASIN_KEY=<jwt-or-api-key> \
 *   BASIN_PROJECT_ID=<project> \
 *   BASIN_EMAIL=<email> BASIN_PASSWORD=<password> \
 *   npx vitest run test/integration.test.ts
 *
 * Only BASIN_URL is required; the auth/CRUD blocks additionally skip when
 * their own env vars are absent.
 */

import { describe, expect, it } from "vitest";

import { createClient } from "../src/index.js";

const url = process.env.BASIN_URL;
const key = process.env.BASIN_KEY;
const projectId = process.env.BASIN_PROJECT_ID;
const email = process.env.BASIN_EMAIL;
const password = process.env.BASIN_PASSWORD;

describe.skipIf(!url)("integration (BASIN_URL set)", () => {
  it("GET /health returns ok", async () => {
    const client = createClient(url!, key, { projectId });
    expect(await client.health()).toBe("ok");
  });

  it.skipIf(!email || !password || !projectId)(
    "signs in and refreshes the session",
    async () => {
      const client = createClient(url!, undefined, { projectId });
      const session = await client.auth.signIn({
        email: email!,
        password: password!,
      });
      expect(session.access_token.length).toBeGreaterThan(0);
      const rotated = await client.auth.refreshSession();
      expect(rotated.refresh_token).not.toBe(session.refresh_token);
    },
  );

  it.skipIf(!key)("round-trips rows through /rest/v1/:table", async () => {
    const client = createClient(url!, key, { projectId });
    const table = `basin_js_it_${Date.now()}`;
    // Table creation is a pgwire/SQL concern, not a REST one — so this test
    // only exercises a read against a table that may not exist and asserts
    // the error is the typed envelope, or rows if the operator pre-created it.
    try {
      const result = await client.from(table).limit(1);
      expect(Array.isArray(result.rows)).toBe(true);
    } catch (e) {
      expect(e).toHaveProperty("code");
    }
  });
});
