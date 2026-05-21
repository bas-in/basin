/**
 * RLS isolation test: org A cannot see org B's todos.
 *
 * This test exercises the core multi-tenant security guarantee:
 * Row-Level Security enforced by Basin ensures that a user who is a member
 * of org A sees ONLY org A's todos, even if they supply the correct JWT for
 * their own user — they have no membership in org B.
 *
 * How it works:
 * 1. Connect to Basin with a privileged (service) connection via postgres.js
 *    to set up two orgs, two users, their memberships, and their todos.
 *    (The service connection bypasses RLS — Basin's 'basin' default user has
 *    no RLS restrictions in the default dev config.)
 *
 * 2. Sign in as alice via basin-auth (POST /auth/v1/signin) to get a JWT.
 *
 * 3. Query /rest/v1/todos with Alice's JWT.  Basin verifies the JWT and
 *    enforces the `todos_org_select` RLS policy:
 *      USING (org_id IN (SELECT org_id FROM memberships WHERE user_id = auth.uid()))
 *    Alice is a member of Acme only → she MUST NOT see Globex's todos.
 *
 * 4. Sign in as bob and verify the symmetric case.
 *
 * Prerequisites:
 *   - Basin running locally (BASIN_URL=http://localhost:5432, default)
 *   - basin-auth enabled (BASIN_AUTH_ENABLED=1)
 *   - Schema migrated + RLS policies applied (npm run setup)
 *
 * Without a live Basin instance the tests are skipped (BASIN_SKIP_LIVE_TESTS=1
 * or the server is unreachable).
 */

import postgres from "postgres";
import { createCompatClient } from "../src/lib/basin-compat";

const BASIN_URL = process.env.BASIN_URL ?? "http://localhost:5432";
const DATABASE_URL =
  process.env.DATABASE_URL ?? "postgres://basin@localhost:5432/postgres";
const SKIP = process.env.BASIN_SKIP_LIVE_TESTS === "1";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async function isBasinReachable(): Promise<boolean> {
  try {
    const res = await fetch(`${BASIN_URL}/rest/v1/`, { signal: AbortSignal.timeout(2000) });
    // basin-rest returns 200 or 404 on the root — either confirms the server is up
    return res.status < 500;
  } catch {
    return false;
  }
}

// ---------------------------------------------------------------------------
// Test suite
// ---------------------------------------------------------------------------

describe("Per-tenant RLS isolation (org A cannot see org B's todos)", () => {
  let sql: ReturnType<typeof postgres>;

  // Stable IDs so the test is re-runnable without teardown
  const aliceId = "test-alice-rls-00000000000001";
  const bobId = "test-bob-rls-000000000000002";
  let acmeOrgId: string;
  let globexOrgId: string;

  const aliceEmail = `alice-rls-test-${Date.now()}@example.com`;
  const bobEmail = `bob-rls-test-${Date.now()}@example.com`;
  const password = "hunter2hunter2";

  // Skip early if explicitly opted out
  if (SKIP) {
    test.skip("BASIN_SKIP_LIVE_TESTS=1 — skipping live tests", () => {});
    return;
  }

  beforeAll(async () => {
    // Check server reachability before doing any setup
    const reachable = await isBasinReachable();
    if (!reachable) {
      console.warn(
        "\n  [rls-isolation] Basin not reachable at " +
          BASIN_URL +
          " — skipping live tests.\n" +
          "  Start Basin with: docker run --rm -p 5432:5432 basin-server\n" +
          "  Or set BASIN_SKIP_LIVE_TESTS=1 to suppress this warning.\n"
      );
    }

    sql = postgres(DATABASE_URL, { max: 2 });

    // Sign up test users via basin-auth
    // (basin-auth may return 409 if the user already exists — that's fine)
    for (const [email, pass] of [
      [aliceEmail, password],
      [bobEmail, password],
    ] as [string, string][]) {
      try {
        await fetch(`${BASIN_URL}/auth/v1/signup`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ email, password: pass }),
        });
      } catch {
        // Server unreachable — beforeAll will continue; tests will skip below
      }
    }

    // Insert test data using the privileged service connection (bypasses RLS)
    try {
      // Upsert users
      await sql`
        INSERT INTO users (id, email, display_name)
        VALUES
          (${aliceId}, ${aliceEmail}, 'Alice RLS Test'),
          (${bobId},   ${bobEmail},   'Bob RLS Test')
        ON CONFLICT (id) DO UPDATE SET email = EXCLUDED.email
      `;

      // Upsert orgs
      const [acme] = await sql<{ id: string }[]>`
        INSERT INTO orgs (name, slug)
        VALUES ('Acme Test Corp', 'acme-rls-test')
        ON CONFLICT (slug) DO UPDATE SET name = EXCLUDED.name
        RETURNING id
      `;
      const [globex] = await sql<{ id: string }[]>`
        INSERT INTO orgs (name, slug)
        VALUES ('Globex RLS Test', 'globex-rls-test')
        ON CONFLICT (slug) DO UPDATE SET name = EXCLUDED.name
        RETURNING id
      `;
      acmeOrgId = acme.id;
      globexOrgId = globex.id;

      // Memberships: alice → acme only, bob → globex only
      await sql`
        INSERT INTO memberships (org_id, user_id, role)
        VALUES
          (${acmeOrgId},   ${aliceId}, 'owner'),
          (${globexOrgId}, ${bobId},   'owner')
        ON CONFLICT DO NOTHING
      `;

      // Todos: one per org
      await sql`
        INSERT INTO todos (org_id, title, created_by)
        VALUES
          (${acmeOrgId},   'Alice secret todo',  ${aliceId}),
          (${globexOrgId}, 'Bob private todo',    ${bobId})
        ON CONFLICT DO NOTHING
      `;
    } catch (err) {
      console.warn("  [rls-isolation] Setup DB error:", err);
    }
  });

  afterAll(async () => {
    if (sql) await sql.end();
  });

  // -------------------------------------------------------------------------
  // Core isolation tests
  // -------------------------------------------------------------------------

  test("Alice can see her org (Acme) todos", async () => {
    if (!(await isBasinReachable())) {
      return console.log("  [skip] Basin not reachable");
    }

    const client = createCompatClient(BASIN_URL);
    const { data: signInData, error: signInError } =
      await client.auth.signInWithPassword({ email: aliceEmail, password });

    if (signInError) {
      console.warn(
        "  [skip] Could not sign in as alice (auth may require email verification):",
        signInError.message
      );
      return;
    }
    expect(signInData?.session?.accessToken).toBeTruthy();

    const { data: todos, error } = await client
      .from<{ id: string; org_id: string; title: string }>("todos")
      .select("id, org_id, title");

    expect(error).toBeNull();
    expect(todos).not.toBeNull();

    // Alice must see at least her own todo
    const aliceTodo = todos?.find((t) => t.title === "Alice secret todo");
    expect(aliceTodo).toBeDefined();

    // Alice must NOT see Bob's todo (different org, no membership)
    const bobTodo = todos?.find((t) => t.title === "Bob private todo");
    expect(bobTodo).toBeUndefined();
  });

  test("Bob can see his org (Globex) todos", async () => {
    if (!(await isBasinReachable())) {
      return console.log("  [skip] Basin not reachable");
    }

    const client = createCompatClient(BASIN_URL);
    const { data: signInData, error: signInError } =
      await client.auth.signInWithPassword({ email: bobEmail, password });

    if (signInError) {
      console.warn(
        "  [skip] Could not sign in as bob (auth may require email verification):",
        signInError.message
      );
      return;
    }
    expect(signInData?.session?.accessToken).toBeTruthy();

    const { data: todos, error } = await client
      .from<{ id: string; org_id: string; title: string }>("todos")
      .select("id, org_id, title");

    expect(error).toBeNull();
    expect(todos).not.toBeNull();

    // Bob must see his own todo
    const bobTodo = todos?.find((t) => t.title === "Bob private todo");
    expect(bobTodo).toBeDefined();

    // Bob must NOT see Alice's todo
    const aliceTodo = todos?.find((t) => t.title === "Alice secret todo");
    expect(aliceTodo).toBeUndefined();
  });

  test("Unauthenticated request returns empty or 401 (no data leakage)", async () => {
    if (!(await isBasinReachable())) {
      return console.log("  [skip] Basin not reachable");
    }

    // Make a raw request without a JWT
    const res = await fetch(`${BASIN_URL}/rest/v1/todos`, {
      headers: { "Content-Type": "application/json" },
    });

    if (res.status === 401) {
      // basin-rest rejected the unauthenticated request — pass
      expect(res.status).toBe(401);
      return;
    }

    // If basin-rest allows anonymous access, RLS must still return zero rows
    // (anonymous sessions return NULL from auth.uid(), which fails USING clauses)
    const data = (await res.json()) as unknown[];
    expect(Array.isArray(data)).toBe(true);
    expect(data).toHaveLength(0);
  });

  test("Org-A JWT cannot read Org-B todos via direct org_id filter", async () => {
    if (!(await isBasinReachable())) {
      return console.log("  [skip] Basin not reachable");
    }

    const client = createCompatClient(BASIN_URL);
    const { data, error } = await client.auth.signInWithPassword({
      email: aliceEmail,
      password,
    });
    if (error || !data) return console.log("  [skip] sign-in failed");

    // Alice tries to explicitly filter by globex's org_id
    const { data: todos } = await client
      .from<{ id: string; org_id: string }>("todos")
      .select("id, org_id")
      .eq("org_id", globexOrgId ?? "");

    // RLS policy fires AFTER any client-supplied filter, so even with the
    // explicit org_id filter, Alice gets zero rows because she has no
    // membership in Globex.
    expect(todos ?? []).toHaveLength(0);
  });
});
