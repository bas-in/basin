/**
 * RLS isolation test: org A cannot see org B's todos.
 *
 * This test exercises the core multi-tenant security guarantee:
 * Row-Level Security enforced by Basin ensures that a user who is a member
 * of org A sees ONLY org A's todos, even if they supply the correct JWT for
 * their own user — they have no membership in org B.
 *
 * How it works:
 * 1. Connect to Basin over pgwire via postgres.js to set up two orgs, two
 *    users, their memberships, and their todos.
 *
 *    NOTE — Basin has NO privileged RLS bypass, contrary to what this comment
 *    used to claim. A pgwire session with no JWT is *anonymous*: `auth.uid()`
 *    is NULL, so a `USING (... = auth.uid())` policy matches nothing and the
 *    session reads zero rows. See `rls_with_auth_uid_filters_per_user` in
 *    tests/integration/tests/auth_rls_uid.rs. Consequently this seeding must
 *    happen BEFORE the SELECT policies are in force, or from a session whose
 *    uid the policies admit — `npm run setup` ordering matters, and if seeding
 *    starts failing that is why. The `beforeAll` below now throws on a setup
 *    error instead of warning, so this cannot silently produce an empty
 *    fixture that makes the isolation assertions trivially true.
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
 * ── WHY THIS FILE IS SHAPED THE WAY IT IS ─────────────────────────────────
 * An earlier revision began every test with
 *
 *     if (!(await isBasinReachable())) return console.log("[skip] ...");
 *
 * which is a PASS, not a skip. In CI — where nothing starts Basin — the suite
 * reported `Tests: 4 passed, 4 total`, including the line "Org-A JWT cannot
 * read Org-B todos", while making zero requests. A green tick asserting
 * cross-tenant isolation that had never once been exercised is worse than no
 * test: it retires the question.
 *
 * So: there is exactly ONE way to not run these, and it is explicit.
 * `BASIN_SKIP_LIVE_TESTS=1` marks the suite skipped, and jest then *reports*
 * it as skipped rather than passed. Any other reason for not reaching Basin —
 * server down, auth off, schema not migrated — is a FAILURE with a message
 * naming the missing prerequisite.
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

/**
 * Fail — do not skip — when Basin is not reachable.
 *
 * Reaching here means the caller did NOT set BASIN_SKIP_LIVE_TESTS=1 (the guard
 * at the top of the suite would have short-circuited), so it believes a live
 * Basin is available. It isn't, and the only honest outcome is red.
 */
async function requireBasin(): Promise<void> {
  if (await isBasinReachable()) return;
  throw new Error(
    `Basin is not reachable at ${BASIN_URL}, and BASIN_SKIP_LIVE_TESTS is not set.\n` +
      `These tests assert per-tenant RLS isolation and cannot do that against nothing.\n` +
      `  Start Basin:      docker run --rm -p 5432:5432 basin-server\n` +
      `  Migrate + policies: npm run setup\n` +
      `  Or, to declare out loud that isolation is NOT being verified in this run:\n` +
      `                    BASIN_SKIP_LIVE_TESTS=1 npm test\n` +
      `(That marks the suite skipped. It never reports as passed.)`
  );
}

/** Assert a basin-rest result set is a real array, not a null from a failed call. */
function requireRows<T>(rows: T[] | null | undefined, what: string): T[] {
  if (rows == null) {
    throw new Error(
      `${what}: basin-rest returned no row set (null/undefined), which means the ` +
        `request failed rather than returning zero rows. An empty-result assertion ` +
        `would have passed here while proving nothing about RLS.`
    );
  }
  if (!Array.isArray(rows)) {
    throw new Error(`${what}: expected an array of rows, got ${typeof rows}`);
  }
  return rows;
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

  // The ONLY sanctioned way to not run these. `test.skip` makes jest report
  // the suite as skipped; it can never be mistaken for four passing isolation
  // assertions the way an early `return` inside each test was.
  if (SKIP) {
    test.skip(
      "BASIN_SKIP_LIVE_TESTS=1 — per-tenant RLS isolation NOT verified in this run",
      () => {}
    );
    return;
  }

  beforeAll(async () => {
    // Reachability is a prerequisite, not a condition. Throwing here fails the
    // whole suite with one clear message instead of letting each test decide to
    // pass quietly.
    await requireBasin();

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
        // Signup failing is not fatal on its own (409 on re-run is normal, and
        // the sign-in assertions below are what actually gate the tests). It is
        // NOT a reason to pass, though: requireBasin() already established the
        // server is up, and every test signs in explicitly and throws if it
        // cannot.
      }
    }

    // Seed over pgwire. This is NOT an RLS bypass (there is none) — it relies on
    // the SELECT policies not yet filtering these writes; see the note above.
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
      // Do NOT swallow this. A failed setup means the assertions below would
      // run against an empty schema, where "Alice cannot see Bob's todo" is
      // trivially true. That is exactly how a vacuous pass gets manufactured.
      throw new Error(
        `RLS test setup failed, so the isolation assertions cannot be trusted: ${err}\n` +
          `  Migrate the schema and apply policies first: npm run setup`
      );
    }
  });

  afterAll(async () => {
    if (sql) await sql.end();
  });

  // -------------------------------------------------------------------------
  // Core isolation tests
  // -------------------------------------------------------------------------

  test("Alice can see her org (Acme) todos", async () => {
    await requireBasin();

    const client = createCompatClient(BASIN_URL);
    const { data: signInData, error: signInError } =
      await client.auth.signInWithPassword({ email: aliceEmail, password });

    if (signInError) {
      throw new Error(
        `Could not sign in as alice: ${signInError.message}\n` +
          `  Without a session there is no JWT, so no RLS policy is exercised and ` +
          `this test would otherwise pass by doing nothing. If basin-auth requires ` +
          `email verification, disable it for the test environment.`
      );
    }
    expect(signInData?.session?.accessToken).toBeTruthy();

    const { data: todos, error } = await client
      .from<{ id: string; org_id: string; title: string }>("todos")
      .select("id, org_id, title");

    expect(error).toBeNull();
    const rows = requireRows(todos, "alice todos");

    // Positive assertion FIRST. It is what proves the query worked at all; the
    // negative assertion below is meaningless without it, because a request
    // that returns nothing also "hides" Bob's row.
    const aliceTodo = rows.find((t) => t.title === "Alice secret todo");
    expect(aliceTodo).toBeDefined();

    // Alice must NOT see Bob's todo (different org, no membership)
    const bobTodo = rows.find((t) => t.title === "Bob private todo");
    expect(bobTodo).toBeUndefined();
  });

  test("Bob can see his org (Globex) todos", async () => {
    await requireBasin();

    const client = createCompatClient(BASIN_URL);
    const { data: signInData, error: signInError } =
      await client.auth.signInWithPassword({ email: bobEmail, password });

    if (signInError) {
      throw new Error(
        `Could not sign in as bob: ${signInError.message}\n` +
          `  Without a session there is no JWT, so no RLS policy is exercised and ` +
          `this test would otherwise pass by doing nothing.`
      );
    }
    expect(signInData?.session?.accessToken).toBeTruthy();

    const { data: todos, error } = await client
      .from<{ id: string; org_id: string; title: string }>("todos")
      .select("id, org_id, title");

    expect(error).toBeNull();
    const rows = requireRows(todos, "bob todos");

    // Positive first, for the same reason as the Alice case.
    const bobTodo = rows.find((t) => t.title === "Bob private todo");
    expect(bobTodo).toBeDefined();

    // Bob must NOT see Alice's todo
    const aliceTodo = rows.find((t) => t.title === "Alice secret todo");
    expect(aliceTodo).toBeUndefined();
  });

  test("Unauthenticated request returns empty or 401 (no data leakage)", async () => {
    await requireBasin();

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
    await requireBasin();

    const client = createCompatClient(BASIN_URL);
    const { data, error } = await client.auth.signInWithPassword({
      email: aliceEmail,
      password,
    });
    if (error || !data) {
      throw new Error(
        `Could not sign in as alice: ${error?.message ?? "no session returned"}\n` +
          `  This test asserts an empty result set, which is also what a failed ` +
          `request produces — so it must not be allowed to run without a session.`
      );
    }

    // Control: Alice's own org must be readable in this same session. Without
    // this, the assertion below passes just as happily when basin-rest is
    // returning nothing for every query.
    const { data: ownTodos } = await client
      .from<{ id: string; org_id: string }>("todos")
      .select("id, org_id")
      .eq("org_id", acmeOrgId ?? "");
    expect(requireRows(ownTodos, "alice own-org control query").length).toBeGreaterThan(0);

    // Alice tries to explicitly filter by globex's org_id
    const { data: todos } = await client
      .from<{ id: string; org_id: string }>("todos")
      .select("id, org_id")
      .eq("org_id", globexOrgId ?? "");

    // RLS policy fires AFTER any client-supplied filter, so even with the
    // explicit org_id filter, Alice gets zero rows because she has no
    // membership in Globex. `requireRows` rather than `todos ?? []`: the old
    // form turned a failed request into a pass.
    expect(requireRows(todos, "alice cross-org query")).toHaveLength(0);
  });
});
