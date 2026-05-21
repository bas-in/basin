/**
 * Optional seed script: creates two demo orgs, two users, and some todos.
 * Useful for manual end-to-end testing without going through the UI.
 *
 * Run: npm run db:seed
 *
 * Environment variables:
 *   DATABASE_URL — defaults to postgres://basin@localhost:5432/postgres
 *   BASIN_URL    — defaults to http://localhost:5432 (for auth sign-up)
 */
import postgres from "postgres";

const DATABASE_URL =
  process.env.DATABASE_URL ?? "postgres://basin@localhost:5432/postgres";

const BASIN_URL = process.env.BASIN_URL ?? "http://localhost:5432";

async function signUp(email: string, password: string): Promise<string | null> {
  /**
   * basin-auth sign-up endpoint: POST /auth/v1/signup
   * Returns { access_token, token_type, expires_in, refresh_token }
   *
   * If email verification is enabled (default in the tutorial stack) the user
   * must click the verification link before they can sign in.  For seeding
   * purposes we capture the user_id from the response if available, or fall
   * back to inserting the users row manually.
   */
  try {
    const res = await fetch(`${BASIN_URL}/auth/v1/signup`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ email, password }),
    });
    if (res.ok) {
      const data = (await res.json()) as {
        user?: { id: string };
        access_token?: string;
      };
      return data.user?.id ?? null;
    }
    const text = await res.text();
    console.warn(`  sign-up ${email} returned ${res.status}: ${text}`);
    return null;
  } catch (err) {
    console.warn(`  could not reach auth API: ${err}`);
    return null;
  }
}

async function main() {
  const sql = postgres(DATABASE_URL, { max: 1 });

  console.log("Seeding demo data…");

  // Sign up two demo users via basin-auth
  const aliceId = await signUp("alice@example.com", "hunter2hunter2");
  const bobId = await signUp("bob@example.com", "hunter2hunter2");

  const aliceUserId = aliceId ?? "seed-alice-0000-0000-000000000001";
  const bobUserId = bobId ?? "seed-bob-00000-0000-000000000002";

  // Upsert user profile rows
  await sql`
    INSERT INTO users (id, email, display_name)
    VALUES
      (${aliceUserId}, 'alice@example.com', 'Alice Demo'),
      (${bobUserId},   'bob@example.com',   'Bob Demo')
    ON CONFLICT (id) DO NOTHING
  `;

  // Create two orgs
  const [acme] = await sql<{ id: string }[]>`
    INSERT INTO orgs (name, slug)
    VALUES ('Acme Corp', 'acme-corp')
    ON CONFLICT (slug) DO UPDATE SET name = EXCLUDED.name
    RETURNING id
  `;
  const [globex] = await sql<{ id: string }[]>`
    INSERT INTO orgs (name, slug)
    VALUES ('Globex Inc', 'globex-inc')
    ON CONFLICT (slug) DO UPDATE SET name = EXCLUDED.name
    RETURNING id
  `;

  // Alice → Acme (owner), Bob → Globex (owner)
  await sql`
    INSERT INTO memberships (org_id, user_id, role)
    VALUES
      (${acme.id},   ${aliceUserId}, 'owner'),
      (${globex.id}, ${bobUserId},   'owner')
    ON CONFLICT DO NOTHING
  `;

  // Todos for Acme (Alice)
  await sql`
    INSERT INTO todos (org_id, title, created_by)
    VALUES
      (${acme.id}, 'Draft Q1 roadmap', ${aliceUserId}),
      (${acme.id}, 'Hire a designer',  ${aliceUserId})
    ON CONFLICT DO NOTHING
  `;

  // Todos for Globex (Bob)
  await sql`
    INSERT INTO todos (org_id, title, created_by)
    VALUES
      (${globex.id}, 'Set up CI pipeline',     ${bobUserId}),
      (${globex.id}, 'Write integration tests', ${bobUserId})
    ON CONFLICT DO NOTHING
  `;

  console.log(`  orgs: acme (${acme.id}), globex (${globex.id})`);
  console.log(`  users: alice (${aliceUserId}), bob (${bobUserId})`);
  console.log("Seed complete.");

  await sql.end();
}

main().catch((err) => {
  console.error("Seed failed:", err);
  process.exit(1);
});
