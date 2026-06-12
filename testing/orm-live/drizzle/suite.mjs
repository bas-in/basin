// Live Drizzle suite — drizzle-kit's own migration engine (generate + migrate)
// plus the relational query builder and transaction manager, against BASIN_DSN.
// TAP-ish output: "ok - drizzle.<test>" / "not ok - drizzle.<test> # <reason>".

import { spawnSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

import pg from "pg";
import { drizzle } from "drizzle-orm/node-postgres";
import { eq, gt, sql, desc } from "drizzle-orm";
import * as schema from "./schema.js";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const RUN = Date.now();

function ok(name) { console.log(`ok - drizzle.${name}`); }
function notok(name, reason) {
  console.log(`not ok - drizzle.${name} # ${String(reason ?? "unknown").replace(/\s+/g, " ").slice(0, 300)}`);
}
function diag(msg) { console.error(`[drizzle] ${msg}`); }

const DSN = process.env.BASIN_DSN;
if (!DSN) { notok("connect", "BASIN_DSN not set"); process.exit(0); }

// ── 1. drizzle-kit generate (offline) + migrate (against the live server) ──
function kit(args) {
  return spawnSync(
    process.execPath,
    [path.join(HERE, "node_modules", "drizzle-kit", "bin.cjs"), ...args],
    { cwd: HERE, encoding: "utf8", env: { ...process.env, BASIN_DSN: DSN }, timeout: 180_000 },
  );
}

{
  const r = kit(["generate", "--config", "drizzle.config.js"]);
  if (r.status === 0) ok("migrate-generate");
  else notok("migrate-generate", (r.stderr || r.stdout || "").trim() || `exit ${r.status}`);
}
{
  // drizzle-kit migrate creates schema "drizzle" + __drizzle_migrations.
  const r = kit(["migrate", "--config", "drizzle.config.js"]);
  if (r.status === 0) ok("migrate-apply");
  else notok("migrate-apply", (r.stderr || r.stdout || "").trim() || `exit ${r.status}`);
}

// ── 2. Connect ───────────────────────────────────────────────────────────────
const pool = new pg.Pool({ connectionString: DSN, max: 2, connectionTimeoutMillis: 10_000 });
const db = drizzle(pool, { schema });
const { users, posts } = schema;

try {
  await pool.query("SELECT 1");
  ok("connect");
} catch (e) {
  notok("connect", e.message ?? e);
  for (const t of ["insert-returning", "select-where", "relational-query", "upsert-on-conflict",
                   "transaction", "transaction-rollback", "pagination", "delete"]) {
    notok(t, "SKIP-CASCADE: no connection");
  }
  await pool.end().catch(() => {});
  process.exit(0);
}

// Setup fallback (not a test): if migrate-apply could not create the tables,
// create them directly so the query-path tests still measure honestly.
try {
  await pool.query("SELECT 1 FROM d_users LIMIT 1");
} catch {
  diag("schema missing — applying DDL fallback (migrate-apply gap)");
  for (const ddl of [
    `CREATE TABLE IF NOT EXISTS "d_users" (
       "id" serial PRIMARY KEY NOT NULL,
       "email" text NOT NULL UNIQUE,
       "name" text,
       "meta" jsonb,
       "created_at" timestamp with time zone DEFAULT now()
     )`,
    `CREATE TABLE IF NOT EXISTS "d_posts" (
       "id" serial PRIMARY KEY NOT NULL,
       "title" text NOT NULL,
       "published" boolean DEFAULT false NOT NULL,
       "views" integer DEFAULT 0 NOT NULL,
       "author_id" integer NOT NULL REFERENCES "d_users"("id")
     )`,
  ]) {
    try { await pool.query(ddl); } catch (e) { diag(`fallback DDL failed: ${String(e.message).split("\n")[0]}`); }
  }
}

async function test(name, fn) {
  try { await fn(); ok(name); } catch (e) { notok(name, e.message ?? e); }
}
function assert(cond, msg) { if (!cond) throw new Error(`assertion failed: ${msg}`); }

// ── 3. Query builder ─────────────────────────────────────────────────────────
let aliceId;
await test("insert-returning", async () => {
  const rows = await db
    .insert(users)
    .values({ email: `alice.${RUN}@drizzle.test`, name: "Alice", meta: { plan: "pro" } })
    .returning({ id: users.id, email: users.email });
  assert(rows.length === 1 && rows[0].id > 0, "RETURNING surfaced the serial id");
  aliceId = rows[0].id;
  await db.insert(posts).values([
    { title: "d-one", authorId: aliceId, views: 5 },
    { title: "d-two", authorId: aliceId, published: true },
  ]);
});

await test("select-where", async () => {
  const rows = await db.select().from(users).where(eq(users.email, `alice.${RUN}@drizzle.test`));
  assert(rows.length === 1, "exactly one match");
  assert(rows[0].meta?.plan === "pro", "jsonb round-trips");
});

await test("relational-query", async () => {
  // db.query.* — Drizzle's relational queries (the LATERAL/json_agg machinery).
  const found = await db.query.users.findMany({
    where: eq(users.id, aliceId ?? -1),
    with: { posts: true },
  });
  assert(found.length === 1, "parent row found");
  assert(found[0].posts.length === 2, "both children hydrated via relational query");
});

await test("upsert-on-conflict", async () => {
  await db
    .insert(users)
    .values({ email: `alice.${RUN}@drizzle.test`, name: "Alice2" })
    .onConflictDoUpdate({ target: users.email, set: { name: sql`excluded.name` } });
  const rows = await db.select().from(users).where(eq(users.email, `alice.${RUN}@drizzle.test`));
  assert(rows.length === 1, "no duplicate row");
  assert(rows[0].name === "Alice2", "DO UPDATE applied excluded.name");
});

// ── 4. Transactions ──────────────────────────────────────────────────────────
await test("transaction", async () => {
  await db.transaction(async (tx) => {
    const [u] = await tx.insert(users).values({ email: `tx.${RUN}@drizzle.test` }).returning();
    await tx.insert(posts).values({ title: "in-tx", authorId: u.id });
  });
  const rows = await db.select().from(users).where(eq(users.email, `tx.${RUN}@drizzle.test`));
  assert(rows.length === 1, "committed tx visible");
});

await test("transaction-rollback", async () => {
  await db
    .transaction(async (tx) => {
      await tx.insert(users).values({ email: `rb.${RUN}@drizzle.test` });
      throw new Error("force rollback");
    })
    .catch(() => {});
  const rows = await db.select().from(users).where(eq(users.email, `rb.${RUN}@drizzle.test`));
  assert(rows.length === 0, "rolled-back insert must not be visible");
});

// ── 5. Pagination + delete ───────────────────────────────────────────────────
await test("pagination", async () => {
  const page = await db
    .select({ id: posts.id, title: posts.title })
    .from(posts)
    .where(gt(posts.id, 0))
    .orderBy(desc(posts.id))
    .limit(2)
    .offset(1);
  assert(page.length <= 2, "limit respected");
  for (let i = 1; i < page.length; i++) assert(page[i - 1].id > page[i].id, "order desc respected");
});

await test("delete", async () => {
  const deleted = await db
    .delete(posts)
    .where(eq(posts.title, "in-tx"))
    .returning({ id: posts.id });
  assert(deleted.length >= 1, "delete-returning reports the removed row");
});

await pool.end().catch(() => {});
