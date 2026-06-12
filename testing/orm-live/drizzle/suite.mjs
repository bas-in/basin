// Live Drizzle suite — drizzle-kit's own migration engine (generate + migrate)
// plus the relational query builder and transaction manager, against BASIN_DSN.
// TAP-ish output: "ok - drizzle.<test>" / "not ok - drizzle.<test> # <reason>".

import { spawnSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

import pg from "pg";
import { drizzle } from "drizzle-orm/node-postgres";
import { eq, gt, sql, desc, asc, and, inArray, count, countDistinct, sum, avg } from "drizzle-orm";
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

// ── 6. Aggregations + groupBy + countDistinct ───────────────────────────────
await test("aggregations", async () => {
  // Fresh author with two posts of known view counts.
  const [a] = await db
    .insert(users)
    .values({ email: `agg.${RUN}@drizzle.test`, name: "Agg" })
    .returning({ id: users.id });
  await db.insert(posts).values([
    { title: "agg-1", authorId: a.id, views: 10 },
    { title: "agg-2", authorId: a.id, views: 30 },
  ]);
  const [row] = await db
    .select({ c: count(), s: sum(posts.views), av: avg(posts.views) })
    .from(posts)
    .where(eq(posts.authorId, a.id));
  assert(Number(row.c) === 2, "count() over the two posts");
  assert(Number(row.s) === 40, "sum(views) = 40");
  assert(Number(row.av) === 20, "avg(views) = 20");
});

await test("group-by", async () => {
  const groups = await db
    .select({ authorId: posts.authorId, n: count() })
    .from(posts)
    .groupBy(posts.authorId)
    .orderBy(asc(posts.authorId));
  assert(groups.length >= 1, "groupBy returns grouped rows");
  for (const g of groups) assert(Number(g.n) >= 1, "per-group count populated");
});

await test("count-distinct", async () => {
  const [row] = await db.select({ n: countDistinct(posts.authorId) }).from(posts);
  assert(Number(row.n) >= 1, "countDistinct(authorId) returns a positive count");
});

// ── 7. JSON column round-trips (->>'key', @> containment) ────────────────────
await test("json-extract-filter", async () => {
  await db.insert(users).values({ email: `j.${RUN}@drizzle.test`, meta: { plan: "scale" } });
  const rows = await db
    .select({ id: users.id })
    .from(users)
    .where(and(eq(users.email, `j.${RUN}@drizzle.test`), sql`${users.meta}->>'plan' = 'scale'`));
  assert(rows.length === 1, "jsonb ->> extract filter matched");
});

await test("json-contains", async () => {
  const rows = await db
    .select({ id: users.id })
    .from(users)
    .where(sql`${users.meta} @> '{"plan":"scale"}'`);
  assert(rows.length >= 1, "jsonb @> containment filter matched");
});

// ── 8. Bulk update + optimistic-lock style WHERE ─────────────────────────────
await test("bulk-update-returning", async () => {
  const updated = await db
    .update(posts)
    .set({ views: sql`${posts.views} + 1` })
    .where(inArray(posts.title, ["agg-1", "agg-2"]))
    .returning({ id: posts.id, views: posts.views });
  assert(updated.length === 2, "bulk update touched both rows");
});

// ── 9. Row-level locking (.for('update') + skipLocked) ──────────────────────
await test("select-for-update", async () => {
  await db.transaction(async (tx) => {
    const rows = await tx
      .select({ id: users.id })
      .from(users)
      .where(eq(users.email, `agg.${RUN}@drizzle.test`))
      .for("update");
    assert(rows.length === 1, "FOR UPDATE select returned the locked row");
  });
});

await test("select-for-update-skip-locked", async () => {
  await db.transaction(async (tx) => {
    const rows = await tx
      .select({ id: posts.id })
      .from(posts)
      .limit(1)
      .for("update", { skipLocked: true });
    assert(Array.isArray(rows), "FOR UPDATE SKIP LOCKED select executed");
  });
});

// ── 10. cursor / keyset pagination ──────────────────────────────────────────
await test("keyset-pagination", async () => {
  const firstPage = await db
    .select({ id: posts.id })
    .from(posts)
    .orderBy(asc(posts.id))
    .limit(2);
  if (firstPage.length === 2) {
    const next = await db
      .select({ id: posts.id })
      .from(posts)
      .where(gt(posts.id, firstPage[1].id))
      .orderBy(asc(posts.id))
      .limit(2);
    for (const p of next) assert(p.id > firstPage[1].id, "keyset page is strictly after the cursor");
  }
});

await pool.end().catch(() => {});
