// Live Prisma suite — drives Prisma's OWN machinery (migrate engine, query
// builder/compiler, interactive transactions) against BASIN_DSN.
// Emits TAP-ish lines the harness scorecard parses:  "ok - prisma.<test>" /
// "not ok - prisma.<test> # <reason>".

import { spawnSync } from "node:child_process";
import { readFileSync, readdirSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const RUN = Date.now(); // unique suffix so re-runs against a durable server don't collide

function ok(name) { console.log(`ok - prisma.${name}`); }
function notok(name, reason) {
  const r = String(reason ?? "unknown").replace(/\s+/g, " ").slice(0, 300);
  console.log(`not ok - prisma.${name} # ${r}`);
}
function diag(msg) { console.error(`[prisma] ${msg}`); }

const DSN = process.env.BASIN_DSN;
if (!DSN) { notok("connect", "BASIN_DSN not set"); process.exit(0); }

// ── 1. Prisma Migrate — its own migration engine, both flows ───────────────
function prismaCli(args) {
  return spawnSync(
    process.execPath,
    [path.join(HERE, "node_modules", "prisma", "build", "index.js"), ...args],
    { cwd: HERE, encoding: "utf8", env: { ...process.env, BASIN_DSN: DSN }, timeout: 180_000 },
  );
}

{
  const r = prismaCli(["migrate", "deploy"]);
  if (r.status === 0) ok("migrate-deploy");
  else notok("migrate-deploy", (r.stderr || r.stdout || "").trim() || `exit ${r.status}`);
}
{
  // migrate dev needs a shadow database (CREATE DATABASE) — expected gap.
  const r = prismaCli(["migrate", "dev", "--name", "live"]);
  if (r.status === 0) ok("migrate-dev");
  else notok("migrate-dev", (r.stderr || r.stdout || "").trim() || `exit ${r.status}`);
}

// ── 2. Client connect (driver adapter over pg) ──────────────────────────────
const { PrismaPg } = await import("@prisma/adapter-pg");
const { PrismaClient } = await import("./generated/prisma/client.ts");
const prisma = new PrismaClient({ adapter: new PrismaPg({ connectionString: DSN }) });

try {
  await prisma.$queryRawUnsafe("SELECT 1");
  ok("connect");
} catch (e) {
  notok("connect", e.message ?? e);
  for (const t of ["crud-create", "crud-find-unique", "crud-update", "crud-delete",
                   "nested-write-include", "transaction", "transaction-rollback", "pagination"]) {
    notok(t, "SKIP-CASCADE: no connection");
  }
  await prisma.$disconnect();
  process.exit(0);
}

// ── Setup (not a test): if migrate-deploy could not apply the schema (known
// advisory-lock gap), apply the committed migration SQL directly so the
// client tests still measure the query path honestly. ──────────────────────
try {
  await prisma.$queryRawUnsafe('SELECT 1 FROM "User" LIMIT 1');
} catch {
  diag("schema missing — applying committed migration.sql directly (migrate-deploy gap fallback)");
  const dir = readdirSync(path.join(HERE, "migrations")).find((d) => d.endsWith("_init"));
  const sql = readFileSync(path.join(HERE, "migrations", dir, "migration.sql"), "utf8");
  for (const stmt of sql.split(/;\s*\n/).map((s) => s.trim()).filter(Boolean)) {
    try { await prisma.$executeRawUnsafe(stmt); }
    catch (e) { diag(`fallback DDL failed: ${String(e.message).split("\n")[0]}`); }
  }
}

async function test(name, fn) {
  try { await fn(); ok(name); } catch (e) { notok(name, e.message ?? e); }
}
function assert(cond, msg) { if (!cond) throw new Error(`assertion failed: ${msg}`); }

// ── 3. CRUD through the Prisma query compiler ───────────────────────────────
await test("crud-create", async () => {
  const u = await prisma.user.create({
    data: { email: `alice.${RUN}@basin.test`, name: "Alice", role: "ADMIN", meta: { plan: "pro", limits: [1, 2, 3] } },
  });
  assert(u.id > 0, "autoincrement id returned");
  assert(u.role === "ADMIN", "enum value round-trips");
  assert(u.meta?.plan === "pro", "Json round-trips");
  assert(u.createdAt instanceof Date, "DateTime default hydrates as Date");
});

await test("crud-find-unique", async () => {
  const u = await prisma.user.findUnique({ where: { email: `alice.${RUN}@basin.test` } });
  assert(u, "row found by unique key");
  assert(u.name === "Alice", "projection correct");
});

await test("crud-update", async () => {
  const u = await prisma.user.update({
    where: { email: `alice.${RUN}@basin.test` },
    data: { name: "Alice Updated", meta: { plan: "team" } },
  });
  assert(u.name === "Alice Updated", "update returns new value");
});

await test("crud-delete", async () => {
  await prisma.user.create({ data: { email: `victim.${RUN}@basin.test` } });
  await prisma.user.delete({ where: { email: `victim.${RUN}@basin.test` } });
  const gone = await prisma.user.findUnique({ where: { email: `victim.${RUN}@basin.test` } });
  assert(gone === null, "deleted row no longer found");
});

// ── 4. Nested writes + include (the defining Prisma shape) ──────────────────
await test("nested-write-include", async () => {
  const u = await prisma.user.create({
    data: {
      email: `author.${RUN}@basin.test`,
      posts: { create: [{ title: "First", views: 10 }, { title: "Second", published: true }] },
      profile: { create: { bio: "live harness" } },
    },
    include: { posts: true, profile: true },
  });
  assert(u.posts.length === 2, "both nested posts created + included");
  assert(u.profile?.bio === "live harness", "nested 1-1 profile included");
});

// ── 5. Interactive transactions ──────────────────────────────────────────────
await test("transaction", async () => {
  await prisma.$transaction(async (tx) => {
    const u = await tx.user.create({ data: { email: `tx.${RUN}@basin.test` } });
    await tx.post.create({ data: { title: "in-tx", authorId: u.id } });
  });
  const u = await prisma.user.findUnique({
    where: { email: `tx.${RUN}@basin.test` }, include: { posts: true },
  });
  assert(u && u.posts.length === 1, "committed transaction visible");
});

await test("transaction-rollback", async () => {
  await prisma
    .$transaction(async (tx) => {
      await tx.user.create({ data: { email: `rb.${RUN}@basin.test` } });
      throw new Error("force rollback");
    })
    .catch(() => {});
  const gone = await prisma.user.findUnique({ where: { email: `rb.${RUN}@basin.test` } });
  assert(gone === null, "rolled-back write must not be visible");
});

// ── 6. Pagination (skip/take + orderBy) ──────────────────────────────────────
await test("pagination", async () => {
  const page = await prisma.post.findMany({ orderBy: { id: "desc" }, skip: 1, take: 2 });
  assert(page.length <= 2, "take respected");
  for (let i = 1; i < page.length; i++) assert(page[i - 1].id > page[i].id, "orderBy desc respected");
  const n = await prisma.post.count();
  assert(typeof n === "number" && n >= page.length, "count() companion query");
});

await prisma.$disconnect();
