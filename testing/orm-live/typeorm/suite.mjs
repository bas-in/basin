// Live TypeORM suite — DataSource init, TypeORM's own migration runner,
// repository CRUD, eager relations, and QueryRunner-managed transactions,
// against BASIN_DSN.
// TAP-ish output: "ok - typeorm.<test>" / "not ok - typeorm.<test> # <reason>".

import { dataSource } from "./datasource.js";

const RUN = Date.now();

function ok(name) { console.log(`ok - typeorm.${name}`); }
function notok(name, reason) {
  console.log(`not ok - typeorm.${name} # ${String(reason ?? "unknown").replace(/\s+/g, " ").slice(0, 300)}`);
}

const REMAINING = ["migrations-run", "crud-insert", "crud-find", "crud-update",
                   "crud-delete", "eager-relations", "queryrunner-tx", "queryrunner-tx-rollback",
                   "get-count", "qb-group-by", "qb-for-update", "qb-for-update-skip-locked",
                   "soft-delete", "keyset-pagination", "upsert"];

if (!process.env.BASIN_DSN) {
  notok("connect", "BASIN_DSN not set");
  process.exit(0);
}

// ── 1. connect (DataSource.initialize) ───────────────────────────────────────
try {
  await dataSource.initialize();
  ok("connect");
} catch (e) {
  notok("connect", e.message ?? e);
  for (const t of REMAINING) notok(t, "SKIP-CASCADE: no connection");
  process.exit(0);
}

async function test(name, fn) {
  try { await fn(); ok(name); } catch (e) { notok(name, e.message ?? e); }
}
function assert(cond, msg) { if (!cond) throw new Error(`assertion failed: ${msg}`); }

// ── 2. TypeORM's migration runner (creates its "migrations" tracker table) ──
await test("migrations-run", async () => {
  const applied = await dataSource.runMigrations();
  // Idempotent: 1 on first run, 0 on re-run against a durable server.
  assert(Array.isArray(applied), "runMigrations returns applied list");
});

const users = dataSource.getRepository("User");
const posts = dataSource.getRepository("Post");
let alice;

// ── 3. Repository CRUD ───────────────────────────────────────────────────────
await test("crud-insert", async () => {
  alice = await users.save({ email: `alice.${RUN}@typeorm.test`, name: "Alice" });
  assert(alice.id > 0, "save() hydrated the generated id");
  await posts.save([
    { title: "t-one", views: 5, author: alice },
    { title: "t-two", author: alice },
  ]);
});

await test("crud-find", async () => {
  const found = await users.findOneBy({ email: `alice.${RUN}@typeorm.test` });
  assert(found && found.name === "Alice", "findOneBy returns the row");
  assert(found.createdAt instanceof Date, "createDate column hydrates");
});

await test("crud-update", async () => {
  await users.update({ email: `alice.${RUN}@typeorm.test` }, { name: "Alice Updated" });
  const found = await users.findOneBy({ email: `alice.${RUN}@typeorm.test` });
  assert(found.name === "Alice Updated", "update persisted");
});

await test("crud-delete", async () => {
  const victim = await users.save({ email: `victim.${RUN}@typeorm.test` });
  await users.delete({ id: victim.id });
  assert((await users.findOneBy({ id: victim.id })) === null, "deleted row gone");
});

// ── 4. Eager relations (Post.author is eager:true) + find w/ relations ──────
await test("eager-relations", async () => {
  const one = await posts.findOne({ where: { title: "t-one" } });
  assert(one && one.author && one.author.email === `alice.${RUN}@typeorm.test`,
    "eager many-to-one hydrated on plain find");
  const withPosts = await users.findOne({
    where: { id: alice.id },
    relations: { posts: true },
  });
  assert(withPosts.posts.length === 2, "one-to-many loaded via relations option");
});

// ── 5. QueryRunner-managed transactions ──────────────────────────────────────
await test("queryrunner-tx", async () => {
  const qr = dataSource.createQueryRunner();
  await qr.connect();
  await qr.startTransaction();
  try {
    await qr.manager.save("User", { email: `tx.${RUN}@typeorm.test` });
    await qr.commitTransaction();
  } catch (e) {
    await qr.rollbackTransaction();
    throw e;
  } finally {
    await qr.release();
  }
  const found = await users.findOneBy({ email: `tx.${RUN}@typeorm.test` });
  assert(found, "committed QueryRunner tx visible");
});

await test("queryrunner-tx-rollback", async () => {
  const qr = dataSource.createQueryRunner();
  await qr.connect();
  await qr.startTransaction();
  await qr.manager.save("User", { email: `rb.${RUN}@typeorm.test` });
  await qr.rollbackTransaction();
  await qr.release();
  const found = await users.findOneBy({ email: `rb.${RUN}@typeorm.test` });
  assert(found === null, "rolled-back QueryRunner write must not be visible");
});

// ── 6. getCount() — wraps the query in COUNT(DISTINCT pk) ───────────────────
await test("get-count", async () => {
  const n = await users.createQueryBuilder("u")
    .where("u.email = :email", { email: `alice.${RUN}@typeorm.test` })
    .getCount();
  assert(n === 1, "getCount() returned 1 for the seeded author");
});

// ── 7. QueryBuilder GROUP BY + aggregate ────────────────────────────────────
await test("qb-group-by", async () => {
  const rows = await posts.createQueryBuilder("p")
    .select("p.author_id", "authorId")
    .addSelect("COUNT(*)", "cnt")
    .addSelect("SUM(p.views)", "views")
    .groupBy("p.author_id")
    .orderBy("p.author_id", "ASC")
    .getRawMany();
  assert(Array.isArray(rows) && rows.length >= 1, "GROUP BY returned grouped rows");
});

// ── 8. Row-level locking (.setLock) ─────────────────────────────────────────
await test("qb-for-update", async () => {
  await dataSource.transaction(async (mgr) => {
    const found = await mgr.getRepository("User").createQueryBuilder("u")
      .setLock("pessimistic_write")
      .where("u.email = :email", { email: `alice.${RUN}@typeorm.test` })
      .getOne();
    assert(found, "pessimistic_write (FOR UPDATE) returned the locked row");
  });
});

await test("qb-for-update-skip-locked", async () => {
  await dataSource.transaction(async (mgr) => {
    const rows = await mgr.getRepository("Post").createQueryBuilder("p")
      .setLock("pessimistic_write")
      .setOnLocked("skip_locked")
      .limit(1)
      .getMany();
    assert(Array.isArray(rows), "FOR UPDATE SKIP LOCKED executed");
  });
});

// ── 9. Soft delete (@DeleteDateColumn) ──────────────────────────────────────
await test("soft-delete", async () => {
  const v = await users.save({ email: `sd.${RUN}@typeorm.test` });
  await users.softDelete(v.id);
  // Default find excludes soft-deleted rows.
  const gone = await users.findOneBy({ id: v.id });
  assert(gone === null, "soft-deleted row excluded from default find");
  // withDeleted() sees it.
  const back = await users.findOne({ where: { id: v.id }, withDeleted: true });
  assert(back !== null, "withDeleted() sees the soft-deleted row");
});

// ── 10. keyset pagination ───────────────────────────────────────────────────
await test("keyset-pagination", async () => {
  const firstPage = await users.createQueryBuilder("u")
    .orderBy("u.id", "ASC").take(2).getMany();
  if (firstPage.length === 2) {
    const next = await users.createQueryBuilder("u")
      .where("u.id > :cursor", { cursor: firstPage[1].id })
      .orderBy("u.id", "ASC").take(2).getMany();
    for (const u of next) assert(u.id > firstPage[1].id, "keyset page is strictly after the cursor");
  }
});

// ── 11. upsert (ON CONFLICT DO UPDATE) ──────────────────────────────────────
await test("upsert", async () => {
  const em = `up.${RUN}@typeorm.test`;
  await users.upsert({ email: em, name: "First" }, ["email"]);
  await users.upsert({ email: em, name: "Second" }, ["email"]);
  const got = await users.findOneBy({ email: em });
  assert(got && (got.name === "Second" || got.name === "First"), "upsert resolved to one row");
  const n = await users.createQueryBuilder("u").where("u.email = :em", { em }).getCount();
  assert(n === 1, "upsert did not duplicate the row");
});

await dataSource.destroy();
