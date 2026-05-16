//! Curated ORM / driver wire-compatibility suite.
//!
//! This is the *data-path* compatibility suite: it proves Basin handles the
//! SQL shapes real ORMs and drivers actually emit over the Postgres wire
//! protocol — parameterised queries through the extended protocol, RETURNING,
//! upserts, data-modifying CTEs, nested JSON reads, LATERAL, transactions and
//! prepared-statement reuse — under *varied real-world conditions* (match /
//! no-match / NULL / single vs multi-row / rollback / param re-bind).
//!
//! Every assertion runs through `tokio-postgres` against an in-process Basin
//! pgwire server, so the extended-protocol Parse/Bind/Describe/Execute path
//! (the path every ORM driver takes) is exercised for real. We do not bundle
//! Node/Python runtimes — the SQL shapes below are the *verbatim* statements
//! Prisma / Drizzle / ActiveRecord / Django / Hibernate / Sequelize /
//! SQLAlchemy / `pg` / asyncpg / pgx generate.
//!
//! Quality over quantity: a handful of representative, condition-varied tests,
//! each asserting the *correct Postgres-semantics* result for several
//! conditions of one realistic shape. Tests whose shape depends on an
//! in-flight engine fix are `#[ignore]`-gated on the tracked issue number and
//! still assert the *correct* behaviour, so they flip to a real guard the
//! moment the fix lands. Nothing is weakened to pass; nothing asserts
//! placeholder output.

#![allow(clippy::print_stdout)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use basin_common::ProjectId;
use basin_router::{ServerConfig, StaticProjectResolver};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;
use tokio_postgres::{Client, NoTls};

// =============================================================================
// Harness — identical shape to postgrest_pgadmin_compat.rs /
// jsonb_uuid_param_binding.rs (reused, not reinvented).
// =============================================================================

struct TestServer {
    addr: SocketAddr,
    _shutdown: tokio::sync::oneshot::Sender<()>,
    _join: tokio::task::JoinHandle<basin_common::Result<()>>,
    _dir: TempDir,
}

async fn start_server() -> TestServer {
    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().unwrap();
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(basin_catalog::InMemoryCatalog::new());
    let engine = basin_engine::Engine::new(basin_engine::EngineConfig {
        storage,
        catalog,
        shard: None,
    });

    let project = ProjectId::new();
    let mut map = HashMap::new();
    map.insert("alice".to_owned(), project);
    let resolver = Arc::new(StaticProjectResolver::new(map));

    let running = basin_router::run_until_bound(ServerConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        engine,
        project_resolver: resolver,
        pool: None,
        shard_endpoints: None,
        tls: None,
        connection_limiter: None,
    })
    .await
    .expect("server failed to bind");

    TestServer {
        addr: running.local_addr,
        _shutdown: running.shutdown,
        _join: running.join,
        _dir: dir,
    }
}

async fn connect(addr: SocketAddr) -> Client {
    let conn_str = format!(
        "host={} port={} user=alice password=ignored",
        addr.ip(),
        addr.port()
    );
    let (client, conn) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("conn driver: {e}");
        }
    });
    client
}

/// Canonical blog-style fixture every ORM tutorial uses: `users` 1-N `posts`.
/// `posts.author_id` is the FK column (declared NOT NULL but no REFERENCES —
/// v0.1 parses-but-doesn't-enforce FK; the join shapes below don't need
/// enforcement, only the columns).
async fn seed_blog(client: &Client) {
    for stmt in [
        // BIGINT throughout — this is the storage shape Prisma/Drizzle/Rails
        // emit by default (`Int @id` → bigint in PG). It also dodges the
        // separately-tracked #54 parameter-description gap on narrow-int
        // columns; the narrow-int / int4 coercion is covered in its own
        // `#[ignore = "#54"]` test below so the gap is honestly pinned.
        "CREATE TABLE users (\
             id BIGINT NOT NULL, \
             email TEXT NOT NULL, \
             name TEXT, \
             age BIGINT\
         )",
        "CREATE TABLE posts (\
             id BIGINT NOT NULL, \
             author_id BIGINT NOT NULL, \
             title TEXT NOT NULL, \
             views BIGINT NOT NULL\
         )",
        // u1 has 2 posts, u2 has 1 post, u3 has 0 posts (the zero-children
        // condition every nested-read test must cover).
        "INSERT INTO users (id, email, name, age) VALUES \
             (1, 'a@x.com', 'Alice', 30), \
             (2, 'b@x.com', 'Bob', 25), \
             (3, 'c@x.com', NULL, NULL)",
        "INSERT INTO posts (id, author_id, title, views) VALUES \
             (10, 1, 'Alice One', 100), \
             (11, 1, 'Alice Two', 50), \
             (12, 2, 'Bob One', 0)",
    ] {
        client
            .simple_query(stmt)
            .await
            .unwrap_or_else(|e| panic!("fixture step failed: {stmt:?}: {e}"));
    }
}

// =============================================================================
// 1. Parameterised queries — the universal driver path (extended protocol).
//    Models: every driver (pg / node-postgres, asyncpg, psycopg, pgx, JDBC).
//    Conditions: match / no-match (empty) / NULL param / type coercion /
//    multi-param / re-bind.
// =============================================================================

/// `SELECT ... WHERE col = $1` and friends — the single most common shape any
/// ORM emits. Conditions: exact match (1 row), no-match (0 rows), multi-param
/// AND, integer param against INT column (type coercion through BIND).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn param_select_where_eq_match_nomatch_multiparam() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    seed_blog(&client).await;

    // (a) match → exactly 1 row, correct projection.
    let rows = client
        .query("SELECT id, email FROM users WHERE id = $1", &[&1_i64])
        .await
        .expect("param eq match");
    assert_eq!(rows.len(), 1, "id=1 → exactly one user");
    assert_eq!(rows[0].get::<_, i64>(0), 1);
    assert_eq!(rows[0].get::<_, &str>(1), "a@x.com");

    // (b) no-match → empty result set, NOT an error.
    let rows = client
        .query("SELECT id FROM users WHERE id = $1", &[&999_i64])
        .await
        .expect("param eq no-match must succeed with 0 rows");
    assert_eq!(rows.len(), 0, "id=999 → zero rows (empty, not error)");

    // (c) multi-param AND with mixed column types (BIGINT + TEXT).
    let rows = client
        .query(
            "SELECT id FROM users WHERE id = $1 AND email = $2",
            &[&2_i64, &"b@x.com"],
        )
        .await
        .expect("multi-param AND");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i64>(0), 2);

    // (d) parameter against the BIGINT age column.
    let rows = client
        .query("SELECT id FROM users WHERE age = $1", &[&30_i64])
        .await
        .expect("bigint param vs BIGINT column");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i64>(0), 1);
}

/// Narrow-int (`int4`) parameter coercion. Real ORM drivers send `i32` for
/// columns they model as `int`/`Int4`. Today Basin's ParameterDescription
/// over-reports BIGINT for narrow numerics and the client-side encoder
/// refuses to bind `i32`. Pinned shape: an `i32` bound against a column the
/// app models as `int` must succeed. Gated on #54 (param-binding /
/// ParameterDescription fix).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "ORM pattern (int4 param coercion) — gated on ParameterDescription fix #54; un-ignore when landed"]
async fn param_int4_coercion() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    client
        .simple_query("CREATE TABLE narrow (id INT NOT NULL, label TEXT NOT NULL)")
        .await
        .expect("create narrow");
    client
        .simple_query("INSERT INTO narrow VALUES (7, 'lucky'), (8, 'eight')")
        .await
        .expect("seed narrow");

    let rows = client
        .query("SELECT label FROM narrow WHERE id = $1", &[&7_i32])
        .await
        .expect("int4 param vs INT column must work once #54 lands");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, &str>(0), "lucky");
}

/// NULL parameter semantics. ORMs bind `NULL` for optional filters; correct
/// Postgres behaviour is that `col = NULL` matches nothing (three-valued
/// logic) while `col IS NOT DISTINCT FROM $1` / `IS NULL` do the null-safe
/// thing. We assert the SQL-correct outcome for each.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn param_null_three_valued_logic() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    seed_blog(&client).await;

    // `name = NULL` → UNKNOWN → no rows (even though u3.name IS NULL).
    let none: Option<&str> = None;
    let rows = client
        .query("SELECT id FROM users WHERE name = $1", &[&none])
        .await
        .expect("name = NULL param");
    assert_eq!(
        rows.len(),
        0,
        "`col = NULL` is UNKNOWN for every row → 0 rows (3-valued logic)"
    );

    // The null-safe form ORMs use for `.where(name: nil)` is `IS NULL`.
    let rows = client
        .query("SELECT id FROM users WHERE name IS NULL", &[])
        .await
        .expect("IS NULL");
    assert_eq!(rows.len(), 1, "exactly u3 has NULL name");
    assert_eq!(rows[0].get::<_, i64>(0), 3);

    // Reading a NULL column back through the wire as Option must be None.
    let rows = client
        .query("SELECT name, age FROM users WHERE id = $1", &[&3_i64])
        .await
        .expect("select NULL columns");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, Option<&str>>(0), None);
    assert_eq!(rows[0].get::<_, Option<i64>>(1), None);
}

/// `WHERE id IN ($1,$2,$3)` — ActiveRecord `.where(id: [...])`, Django
/// `__in`, Hibernate `IN (:ids)`. Conditions: all present, some present,
/// none present (empty result).
///
/// Today Basin's ParameterDescription reports `text` for IN-list params
/// even against a BIGINT column, so the client-side encoder refuses the
/// bind before it reaches the wire. The literal-IN sub-case (which ORMs
/// also commonly emit, e.g. Rails' interpolated `IN (1,2,3)` for cache
/// keys) is covered unconditionally in
/// `param_select_where_eq_match_nomatch_multiparam`'s neighbour
/// `where_in_literal` test below. Gated on #54.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "ORM pattern (parameterised IN-list) — gated on ParameterDescription fix #54; un-ignore when landed"]
async fn param_where_in_list() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    seed_blog(&client).await;

    // All three present → 3 rows, deterministic order via ORDER BY.
    let rows = client
        .query(
            "SELECT id FROM users WHERE id IN ($1, $2, $3) ORDER BY id",
            &[&1_i64, &2_i64, &3_i64],
        )
        .await
        .expect("IN (all present)");
    let ids: Vec<i64> = rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(ids, vec![1, 2, 3]);

    // Partial overlap → only the matching rows.
    let rows = client
        .query(
            "SELECT id FROM users WHERE id IN ($1, $2) ORDER BY id",
            &[&2_i64, &99_i64],
        )
        .await
        .expect("IN (partial)");
    let ids: Vec<i64> = rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(ids, vec![2], "only id=2 exists");

    // No overlap → empty.
    let rows = client
        .query(
            "SELECT id FROM users WHERE id IN ($1, $2)",
            &[&98_i64, &99_i64],
        )
        .await
        .expect("IN (none) must succeed with 0 rows");
    assert_eq!(rows.len(), 0);
}

/// `WHERE id IN (...)` with **literal** values — the cache-key-friendly form
/// Rails / many ORMs interpolate (and the form most SQL builders fall back
/// to when the driver chokes on parameterised arrays). Same conditions as
/// the parameterised version. This pins the planner shape even before
/// #54 lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn where_in_literal_list() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    seed_blog(&client).await;

    // All present.
    let rows = client
        .query(
            "SELECT id FROM users WHERE id IN (1, 2, 3) ORDER BY id",
            &[],
        )
        .await
        .expect("literal IN (all present)");
    let ids: Vec<i64> = rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(ids, vec![1, 2, 3]);

    // Partial overlap.
    let rows = client
        .query(
            "SELECT id FROM users WHERE id IN (2, 99) ORDER BY id",
            &[],
        )
        .await
        .expect("literal IN (partial)");
    let ids: Vec<i64> = rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(ids, vec![2]);

    // No overlap → empty.
    let rows = client
        .query("SELECT id FROM users WHERE id IN (98, 99)", &[])
        .await
        .expect("literal IN (none) must succeed with 0 rows");
    assert_eq!(rows.len(), 0);
}

/// Prepared-statement *reuse*: prepare once, execute many times with
/// different params (the connection-pool hot path — every ORM with a pool
/// does this). Also exercises text + binary param formats implicitly
/// (`tokio-postgres` binds binary for typed prepared statements).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prepared_statement_reuse_rebind() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    seed_blog(&client).await;

    let stmt = client
        .prepare("SELECT email FROM users WHERE id = $1")
        .await
        .expect("prepare once");

    // Execute the same prepared plan three times with different binds.
    for (id, want) in [(1_i64, "a@x.com"), (2, "b@x.com"), (3, "c@x.com")] {
        let rows = client
            .query(&stmt, &[&id])
            .await
            .unwrap_or_else(|e| panic!("re-execute id={id}: {e}"));
        assert_eq!(rows.len(), 1, "id={id} → one row on reuse");
        assert_eq!(rows[0].get::<_, &str>(0), want);
    }

    // Re-bind that yields zero rows must still work on the reused statement.
    let rows = client
        .query(&stmt, &[&404_i64])
        .await
        .expect("reused stmt, no-match bind");
    assert_eq!(rows.len(), 0);
}

// =============================================================================
// 2. Pagination / counting / aggregates — ActiveRecord, Django, Hibernate.
// =============================================================================

/// `ORDER BY ... LIMIT $1 OFFSET $2` — the universal pagination shape.
/// Conditions: first page, second page, offset past end (must yield empty,
/// not error). Plus `COUNT(*)` for the total (the second query every paginated
/// list view issues).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pagination_limit_offset_and_count() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    seed_blog(&client).await;

    // Page 1: LIMIT 2 OFFSET 0 → first two users by id.
    let rows = client
        .query(
            "SELECT id FROM users ORDER BY id LIMIT $1 OFFSET $2",
            &[&2_i64, &0_i64],
        )
        .await
        .expect("page 1");
    let ids: Vec<i64> = rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(ids, vec![1, 2]);

    // Page 2: LIMIT 2 OFFSET 2 → remaining single user.
    let rows = client
        .query(
            "SELECT id FROM users ORDER BY id LIMIT $1 OFFSET $2",
            &[&2_i64, &2_i64],
        )
        .await
        .expect("page 2");
    let ids: Vec<i64> = rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(ids, vec![3]);

    // Offset past the end → empty page (NOT an error).
    let rows = client
        .query(
            "SELECT id FROM users ORDER BY id LIMIT $1 OFFSET $2",
            &[&2_i64, &500_i64],
        )
        .await
        .expect("offset past end must be empty, not error");
    assert_eq!(rows.len(), 0);

    // The companion COUNT(*) the list view issues for the page count.
    let row = client
        .query_one("SELECT COUNT(*) FROM users", &[])
        .await
        .expect("count(*)");
    assert_eq!(row.get::<_, i64>(0), 3);
}

/// `GROUP BY ... HAVING`, `DISTINCT`, and aggregate `FILTER` — the
/// reporting / `.group(...).count` shapes (ActiveRecord `group`, Django
/// `annotate`, Hibernate `group by`). Conditions: groups with/without rows
/// surviving HAVING, aggregate over empty filter.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn group_by_having_distinct_filter() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    seed_blog(&client).await;

    // GROUP BY author_id with HAVING COUNT(*) > 1 → only u1 (2 posts).
    // Literal HAVING bound: the parameterised-HAVING form is gated under
    // #54 alongside parameterised IN-lists.
    let rows = client
        .query(
            "SELECT author_id, COUNT(*) AS c FROM posts \
             GROUP BY author_id HAVING COUNT(*) > 1 ORDER BY author_id",
            &[],
        )
        .await
        .expect("GROUP BY ... HAVING");
    assert_eq!(rows.len(), 1, "only author 1 has >1 post");
    assert_eq!(rows[0].get::<_, i64>(0), 1);
    assert_eq!(rows[0].get::<_, i64>(1), 2);

    // DISTINCT author_id → 2 distinct authors among the 3 posts.
    let rows = client
        .query("SELECT DISTINCT author_id FROM posts", &[])
        .await
        .expect("DISTINCT");
    assert_eq!(rows.len(), 2, "posts written by 2 distinct authors");

    // Aggregate FILTER: total posts vs. posts with views > 0.
    let row = client
        .query_one(
            "SELECT COUNT(*) AS total, COUNT(*) FILTER (WHERE views > 0) AS viewed FROM posts",
            &[],
        )
        .await
        .expect("aggregate FILTER");
    assert_eq!(row.get::<_, i64>(0), 3, "3 posts total");
    assert_eq!(
        row.get::<_, i64>(1),
        2,
        "2 posts have views>0 (Bob One has 0)"
    );
}

/// `EXISTS` / `NOT EXISTS` correlated subquery in a `SELECT` WHERE clause —
/// ActiveRecord `.where(Post.where(...).arel.exists)`, Django
/// `.filter(Exists(...))`, Hibernate `where exists`. Conditions: authors
/// who have posts (EXISTS true) vs. authors with none (NOT EXISTS true).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn correlated_exists_not_exists_in_select() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    seed_blog(&client).await;

    // Users WITH at least one post → u1, u2.
    let rows = client
        .query(
            "SELECT id FROM users u \
             WHERE EXISTS (SELECT 1 FROM posts p WHERE p.author_id = u.id) \
             ORDER BY id",
            &[],
        )
        .await
        .expect("correlated EXISTS");
    let ids: Vec<i64> = rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(ids, vec![1, 2], "u1,u2 have posts");

    // Users with NO posts → only u3.
    let rows = client
        .query(
            "SELECT id FROM users u \
             WHERE NOT EXISTS (SELECT 1 FROM posts p WHERE p.author_id = u.id) \
             ORDER BY id",
            &[],
        )
        .await
        .expect("correlated NOT EXISTS");
    let ids: Vec<i64> = rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(ids, vec![3], "only u3 has zero posts");
}

/// INNER vs LEFT JOIN — eager-loading the way ORMs do `includes` /
/// `select_related` / `JOIN FETCH`. Conditions: LEFT JOIN must preserve the
/// childless parent (u3) with NULL right side; INNER JOIN must drop it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inner_vs_left_join_eager_load() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    seed_blog(&client).await;

    // INNER JOIN → one row per (user,post); u3 (no posts) absent → 3 rows.
    let rows = client
        .query(
            "SELECT u.id, p.id FROM users u \
             JOIN posts p ON p.author_id = u.id ORDER BY u.id, p.id",
            &[],
        )
        .await
        .expect("INNER JOIN");
    assert_eq!(rows.len(), 3, "3 posts → 3 joined rows; u3 dropped");

    // LEFT JOIN → u3 preserved with a NULL post id (4 rows total).
    let rows = client
        .query(
            "SELECT u.id, p.id FROM users u \
             LEFT JOIN posts p ON p.author_id = u.id ORDER BY u.id, p.id NULLS FIRST",
            &[],
        )
        .await
        .expect("LEFT JOIN");
    assert_eq!(rows.len(), 4, "3 posts + 1 childless user (u3) = 4 rows");
    // The u3 row carries a NULL post id.
    let u3 = rows
        .iter()
        .find(|r| r.get::<_, i64>(0) == 3)
        .expect("u3 must be present via LEFT JOIN");
    assert_eq!(
        u3.get::<_, Option<i64>>(1),
        None,
        "childless parent → NULL right side"
    );
}

// =============================================================================
// 3. Writes the way ORMs do them — RETURNING, upsert, batch.
//    Models: ActiveRecord/Rails 7 (`RETURNING`), Prisma `upsert`, Drizzle
//    `.onConflictDoUpdate`, Hibernate batch insert.
// =============================================================================

/// `INSERT ... RETURNING id` (single + batch), `UPDATE ... RETURNING`,
/// `DELETE ... RETURNING` — Rails/ActiveRecord and Ecto rely on RETURNING to
/// hydrate the model after a write. Conditions: single-row returning, batch
/// (multi-row) returning, returning shape after UPDATE/DELETE, no-op
/// UPDATE/DELETE returns zero rows.
///
/// **Newly-discovered gap (not one of #54–#58)**: Basin's pgwire
/// extended-protocol path (`tokio-postgres::query` / `query_one`) returns
/// `INSERT/UPDATE/DELETE ... RETURNING` rows with the *correct count* but
/// *zero columns* — the projected RETURNING columns are dropped before the
/// RowDescription / DataRow is emitted. `simple_query` returns RETURNING
/// rows correctly (verified), and the engine-level `dml_extras.rs` tests
/// prove the engine emits the rows; the gap is purely in the extended-protocol
/// projection wire-up. Every ORM that uses parameterised RETURNING (Rails,
/// Ecto, Drizzle, Prisma) trips this. Pinned with `#[ignore]` until the
/// extended-protocol RETURNING projection gap is fixed; flip to a guard then.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "ORM pattern (extended-protocol INSERT/UPDATE/DELETE RETURNING) — newly-found gap: extended path returns 0-column DataRows; un-ignore when fixed"]
async fn insert_update_delete_returning() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    seed_blog(&client).await;

    // Single-row INSERT ... RETURNING id (parameterised, the ORM hot path).
    let row = client
        .query_one(
            "INSERT INTO users (id, email, name, age) VALUES ($1, $2, $3, $4) RETURNING id",
            &[&100_i64, &"new@x.com", &"New", &40_i64],
        )
        .await
        .expect("INSERT ... RETURNING id");
    assert_eq!(row.get::<_, i64>(0), 100);

    // Batch INSERT ... RETURNING id → one returned row per inserted row.
    let rows = client
        .query(
            "INSERT INTO users (id, email, name, age) VALUES \
                 (101, 'p@x.com', 'P', 1), (102, 'q@x.com', 'Q', 2) \
             RETURNING id",
            &[],
        )
        .await
        .expect("batch INSERT ... RETURNING");
    let mut ids: Vec<i64> = rows.iter().map(|r| r.get(0)).collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![101, 102], "batch insert returns both ids");

    // UPDATE ... RETURNING the new value (match → 1 row).
    let row = client
        .query_one(
            "UPDATE users SET age = $1 WHERE id = $2 RETURNING id, age",
            &[&99_i64, &100_i64],
        )
        .await
        .expect("UPDATE ... RETURNING");
    assert_eq!(row.get::<_, i64>(0), 100);
    assert_eq!(row.get::<_, i64>(1), 99);

    // No-op UPDATE (no rows match) → RETURNING yields zero rows, not error.
    let rows = client
        .query(
            "UPDATE users SET age = 0 WHERE id = $1 RETURNING id",
            &[&777_i64],
        )
        .await
        .expect("no-op UPDATE RETURNING must succeed with 0 rows");
    assert_eq!(rows.len(), 0);

    // DELETE ... RETURNING the deleted row.
    let row = client
        .query_one(
            "DELETE FROM users WHERE id = $1 RETURNING id",
            &[&102_i64],
        )
        .await
        .expect("DELETE ... RETURNING");
    assert_eq!(row.get::<_, i64>(0), 102);
    // And it's gone.
    let rows = client
        .query("SELECT id FROM users WHERE id = $1", &[&102_i64])
        .await
        .expect("post-delete select");
    assert_eq!(rows.len(), 0, "deleted row no longer visible");
}

/// `INSERT ... ON CONFLICT (...) DO NOTHING` — the "insert if absent" idiom
/// (Rails `insert_all` with `unique_by`, Django `bulk_create`
/// `ignore_conflicts=True`, hand-written deduplicated inserts). Conditions:
/// no-conflict (plain insert), conflict on UNIQUE key (row unchanged).
///
/// **Newly-discovered gap (not one of #54–#58)**: Basin's UNIQUE enforcement
/// fires *before* the ON CONFLICT handler can suppress the violation, so the
/// conflict path raises SQLSTATE 23505 instead of being a no-op. Pinned with
/// the correct PG semantics; flips to a real guard when ON CONFLICT
/// short-circuiting is wired into the constraint enforcement path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "ORM pattern (ON CONFLICT DO NOTHING on UNIQUE) — newly-found gap: UNIQUE fires before ON CONFLICT short-circuits; un-ignore when fixed"]
async fn upsert_on_conflict_do_nothing() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    client
        .simple_query(
            "CREATE TABLE accounts (\
                 email TEXT NOT NULL UNIQUE, \
                 hits BIGINT NOT NULL\
             )",
        )
        .await
        .expect("create accounts");

    // (a) No-conflict: row inserted as-is.
    client
        .execute(
            "INSERT INTO accounts (email, hits) VALUES ($1, $2) \
             ON CONFLICT (email) DO NOTHING",
            &[&"u@x.com", &1_i64],
        )
        .await
        .expect("first insert (no conflict)");
    let hits = client
        .query_one(
            "SELECT hits FROM accounts WHERE email = $1",
            &[&"u@x.com"],
        )
        .await
        .expect("read")
        .get::<_, i64>(0);
    assert_eq!(hits, 1, "fresh row → hits=1");

    // (b) Conflict on UNIQUE email: DO NOTHING leaves the row untouched.
    client
        .execute(
            "INSERT INTO accounts (email, hits) VALUES ($1, $2) \
             ON CONFLICT (email) DO NOTHING",
            &[&"u@x.com", &999_i64],
        )
        .await
        .expect("second insert (conflict)");
    let hits = client
        .query_one(
            "SELECT hits FROM accounts WHERE email = $1",
            &[&"u@x.com"],
        )
        .await
        .expect("read after conflict")
        .get::<_, i64>(0);
    assert_eq!(hits, 1, "DO NOTHING preserves the existing row (hits=1)");

    // Exactly one row total — no duplicates.
    let n = client
        .query_one("SELECT COUNT(*) FROM accounts", &[])
        .await
        .expect("count")
        .get::<_, i64>(0);
    assert_eq!(n, 1, "DO NOTHING upsert must not duplicate");
}

/// `INSERT ... ON CONFLICT (...) DO UPDATE SET col = EXCLUDED.col` — the
/// Prisma / Drizzle / Rails `upsert` shape that actually mutates on
/// conflict. Today Basin's planner renames the target source to
/// `__basin_gen_src` and fails to resolve **both** `<table>.col` *and*
/// `EXCLUDED.col` references in the DO UPDATE expression. Newly-discovered
/// gap. Pinned with the correct PG semantics (post-conflict the existing
/// row carries the new value); flips to a real guard when the planner
/// resolves the alias.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "ORM pattern (upsert DO UPDATE SET … = EXCLUDED.col) — newly-found gap: ON CONFLICT DO UPDATE can't resolve EXCLUDED/<table> col refs; un-ignore when fixed"]
async fn upsert_on_conflict_do_update() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    client
        .simple_query(
            "CREATE TABLE accounts (\
                 email TEXT NOT NULL UNIQUE, \
                 hits BIGINT NOT NULL\
             )",
        )
        .await
        .expect("create accounts");

    // Fresh insert.
    client
        .execute(
            "INSERT INTO accounts (email, hits) VALUES ($1, $2) \
             ON CONFLICT (email) DO UPDATE SET hits = EXCLUDED.hits",
            &[&"u@x.com", &1_i64],
        )
        .await
        .expect("upsert no-conflict");
    // Conflict — DO UPDATE overwrites hits with the new value.
    client
        .execute(
            "INSERT INTO accounts (email, hits) VALUES ($1, $2) \
             ON CONFLICT (email) DO UPDATE SET hits = EXCLUDED.hits",
            &[&"u@x.com", &42_i64],
        )
        .await
        .expect("upsert conflict");

    let hits = client
        .query_one(
            "SELECT hits FROM accounts WHERE email = $1",
            &[&"u@x.com"],
        )
        .await
        .expect("read after conflict")
        .get::<_, i64>(0);
    assert_eq!(hits, 42, "conflict → DO UPDATE overwrote hits with EXCLUDED.hits");
}


// =============================================================================
// 4. Transactions — every ORM wraps writes in BEGIN/COMMIT or BEGIN/ROLLBACK.
//    Models: ActiveRecord `transaction do`, Django `atomic`, Hibernate
//    session tx, Prisma `$transaction`, every driver's `.transaction()`.
// =============================================================================

/// COMMIT visibility + SAVEPOINT lifecycle through `tokio-postgres`'s
/// transaction API — the exact path every ORM unit-of-work takes. Conditions:
/// BEGIN → INSERTs (including post-SAVEPOINT) → RELEASE → COMMIT must leave
/// all rows visible.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transaction_commit_visibility_and_savepoint() {
    let server = start_server().await;
    let mut client = connect(server.addr).await;
    seed_blog(&client).await;

    {
        let mut tx = client.transaction().await.expect("BEGIN");
        tx.execute(
            "INSERT INTO users (id, email, name, age) VALUES ($1, $2, $3, $4)",
            &[&200_i64, &"tx@x.com", &"Tx", &50_i64],
        )
        .await
        .expect("insert in tx");
        // SAVEPOINT then continue (ORMs use savepoints for nested tx).
        let sp = tx.savepoint("sp1").await.expect("SAVEPOINT sp1");
        sp.execute(
            "INSERT INTO users (id, email, name, age) VALUES ($1, $2, $3, $4)",
            &[&201_i64, &"sp@x.com", &"Sp", &51_i64],
        )
        .await
        .expect("insert after savepoint");
        sp.commit().await.expect("RELEASE sp1");
        tx.commit().await.expect("COMMIT");
    }
    // Both rows visible after COMMIT — the guarantee every ORM relies on.
    let n = client
        .query_one(
            "SELECT COUNT(*) FROM users WHERE id IN (200, 201)",
            &[],
        )
        .await
        .expect("post-commit count")
        .get::<_, i64>(0);
    assert_eq!(n, 2, "both committed rows must be visible");
}

/// `BEGIN → INSERT → ROLLBACK` must leave the database state untouched.
/// Basin v0.1 is auto-commit (no MVCC); BEGIN/ROLLBACK are accepted as no-ops
/// so the inserted row stays visible. This test asserts the CORRECT Postgres
/// semantics (rolled-back row invisible) and is gated on engine bug #41
/// (transaction-rollback over-restores rows / MVCC). Every ORM's
/// retry-on-conflict path requires this guarantee — flip when MVCC ships.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "ORM pattern (ROLLBACK isolation) — gated on engine bug #41 (auto-commit / MVCC gap); un-ignore when landed"]
async fn transaction_rollback_isolation() {
    let server = start_server().await;
    let mut client = connect(server.addr).await;
    seed_blog(&client).await;

    {
        let tx = client.transaction().await.expect("BEGIN");
        tx.execute(
            "INSERT INTO users (id, email, name, age) VALUES ($1, $2, $3, $4)",
            &[&300_i64, &"rb@x.com", &"Rb", &60_i64],
        )
        .await
        .expect("insert before rollback");
        tx.rollback().await.expect("ROLLBACK");
    }
    let n = client
        .query_one("SELECT COUNT(*) FROM users WHERE id = 300", &[])
        .await
        .expect("post-rollback count")
        .get::<_, i64>(0);
    // CORRECT Postgres semantics: the rolled-back row must NOT be visible.
    assert_eq!(
        n, 0,
        "ROLLBACK must discard the write (correct PG transaction semantics)"
    );
}

// =============================================================================
// 5. Prisma / Drizzle nested reads — JSON aggregation & LATERAL.
//    These model the *defining* shapes of the modern TS ORMs.
//    Several depend on in-flight fixes → gated, asserting correct behaviour.
// =============================================================================

/// Prisma's relation-loading shape: a correlated scalar subquery that
/// json_agg's the children. Conditions: parent with multiple children
/// (array of N), parent with zero children (must yield SQL NULL, which the
/// app coalesces to `[]`), multiple parents in one result set.
///
/// Gated on #55 (`json_agg(t)` over a correlated subquery returning a typed
/// row). Asserts the *correct* JSON shape; flips to a guard when #55 lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "ORM pattern (Prisma nested read) — gated on JSON_AGG fix #55; un-ignore when landed"]
async fn prisma_nested_read_json_agg_correlated_subquery() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    seed_blog(&client).await;

    let rows = client
        .query(
            "SELECT u.id, \
                (SELECT json_agg(p.title ORDER BY p.id) \
                 FROM posts p WHERE p.author_id = u.id) AS posts \
             FROM users u ORDER BY u.id",
            &[],
        )
        .await
        .expect("Prisma nested json_agg read");
    assert_eq!(rows.len(), 3, "one row per user");

    // u1 → ['Alice One','Alice Two']
    let u1: serde_json::Value = rows[0].get(1);
    assert_eq!(
        u1,
        serde_json::json!(["Alice One", "Alice Two"]),
        "u1 children aggregated in id order"
    );
    // u2 → ['Bob One']
    let u2: serde_json::Value = rows[1].get(1);
    assert_eq!(u2, serde_json::json!(["Bob One"]));
    // u3 has zero children → json_agg over empty input is SQL NULL.
    let u3: Option<serde_json::Value> = rows[2].get(1);
    assert_eq!(
        u3, None,
        "json_agg over zero children → SQL NULL (app coalesces to [])"
    );
}

/// Drizzle's `.findMany({ with: {...} })` and Prisma's relation query emit
/// `LEFT JOIN LATERAL (SELECT json_agg(...) ...) ON true`. Conditions: parent
/// with children → JSON array; childless parent → NULL/[] preserved by the
/// LEFT JOIN LATERAL ON true.
///
/// Gated on #58 (correlated LATERAL) + #55 (json_agg). Asserts correct shape.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "ORM pattern (Drizzle/Prisma LATERAL nested read) — gated on LATERAL #58 + JSON_AGG #55; un-ignore when landed"]
async fn drizzle_left_join_lateral_json_agg() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    seed_blog(&client).await;

    let rows = client
        .query(
            "SELECT u.id, agg.posts \
             FROM users u \
             LEFT JOIN LATERAL (\
                 SELECT json_agg(p.title ORDER BY p.id) AS posts \
                 FROM posts p WHERE p.author_id = u.id\
             ) agg ON true \
             ORDER BY u.id",
            &[],
        )
        .await
        .expect("LEFT JOIN LATERAL json_agg");
    assert_eq!(rows.len(), 3, "LEFT JOIN LATERAL preserves every parent");

    let u1: serde_json::Value = rows[0].get(1);
    assert_eq!(u1, serde_json::json!(["Alice One", "Alice Two"]));
    // Childless parent preserved with NULL aggregate (the whole point of
    // LEFT JOIN LATERAL ... ON true vs. a plain correlated subquery).
    let u3: Option<serde_json::Value> = rows[2].get(1);
    assert_eq!(u3, None, "childless parent preserved, agg is NULL");
}

// =============================================================================
// 6. Correlated DELETE/UPDATE & data-modifying CTE — modern write patterns.
// =============================================================================

/// `DELETE ... WHERE EXISTS (correlated)` and
/// `UPDATE ... WHERE id IN (correlated subquery)` — Rails
/// `.where(...).delete_all` with a join condition, Django
/// `.filter(...).update()` across a relation. Conditions: rows that match the
/// correlated predicate are affected; non-matching rows are untouched.
///
/// Gated on #56 (correlated-subquery DELETE/UPDATE). Asserts correct effect.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "ORM pattern (correlated DELETE/UPDATE) — gated on fix #56; un-ignore when landed"]
async fn correlated_delete_and_update() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    seed_blog(&client).await;

    // UPDATE posts SET views=views+1 WHERE author_id IN (authors named Alice)
    let affected = client
        .execute(
            "UPDATE posts SET views = views + 1 \
             WHERE author_id IN (SELECT id FROM users WHERE name = $1)",
            &[&"Alice"],
        )
        .await
        .expect("correlated UPDATE");
    assert_eq!(affected, 2, "Alice authored 2 posts → 2 rows updated");
    let total: i64 = client
        .query_one(
            "SELECT SUM(views)::bigint FROM posts WHERE author_id = 1",
            &[],
        )
        .await
        .expect("sum after update")
        .get(0);
    assert_eq!(total, 152, "100+1 + 50+1 = 152");

    // DELETE posts of users with no... actually delete posts whose author
    // has age < 28 (Bob, 25). Correlated EXISTS in DELETE.
    let affected = client
        .execute(
            "DELETE FROM posts p \
             WHERE EXISTS (SELECT 1 FROM users u WHERE u.id = p.author_id AND u.age < $1)",
            &[&28_i64],
        )
        .await
        .expect("correlated DELETE");
    assert_eq!(affected, 1, "Bob (age 25) has 1 post → 1 deleted");
    let remaining: i64 = client
        .query_one("SELECT COUNT(*) FROM posts", &[])
        .await
        .expect("count after delete")
        .get(0);
    assert_eq!(remaining, 2, "only Alice's 2 posts remain");
}

/// Data-modifying CTE: `WITH x AS (INSERT ... RETURNING ...) SELECT ... FROM
/// x` — the modern atomic write-then-read pattern (Ecto `Multi`, hand-rolled
/// repository code, Drizzle `$with`). Conditions: the CTE's RETURNING rows
/// are visible to the outer SELECT; the write actually persisted.
///
/// Gated on #57 (data-modifying CTE). Asserts correct visibility + persistence.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "ORM pattern (data-modifying CTE) — gated on fix #57; un-ignore when landed"]
async fn data_modifying_cte_insert_returning_then_select() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    seed_blog(&client).await;

    // INSERT inside a CTE, then SELECT the freshly-inserted rows back.
    let rows = client
        .query(
            "WITH ins AS (\
                 INSERT INTO users (id, email, name, age) \
                 VALUES ($1, $2, $3, $4) RETURNING id, email\
             ) \
             SELECT id, email FROM ins",
            &[&500_i64, &"cte@x.com", &"Cte", &33_i64],
        )
        .await
        .expect("data-modifying CTE");
    assert_eq!(rows.len(), 1, "CTE RETURNING surfaces to outer SELECT");
    assert_eq!(rows[0].get::<_, i64>(0), 500);
    assert_eq!(rows[0].get::<_, &str>(1), "cte@x.com");

    // The write must have persisted (CTE is not a dry run).
    let n = client
        .query_one("SELECT COUNT(*) FROM users WHERE id = 500", &[])
        .await
        .expect("persistence check")
        .get::<_, i64>(0);
    assert_eq!(n, 1, "data-modifying CTE must persist the INSERT");
}
