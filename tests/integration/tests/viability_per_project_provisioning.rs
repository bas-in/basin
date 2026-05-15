//! Viability benchmarks for the per-project pgwire connection-URL feature.
//!
//! Three claims this test makes measurable, dashboard-trackable, and fail-loud
//! on regression. All three skip cleanly when the local Postgres dev catalog
//! is unreachable — the existing pattern from `per_project_credentials.rs`.
//!
//! ## Claims
//!
//! 1. **Provision throughput** — `AuthService::provision_project_db` issues
//!    a fresh `(pgwire_user, password)` row + bcrypt-hashed password in
//!    under 50 ms p99 at the test bcrypt-cost (4). Production cost-12 is
//!    documented inline; this card tracks the test-cost number so a
//!    regression in the SQL / random-byte / URL-build path is caught.
//!
//! 2. **bcrypt validate cost on connect** — opening a fresh pgwire
//!    connection through the credential validator finishes in under
//!    300 ms p99 at test bcrypt-cost. The wedge customer's pool keeps
//!    this off the hot path; the number still matters because every
//!    cold connection pays it once.
//!
//! 3. **Cluster-by sort overhead** — Phase 5.7 B2 sorts every batch by
//!    the configured cluster columns before flushing to Parquet. This
//!    card asserts the sort costs at most 2× the no-cluster write
//!    throughput on a 10k-row batch — anything worse means the lexsort
//!    path got accidentally serialised.
//!
//! Each claim becomes its own JSON card under `benchmark/data/`, picked
//! up by `bundle.py` and rendered on the LocalFS dashboard.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use basin_auth::{config::SmtpConfig, config::SmtpTls, AuthConfig, AuthService};
use basin_common::{PartitionKey, TableName, ProjectId};
use basin_integration_tests::benchmark::{report_viability, BarOp, PrimaryMetric};
use basin_storage::{Storage, StorageConfig, WriteOptions};
use object_store::local::LocalFileSystem;
use serde_json::json;
use tempfile::TempDir;
use ulid::Ulid;

const PG_URL: &str = "postgres://postgres:postgres@127.0.0.1:5432/postgres";

async fn pg_alive() -> bool {
    matches!(
        tokio::time::timeout(
            Duration::from_secs(2),
            tokio_postgres::connect(PG_URL, tokio_postgres::NoTls),
        )
        .await,
        Ok(Ok(_))
    )
}

fn unique_schema() -> String {
    format!(
        "basin_viability_prov_{}",
        Ulid::new().to_string().to_lowercase()
    )
}

fn auth_cfg(schema: &str) -> AuthConfig {
    AuthConfig {
        jwt_secret: vec![9u8; 32],
        token_ttl: Duration::from_secs(60),
        refresh_ttl: Duration::from_secs(86_400),
        catalog_dsn: Some(PG_URL.to_owned()),
        catalog_schema: schema.to_owned(),
        smtp: SmtpConfig {
            host: "smtp.invalid".into(),
            port: 587,
            username: "u".into(),
            password: "p".into(),
            from_email: "noreply@example.com".into(),
            from_name: Some("Basin".into()),
            tls: SmtpTls::StartTls,
        },
        bcrypt_cost: 4,
        password_min_len: 10,
        rate_limit_per_ip_per_min: 100_000,
        email_enabled: true,
        pgwire_public_host: "127.0.0.1:5433".into(),
    }
}

async fn try_auth() -> Option<(AuthService, String)> {
    if !pg_alive().await {
        eprintln!("postgres unreachable, skipping viability_per_project_provisioning");
        return None;
    }
    let schema = unique_schema();
    let cfg = auth_cfg(&schema);
    let mailer = basin_auth::email::StubMailer::new(cfg.smtp.from_email.clone());
    let svc = AuthService::connect_with_mailer(cfg, Arc::new(mailer))
        .await
        .ok()?;
    Some((svc, schema))
}

// ---------------------------------------------------------------------------
// 1. Provision throughput
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn viability_per_project_provision_throughput() {
    let Some((auth, _schema)) = try_auth().await else {
        return;
    };

    const N: usize = 100;
    const BAR_P99_MS: f64 = 50.0;

    let mut samples_ms: Vec<f64> = Vec::with_capacity(N);
    for _ in 0..N {
        let project = ProjectId::new();
        let started = Instant::now();
        let _ = auth
            .provision_project_db(&project, None)
            .await
            .expect("provision");
        samples_ms.push(started.elapsed().as_secs_f64() * 1000.0);
    }
    samples_ms.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p50 = samples_ms[N / 2];
    let p99 = samples_ms[(N as f64 * 0.99) as usize];
    let mean = samples_ms.iter().sum::<f64>() / N as f64;

    let pass = p99 < BAR_P99_MS;
    println!(
        "[VIABILITY per_project_provision_throughput] N={N} \
         p50={p50:.2}ms p99={p99:.2}ms mean={mean:.2}ms (bar p99 < {BAR_P99_MS}ms) \
         {}",
        if pass { "PASS" } else { "FAIL" }
    );

    report_viability(
        "per_project_provision_throughput",
        "Per-project connection-URL provision latency",
        "Issuing a fresh pgwire (user, password) row + bcrypt hash + URL \
         finishes within p99 < 50 ms at test bcrypt-cost (4). Production \
         cost-12 is documented inline; this card tracks the test-cost \
         number so a regression in the SQL / random-byte / URL-build path \
         is caught. The bar is expressed against p99 across 100 \
         provisions; mean / p50 are reported as distribution context.",
        pass,
        PrimaryMetric {
            label: "p99 provision latency".into(),
            value: p99,
            unit: "ms".into(),
            bar: BarOp::lt(BAR_P99_MS),
        },
        json!({
            "n_samples": N,
            "p50_ms": p50,
            "p99_ms": p99,
            "mean_ms": mean,
            "bar_p99_ms": BAR_P99_MS,
            "bcrypt_cost": 4,
        }),
    );
    assert!(
        pass,
        "provision p99 {p99:.2}ms exceeds bar {BAR_P99_MS}ms (samples = {N})"
    );
}

// ---------------------------------------------------------------------------
// 2. bcrypt validate cost on connect
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn viability_per_project_bcrypt_validate_cost() {
    let Some((auth, _schema)) = try_auth().await else {
        return;
    };

    const N: usize = 50;
    const BAR_P99_MS: f64 = 300.0;

    // Provision once; reuse the same pgwire credentials across many
    // validations. This isolates the bcrypt-verify path from the
    // provision path.
    let project = ProjectId::new();
    let info = auth
        .provision_project_db(&project, None)
        .await
        .expect("provision");
    let user = info.pgwire_user.clone();
    let password = info.password_secret.clone();

    let mut samples_ms: Vec<f64> = Vec::with_capacity(N);
    for _ in 0..N {
        let started = Instant::now();
        let _ = auth
            .validate_pgwire_credentials(&user, &password)
            .await
            .expect("validate");
        samples_ms.push(started.elapsed().as_secs_f64() * 1000.0);
    }
    samples_ms.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p50 = samples_ms[N / 2];
    let p99 = samples_ms[(N as f64 * 0.99) as usize];
    let mean = samples_ms.iter().sum::<f64>() / N as f64;

    let pass = p99 < BAR_P99_MS;
    println!(
        "[VIABILITY per_project_bcrypt_validate_cost] N={N} \
         p50={p50:.2}ms p99={p99:.2}ms mean={mean:.2}ms (bar p99 < {BAR_P99_MS}ms) \
         {}",
        if pass { "PASS" } else { "FAIL" }
    );

    report_viability(
        "per_project_bcrypt_validate_cost",
        "bcrypt validation cost on pgwire connect",
        "Every cold pgwire connection pays one bcrypt-verify on the way \
         in. At test bcrypt-cost (4) p99 stays under 300 ms across 50 \
         validations. Production cost-12 will run ~50× higher; the \
         connection pool keeps this off the hot path for warm reuse. The \
         number we publish here tracks regressions in the validation \
         path itself, not in bcrypt's CPU envelope.",
        pass,
        PrimaryMetric {
            label: "p99 validate latency".into(),
            value: p99,
            unit: "ms".into(),
            bar: BarOp::lt(BAR_P99_MS),
        },
        json!({
            "n_samples": N,
            "p50_ms": p50,
            "p99_ms": p99,
            "mean_ms": mean,
            "bar_p99_ms": BAR_P99_MS,
            "bcrypt_cost": 4,
        }),
    );
    assert!(
        pass,
        "validate p99 {p99:.2}ms exceeds bar {BAR_P99_MS}ms (samples = {N})"
    );
}

// ---------------------------------------------------------------------------
// 3. Cluster-by sort overhead on insert
// ---------------------------------------------------------------------------

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

fn shuffled_batch(rows: usize) -> RecordBatch {
    let mut ids: Vec<i64> = (0..rows as i64).collect();
    // xorshift shuffle so the batch is unsorted on `id`. Cluster-by would
    // re-sort it; without cluster-by, ids land in shuffle order.
    let mut state = 0xCAFE_BABE_DEAD_BEEFu64;
    for i in (1..ids.len()).rev() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let j = (state as usize) % (i + 1);
        ids.swap(i, j);
    }
    let id_arr: Int64Array = ids.iter().copied().collect();
    let payloads: Vec<String> = (0..rows).map(|i| format!("p{i:08}")).collect();
    let payload_arr: StringArray = payloads.iter().map(|s| Some(s.as_str())).collect();
    RecordBatch::try_new(schema(), vec![Arc::new(id_arr), Arc::new(payload_arr)]).unwrap()
}

async fn write_with_options(
    storage: &Storage,
    project: &ProjectId,
    table: &TableName,
    batch: &RecordBatch,
    opts: &WriteOptions,
) -> f64 {
    let part = PartitionKey::default_key();
    let started = Instant::now();
    storage
        .write_batch_with_options(project, table, &part, batch, opts)
        .await
        .expect("write");
    started.elapsed().as_secs_f64() * 1000.0
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn viability_cluster_by_sort_overhead() {
    let dir = TempDir::new().unwrap();
    let object_store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let storage = Storage::new(StorageConfig {
        object_store,
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });

    let no_cluster = WriteOptions {
        bloom_filter_columns: vec![],
        max_row_group_size: None,
        cluster_columns: vec![],
    };
    let with_cluster = WriteOptions {
        bloom_filter_columns: vec![],
        max_row_group_size: None,
        cluster_columns: vec!["id".to_string()],
    };

    const ROWS: usize = 10_000;
    const N: usize = 5;
    const BAR_RATIO: f64 = 2.0;

    let mut no_cluster_ms: Vec<f64> = Vec::with_capacity(N);
    let mut with_cluster_ms: Vec<f64> = Vec::with_capacity(N);

    let project = ProjectId::new();
    for i in 0..N {
        // Different table per iteration so the writer doesn't merge into
        // a single Parquet file across runs.
        let table_a = TableName::new(format!("nc_{i}")).unwrap();
        let table_b = TableName::new(format!("cl_{i}")).unwrap();
        let batch = shuffled_batch(ROWS);
        no_cluster_ms
            .push(write_with_options(&storage, &project, &table_a, &batch, &no_cluster).await);
        with_cluster_ms
            .push(write_with_options(&storage, &project, &table_b, &batch, &with_cluster).await);
    }

    let mean = |xs: &[f64]| xs.iter().sum::<f64>() / xs.len() as f64;
    let nc_mean = mean(&no_cluster_ms);
    let cl_mean = mean(&with_cluster_ms);
    let ratio = if nc_mean > 0.0 {
        cl_mean / nc_mean
    } else {
        f64::INFINITY
    };

    let pass = ratio < BAR_RATIO;
    println!(
        "[VIABILITY cluster_by_sort_overhead] rows={ROWS} N={N} \
         no_cluster_mean={nc_mean:.2}ms cluster_mean={cl_mean:.2}ms \
         ratio={ratio:.2}x (bar < {BAR_RATIO}x) {}",
        if pass { "PASS" } else { "FAIL" }
    );

    report_viability(
        "cluster_by_sort_overhead",
        "Cluster-by physical sort overhead on insert",
        "Phase 5.7 B2 lexsorts every batch by the configured cluster \
         columns before Parquet flush. The bar is < 2× slowdown vs an \
         identical write with no cluster columns. Anything worse means \
         the sort path got accidentally serialised or is paying \
         per-row overhead instead of per-batch.",
        pass,
        PrimaryMetric {
            label: "cluster_mean / no_cluster_mean ratio".into(),
            value: ratio,
            unit: "x".into(),
            bar: BarOp::lt(BAR_RATIO),
        },
        json!({
            "rows_per_batch": ROWS,
            "n_iterations": N,
            "no_cluster_mean_ms": nc_mean,
            "cluster_mean_ms": cl_mean,
            "ratio_x": ratio,
            "bar_ratio_x": BAR_RATIO,
        }),
    );
    assert!(
        pass,
        "cluster_by overhead {ratio:.2}x exceeds bar {BAR_RATIO}x"
    );
}
