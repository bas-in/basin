//! Oracle 3 — **recorded golden answers from the incumbent engine**.
//!
//! See `docs/migration/df-removal/20-oracles.md`. Three oracles are available
//! to the DataFusion removal and they expire differently:
//!
//! | Oracle | Available | Answers the question |
//! |---|---|---|
//! | PostgreSQL differential | always | is Basin *correct*? |
//! | In-process shadow compare | until DataFusion is unlinked | do the two engines agree *now*? |
//! | **Recorded golden answers (this file)** | forever, once recorded | did the owned engine *change an answer Basin used to give*? |
//!
//! The third is the only one that survives removal, and it only exists if it is
//! recorded **before** removal. PostgreSQL cannot substitute for it: Basin has
//! known, deliberate divergences from PostgreSQL, plus a large surface
//! PostgreSQL has no opinion about at all — Vortex/Parquet file pruning, the
//! hot-tier + tombstone overlay, RLS predicate injection, the catalog surface.
//! A PostgreSQL-only oracle cannot tell "we broke it" from "we fixed it".
//!
//! # The two modes
//!
//! **Record** (the incumbent talking — DataFusion, `BASIN_OWNED_ENGINE` unset):
//!
//! ```text
//! BASIN_GOLDEN_RECORD=1 cargo test -p basin-integration-tests \
//!     --test golden_answers -- --ignored --nocapture record_golden_answers
//! ```
//!
//! **Compare** (any engine, no DataFusion required — it reads files, not a
//! second engine):
//!
//! ```text
//! BASIN_OWNED_ENGINE=1 cargo test -p basin-integration-tests \
//!     --test golden_answers -- --nocapture golden_answers_match
//! ```
//!
//! Compare mode runs by default with the incumbent too, where it must be green:
//! that is the harness proving it still describes the engine it was recorded
//! from.
//!
//! # Corpus (see `corpus()` for the per-suite rationale)
//!
//! * `base` — the 231-query routing corpus from
//!   `crates/basin-engine/tests/fallback_histogram.rs`, replayed verbatim and
//!   in order, plus the fixture DDL/DML that corpus runs against (whose command
//!   tags are answers too). Every shape there was checked against a live
//!   PostgreSQL 18.2 before being added. **That corpus is a *routing* corpus —
//!   it measures who serves a query, not what the query returns** — so the
//!   suites below exist to make the recorded answers discriminating.
//! * `scale` — the same feature areas over a 200-row deterministic fixture.
//!   The base fixture is 3–5 rows wide; an aggregate over 3 rows or a window
//!   over 4 can agree by coincidence.
//! * `storage` — multi-file scans with prunable predicates, over **both**
//!   Vortex (default) and Parquet, plus the hot-tier/tombstone overlay
//!   (UPDATE + DELETE + unflushed INSERT read back). PostgreSQL has no opinion
//!   about any of it.
//! * `rls` — row-level-security predicate injection across principals, an
//!   engine behaviour with no PostgreSQL-differential coverage here.
//! * `catalog` — the pg_catalog / information_schema surface.
//! * `errors` — **what the incumbent CANNOT do.** A rejection is an answer:
//!   "it used to error and now returns rows" is exactly the kind of silent
//!   change this oracle exists to catch.
//!
//! # Determinism (a golden file that encodes an accident is a trap)
//!
//! * The project id is a fixed ULID, not `ProjectId::new()`.
//! * Rows are sorted lexicographically unless the statement has a **top-level**
//!   `ORDER BY` (one inside a window spec, subquery or CTE does not order the
//!   result). Statements that are row-limited without a top-level `ORDER BY`
//!   carry a `CAVEAT` line: which rows come back is engine-defined.
//! * Every read-only statement is executed **twice** and its two outcomes
//!   compared. Anything that disagrees with itself is written as `EXCLUDED`
//!   with a reason rather than recorded flaky.
//! * Volatile shapes (`now()`, `random()`, `version()`, …) are listed in the
//!   corpus and written as `EXCLUDED` with a reason, so the exclusion is
//!   visible in the golden file rather than being a silent omission.
//! * Temp-dir paths, storage date partitions and ULID file names are redacted
//!   out of values and error text — a recorded error naming today's data file
//!   would stop matching tomorrow.
//! * Both tests run on tokio's **current-thread** runtime. Floating-point
//!   summation is not associative, so an aggregate split across a
//!   thread-count-dependent number of partitions can differ in the last digits
//!   between runs on the same data; `stddev_pop`/`var_pop` over the 200-row
//!   fixture demonstrably did.
//! * Shapes SQL does not determine at all — `array_agg`/`string_agg` without an
//!   in-aggregate `ORDER BY`, and row limits without a top-level `ORDER BY` —
//!   are excluded by policy, because recording them would freeze an
//!   implementation detail and fail every engine that chose differently.
//!
//! # File format
//!
//! One file per (suite, area), line-oriented so `git diff` points at the row
//! that moved. `\N` is NULL (an empty field is the empty string); values are
//! tab-separated with `\\`, `\t`, `\n`, `\r` escaped.

#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::util::display::{ArrayFormatter, FormatOptions};
use arrow_array::{Array, RecordBatch};
use arrow_schema::Schema;
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use futures::FutureExt;
use object_store::local::LocalFileSystem;
use tempfile::TempDir;
use ulid::Ulid;

// ── Golden file location ─────────────────────────────────────────────────────

fn golden_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("golden-answers")
}

/// A fixed project id. `ProjectId::new()` is a fresh ULID per run and leaks
/// into storage paths and some catalog answers; a constant removes that whole
/// class of per-run drift.
fn golden_project() -> ProjectId {
    ProjectId::from_ulid(Ulid::from_string("01JBAS1NGDMDEN00000000000A").expect("valid ULID"))
}

// ── Recorded shapes ──────────────────────────────────────────────────────────

#[derive(Clone, PartialEq, Eq, Debug)]
enum Outcome {
    /// A result set: `cols` are `(name, type, nullable)`; `rows` are formatted,
    /// escaped, tab-joined rows.
    Rows {
        cols: Vec<(String, String, bool)>,
        rows: Vec<String>,
    },
    /// DDL/DML with no result set — the Postgres-style command tag.
    Tag(String),
    /// The statement was rejected. This is an answer.
    Error(String),
    /// The statement aborted the task. Also an answer, and a worse one.
    Panic(String),
    /// Deliberately not recorded, with the reason. Never a silent omission.
    Excluded(String),
}

impl Outcome {
    fn status(&self) -> &'static str {
        match self {
            Outcome::Rows { .. } => "rows",
            Outcome::Tag(_) => "tag",
            Outcome::Error(_) => "error",
            Outcome::Panic(_) => "panic",
            Outcome::Excluded(_) => "excluded",
        }
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
struct Block {
    idx: usize,
    suite: String,
    area: String,
    sql: String,
    /// `sorted` (no top-level ORDER BY — rows sorted for determinism) or
    /// `as-returned` (the statement orders its own output).
    order: String,
    caveat: Option<String>,
    outcome: Outcome,
}

// ── Corpus ───────────────────────────────────────────────────────────────────

struct Stmt {
    area: &'static str,
    sql: String,
    /// Index into the suite's session list (RLS runs the same table as
    /// different principals).
    sess: usize,
    /// `Some(reason)` = do not execute, do not record; write the reason.
    exclude: Option<&'static str>,
}

fn st(area: &'static str, sql: impl Into<String>) -> Stmt {
    Stmt {
        area,
        sql: sql.into(),
        sess: 0,
        exclude: None,
    }
}

fn st_as(area: &'static str, sql: impl Into<String>, sess: usize) -> Stmt {
    Stmt {
        area,
        sql: sql.into(),
        sess,
        exclude: None,
    }
}

fn st_excluded(area: &'static str, sql: impl Into<String>, reason: &'static str) -> Stmt {
    Stmt {
        area,
        sql: sql.into(),
        sess: 0,
        exclude: Some(reason),
    }
}

// ── SQL shape helpers (determinism decisions) ────────────────────────────────

fn word_boundary(b: &[u8], start: usize, len: usize) -> bool {
    let before_ok = start == 0 || !(b[start - 1].is_ascii_alphanumeric() || b[start - 1] == b'_');
    let end = start + len;
    let after_ok = end >= b.len() || !(b[end].is_ascii_alphanumeric() || b[end] == b'_');
    before_ok && after_ok
}

/// Does `sql` contain any of `kws` at parenthesis depth 0, outside string
/// literals?
///
/// Depth matters. `SELECT id, row_number() OVER (ORDER BY id) FROM t` has an
/// `ORDER BY` but no defined output order — treating it as ordered would freeze
/// an accident of the incumbent's execution into the golden file and produce
/// false failures forever.
fn depth0_keyword(sql: &str, kws: &[&str]) -> bool {
    let lower = sql.to_ascii_lowercase();
    let lb = lower.as_bytes();
    let mut depth = 0i32;
    let mut in_str = false;
    let mut i = 0usize;
    while i < lb.len() {
        let c = lb[i];
        if in_str {
            if c == b'\'' {
                in_str = false;
            }
            i += 1;
            continue;
        }
        match c {
            b'\'' => in_str = true,
            b'(' => depth += 1,
            b')' => depth -= 1,
            _ => {
                if depth == 0 {
                    for k in kws {
                        if lower[i..].starts_with(k) && word_boundary(lb, i, k.len()) {
                            return true;
                        }
                    }
                }
            }
        }
        i += 1;
    }
    false
}

fn has_top_level_order_by(sql: &str) -> bool {
    depth0_keyword(sql, &["order by"])
}

/// A top-level row limiter that actually drops rows. `LIMIT ALL` is not one —
/// it returns everything, so the answer is fully determined once sorted.
fn has_row_limiter(sql: &str) -> bool {
    if depth0_keyword(sql, &["limit all"]) {
        return depth0_keyword(sql, &["offset", "fetch"]);
    }
    depth0_keyword(sql, &["limit", "offset", "fetch"])
}

/// Shapes whose answer SQL itself does not determine. Recording one of these
/// freezes an implementation detail of the incumbent, and every future engine
/// that chooses differently fails a test without having changed any *answer*.
/// So they are excluded by policy — visibly, in the golden file, with the
/// reason — rather than recorded and quietly trusted.
/// Statements the **incumbent itself** answers inconsistently, so there is no
/// answer to record. Keyed on exact SQL, because the base corpus is replayed
/// verbatim from `fallback_histogram.rs` and must not be edited here.
///
/// Each entry needs measured evidence, recorded next to it.
fn nondeterministic_in_incumbent(sql: &str) -> Option<&'static str> {
    match sql {
        // MEASURED: four consecutive runs of the *identical* test binary (no
        // recompilation between them) returned `2024-01-15` three times and
        // the literal `YYYY-MM-DD` once. The two in-process executions inside
        // any single run always agreed, so this is per-process, not per-query.
        // Consistent with there being three separate `to_char` registrations
        // in basin-engine (`udf.rs` ToCharPgUdf, `datetime_more_udf.rs`
        // ToCharMoreUdf, plus the name list in `sql_functions.rs`), reached
        // through a name-keyed registry that is collected into a Vec in
        // HashMap iteration order — which Rust randomises per process.
        // This is a finding, not a harness limitation: the incumbent has no
        // stable answer for this shape, so the owned engine cannot be held to
        // one.
        "SELECT to_char(day, 'YYYY-MM-DD') FROM d" => Some(
            "the INCUMBENT has no stable answer: 4 runs of one binary returned '2024-01-15' \
             three times and the format string 'YYYY-MM-DD' once — basin-engine registers two \
             different to_char UDFs under the same name",
        ),
        // MEASURED: `ORDER BY count(*) DESC` is not a TOTAL order here — three
        // of the four groups tie at count 1 ('b', 'c' and the NULL group), so
        // their relative order is engine-defined. Observed as
        // (\N, b, c) in one process and (b, c, \N) in another, same binary.
        // The two in-process executions agree with each other inside any one
        // run, so the self-consistency check cannot catch it; only recording
        // from two separate processes and diffing did.
        "SELECT name, count(*) FROM t GROUP BY name ORDER BY count(*) DESC" => Some(
            "ORDER BY is not a total order: three groups tie at count 1, and the tied rows came \
             back as (\\N, b, c) in one process and (b, c, \\N) in another",
        ),
        _ => None,
    }
}

fn undetermined_by_sql(sql: &str) -> Option<&'static str> {
    let lower = sql.to_ascii_lowercase();
    if (lower.contains("array_agg(") || lower.contains("string_agg("))
        && !lower.contains("order by")
    {
        return Some(
            "order-undefined: array_agg/string_agg without ORDER BY inside the aggregate has no \
             SQL-defined element order, so any recorded order is an implementation detail",
        );
    }
    if has_row_limiter(sql) && !has_top_level_order_by(sql) {
        return Some(
            "row-limited without a top-level ORDER BY: SQL does not define WHICH rows come back, \
             so any recorded set is an implementation detail",
        );
    }
    None
}

/// Statements that change state cannot be run twice for the self-consistency
/// check, and must be run exactly once in both record and compare mode.
fn is_read_only(sql: &str) -> bool {
    let head = sql.trim_start().split_whitespace().next().unwrap_or("");
    matches!(
        head.to_ascii_uppercase().as_str(),
        "SELECT" | "WITH" | "VALUES" | "TABLE" | "EXPLAIN" | "SHOW"
    ) && !depth0_keyword(sql, &["insert", "update", "delete"])
}

// ── Value formatting ─────────────────────────────────────────────────────────

fn esc(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            _ => out.push(c),
        }
    }
    out
}

/// Replace tokens that are fresh on every run but carry no answer:
///
/// * ULIDs — Basin names every data file with one, and file names surface in
///   error text (`Failed to read Vortex file: …/01KZYFP96ET7Y0BGF2ES37W9JD.vortex`).
/// * The `/YYYY/MM/DD/` date partition in storage paths, which is *today*.
///
/// Both were caught by recording twice in separate processes and diffing; both
/// would otherwise have made a recorded error text unmatchable tomorrow.
fn normalize_volatile_tokens(s: &str) -> String {
    let b: Vec<char> = s.chars().collect();
    let mut out = String::with_capacity(s.len());
    let mut i = 0usize;
    let crock = |c: char| c.is_ascii_digit() || (c.is_ascii_uppercase() && c != 'I' && c != 'L' && c != 'O' && c != 'U');
    let digit = |c: Option<&char>| c.is_some_and(|c| c.is_ascii_digit());
    while i < b.len() {
        // /YYYY/MM/DD/
        if b[i] == '/'
            && i + 11 < b.len()
            && digit(b.get(i + 1))
            && digit(b.get(i + 2))
            && digit(b.get(i + 3))
            && digit(b.get(i + 4))
            && b.get(i + 5) == Some(&'/')
            && digit(b.get(i + 6))
            && digit(b.get(i + 7))
            && b.get(i + 8) == Some(&'/')
            && digit(b.get(i + 9))
            && digit(b.get(i + 10))
            && b.get(i + 11) == Some(&'/')
        {
            out.push_str("/<DATE>/");
            i += 12;
            continue;
        }
        // A bare 26-character Crockford token = a ULID (file name, id, …).
        let boundary_before = i == 0 || !(b[i - 1].is_ascii_alphanumeric());
        if boundary_before && i + 26 <= b.len() && b[i..i + 26].iter().all(|c| crock(*c)) {
            let after_ok = i + 26 >= b.len() || !b[i + 26].is_ascii_alphanumeric();
            if after_ok {
                out.push_str("<ULID>");
                i += 26;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    out
}

fn format_rows(schema: &Schema, batches: &[RecordBatch]) -> (Vec<(String, String, bool)>, Vec<String>) {
    let cols: Vec<(String, String, bool)> = schema
        .fields()
        .iter()
        .map(|f| (f.name().clone(), format!("{:?}", f.data_type()), f.is_nullable()))
        .collect();

    let opts = FormatOptions::default();
    let mut rows = Vec::new();
    for b in batches {
        let fmts: Vec<Option<ArrayFormatter<'_>>> = b
            .columns()
            .iter()
            .map(|c| ArrayFormatter::try_new(c.as_ref(), &opts).ok())
            .collect();
        for r in 0..b.num_rows() {
            let mut cells: Vec<String> = Vec::with_capacity(b.num_columns());
            for (ci, col) in b.columns().iter().enumerate() {
                if col.is_null(r) {
                    cells.push("\\N".to_string());
                } else {
                    match &fmts[ci] {
                        Some(f) => cells.push(esc(&f.value(r).to_string())),
                        None => cells.push("<unformattable>".to_string()),
                    }
                }
            }
            rows.push(cells.join("\t"));
        }
    }
    (cols, rows)
}

// ── Engine plumbing ──────────────────────────────────────────────────────────

fn engine_in(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

/// Silence the default panic hook. Panicking statements are *expected* findings
/// here (the routing corpus has known panics), and the default hook's
/// `file:line` output would both drown the report and bake source line numbers
/// — which move whenever an unrelated crate is edited — into nothing useful.
fn silence_panics() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| std::panic::set_hook(Box::new(|_| {})));
}

fn panic_text(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else {
        "<non-string panic payload>".to_string()
    }
}

struct Runner {
    redact: Vec<(String, &'static str)>,
    blocks: Vec<Block>,
    /// Compare mode only: whether the owned engine served each statement.
    served: Vec<bool>,
}

impl Runner {
    fn new(redact: Vec<(String, &'static str)>) -> Self {
        Self {
            redact,
            blocks: Vec::new(),
            served: Vec::new(),
        }
    }

    fn scrub(&self, s: &str) -> String {
        let mut out = s.to_string();
        for (needle, replacement) in &self.redact {
            if !needle.is_empty() {
                out = out.replace(needle.as_str(), replacement);
            }
        }
        normalize_volatile_tokens(&out)
    }

    async fn exec_outcome(&self, sess: &ProjectSession, sql: &str, sort: bool) -> Outcome {
        let res = std::panic::AssertUnwindSafe(sess.execute(sql))
            .catch_unwind()
            .await;
        match res {
            Err(p) => Outcome::Panic(self.scrub(&panic_text(&p))),
            Ok(Err(e)) => Outcome::Error(self.scrub(&e.to_string())),
            Ok(Ok(ExecResult::Empty { tag })) => Outcome::Tag(self.scrub(&tag)),
            Ok(Ok(ExecResult::Rows { schema, batches })) => {
                let (cols, mut rows) = format_rows(&schema, &batches);
                let rows: Vec<String> = rows.drain(..).map(|r| self.scrub(&r)).collect();
                let mut rows = rows;
                if sort {
                    rows.sort();
                }
                Outcome::Rows { cols, rows }
            }
        }
    }

    async fn run_suite(
        &mut self,
        suite: &str,
        engine: &Engine,
        sessions: &[ProjectSession],
        stmts: &[Stmt],
    ) {
        for (i, s) in stmts.iter().enumerate() {
            let idx = i + 1;
            let ordered = has_top_level_order_by(&s.sql);
            let caveat = if !ordered && has_row_limiter(&s.sql) {
                Some(
                    "row-limited without a top-level ORDER BY: which rows are returned is \
                     engine-defined, not SQL-defined"
                        .to_string(),
                )
            } else {
                None
            };

            let outcome = match s.exclude.or_else(|| nondeterministic_in_incumbent(&s.sql)) {
                Some(reason) => {
                    self.served.push(false);
                    Outcome::Excluded(reason.to_string())
                }
                None => {
                    let before = engine.owned_engine_served_count();
                    let first = self
                        .exec_outcome(&sessions[s.sess], &s.sql, !ordered)
                        .await;
                    self.served
                        .push(engine.owned_engine_served_count() > before);
                    // An order-undefined shape only loses its answer when it
                    // returns ROWS. If it errors, the rejection is a perfectly
                    // deterministic answer and is kept — `SELECT id FROM t
                    // LIMIT -1` is a rejection today, and "it stopped being
                    // one" is exactly what this oracle should catch.
                    if let (Some(reason), Outcome::Rows { .. }) =
                        (undetermined_by_sql(&s.sql), &first)
                    {
                        self.blocks.push(Block {
                            idx,
                            suite: suite.to_string(),
                            area: s.area.to_string(),
                            sql: s.sql.clone(),
                            order: if ordered { "as-returned" } else { "sorted" }.to_string(),
                            caveat: caveat.clone(),
                            outcome: Outcome::Excluded(reason.to_string()),
                        });
                        continue;
                    }
                    if is_read_only(&s.sql) {
                        // Self-consistency: a statement that disagrees with
                        // itself inside one process cannot be a golden answer.
                        let second = self
                            .exec_outcome(&sessions[s.sess], &s.sql, !ordered)
                            .await;
                        if second != first {
                            Outcome::Excluded(
                                "non-deterministic: two consecutive in-process runs of this \
                                 statement disagreed"
                                    .to_string(),
                            )
                        } else {
                            first
                        }
                    } else {
                        first
                    }
                }
            };

            self.blocks.push(Block {
                idx,
                suite: suite.to_string(),
                area: s.area.to_string(),
                sql: s.sql.clone(),
                order: if ordered { "as-returned" } else { "sorted" }.to_string(),
                caveat,
                outcome,
            });
        }
    }
}

// ── Serialisation ────────────────────────────────────────────────────────────

fn slug(s: &str) -> String {
    let mut out = String::new();
    let mut dash = false;
    for c in s.chars() {
        if c.is_ascii_alphanumeric() {
            out.push(c.to_ascii_lowercase());
            dash = false;
        } else if !dash && !out.is_empty() {
            out.push('-');
            dash = true;
        }
    }
    out.trim_matches('-').to_string()
}

fn file_for(suite: &str, area: &str) -> String {
    format!("{}--{}.golden", slug(suite), slug(area))
}

fn serialize(blocks: &[&Block]) -> String {
    let mut s = String::new();
    for b in blocks {
        let _ = writeln!(s, "QUERY {:04}", b.idx);
        let _ = writeln!(s, "AREA {}", b.area);
        let _ = writeln!(s, "SQL {}", b.sql);
        let _ = writeln!(s, "ORDER {}", b.order);
        if let Some(c) = &b.caveat {
            let _ = writeln!(s, "CAVEAT {c}");
        }
        let _ = writeln!(s, "STATUS {}", b.outcome.status());
        match &b.outcome {
            Outcome::Rows { cols, rows } => {
                for (n, t, nullable) in cols {
                    let _ = writeln!(
                        s,
                        "COL {}\t{}\t{}",
                        n,
                        t,
                        if *nullable { "null" } else { "notnull" }
                    );
                }
                let _ = writeln!(s, "NROWS {}", rows.len());
                for r in rows {
                    if r.is_empty() {
                        let _ = writeln!(s, "ROW");
                    } else {
                        let _ = writeln!(s, "ROW {r}");
                    }
                }
            }
            Outcome::Tag(t) => {
                let _ = writeln!(s, "TAG {t}");
            }
            Outcome::Error(e) => {
                let _ = writeln!(s, "ERROR {}", esc(e));
            }
            Outcome::Panic(p) => {
                let _ = writeln!(s, "PANIC {}", esc(p));
            }
            Outcome::Excluded(r) => {
                let _ = writeln!(s, "EXCLUDED {}", esc(r));
            }
        }
        let _ = writeln!(s, "END");
        let _ = writeln!(s);
    }
    s
}

fn unesc(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut it = s.chars();
    while let Some(c) = it.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match it.next() {
            Some('\\') => out.push('\\'),
            Some('t') => out.push('\t'),
            Some('n') => out.push('\n'),
            Some('r') => out.push('\r'),
            Some(other) => {
                out.push('\\');
                out.push(other);
            }
            None => out.push('\\'),
        }
    }
    out
}

fn parse(text: &str, suite: &str, file: &str) -> Vec<Block> {
    let mut blocks = Vec::new();
    let mut cur: Option<Block> = None;
    let mut cols: Vec<(String, String, bool)> = Vec::new();
    let mut rows: Vec<String> = Vec::new();
    let mut nrows: Option<usize> = None;
    let mut status = String::new();

    for (lineno, line) in text.lines().enumerate() {
        let ctx = format!("{file}:{}", lineno + 1);
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (key, rest) = match line.split_once(' ') {
            Some((k, r)) => (k, r),
            None => (line, ""),
        };
        match key {
            "QUERY" => {
                cols.clear();
                rows.clear();
                nrows = None;
                status.clear();
                cur = Some(Block {
                    idx: rest.trim().parse().unwrap_or_else(|_| panic!("bad QUERY at {ctx}")),
                    suite: suite.to_string(),
                    area: String::new(),
                    sql: String::new(),
                    order: String::new(),
                    caveat: None,
                    outcome: Outcome::Excluded("unparsed".into()),
                });
            }
            "AREA" => cur.as_mut().expect(&ctx).area = rest.to_string(),
            "SQL" => cur.as_mut().expect(&ctx).sql = rest.to_string(),
            "ORDER" => cur.as_mut().expect(&ctx).order = rest.to_string(),
            "CAVEAT" => cur.as_mut().expect(&ctx).caveat = Some(rest.to_string()),
            "STATUS" => status = rest.to_string(),
            "COL" => {
                let mut p = rest.split('\t');
                let n = p.next().unwrap_or_default().to_string();
                let t = p.next().unwrap_or_default().to_string();
                let nullable = p.next().unwrap_or("null") == "null";
                cols.push((n, t, nullable));
            }
            "NROWS" => nrows = Some(rest.trim().parse().unwrap_or_else(|_| panic!("bad NROWS at {ctx}"))),
            "ROW" => rows.push(rest.to_string()),
            "TAG" => cur.as_mut().expect(&ctx).outcome = Outcome::Tag(rest.to_string()),
            "ERROR" => cur.as_mut().expect(&ctx).outcome = Outcome::Error(unesc(rest)),
            "PANIC" => cur.as_mut().expect(&ctx).outcome = Outcome::Panic(unesc(rest)),
            "EXCLUDED" => cur.as_mut().expect(&ctx).outcome = Outcome::Excluded(unesc(rest)),
            "END" => {
                let mut b = cur.take().unwrap_or_else(|| panic!("END without QUERY at {ctx}"));
                if status == "rows" {
                    assert_eq!(
                        Some(rows.len()),
                        nrows,
                        "NROWS disagrees with the ROW lines at {ctx} — golden file corrupt"
                    );
                    b.outcome = Outcome::Rows {
                        cols: std::mem::take(&mut cols),
                        rows: std::mem::take(&mut rows),
                    };
                }
                blocks.push(b);
            }
            other => panic!("unknown key {other:?} at {ctx}"),
        }
    }
    assert!(cur.is_none(), "unterminated block in {file}");
    blocks
}

// ── Divergence reporting ─────────────────────────────────────────────────────

/// One line per real difference, precise about *where*.
fn diff_block(expected: &Block, actual: &Block, served: bool) -> Vec<String> {
    let mut out = Vec::new();
    let mut note = |s: String| out.push(s);

    if expected.outcome.status() != actual.outcome.status() {
        note(format!(
            "STATUS: expected {}, actual {} | {}",
            expected.outcome.status(),
            actual.outcome.status(),
            short(&expected.outcome)
        ));
        note(format!("   expected: {}", short(&expected.outcome)));
        note(format!("   actual  : {}", short(&actual.outcome)));
        return finish(out, served);
    }

    match (&expected.outcome, &actual.outcome) {
        (
            Outcome::Rows {
                cols: ec,
                rows: er,
            },
            Outcome::Rows {
                cols: ac,
                rows: ar,
            },
        ) => {
            if ec.len() != ac.len() {
                note(format!(
                    "SCHEMA: expected {} columns {:?}, actual {} columns {:?}",
                    ec.len(),
                    ec.iter().map(|c| &c.0).collect::<Vec<_>>(),
                    ac.len(),
                    ac.iter().map(|c| &c.0).collect::<Vec<_>>()
                ));
            } else {
                for (i, (e, a)) in ec.iter().zip(ac.iter()).enumerate() {
                    if e.0 != a.0 {
                        note(format!("COLUMN {i} NAME: expected {:?}, actual {:?}", e.0, a.0));
                    }
                    if e.1 != a.1 {
                        note(format!(
                            "COLUMN {i} ({}) TYPE: expected {}, actual {}",
                            e.0, e.1, a.1
                        ));
                    }
                    if e.2 != a.2 {
                        note(format!(
                            "COLUMN {i} ({}) NULLABILITY: expected {}, actual {}",
                            e.0, e.2, a.2
                        ));
                    }
                }
            }
            if er.len() != ar.len() {
                note(format!(
                    "ROWCOUNT: expected {}, actual {}",
                    er.len(),
                    ar.len()
                ));
            }
            let mut es = er.clone();
            let mut as_ = ar.clone();
            es.sort();
            as_.sort();
            if es == as_ && er != ar {
                note(
                    "ROW ORDER: same rows, different order (statement has a top-level ORDER BY, \
                     so this is a real ordering difference)"
                        .to_string(),
                );
            }
            let mut shown = 0;
            for i in 0..er.len().max(ar.len()) {
                let e = er.get(i);
                let a = ar.get(i);
                if e != a {
                    if shown < 5 {
                        note(format!(
                            "ROW {i}: expected {}, actual {}",
                            e.map(|s| format!("{s:?}")).unwrap_or_else(|| "<missing>".into()),
                            a.map(|s| format!("{s:?}")).unwrap_or_else(|| "<missing>".into())
                        ));
                    }
                    shown += 1;
                }
            }
            if shown > 5 {
                note(format!("… and {} further differing rows", shown - 5));
            }
        }
        (Outcome::Tag(e), Outcome::Tag(a)) if e != a => {
            note(format!("TAG: expected {e:?}, actual {a:?}"));
        }
        (Outcome::Error(e), Outcome::Error(a)) if e != a => {
            note(format!("ERROR TEXT: expected {e:?}, actual {a:?}"));
        }
        (Outcome::Panic(e), Outcome::Panic(a)) if e != a => {
            note(format!("PANIC TEXT: expected {e:?}, actual {a:?}"));
        }
        (Outcome::Excluded(e), Outcome::Excluded(a)) if e != a => {
            note(format!("EXCLUSION REASON: expected {e:?}, actual {a:?}"));
        }
        _ => {}
    }
    finish(out, served)
}

fn finish(out: Vec<String>, served: bool) -> Vec<String> {
    if out.is_empty() {
        return out;
    }
    let mut v = out;
    v.push(format!(
        "   (served by: {})",
        if served { "OWNED engine" } else { "fallback / incumbent path" }
    ));
    v
}

fn short(o: &Outcome) -> String {
    match o {
        Outcome::Rows { cols, rows } => format!(
            "{} rows × {} cols{}",
            rows.len(),
            cols.len(),
            rows.first().map(|r| format!(" (first: {r:?})")).unwrap_or_default()
        ),
        Outcome::Tag(t) => format!("tag {t:?}"),
        Outcome::Error(e) => format!("error {e:?}"),
        Outcome::Panic(p) => format!("panic {p:?}"),
        Outcome::Excluded(r) => format!("excluded ({r})"),
    }
}

// ── The corpus ───────────────────────────────────────────────────────────────

/// The base fixture, copied verbatim from `fallback_histogram.rs` (that file is
/// read-only for this work). The DDL/DML statements are recorded too: an
/// `INSERT 0 3` command tag that becomes `INSERT 0 4` is a changed answer.
fn base_fixture() -> Vec<Stmt> {
    [
        "CREATE TABLE t (id BIGINT NOT NULL, name TEXT, amt DOUBLE PRECISION)",
        "INSERT INTO t VALUES (1,'a',1.5),(2,'b',2.5),(3,'c',3.5)",
        "INSERT INTO t VALUES (100,NULL,NULL)",
        "INSERT INTO t VALUES (101,'a',10.5)",
        "CREATE TABLE u (uid BIGINT NOT NULL, tid BIGINT, tag TEXT, n INTEGER)",
        "INSERT INTO u VALUES (10,1,'x',7),(11,1,'y',8),(12,2,'z',9)",
        "INSERT INTO u VALUES (13,NULL,'w',NULL)",
        "CREATE TABLE d (id BIGINT NOT NULL, day DATE, ts TIMESTAMP, flag BOOLEAN)",
        "INSERT INTO d VALUES (1,'2024-01-15','2024-01-15 10:30:00',true),(2,'2024-06-30','2024-06-30 23:59:59',false)",
        "CREATE TABLE e (id BIGINT NOT NULL, n NUMERIC(10,2), iv INTERVAL, tz TIMESTAMPTZ, tags TEXT[], big BIGINT, small SMALLINT)",
        "INSERT INTO e VALUES (1, 12345.67, INTERVAL '1 day 2 hours', TIMESTAMPTZ '2024-01-15 10:30:00+00', ARRAY['x','y'], 9223372036854775807, 32767), (2, -12345.67, INTERVAL '-3 days', TIMESTAMPTZ '2024-06-30 23:59:59+00', ARRAY[]::TEXT[], -9223372036854775807, -32768), (3, 0.00, INTERVAL '0', TIMESTAMPTZ '2024-01-01 00:00:00+00', NULL, 0, 0)",
        "CREATE TABLE mb (id BIGINT NOT NULL, s TEXT)",
        "INSERT INTO mb VALUES (1,'héllo'), (2,'日本語'), (3,'🎉party🎉'), (4,'naïve café')",
        "CREATE TABLE p (id BIGINT PRIMARY KEY, val INTEGER NOT NULL DEFAULT 0, tag TEXT)",
        "INSERT INTO p VALUES (1,10,'a'),(2,20,'b'),(3,30,'c')",
    ]
    .into_iter()
    .map(|s| st("Fixture", s))
    .collect()
}

/// The 231-query routing corpus, verbatim and in order.
fn base_queries() -> Vec<Stmt> {
    let queries: &[(&str, &str)] = &[
        ("Basics", "SELECT id FROM t"),
        ("Basics", "SELECT DISTINCT name FROM t"),
        ("Basics", "SELECT generate_series(1,3)"),
        ("Basics", "SELECT day, ts FROM d"),
        ("Basics", "SELECT * FROM (VALUES (1,'a'),(2,'b')) AS v(i, s)"),
        ("Predicates", "SELECT id FROM t WHERE id > 1"),
        ("Predicates", "SELECT id FROM t WHERE name LIKE 'a%'"),
        ("Predicates", "SELECT id FROM t WHERE id IN (1,2)"),
        ("Predicates", "SELECT id FROM t WHERE amt BETWEEN 1.0 AND 3.0"),
        ("Predicates", "SELECT id FROM t WHERE name IS NOT NULL"),
        ("Predicates", "SELECT id FROM t WHERE name IS DISTINCT FROM 'a'"),
        ("Predicates", "SELECT id FROM d WHERE flag"),
        ("Predicates", "SELECT id FROM t WHERE id >= 2"),
        ("Predicates", "SELECT id FROM t WHERE id != 2"),
        ("Predicates", "SELECT id FROM t WHERE id <> 2"),
        ("Predicates", "SELECT id FROM t WHERE name ILIKE 'A%'"),
        ("Predicates", "SELECT id FROM t WHERE name NOT LIKE 'a%'"),
        ("Predicates", "SELECT id FROM t WHERE name NOT ILIKE 'A%'"),
        ("Predicates", "SELECT id FROM t WHERE id IS NULL"),
        ("Predicates", "SELECT id FROM t WHERE name IS NOT DISTINCT FROM 'a'"),
        ("Predicates", "SELECT id FROM t WHERE id NOT BETWEEN 1 AND 2"),
        ("Predicates", "SELECT id FROM t WHERE id > 1 AND name = 'b'"),
        ("Predicates", "SELECT id FROM t WHERE id > 1 OR name = 'a'"),
        ("Predicates", "SELECT id FROM t WHERE NOT (id > 1)"),
        ("Predicates", "SELECT id FROM t WHERE id NOT IN (1,2)"),
        ("Predicates", "SELECT id FROM t WHERE (id > 1 AND name IS NOT NULL) OR id = 1"),
        ("Predicates", "SELECT id FROM t WHERE amt > ANY (SELECT amt FROM t WHERE id < 3)"),
        ("Predicates", "SELECT id FROM t WHERE amt > ALL (SELECT amt FROM t WHERE id < 2)"),
        ("Predicates", "SELECT id FROM t WHERE amt = SOME (SELECT amt FROM t)"),
        ("Predicates", "SELECT id FROM t WHERE id::TEXT LIKE '%2%'"),
        ("NULL semantics", "SELECT (NULL AND FALSE) IS NULL"),
        ("NULL semantics", "SELECT (NULL OR TRUE) AS x"),
        ("NULL semantics", "SELECT (NOT NULL) IS NULL"),
        ("NULL semantics", "SELECT NULL = NULL"),
        ("NULL semantics", "SELECT count(*), count(name), count(amt) FROM t"),
        ("NULL semantics", "SELECT sum(amt) FROM t WHERE name IS NULL"),
        ("NULL semantics", "SELECT DISTINCT name FROM t ORDER BY name"),
        ("NULL semantics", "SELECT name, count(*) FROM t GROUP BY name ORDER BY name NULLS FIRST"),
        ("NULL semantics", "SELECT id FROM t ORDER BY name NULLS FIRST"),
        ("NULL semantics", "SELECT id FROM t ORDER BY name ASC NULLS FIRST, id DESC"),
        ("NULL semantics", "SELECT id FROM t WHERE id IN (1, NULL)"),
        ("NULL semantics", "SELECT id FROM t WHERE id NOT IN (SELECT tid FROM u)"),
        ("NULL semantics", "SELECT COALESCE(NULL, NULL, 3)"),
        ("Aggregates", "SELECT count(*) FROM t"),
        ("Aggregates", "SELECT name, sum(amt) FROM t GROUP BY name"),
        ("Aggregates", "SELECT name, count(*) FROM t GROUP BY name HAVING count(*) > 0"),
        ("Aggregates", "SELECT sum(amt) FILTER (WHERE id > 1) FROM t"),
        ("Aggregates", "SELECT min(amt), max(amt), avg(amt) FROM t"),
        ("Aggregates", "SELECT count(DISTINCT name) FROM t"),
        ("Aggregates", "SELECT name, count(*) FROM t GROUP BY name ORDER BY count(*) DESC"),
        ("Aggregates", "SELECT tid, count(*) FROM u GROUP BY tid HAVING count(*) > 1"),
        ("Aggregates", "SELECT string_agg(name, ',') FROM t"),
        ("Aggregates", "SELECT array_agg(id) FROM t"),
        ("Aggregates", "SELECT variance(amt), stddev(amt) FROM t"),
        ("Aggregates", "SELECT var_pop(amt), var_samp(amt) FROM t"),
        ("Aggregates", "SELECT stddev_pop(amt), stddev_samp(amt) FROM t"),
        ("Aggregates", "SELECT bool_and(flag), bool_or(flag) FROM d"),
        ("Aggregates", "SELECT every(flag) FROM d"),
        ("Aggregates", "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY amt) FROM t"),
        ("Aggregates", "SELECT mode() WITHIN GROUP (ORDER BY name) FROM t"),
        ("Aggregates", "SELECT sum(amt), count(*), avg(amt) FROM t WHERE id > 1000"),
        ("Aggregates", "SELECT max(id) FROM t WHERE id > 1000"),
        ("Aggregates", "SELECT count(DISTINCT name) FILTER (WHERE id > 1) FROM t"),
        ("Aggregates", "SELECT tid, tag, count(*) FROM u GROUP BY ROLLUP (tid, tag)"),
        ("Aggregates", "SELECT tid, count(*) FROM u GROUP BY CUBE (tid)"),
        ("Aggregates", "SELECT tid, tag, count(*) FROM u GROUP BY GROUPING SETS ((tid), (tag), ())"),
        ("Aggregates", "SELECT name, sum(amt) FROM t GROUP BY name HAVING sum(amt) > 1 AND count(*) >= 1"),
        ("Aggregates", "SELECT min(name), max(name) FROM t"),
        ("Aggregates", "SELECT sum(id) FILTER (WHERE amt IS NOT NULL) FROM t"),
        ("Aggregates", "SELECT array_agg(DISTINCT name) FROM t"),
        ("Aggregates", "SELECT string_agg(DISTINCT name, ',') FROM t"),
        ("Windows", "SELECT id, row_number() OVER (ORDER BY id) FROM t"),
        ("Windows", "SELECT id, lag(id) OVER (ORDER BY id) FROM t"),
        ("Windows", "SELECT tid, n, rank() OVER (PARTITION BY tid ORDER BY n) FROM u"),
        ("Windows", "SELECT tid, sum(n) OVER (PARTITION BY tid) FROM u"),
        ("Windows", "SELECT n, sum(n) OVER (ORDER BY n ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM u"),
        ("Windows", "SELECT n, first_value(n) OVER (PARTITION BY tid ORDER BY n) FROM u"),
        ("Windows", "SELECT n, ntile(2) OVER (ORDER BY n) FROM u"),
        ("Windows", "SELECT n, percent_rank() OVER (ORDER BY n) FROM u"),
        ("Windows", "SELECT n, cume_dist() OVER (ORDER BY n) FROM u"),
        ("Windows", "SELECT n, lead(n, 1, 0) OVER (ORDER BY n) FROM u"),
        ("Windows", "SELECT n, last_value(n) OVER (PARTITION BY tid ORDER BY n ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM u"),
        ("Windows", "SELECT n, nth_value(n, 2) OVER (PARTITION BY tid ORDER BY n) FROM u"),
        ("Windows", "SELECT n, dense_rank() OVER (ORDER BY n) FROM u"),
        ("Windows", "SELECT n, sum(n) OVER (ORDER BY n RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM u"),
        ("Windows", "SELECT n, avg(n) OVER w FROM u WINDOW w AS (PARTITION BY tid ORDER BY n)"),
        ("Windows", "SELECT n, sum(n) OVER (PARTITION BY tid ORDER BY n ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM u"),
        ("Windows", "SELECT n, count(*) OVER () FROM u"),
        ("Windows", "SELECT n, sum(n) OVER (ORDER BY n GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM u"),
        ("Joins", "SELECT a.id FROM t a JOIN t b ON a.id = b.id"),
        ("Joins", "SELECT a.id FROM t a LEFT JOIN t b ON a.id = b.id"),
        ("Joins", "SELECT t.id, u.tag FROM t JOIN u ON u.tid = t.id"),
        ("Joins", "SELECT t.id, u.tag FROM t RIGHT JOIN u ON u.tid = t.id"),
        ("Joins", "SELECT t.id, u.tag FROM t FULL JOIN u ON u.tid = t.id"),
        ("Joins", "SELECT t.id, u.tag FROM t CROSS JOIN u"),
        ("Joins", "SELECT t.id FROM t JOIN u ON u.tid = t.id AND u.n > 7"),
        ("Joins", "SELECT t.id FROM t JOIN u ON u.tid = t.id WHERE u.tag <> 'x'"),
        ("Joins", "SELECT t.id, u.tag, d.flag FROM t JOIN u ON u.tid = t.id JOIN d ON d.id = t.id"),
        ("Joins", "SELECT t.id, g.i FROM t, LATERAL generate_series(1, 2) AS g(i)"),
        ("Joins", "SELECT t.id, d.day FROM t JOIN d USING (id)"),
        ("Joins", "SELECT t.id, d.day FROM t NATURAL JOIN d"),
        ("Joins", "SELECT t.id, u.n FROM t JOIN u ON t.id < u.n"),
        ("Joins", "SELECT t.id, u.tag FROM t LEFT JOIN u ON u.tid = t.id WHERE u.tid IS NULL"),
        ("Joins", "SELECT t.id, u.tag FROM t JOIN u ON t.id IS NOT DISTINCT FROM u.tid"),
        ("Joins", "SELECT u.uid FROM u WHERE u.tid IS NULL"),
        ("Joins", "SELECT a.id, b.id FROM t a JOIN t b ON a.id <> b.id AND a.id < b.id"),
        ("Joins", "SELECT t.id, u.tag, d.flag FROM t LEFT JOIN u ON u.tid = t.id LEFT JOIN d ON d.id = t.id"),
        ("Joins", "SELECT t.id, u.tag FROM t, u WHERE t.id = u.tid"),
        ("Joins", "SELECT count(*) FROM t CROSS JOIN d"),
        ("Joins", "SELECT t.name, sum(u.n) FROM t JOIN u ON u.tid = t.id GROUP BY t.name"),
        ("Subqueries", "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM t u WHERE u.id = t.id)"),
        ("Subqueries", "SELECT id FROM t WHERE id = (SELECT max(id) FROM t)"),
        ("Subqueries", "SELECT id FROM t WHERE NOT EXISTS (SELECT 1 FROM u WHERE u.tid = t.id)"),
        ("Subqueries", "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.tid = t.id AND u.n > 7)"),
        ("Subqueries", "SELECT id FROM t WHERE id IN (SELECT tid FROM u)"),
        ("Subqueries", "SELECT id FROM t WHERE id NOT IN (SELECT tid FROM u WHERE tid IS NOT NULL)"),
        ("Subqueries", "SELECT id, (SELECT count(*) FROM u WHERE u.tid = t.id) FROM t"),
        ("Subqueries", "SELECT id FROM t WHERE amt > (SELECT avg(amt) FROM t)"),
        ("Subqueries", "SELECT id, (SELECT name FROM t t2 WHERE t2.id = t.id + 1) FROM t"),
        ("Subqueries", "SELECT * FROM (SELECT id, name FROM t WHERE id > 1) AS sub"),
        ("Subqueries", "SELECT id FROM t WHERE amt > ALL (SELECT n FROM u WHERE u.tid = t.id)"),
        ("Subqueries", "SELECT id FROM (SELECT id, row_number() OVER (ORDER BY id) rn FROM t) x WHERE rn = 1"),
        ("Subqueries", "SELECT t.id FROM t WHERE t.id = ANY (SELECT tid FROM u WHERE tid IS NOT NULL)"),
        ("Subqueries", "SELECT (SELECT count(*) FROM t) AS total"),
        ("Subqueries", "SELECT id FROM t t1 WHERE EXISTS (SELECT 1 FROM t t2 WHERE t2.amt > t1.amt)"),
        ("Set operations", "SELECT id FROM t UNION SELECT id FROM t"),
        ("Set operations", "SELECT id FROM t EXCEPT SELECT id FROM t"),
        ("Set operations", "SELECT id FROM t UNION ALL SELECT tid FROM u"),
        ("Set operations", "SELECT id FROM t INTERSECT SELECT tid FROM u"),
        ("Set operations", "SELECT id FROM t UNION SELECT tid FROM u ORDER BY 1"),
        ("Set operations", "SELECT id FROM t UNION ALL SELECT id FROM t ORDER BY id LIMIT 3"),
        ("Set operations", "SELECT tid FROM u INTERSECT ALL SELECT tid FROM u"),
        ("Set operations", "SELECT id FROM t EXCEPT ALL SELECT tid FROM u"),
        ("Set operations", "(SELECT id FROM t ORDER BY id LIMIT 2) UNION (SELECT tid FROM u ORDER BY tid LIMIT 2)"),
        ("Set operations", "SELECT id FROM t WHERE id < 3 UNION SELECT id FROM t WHERE id > 1"),
        ("Set operations", "SELECT id::TEXT FROM t UNION SELECT name FROM t"),
        ("CTEs", "WITH x AS (SELECT id FROM t) SELECT id FROM x"),
        ("CTEs", "WITH a AS (SELECT id FROM t), b AS (SELECT tid FROM u) SELECT a.id FROM a JOIN b ON b.tid = a.id"),
        ("CTEs", "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 5) SELECT n FROM r"),
        ("CTEs", "WITH a AS (SELECT id FROM t), b AS (SELECT id FROM a WHERE id > 1) SELECT * FROM b"),
        ("CTEs", "WITH x(a, b) AS (SELECT id, name FROM t) SELECT a, b FROM x"),
        ("CTEs", "WITH a AS (SELECT id FROM t), b AS (SELECT tid AS id FROM u), c AS (SELECT id FROM a UNION SELECT id FROM b) SELECT * FROM c"),
        ("CTEs", "WITH RECURSIVE r AS (SELECT id, name FROM t WHERE id = 1 UNION ALL SELECT t.id, t.name FROM t JOIN r ON t.id = r.id + 1) SELECT * FROM r"),
        ("CTEs", "WITH a AS MATERIALIZED (SELECT id FROM t) SELECT * FROM a"),
        ("CTEs", "WITH a AS NOT MATERIALIZED (SELECT id FROM t) SELECT * FROM a"),
        ("CTEs", "WITH a AS (SELECT id FROM t) SELECT (SELECT count(*) FROM a)"),
        ("Expressions", "SELECT upper(name) FROM t"),
        ("Expressions", "SELECT name || '!' FROM t"),
        ("Expressions", "SELECT CASE WHEN id > 1 THEN 'big' ELSE 'small' END FROM t"),
        ("Expressions", "SELECT COALESCE(name, 'none'), NULLIF(id, 1) FROM t"),
        ("Expressions", "SELECT GREATEST(id, 2), LEAST(id, 2) FROM t"),
        ("Expressions", "SELECT id::TEXT, amt::INTEGER FROM t"),
        ("Expressions", "SELECT substring(name FROM 1 FOR 1), length(name), trim(name) FROM t"),
        ("Expressions", "SELECT replace(name, 'a', 'A'), position('a' IN name) FROM t"),
        ("Expressions", "SELECT extract(YEAR FROM day) FROM d"),
        ("Expressions", "SELECT date_trunc('month', ts) FROM d"),
        ("Expressions", "SELECT day + INTERVAL '10 days' FROM d"),
        ("Expressions", "SELECT CASE id WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'many' END FROM t"),
        ("Expressions", "SELECT CASE WHEN id = 1 THEN 'a' WHEN id = 2 THEN 'b' ELSE 'c' END FROM t"),
        ("Expressions", "SELECT id % 2 FROM t"),
        ("Expressions", "SELECT id ^ 2 FROM t"),
        ("Expressions", "SELECT amt + id * 2 - 1 FROM t"),
        ("Expressions", "SELECT id::FLOAT8 / 2 FROM t"),
        ("Expressions", "SELECT lower(name), upper(name), initcap(name) FROM t"),
        ("Expressions", "SELECT lpad(name, 5, '*'), rpad(name, 5, '*') FROM t"),
        ("Expressions", "SELECT split_part('a,b,c', ',', 2)"),
        ("Expressions", "SELECT left(name, 1), right(name, 1) FROM t"),
        ("Expressions", "SELECT repeat(name, 2) FROM t"),
        ("Expressions", "SELECT name ~ '^a' FROM t"),
        ("Expressions", "SELECT name ~* '^A' FROM t"),
        ("Expressions", "SELECT name SIMILAR TO 'a%' FROM t"),
        ("Expressions", "SELECT ROW(id, name) FROM t"),
        ("Expressions", "SELECT (id, name) = (1, 'a') FROM t"),
        ("Expressions", "SELECT ARRAY[1,2,3]"),
        ("Expressions", "SELECT ARRAY['x','y']"),
        ("Expressions", "SELECT 1 = ANY(ARRAY[1,2,3])"),
        ("Expressions", "SELECT extract(EPOCH FROM ts) FROM d"),
        ("Expressions", "SELECT extract(DOW FROM day) FROM d"),
        ("Expressions", "SELECT date_part('month', day) FROM d"),
        ("Expressions", "SELECT age(ts, '2024-01-01'::timestamp) FROM d"),
        ("Expressions", "SELECT ts - INTERVAL '1 hour' FROM d"),
        ("Expressions", "SELECT day - INTERVAL '1 day' FROM d"),
        ("Expressions", "SELECT to_char(day, 'YYYY-MM-DD') FROM d"),
        ("Expressions", "SELECT CURRENT_DATE = CURRENT_DATE"),
        ("Expressions", "SELECT INTERVAL '1 day' + INTERVAL '2 hours'"),
        ("Expressions", "SELECT INTERVAL '1 month' > INTERVAL '1 day'"),
        ("Ordering/Pagination", "SELECT id, name FROM t ORDER BY id LIMIT 2"),
        ("Ordering/Pagination", "SELECT id FROM t LIMIT 2 OFFSET 1"),
        ("Ordering/Pagination", "SELECT id FROM t ORDER BY amt DESC NULLS LAST"),
        ("Ordering/Pagination", "SELECT id FROM t ORDER BY amt / 2"),
        ("Ordering/Pagination", "SELECT DISTINCT ON (tid) tid, n FROM u ORDER BY tid, n"),
        ("Ordering/Pagination", "SELECT id FROM t ORDER BY id DESC, name ASC"),
        ("Ordering/Pagination", "SELECT id FROM t ORDER BY 1 DESC"),
        ("Ordering/Pagination", "SELECT id FROM t LIMIT ALL"),
        ("Ordering/Pagination", "SELECT id FROM t OFFSET 1"),
        ("Ordering/Pagination", "SELECT id FROM t FETCH FIRST 2 ROWS ONLY"),
        ("Ordering/Pagination", "SELECT id FROM t ORDER BY id FETCH FIRST 1 ROW WITH TIES"),
        ("Ordering/Pagination", "SELECT DISTINCT ON (tid, tag) tid, tag, n FROM u ORDER BY tid, tag, n"),
        ("DML", "INSERT INTO t VALUES (4,'d',4.5)"),
        ("DML", "INSERT INTO t SELECT 5, 'e', 5.5"),
        ("DML", "UPDATE t SET amt = amt + 1 WHERE id = 1"),
        ("DML", "DELETE FROM t WHERE id = 5"),
        ("DML", "INSERT INTO t VALUES (6,'f',6.5) RETURNING id"),
        ("DML", "INSERT INTO p VALUES (4,40,'d') ON CONFLICT (id) DO NOTHING"),
        ("DML", "INSERT INTO p VALUES (1,99,'z') ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val"),
        ("DML", "UPDATE p SET val = val + 1 WHERE id = 1 RETURNING id, val"),
        ("DML", "DELETE FROM p WHERE id = 2 RETURNING *"),
        ("DML", "UPDATE p SET val = u.n FROM u WHERE p.id = u.tid AND u.n IS NOT NULL"),
        ("DML", "DELETE FROM p USING u WHERE p.id = u.tid AND u.n > 100"),
        ("DML", "INSERT INTO p SELECT id, amt::INTEGER, name FROM t WHERE id > 100"),
        ("DML", "INSERT INTO p (id, tag) VALUES (5, 'e')"),
        ("DML", "INSERT INTO t (id, name) VALUES (7, 'g')"),
        ("DML", "UPDATE t SET name = upper(name) WHERE id IN (SELECT tid FROM u)"),
        ("Literals/Types", "SELECT 9223372036854775807::BIGINT"),
        ("Literals/Types", "SELECT -9223372036854775807::BIGINT"),
        ("Literals/Types", "SELECT 2147483647::INTEGER"),
        ("Literals/Types", "SELECT 'Infinity'::DOUBLE PRECISION"),
        ("Literals/Types", "SELECT '-Infinity'::DOUBLE PRECISION"),
        ("Literals/Types", "SELECT 'NaN'::DOUBLE PRECISION"),
        ("Literals/Types", "SELECT 0.0 = -0.0"),
        ("Literals/Types", "SELECT true, false, NULL::BOOLEAN"),
        ("Literals/Types", "SELECT s FROM mb WHERE s = 'héllo'"),
        ("Literals/Types", "SELECT length(s), char_length(s) FROM mb"),
        ("Literals/Types", "SELECT upper(s) FROM mb"),
        ("Literals/Types", "SELECT n, iv, tz, tags, big, small FROM e"),
        ("Literals/Types", "SELECT n FROM e WHERE tags IS NULL"),
        ("Literals/Types", "SELECT tags[1] FROM e WHERE id = 1"),
        ("Literals/Types", "SELECT array_length(tags, 1) FROM e WHERE id = 1"),
        ("Literals/Types", "SELECT big + 0 FROM e"),
        ("Literals/Types", "SELECT '2024-01-15'::DATE + 1"),
        ("Literals/Types", "SELECT '{1,2,3}'::INTEGER[]"),
    ];
    queries.iter().map(|(a, q)| st(a, *q)).collect()
}

/// 200 deterministic rows. No clock, no RNG: every value is a function of `id`.
fn big_rows(from: i64, to: i64) -> String {
    let mut tuples = Vec::new();
    for id in from..=to {
        let grp = ["alpha", "beta", "gamma", "delta"][(id % 4) as usize];
        let k = if id % 17 == 0 {
            "NULL".to_string()
        } else {
            ((id * 7) % 13).to_string()
        };
        let v = if id % 23 == 0 {
            "NULL".to_string()
        } else {
            format!("{:.2}", id as f64 / 4.0)
        };
        let flag = if id % 3 == 0 { "true" } else { "false" };
        tuples.push(format!("({id},'{grp}',{k},{v},{flag})"));
    }
    tuples.join(",")
}

fn scale_suite() -> Vec<Stmt> {
    let mut v = vec![st(
        "Fixture",
        "CREATE TABLE big (id BIGINT NOT NULL, grp TEXT, k INTEGER, v DOUBLE PRECISION, flag BOOLEAN)",
    )];
    // Four INSERTs, not one: four files gives the multi-file scan path
    // something to merge and the pruning path something to prune.
    for chunk in 0..4i64 {
        let from = chunk * 50 + 1;
        v.push(st(
            "Fixture",
            format!("INSERT INTO big VALUES {}", big_rows(from, from + 49)),
        ));
    }
    let q: &[(&str, &str)] = &[
        ("Aggregates@200", "SELECT count(*) FROM big"),
        ("Aggregates@200", "SELECT count(*), count(k), count(v) FROM big"),
        ("Aggregates@200", "SELECT sum(id), avg(id), min(id), max(id) FROM big"),
        ("Aggregates@200", "SELECT grp, count(*), sum(v), min(k), max(k) FROM big GROUP BY grp ORDER BY grp"),
        ("Aggregates@200", "SELECT count(DISTINCT k) FROM big"),
        ("Aggregates@200", "SELECT grp, avg(v) FROM big GROUP BY grp HAVING avg(v) > 20 ORDER BY grp"),
        ("Aggregates@200", "SELECT sum(v) FILTER (WHERE flag) FROM big"),
        ("Aggregates@200", "SELECT stddev_pop(v), var_pop(v) FROM big"),
        ("Aggregates@200", "SELECT max(v) - min(v) FROM big"),
        ("Aggregates@200", "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY v) FROM big"),
        ("Aggregates@200", "SELECT k, count(*) FROM big GROUP BY k ORDER BY k NULLS LAST"),
        ("Aggregates@200", "SELECT grp, bool_and(flag), bool_or(flag) FROM big GROUP BY grp ORDER BY grp"),
        ("Aggregates@200", "SELECT sum(CASE WHEN flag THEN 1 ELSE 0 END) FROM big"),
        ("Aggregates@200", "SELECT count(*) FROM big WHERE k IS NULL"),
        // The count is a third sort key on purpose: ROLLUP emits both "k IS
        // NULL" groups and the subtotal row with a NULL k, so (grp, k) alone is
        // not a total order and the row order flapped between processes.
        ("Aggregates@200", "SELECT grp, k, count(*) FROM big GROUP BY ROLLUP (grp, k) ORDER BY grp NULLS LAST, k NULLS LAST, 3"),
        ("Aggregates@200", "SELECT grp, count(*) FROM big GROUP BY GROUPING SETS ((grp),()) ORDER BY grp NULLS LAST"),
        ("Aggregates@200", "SELECT count(*) FROM (SELECT DISTINCT k, grp FROM big) x"),
        ("Aggregates@200", "SELECT grp, string_agg(id::TEXT, ',' ORDER BY id) FROM big WHERE id <= 20 GROUP BY grp ORDER BY grp"),
        ("Aggregates@200", "SELECT grp, array_agg(id ORDER BY id) FROM big WHERE id <= 12 GROUP BY grp ORDER BY grp"),
        ("Windows@200", "SELECT id, k, rank() OVER (PARTITION BY grp ORDER BY k, id) FROM big ORDER BY id LIMIT 20"),
        ("Windows@200", "SELECT id, dense_rank() OVER (ORDER BY k NULLS LAST) FROM big ORDER BY id LIMIT 20"),
        ("Windows@200", "SELECT id, sum(v) OVER (PARTITION BY grp ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM big ORDER BY id LIMIT 25"),
        ("Windows@200", "SELECT id, ntile(4) OVER (ORDER BY id) FROM big ORDER BY id LIMIT 10"),
        ("Windows@200", "SELECT id, lag(v) OVER (ORDER BY id), lead(v) OVER (ORDER BY id) FROM big ORDER BY id LIMIT 12"),
        ("Windows@200", "SELECT id, v, row_number() OVER (PARTITION BY grp ORDER BY v DESC NULLS LAST, id) rn FROM big ORDER BY grp, rn LIMIT 20"),
        ("Windows@200", "SELECT id, count(*) OVER (PARTITION BY grp) FROM big ORDER BY id LIMIT 8"),
        ("Joins@200", "SELECT a.grp, count(*) FROM big a JOIN big b ON a.k = b.k GROUP BY a.grp ORDER BY a.grp"),
        ("Joins@200", "SELECT count(*) FROM big a JOIN big b ON a.k = b.k AND a.id < b.id"),
        ("Joins@200", "SELECT count(*) FROM big a LEFT JOIN big b ON a.k = b.k AND b.flag"),
        ("Joins@200", "SELECT a.id, b.id FROM big a JOIN big b ON b.id = a.id + 100 ORDER BY a.id LIMIT 10"),
        ("Ordering@200", "SELECT id FROM big ORDER BY id OFFSET 150 LIMIT 10"),
        ("Ordering@200", "SELECT id, v FROM big ORDER BY v DESC NULLS LAST, id LIMIT 10"),
        ("Ordering@200", "SELECT DISTINCT ON (grp) grp, id, k FROM big ORDER BY grp, id"),
        ("Ordering@200", "SELECT id FROM big ORDER BY k NULLS FIRST, id LIMIT 20"),
        ("Ordering@200", "SELECT id FROM big ORDER BY grp, id DESC LIMIT 12"),
        ("Subqueries@200", "SELECT id FROM big WHERE id IN (SELECT id FROM big WHERE k = 3) ORDER BY id"),
        ("Subqueries@200", "SELECT id FROM big WHERE v > (SELECT avg(v) FROM big) ORDER BY id LIMIT 15"),
        ("Subqueries@200", "SELECT id, (SELECT count(*) FROM big b WHERE b.k = a.k) FROM big a ORDER BY id LIMIT 10"),
        ("Subqueries@200", "SELECT count(*) FROM big a WHERE EXISTS (SELECT 1 FROM big b WHERE b.id = a.id + 1)"),
        ("Set ops@200", "SELECT id FROM big WHERE id <= 100 EXCEPT SELECT id FROM big WHERE id % 2 = 0 ORDER BY 1"),
        ("Set ops@200", "SELECT id FROM big WHERE id <= 10 UNION SELECT id FROM big WHERE id >= 195 ORDER BY 1"),
        ("Set ops@200", "SELECT k FROM big INTERSECT SELECT k FROM big WHERE id < 20 ORDER BY 1 NULLS LAST"),
        ("CTEs@200", "WITH per AS (SELECT grp, sum(v) s FROM big GROUP BY grp) SELECT grp, s FROM per ORDER BY grp"),
        ("CTEs@200", "WITH r AS (SELECT id, v FROM big WHERE id <= 20) SELECT count(*), sum(v) FROM r"),
    ];
    v.extend(q.iter().map(|(a, s)| st(a, *s)));
    v
}

/// Pruning + overlay, over both file formats. None of this has a PostgreSQL
/// answer to compare against — it is Basin behaviour or nothing.
fn storage_suite() -> Vec<Stmt> {
    let mut v = Vec::new();
    for (tbl, with) in [("vx", ""), ("pq", " WITH (basin.file_format='parquet')")] {
        v.push(st(
            "Fixture",
            format!(
                "CREATE TABLE {tbl} (id BIGINT NOT NULL, grp TEXT, k INTEGER, v DOUBLE PRECISION, flag BOOLEAN){with}"
            ),
        ));
        for chunk in 0..4i64 {
            let from = chunk * 50 + 1;
            v.push(st(
                "Fixture",
                format!("INSERT INTO {tbl} VALUES {}", big_rows(from, from + 49)),
            ));
        }
    }
    // Prunable predicates: the four INSERTs give four files with disjoint id
    // ranges, so each of these can skip files by min/max. Whatever the engine
    // skips, the ANSWER must not move.
    for tbl in ["vx", "pq"] {
        let area: &'static str = if tbl == "vx" { "Vortex prune" } else { "Parquet prune" };
        for q in [
            format!("SELECT count(*) FROM {tbl}"),
            format!("SELECT id, v FROM {tbl} WHERE id < 25 ORDER BY id"),
            format!("SELECT id FROM {tbl} WHERE id BETWEEN 60 AND 70 ORDER BY id"),
            format!("SELECT id FROM {tbl} WHERE id > 190 ORDER BY id"),
            format!("SELECT id, grp, k FROM {tbl} WHERE id = 137"),
            format!("SELECT id FROM {tbl} WHERE id IN (7, 77, 177) ORDER BY id"),
            format!("SELECT id FROM {tbl} WHERE id > 10000 ORDER BY id"),
            format!("SELECT count(*), min(id), max(id) FROM {tbl} WHERE grp = 'alpha'"),
            format!("SELECT sum(v) FROM {tbl} WHERE id <= 100"),
            format!("SELECT id FROM {tbl} WHERE grp > 'c' ORDER BY id LIMIT 10"),
            format!("SELECT count(*) FROM {tbl} WHERE v IS NULL"),
            format!("SELECT count(*) FROM {tbl} WHERE id < 25 AND flag"),
            format!("SELECT id FROM {tbl} WHERE id BETWEEN 45 AND 55 ORDER BY id"),
        ] {
            v.push(st(area, q));
        }
    }
    // Overlay: mutate on top of the settled files, then read back. UPDATE and
    // DELETE produce tombstones; the trailing INSERT lands in the hot tier with
    // no flush behind it. The read path has to reconcile all three.
    for tbl in ["vx", "pq"] {
        let area: &'static str = if tbl == "vx" {
            "Overlay (Vortex)"
        } else {
            "Overlay (Parquet)"
        };
        v.push(st(area, format!("UPDATE {tbl} SET v = v * 2 WHERE id BETWEEN 20 AND 30")));
        v.push(st(area, format!("DELETE FROM {tbl} WHERE id % 10 = 0")));
        v.push(st(
            area,
            format!("INSERT INTO {tbl} VALUES (201,'alpha',1,50.25,true),(202,'beta',2,50.50,false)"),
        ));
        for q in [
            format!("SELECT count(*) FROM {tbl}"),
            format!("SELECT id, v FROM {tbl} WHERE id BETWEEN 18 AND 32 ORDER BY id"),
            format!("SELECT id, v FROM {tbl} WHERE id >= 195 ORDER BY id"),
            format!("SELECT sum(v), count(*), count(v) FROM {tbl}"),
            format!("SELECT id FROM {tbl} WHERE id = 20"),
            format!("SELECT id FROM {tbl} WHERE id = 30"),
            format!("SELECT grp, count(*) FROM {tbl} GROUP BY grp ORDER BY grp"),
            format!("SELECT count(*) FROM {tbl} WHERE id % 10 = 0"),
            format!("SELECT id, k, v FROM {tbl} ORDER BY id DESC LIMIT 6"),
        ] {
            v.push(st(area, q));
        }
    }
    v
}

/// RLS predicate injection. Sessions: 0 = admin (no principal), 1 = alice,
/// 2 = bob.
fn rls_suite() -> Vec<Stmt> {
    vec![
        st("Fixture", "CREATE TABLE docs (id BIGINT NOT NULL, owner TEXT NOT NULL, body TEXT)"),
        st(
            "Fixture",
            "INSERT INTO docs VALUES (1,'alice','a1'),(2,'alice','a2'),(3,'alice','a3'),(4,'bob','b1'),(5,'bob','b2'),(6,'carol','c1')",
        ),
        st_as("RLS off", "SELECT id, owner FROM docs ORDER BY id", 1),
        st_as("RLS off", "SELECT id, owner FROM docs ORDER BY id", 2),
        st_as("RLS off", "SELECT count(*) FROM docs", 1),
        st_as("RLS enabled, no policy", "ALTER TABLE docs ENABLE ROW LEVEL SECURITY", 0),
        st_as("RLS enabled, no policy", "SELECT id, owner FROM docs ORDER BY id", 1),
        st_as("RLS enabled, no policy", "SELECT count(*) FROM docs", 1),
        st_as("RLS enabled, no policy", "SELECT id, owner FROM docs ORDER BY id", 0),
        st_as(
            "RLS with policy",
            "CREATE POLICY p_owner ON docs FOR ALL TO PUBLIC USING (owner = current_user)",
            0,
        ),
        st_as("RLS with policy", "SELECT current_user", 1),
        st_as("RLS with policy", "SELECT id, owner FROM docs ORDER BY id", 1),
        st_as("RLS with policy", "SELECT id, owner FROM docs ORDER BY id", 2),
        st_as("RLS with policy", "SELECT id, owner FROM docs ORDER BY id", 0),
        st_as("RLS with policy", "SELECT count(*), min(id), max(id) FROM docs", 1),
        st_as("RLS with policy", "SELECT owner, count(*) FROM docs GROUP BY owner ORDER BY owner", 1),
        st_as("RLS with policy", "SELECT d.id, d2.owner FROM docs d JOIN docs d2 ON d.id = d2.id ORDER BY d.id", 1),
        st_as("RLS with policy", "SELECT id FROM docs WHERE owner = 'bob' ORDER BY id", 1),
        st_as("RLS with policy", "SELECT id FROM docs WHERE id IN (SELECT id FROM docs) ORDER BY id", 2),
        st_as("RLS write path", "UPDATE docs SET body = 'edited' WHERE id = 1", 1),
        st_as("RLS write path", "UPDATE docs SET body = 'hijacked' WHERE id = 1", 2),
        st_as("RLS write path", "DELETE FROM docs WHERE id = 4", 1),
        st_as("RLS write path", "SELECT id, owner, body FROM docs ORDER BY id", 0),
        st_as("RLS disabled again", "ALTER TABLE docs DISABLE ROW LEVEL SECURITY", 0),
        st_as("RLS disabled again", "SELECT id, owner, body FROM docs ORDER BY id", 1),
    ]
}

fn catalog_suite() -> Vec<Stmt> {
    let mut v = vec![
        st("Fixture", "CREATE TABLE t (id BIGINT NOT NULL, name TEXT, amt DOUBLE PRECISION)"),
        st("Fixture", "CREATE TABLE p (id BIGINT PRIMARY KEY, val INTEGER NOT NULL DEFAULT 0, tag TEXT)"),
        st("Fixture", "CREATE VIEW tv AS SELECT id, name FROM t"),
    ];
    let q: &[(&str, &str)] = &[
        ("information_schema", "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name"),
        ("information_schema", "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = 't' ORDER BY column_name"),
        ("information_schema", "SELECT column_name, ordinal_position FROM information_schema.columns WHERE table_name = 'p' ORDER BY ordinal_position"),
        ("information_schema", "SELECT count(*) FROM information_schema.columns WHERE table_name = 't'"),
        ("pg_catalog", "SELECT relname FROM pg_class WHERE relname IN ('t','u','p','tv') ORDER BY relname"),
        ("pg_catalog", "SELECT relname, relkind FROM pg_class WHERE relname IN ('t','p','tv') ORDER BY relname"),
        ("pg_catalog", "SELECT a.attname, a.attnum, a.attnotnull FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid WHERE c.relname = 't' AND a.attnum > 0 ORDER BY a.attnum"),
        ("pg_catalog", "SELECT typname FROM pg_type WHERE typname IN ('int8','text','bool','float8') ORDER BY typname"),
        ("pg_catalog", "SELECT nspname FROM pg_namespace ORDER BY nspname"),
        ("pg_catalog", "SELECT count(*) FROM pg_class WHERE relkind = 'r'"),
        ("pg_catalog", "SELECT conname, contype FROM pg_constraint ORDER BY conname, contype"),
        ("pg_catalog", "SELECT tablename, indexname FROM pg_indexes ORDER BY tablename, indexname"),
        ("pg_catalog", "SELECT current_schema()"),
        ("pg_catalog", "SELECT has_table_privilege('t', 'SELECT')"),
        ("pg_catalog", "SELECT pg_typeof(1::BIGINT), pg_typeof('x'::TEXT)"),
        ("pg_catalog", "SELECT format_type(oid, NULL) FROM pg_type WHERE typname = 'int8'"),
    ];
    v.extend(q.iter().map(|(a, s)| st(a, *s)));
    // Explicit, visible exclusions. Each of these is a real shape an
    // application issues; none of them can be a golden answer.
    v.push(st_excluded("Volatile (not recorded)", "SELECT now()", "volatile: wall-clock value differs on every run"));
    v.push(st_excluded("Volatile (not recorded)", "SELECT CURRENT_TIMESTAMP", "volatile: wall-clock value differs on every run"));
    v.push(st_excluded("Volatile (not recorded)", "SELECT random()", "volatile: RNG output differs on every run"));
    v.push(st_excluded("Volatile (not recorded)", "SELECT version()", "volatile: embeds a build/version string that changes with any release"));
    v.push(st_excluded("Volatile (not recorded)", "SELECT current_database()", "volatile: embeds the per-run project identity"));
    v.push(st_excluded("Volatile (not recorded)", "SELECT pg_backend_pid()", "volatile: per-process value"));
    v.push(st_excluded("Volatile (not recorded)", "EXPLAIN SELECT id FROM t WHERE id > 1", "plan text is engine-internal by construction: the owned engine is a different planner, so a recorded plan would be a guaranteed false failure rather than a changed answer"));
    v
}

/// What the incumbent CANNOT do. Recording rejections matters as much as
/// recording rows: "it used to error and now returns rows" is a silent
/// behaviour change and this is the only oracle that will see it.
fn errors_suite() -> Vec<Stmt> {
    let mut v = vec![
        st("Fixture", "CREATE TABLE t (id BIGINT NOT NULL, name TEXT, amt DOUBLE PRECISION)"),
        st("Fixture", "INSERT INTO t VALUES (1,'a',1.5),(2,'b',2.5),(3,'c',3.5)"),
        st("Fixture", "CREATE TABLE d (id BIGINT NOT NULL, day DATE, ts TIMESTAMP, flag BOOLEAN)"),
        st("Fixture", "CREATE TABLE p (id BIGINT PRIMARY KEY, val INTEGER NOT NULL DEFAULT 0, tag TEXT)"),
        st("Fixture", "INSERT INTO p VALUES (1,10,'a'),(2,20,'b')"),
    ];
    let q: &[(&str, &str)] = &[
        ("Missing objects", "SELECT * FROM no_such_table"),
        ("Missing objects", "SELECT no_such_column FROM t"),
        ("Missing objects", "SELECT no_such_fn(1)"),
        ("Missing objects", "DROP TABLE no_such_table"),
        ("Missing objects", "UPDATE no_such_table SET x = 1"),
        ("Missing objects", "ALTER TABLE t DROP COLUMN nope"),
        ("Type errors", "SELECT 'abc'::INTEGER"),
        ("Type errors", "SELECT '2024-13-45'::DATE"),
        ("Type errors", "SELECT ARRAY[1,2] + 1"),
        ("Type errors", "SELECT id + name FROM t"),
        ("Type errors", "INSERT INTO t VALUES ('not a number','x',1.0)"),
        ("Arithmetic", "SELECT 1/0"),
        ("Arithmetic", "SELECT 1.0/0.0"),
        ("Arithmetic", "SELECT 9223372036854775807::BIGINT + 1"),
        ("Constraints", "INSERT INTO p VALUES (1,1,'dup')"),
        ("Constraints", "INSERT INTO p (id, val) VALUES (9, NULL)"),
        ("Constraints", "INSERT INTO t VALUES (1)"),
        ("Constraints", "CREATE TABLE t (x INTEGER)"),
        ("Semantic errors", "SELECT id FROM t GROUP BY name"),
        ("Semantic errors", "SELECT sum(sum(amt)) FROM t"),
        ("Semantic errors", "SELECT id FROM t WHERE row_number() OVER () = 1"),
        ("Semantic errors", "SELECT id FROM t JOIN d ON d.id = t.id WHERE id = 1"),
        ("Semantic errors", "SELECT id FROM t UNION SELECT id, name FROM t"),
        ("Parse errors", "SELECT FROM"),
        ("Parse errors", "SELEC 1"),
        ("Parse errors", "SELECT * FROM t WHERE"),
        ("Unsupported", "SELECT id FROM t FOR UPDATE"),
        ("Unsupported", "SELECT id FROM t LIMIT -1"),
    ];
    v.extend(q.iter().map(|(a, s)| st(a, *s)));
    // Accepted rather than rejected today, and it samples at random: two
    // recordings a second apart returned different row counts. There is no
    // answer here to record.
    v.push(st_excluded(
        "Unsupported",
        "SELECT id FROM t TABLESAMPLE BERNOULLI (10)",
        "random sampling: BERNOULLI selects a different subset on every run",
    ));
    v
}

// ── Suite driver ─────────────────────────────────────────────────────────────

/// Everything the harness replays, in the order it replays it. Both modes call
/// this, so record and compare are the same sequence of statements against the
/// same fixtures by construction.
async fn run_everything() -> (Vec<Block>, Vec<bool>) {
    silence_panics();
    let project = golden_project();

    let mut blocks = Vec::new();
    let mut served = Vec::new();

    // Keep every TempDir alive for the whole run.
    let mut dirs: Vec<TempDir> = Vec::new();

    for (name, stmts, n_sessions) in [
        ("base", {
            let mut v = base_fixture();
            v.extend(base_queries());
            v
        }, 1usize),
        ("scale", scale_suite(), 1),
        ("storage", storage_suite(), 1),
        ("rls", rls_suite(), 3),
        ("catalog", catalog_suite(), 1),
        ("errors", errors_suite(), 1),
    ] {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let mut sessions = vec![eng.open_session(project).await.unwrap()];
        if n_sessions >= 2 {
            sessions.push(eng.open_session_as(project, "alice").await.unwrap());
        }
        if n_sessions >= 3 {
            sessions.push(eng.open_session_as(project, "bob").await.unwrap());
        }
        let redact = vec![
            (dir.path().display().to_string(), "<TMPDIR>"),
            (project.to_string(), "<PROJECT>"),
        ];
        let mut runner = Runner::new(redact);
        runner.run_suite(name, &eng, &sessions, &stmts).await;
        blocks.extend(runner.blocks);
        served.extend(runner.served);
        dirs.push(dir);
    }
    (blocks, served)
}

// ── Record ───────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore = "recorder; run with BASIN_GOLDEN_RECORD=1 -- --ignored --nocapture"]
async fn record_golden_answers() {
    assert_eq!(
        std::env::var("BASIN_GOLDEN_RECORD").as_deref(),
        Ok("1"),
        "refusing to overwrite the golden files unless BASIN_GOLDEN_RECORD=1"
    );
    assert_ne!(
        std::env::var("BASIN_OWNED_ENGINE").as_deref(),
        Ok("1"),
        "golden answers are the INCUMBENT's answers: record with BASIN_OWNED_ENGINE unset"
    );

    let out_dir = match std::env::var("BASIN_GOLDEN_DIR") {
        Ok(d) => PathBuf::from(d),
        Err(_) => golden_dir(),
    };
    std::fs::create_dir_all(&out_dir).unwrap();

    let (blocks, _) = run_everything().await;

    let mut by_file: BTreeMap<String, Vec<&Block>> = BTreeMap::new();
    for b in &blocks {
        by_file.entry(file_for(&b.suite, &b.area)).or_default().push(b);
    }

    // Remove stale files so a shrunk corpus cannot leave orphans behind.
    if let Ok(rd) = std::fs::read_dir(&out_dir) {
        for entry in rd.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if name.ends_with(".golden") && !by_file.contains_key(&name) {
                std::fs::remove_file(entry.path()).unwrap();
            }
        }
    }

    for (file, bs) in &by_file {
        let header = format!(
            "# Basin golden answers — recorded from the INCUMBENT engine (BASIN_OWNED_ENGINE unset).\n\
             # suite: {}   area: {}\n\
             # Do not hand-edit. Regenerate: BASIN_GOLDEN_RECORD=1 cargo test -p basin-integration-tests \\\n\
             #   --test golden_answers -- --ignored --nocapture record_golden_answers\n\
             # Rows are sorted unless ORDER is `as-returned`. `\\N` is NULL. Cells are tab-separated.\n\n",
            bs[0].suite, bs[0].area
        );
        std::fs::write(out_dir.join(file), format!("{header}{}", serialize(bs))).unwrap();
    }

    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    for b in &blocks {
        *counts.entry(b.outcome.status()).or_default() += 1;
    }
    let mut per_suite: BTreeMap<&str, usize> = BTreeMap::new();
    for b in &blocks {
        *per_suite.entry(b.suite.as_str()).or_default() += 1;
    }

    // Provenance matters more than usual here: this branch is under active
    // development, so "the incumbent" is itself a moving target. A recording
    // is only meaningful next to the commit it was taken at.
    let commit = std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "<unknown>".to_string());

    let manifest = format!(
        "# Basin golden answers — provenance\n\
         #\n\
         # These are the answers the INCUMBENT engine (DataFusion path,\n\
         # BASIN_OWNED_ENGINE unset) gave, recorded so that the question\n\
         # \"did the owned engine change an answer Basin used to give?\"\n\
         # stays answerable after DataFusion is unlinked.\n\
         #\n\
         recorded_by       golden_answers.rs (tests/integration/tests)\n\
         engine            incumbent (BASIN_OWNED_ENGINE unset)\n\
         commit            {commit}\n\
         statements        {}\n\
         files             {}\n\
         per_suite         {:?}\n\
         per_status        {:?}\n",
        blocks.len(),
        by_file.len(),
        per_suite,
        counts
    );
    std::fs::write(out_dir.join("MANIFEST"), manifest).unwrap();

    println!(
        "recorded {} statements into {} files under {}",
        blocks.len(),
        by_file.len(),
        out_dir.display()
    );
    println!("per status: {counts:?}");
    println!("per suite : {per_suite:?}");
}

// ── Compare ──────────────────────────────────────────────────────────────────

#[tokio::test]
async fn golden_answers_match() {
    let dir = golden_dir();
    assert!(
        dir.join("MANIFEST").exists(),
        "no golden answers recorded at {} — run record_golden_answers first",
        dir.display()
    );

    // Load every golden file. Nothing here touches DataFusion: the oracle is
    // the files.
    let mut expected: BTreeMap<(String, usize), Block> = BTreeMap::new();
    for entry in std::fs::read_dir(&dir).unwrap().flatten() {
        let name = entry.file_name().to_string_lossy().to_string();
        if !name.ends_with(".golden") {
            continue;
        }
        let suite = name.split("--").next().unwrap().to_string();
        let text = std::fs::read_to_string(entry.path()).unwrap();
        for b in parse(&text, &suite, &name) {
            let key = (b.suite.clone(), b.idx);
            assert!(
                expected.insert(key.clone(), b).is_none(),
                "duplicate block {key:?} across golden files"
            );
        }
    }
    assert!(
        !expected.is_empty(),
        "golden directory {} parsed to zero blocks — the harness would compare nothing",
        dir.display()
    );

    let (actual, served) = run_everything().await;

    assert_eq!(
        actual.len(),
        expected.len(),
        "corpus has {} statements but {} were recorded — re-record after changing the corpus",
        actual.len(),
        expected.len()
    );

    let owned = std::env::var("BASIN_OWNED_ENGINE").as_deref() == Ok("1");
    println!(
        "\n─── golden-answer comparison ───\n\
         engine under test : {}\n\
         golden answers    : {} statements from {}\n",
        if owned { "OWNED (BASIN_OWNED_ENGINE=1)" } else { "incumbent (DataFusion path)" },
        expected.len(),
        dir.display()
    );

    let mut diverged: Vec<(&Block, Vec<String>)> = Vec::new();
    let mut compared = 0usize;
    let mut per_area: BTreeMap<String, (usize, usize)> = BTreeMap::new();
    let mut owned_served = 0usize;
    let mut skipped: Vec<(&Block, String)> = Vec::new();

    for (i, a) in actual.iter().enumerate() {
        let key = (a.suite.clone(), a.idx);
        let e = expected
            .get(&key)
            .unwrap_or_else(|| panic!("no recorded answer for {key:?} ({})", a.sql));
        assert_eq!(
            e.sql, a.sql,
            "corpus drifted at {key:?}: recorded SQL and replayed SQL differ — re-record"
        );
        // A statement either side declares non-deterministic has no answer to
        // compare. Skipping it is stated, counted and listed — never silent —
        // and is the honest treatment: comparing `array_agg` output whose
        // element order is undefined manufactures failures, it does not find
        // them.
        if let Outcome::Excluded(r) = &e.outcome {
            skipped.push((a, format!("recorded as excluded: {r}")));
            continue;
        }
        if let Outcome::Excluded(r) = &a.outcome {
            skipped.push((a, format!("unstable under the engine under test: {r}")));
            continue;
        }
        compared += 1;
        if served[i] {
            owned_served += 1;
        }
        let entry = per_area
            .entry(format!("{}/{}", a.suite, a.area))
            .or_insert((0, 0));
        entry.0 += 1;
        let d = diff_block(e, a, served[i]);
        if !d.is_empty() {
            entry.1 += 1;
            diverged.push((a, d));
        }
    }

    if !diverged.is_empty() {
        println!("─── divergences ───");
        for (b, lines) in &diverged {
            println!("\n[{}/{} #{:04}] {}", b.suite, b.area, b.idx, b.sql);
            for l in lines {
                println!("  {l}");
            }
        }
        println!("\n─── divergences per area ───");
        for (area, (total, bad)) in &per_area {
            if *bad > 0 {
                println!("  {area:<40} {bad:>4} / {total}");
            }
        }
    }

    if !skipped.is_empty() {
        println!("\n─── skipped (no deterministic answer exists) ───");
        for (b, why) in &skipped {
            println!("  [{}/{} #{:04}] {} — {why}", b.suite, b.area, b.idx, b.sql);
        }
    }

    println!(
        "\ncompared {compared} recorded answers ({} skipped as non-deterministic); {} diverged; \
         owned engine served {owned_served} of {} statements",
        skipped.len(),
        diverged.len(),
        actual.len()
    );

    assert!(
        compared > 0,
        "harness compared nothing — that is a broken harness, not a passing test"
    );
    assert!(
        diverged.is_empty(),
        "{} of {compared} recorded answers changed (see the report above)",
        diverged.len()
    );
}
