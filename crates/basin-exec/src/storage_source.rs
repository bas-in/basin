//! [`StorageBatchSource`] — a [`BatchSource`] backed by real Basin storage.
//!
//! Before this module, the owned engine could execute a full physical plan,
//! but only against [`crate::scan::VecBatchSource`]: an in-memory `Vec` of
//! batches assembled by a test. It had never read a byte off disk or object
//! storage. This module is what turns that into an engine — `Scan` now has
//! a source that opens a table's real data files (Vortex, the default
//! format since ADR 0015, or Parquet, fully supported alongside it) through
//! [`basin_storage::Storage`] and hands back the same `RecordBatch`es a
//! `VecBatchSource` would have.
//!
//! # Why this is the one module in `basin-exec` allowed to know about storage
//!
//! `scan.rs` spells out the reason `Scan` takes a `Box<dyn BatchSource>`
//! rather than a `basin-storage` handle directly: every other operator test
//! in this crate would otherwise need object storage spun up just to test a
//! hash join or a sort. That property is worth preserving, so
//! `basin-storage` is a dependency of `basin-exec` as a whole (Cargo has no
//! finer grain than the crate), but every symbol this module needs from it
//! is used *only* here — `Scan`, `Filter`, `Project`, `HashJoin`, and every
//! other operator remain exactly as storage-free and unit-testable as they
//! were before this file existed.
//!
//! # Bridging an async storage layer to a sync pull interface
//!
//! [`Operator::next_batch`] (and [`BatchSource::next_batch`], the trait this
//! module implements) is a plain synchronous method: `Scan` and everything
//! above it in the operator tree is not `async`, by design (see the crate's
//! top-level docs on why the engine is pull-based rather than a stream of
//! futures). `basin_storage::Storage::read` is genuinely async — it issues
//! object-store RPCs. [`StorageBatchSource`] bridges the two with a
//! dedicated background OS thread that owns its own single-threaded Tokio
//! runtime and drives the storage read stream; each batch (or error) crosses
//! back to the calling thread over a rendezvous [`std::sync::mpsc`] channel.
//!
//! This is deliberately *not* `tokio::runtime::Handle::current().block_on(..)`
//! or a fresh `Runtime::block_on` called directly on the caller's thread:
//! either would panic if `next_batch` is ever called from a thread that is
//! itself already inside a Tokio runtime (a real possibility once this
//! source is wired into `basin-engine`, which is async throughout). Running
//! the bridge on its own thread means `next_batch` works identically whether
//! the caller is a plain synchronous test (every test below) or a future
//! async caller — it never touches whatever runtime context the calling
//! thread may or may not have.
//!
//! # Existence is not liveness: where a scan's file set comes from
//!
//! This module has two constructors and the difference between them is a
//! correctness property, not a convenience.
//!
//! [`StorageBatchSource::new`] reads through `Storage::read`, which LISTs the
//! table's object-store prefix. That answers "which files physically exist".
//! It does NOT answer "which files are still part of this table", and nothing
//! at the storage layer can: a superseded file is deliberately retained after
//! it stops being live — `basin-shard` holds superseded compaction inputs for
//! `BASIN_SUPERSEDED_DELETE_GRACE_SECS` (**300 s** by default) so in-flight
//! scans do not 404, and the engine's UPDATE/DELETE deletes its rewritten
//! inputs from a detached task. A LIST-sourced scan inside that window
//! returns each affected row once per physically present copy, at the
//! PRE- and POST-update values both.
//!
//! [`StorageBatchSource::new_live_paths`] reads exactly the paths the caller
//! took from `basin_catalog::TableMetadata::live_data_files()`, via
//! `Storage::read_paths_with_schema`. That is the file set the DataFusion
//! read path has always used (bug #41), and it is what
//! [`StorageTableResolver`] — hence every SQL scan built through
//! [`crate::build::build`] — now uses.
//!
//! The pull in the other direction is a trap that has already been sprung
//! once (bc57fa48): do not push the catalog down into `Storage::read` itself.
//! A file exists from the moment `Storage::write_batch` puts it, which is
//! before its commit lands (`note_uncommitted_file` exists for precisely that
//! window), so a catalog-authoritative `Storage::read` LOSES freshly written
//! rows. The engine may source from the catalog only because it commits its
//! own writes before making them visible.
//!
//! The channel has capacity 1 (a rendezvous with one slot of read-ahead), so
//! `next_batch` does bounded work per call — one `recv()` — honouring the
//! same "return control between batches" cancellation contract
//! [`crate::scan::Scan`] documents. The background thread produces at most
//! one batch of read-ahead beyond what has been consumed; it is not free to
//! race arbitrarily far ahead of the caller.
//!
//! # Pushdown: what reaches storage today, and what does not
//!
//! [`StorageBatchSource::new`] takes a [`basin_storage::ReadOptions`]
//! directly, so a caller that constructs one has full access to storage's
//! pushdown: `opts.projection` narrows the columns actually decoded, and
//! `opts.filters` (translated to [`basin_storage::Predicate`]) drives
//! storage's file-level stats prune, row-group stats prune, and bloom-filter
//! prune — all *before* a single row reaches Arrow. The tests below exercise
//! both and assert on `Storage::read_counters()` that the pruning actually
//! fires, not just that the row count comes out right.
//!
//! [`StorageTableResolver`] — the [`TableResolver`] implementation this
//! module also provides, for plugging into [`crate::build::build`] — threads
//! that pushdown through: [`TableResolver::open`] receives the scan's
//! `projection` and `filters` and reports back, via
//! [`crate::build::ScanPushdown`], which of them it honoured.
//!
//! # The two index spaces, and which one a filter is written in
//!
//! This is the seam where a wrong answer is cheapest to produce, so it is
//! stated once, here, and both sides refer back to it.
//!
//! - `projection: &[usize]` — positions in the **table's** schema. That is
//!   what [`StorageTableResolver::register`] pins, and what turns into column
//!   names for `ReadOptions::projection`.
//! - a filter's `ColumnRef { index, .. }` — a position within **that same
//!   `projection` list**, i.e. within the scan's own output. The logical layer
//!   numbers them that way: filter pushdown moves a `Filter` predicate whose
//!   indices address its input's output into the scan, and projection pruning
//!   renumbers them in step with the projection it shrinks
//!   (`basin_plan::opt::projection`).
//!
//! The two coincide exactly when the projection is the identity `0..n`, which
//! it is for every plan that has not been through projection pruning — which
//! is why resolving a filter's index straight into the table schema looked
//! right in every hand-built plan and was wrong for real queries. `SELECT n
//! FROM u WHERE n > 7` prunes to `projection=[n]` and renumbers the predicate
//! to position 0; read against the table that is the FIRST table column, and
//! storage then filtered on that column instead. `expr_to_predicate` resolves
//! through `projection` for this reason, and `build.rs` performs the matching
//! translation on whatever filters the source declines.

use std::collections::HashMap;
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;

use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use futures::StreamExt;

use basin_common::{ProjectId, TableName};
use basin_plan::TableId;
use basin_storage::{DataFile, ReadOptions, Storage};
use object_store::path::Path as ObjectPath;

use crate::build::TableResolver;
use crate::operator::ExecError;
use crate::scan::BatchSource;

/// A [`BatchSource`] that reads a table's real data files through
/// [`basin_storage::Storage`].
///
/// See the module docs for the async-to-sync bridge and for exactly which
/// pushdown reaches storage when this is constructed directly (as opposed to
/// via [`StorageTableResolver`]).
pub struct StorageBatchSource {
    schema: SchemaRef,
    rx: mpsc::Receiver<Result<RecordBatch, ExecError>>,
    // Held only so the thread is not detached from a `Debug`/lifetime point
    // of view; we never join it (see `new`'s doc comment on shutdown). The
    // `Option` lets a future `Drop` impl take it without a placeholder.
    _worker: Option<thread::JoinHandle<()>>,
}

impl std::fmt::Debug for StorageBatchSource {
    // Manual `Debug`, matching `Scan`'s rationale in `scan.rs`: the `mpsc`
    // receiver and the worker thread are not `Debug` and their internals are
    // not this struct's business to expose.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageBatchSource")
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

/// Where a [`StorageBatchSource`] gets its file set from — the distinction
/// that decides whether a scan sees the LIVE rows or merely the rows that
/// happen to be on disk. See [`StorageBatchSource::new_live_paths`].
enum FileSet {
    /// LIST the table's prefix, i.e. `Storage::read`. Answers "what
    /// physically exists", which is NOT "what is live".
    ListTablePrefix(TableName),
    /// Exactly these paths, sourced by the caller from
    /// `basin_catalog::TableMetadata::live_data_files()`.
    LivePaths(Vec<ObjectPath>),
}

impl StorageBatchSource {
    /// Open a storage-backed source that LISTs `table`'s prefix for its file
    /// set — i.e. [`basin_storage::Storage::read`].
    ///
    /// # This is not the constructor a SQL scan wants
    ///
    /// The LIST is authoritative for a file's EXISTENCE and says nothing
    /// about its LIVENESS. A file the catalog has superseded (copy-on-write
    /// UPDATE/DELETE, compaction, stripe merge) stays physically present long
    /// after it stops being part of the table — `basin-shard` retains
    /// superseded compaction inputs for `BASIN_SUPERSEDED_DELETE_GRACE_SECS`,
    /// **300 seconds by default**, so that in-flight scans do not 404, and
    /// the engine's UPDATE/DELETE deletes its rewritten inputs from a
    /// detached task. Inside that window a LIST-sourced scan enumerates both
    /// the superseded file and its replacement, and returns every affected
    /// row once per physically present copy — not clean duplication, but the
    /// PRE- and POST-update images of the same row side by side, which no
    /// amount of deduplication downstream can tell apart.
    ///
    /// Use [`new_live_paths`](Self::new_live_paths) for anything that must
    /// return a table's current rows. This constructor remains for callers
    /// that genuinely want "every file on disk" and hold no catalog — the
    /// raw-`Storage` tests below, which write with `Storage::write_batch` and
    /// never register a catalog row at all.
    ///
    /// `table_schema` is the table's full Arrow schema, in the column order
    /// a caller's downstream `Scan` expects (see the module docs' pushdown
    /// section for why this matters when `opts.projection` is `Some`). When
    /// `opts.projection` is `Some(names)`, [`BatchSource::schema`] reports
    /// only those columns, in that order, and every batch this source
    /// yields has exactly that shape — the source is internally consistent,
    /// it is the *caller's* responsibility to only pair a projected source
    /// with a `Scan` whose own projection/filters were written against the
    /// reduced schema (or, more simply, not to feed a projected source into
    /// `Scan` at all — construct it directly and consume it when the caller
    /// already knows exactly which columns and rows it wants).
    ///
    /// The read does not start until the first [`BatchSource::next_batch`]
    /// call's background thread issues it; construction only spawns the
    /// thread and validates `opts.projection` against `table_schema`.
    pub fn new(
        storage: Storage,
        project: ProjectId,
        table: TableName,
        table_schema: SchemaRef,
        opts: ReadOptions,
    ) -> Result<Self, ExecError> {
        Self::open(
            storage,
            project,
            FileSet::ListTablePrefix(table),
            table_schema,
            opts,
        )
    }

    /// Open a storage-backed source over exactly `live_paths` — the file set
    /// the caller took from `basin_catalog::TableMetadata::live_data_files()`.
    ///
    /// This is the constructor every SQL scan must use, and the reason is the
    /// whole of [`new`](Self::new)'s doc comment: the catalog is the only
    /// component that knows which of the physically present files are still
    /// part of the table. The DataFusion read path has always done exactly
    /// this (`basin_engine::session::refresh_table_inner` builds its
    /// `ListingTable` over `meta.live_data_files()` — bug #41); the owned
    /// engine re-introduced #41 by reading through `Storage::read`, and this
    /// is how it stops.
    ///
    /// Note the direction of the danger, because the opposite mistake is also
    /// a real one and has been shipped before (bc57fa48): the catalog must
    /// NOT be made authoritative down inside `basin-storage`, where a
    /// flushed-but-not-yet-committed file (`Storage::write_batch` puts the
    /// object before the commit lands — see `note_uncommitted_file`) would
    /// become invisible and reads would LOSE rows. The engine can source from
    /// the catalog safely only because it commits every one of its own writes
    /// before making them visible; storage cannot assume that on its callers'
    /// behalf. `Storage::read` is therefore left exactly as it was.
    ///
    /// An empty `live_paths` is a table with no live data at this snapshot
    /// (genesis, TRUNCATE, a rollback): the source yields zero batches, which
    /// is what DataFusion's path does too (it registers an empty `MemTable`).
    ///
    /// `table_schema` and `opts` behave exactly as in [`new`](Self::new). The
    /// full (pre-projection) `table_schema` is also handed to storage as the
    /// catalog schema, so schema-evolution NULL synthesis for columns added
    /// after a file was written works here exactly as it does under
    /// `Storage::read`.
    pub fn new_live_paths(
        storage: Storage,
        project: ProjectId,
        live_paths: Vec<ObjectPath>,
        table_schema: SchemaRef,
        opts: ReadOptions,
    ) -> Result<Self, ExecError> {
        Self::open(
            storage,
            project,
            FileSet::LivePaths(live_paths),
            table_schema,
            opts,
        )
    }

    fn open(
        storage: Storage,
        project: ProjectId,
        files: FileSet,
        table_schema: SchemaRef,
        opts: ReadOptions,
    ) -> Result<Self, ExecError> {
        let schema = match &opts.projection {
            Some(names) => {
                let mut fields = Vec::with_capacity(names.len());
                for name in names {
                    let field = table_schema.field_with_name(name).map_err(|_| {
                        ExecError::Internal(format!(
                            "storage source: projected column {name:?} not found in table \
                             schema {table_schema:?}"
                        ))
                    })?;
                    fields.push(field.clone());
                }
                Arc::new(Schema::new(fields))
            }
            None => table_schema.clone(),
        };

        // Rendezvous-plus-one channel: bounds the background thread to at
        // most one batch of read-ahead past what `next_batch` has consumed.
        // See the module docs' cancellation-contract paragraph.
        let (tx, rx) = mpsc::sync_channel::<Result<RecordBatch, ExecError>>(1);
        let worker = thread::Builder::new()
            .name("basin-storage-scan".into())
            .spawn(move || {
                let rt = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(rt) => rt,
                    Err(e) => {
                        let _ = tx.send(Err(ExecError::Internal(format!(
                            "storage source: failed to start a runtime: {e}"
                        ))));
                        return;
                    }
                };
                rt.block_on(async move {
                    let opened = match files {
                        FileSet::ListTablePrefix(table) => {
                            storage.read(&project, &table, opts).await
                        }
                        FileSet::LivePaths(paths) => {
                            // `Storage::read` warms the project's bucket
                            // assignment before reading (#36 stripe pool);
                            // `read_paths_with_schema` does not, so do it here
                            // to keep the two paths byte-identical in where
                            // they route their GETs. A no-op when no bucket
                            // pool is configured, which is the common case.
                            match storage.ensure_bucket_assignment(&project).await {
                                Ok(()) => {}
                                Err(e) => {
                                    let _ = tx.send(Err(ExecError::Internal(e.to_string())));
                                    return;
                                }
                            }
                            storage
                                .read_paths_with_schema(
                                    &project,
                                    paths,
                                    opts,
                                    Some(table_schema.clone()),
                                )
                                .await
                        }
                    };
                    let mut stream = match opened {
                        Ok(s) => s,
                        Err(e) => {
                            let _ = tx.send(Err(ExecError::Internal(e.to_string())));
                            return;
                        }
                    };
                    while let Some(item) = stream.next().await {
                        let mapped = item.map_err(|e| ExecError::Internal(e.to_string()));
                        let is_err = mapped.is_err();
                        // `send` returning `Err` means the receiver — and so
                        // `StorageBatchSource` — was dropped; nothing left to
                        // do but stop pulling from storage.
                        if tx.send(mapped).is_err() {
                            return;
                        }
                        if is_err {
                            return;
                        }
                    }
                });
            })
            .map_err(|e| ExecError::Internal(format!("storage source: spawn failed: {e}")))?;

        Ok(Self {
            schema,
            rx,
            _worker: Some(worker),
        })
    }
}

impl BatchSource for StorageBatchSource {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecError> {
        // One `recv()`: exactly one unit of bounded work, matching the
        // "return control between batches" contract `BatchSource` and
        // `Operator` both document. A disconnected channel (the background
        // thread finished, whether at EOF or after its last successful send)
        // reads as end-of-stream, since every real error was already sent as
        // an `Err` payload before the thread exits.
        match self.rx.recv() {
            Ok(Ok(batch)) => Ok(Some(batch)),
            Ok(Err(e)) => Err(e),
            Err(mpsc::RecvError) => Ok(None),
        }
    }
}

/// One table [`StorageTableResolver`] knows how to open.
struct TableEntry {
    project: ProjectId,
    schema: SchemaRef,
    /// The file set this table's scans read, pinned at registration time.
    /// See [`StorageTableResolver::register`] for why this is a file list and
    /// not a table name, and [`prune_live_files`] for why it carries each
    /// file's stats rather than just its path.
    live_files: Vec<DataFile>,
}

/// File-level statistics prune over a pinned live set: drop every file whose
/// per-column min/max PROVE the predicate cannot match, before any of them is
/// opened.
///
/// This is the prune `Storage::read` performs inside
/// `reader::resolve_table_read_paths`, reproduced here because the liveness
/// fix takes the file set from the catalog instead and so no longer goes
/// through that function. It is the same `evaluate_compound_for_pruning` over
/// the same `ColumnStats` shape — the catalog persists per-file stats at
/// write-commit time (Phase 5.7 A4) precisely so a caller can prune without a
/// footer fetch, which makes this strictly cheaper than the path it replaces:
/// `Storage::read`'s prune has to LIST and read footers first.
///
/// A file with no recorded stats (written before A4, envelope-encrypted, or
/// an unsupported Vortex type) yields no per-column entry, so
/// `evaluate_compound_for_pruning` returns `Mixed` and the file is KEPT. The
/// prune can therefore only ever drop a file it can prove is empty of matches
/// — never guess one away.
///
/// Losing this costs no correctness at all (the surviving scan re-applies
/// every predicate), only I/O, which is exactly why
/// `pushdown_reaches_storage_through_build` asserts on the open counter and
/// not on the row count.
fn prune_live_files(
    files: &[DataFile],
    filters: &[basin_storage::Predicate],
    schema: &Schema,
) -> Vec<ObjectPath> {
    if filters.is_empty() {
        return files.iter().map(|f| f.path.clone()).collect();
    }
    let mut atoms: Vec<basin_storage::CompoundPredicate> = filters
        .iter()
        .cloned()
        .map(basin_storage::CompoundPredicate::Atom)
        .collect();
    let compound = if atoms.len() == 1 {
        atoms.pop().expect("len == 1")
    } else {
        basin_storage::CompoundPredicate::And(atoms)
    };
    files
        .iter()
        .filter(|f| {
            !matches!(
                basin_storage::evaluate_compound_for_pruning(
                    &compound,
                    &f.column_stats,
                    schema,
                    f.row_count,
                ),
                basin_storage::PruneOutcome::NoMatch
            )
        })
        .map(|f| f.path.clone())
        .collect()
}

/// A [`TableResolver`] backed by [`basin_storage::Storage`].
///
/// Maps a plan-level [`TableId`] to a real `(project, table name, schema)`
/// and opens a [`StorageBatchSource`] for it on demand. See the module docs
/// for why this resolver cannot thread a `Scan`'s pushdown through today —
/// callers that need pushdown should construct [`StorageBatchSource`]
/// directly with a populated [`ReadOptions`] instead of going through
/// [`crate::build::build`].
pub struct StorageTableResolver {
    storage: Storage,
    tables: HashMap<u32, TableEntry>,
}

impl StorageTableResolver {
    pub fn new(storage: Storage) -> Self {
        Self {
            storage,
            tables: HashMap::new(),
        }
    }

    /// Register `table` as backed by `project` with Arrow schema `schema`,
    /// reading exactly `live_files`. `schema`'s field order must match the
    /// `ColId` numbering the planner used when it built `table`'s
    /// `LogicalPlan::Scan` nodes — mirrors
    /// [`crate::build::MemTableResolver::insert`]'s contract.
    ///
    /// `live_files` is a FILE SET, deliberately, rather than the table name it
    /// used to be. A resolver is what a SQL scan opens its table through, and
    /// a SQL scan must see the table's LIVE rows; only the caller holds the
    /// catalog that can say which files those are. Source it from
    /// `basin_catalog::TableMetadata::live_data_files()` — see
    /// [`StorageBatchSource::new_live_paths`] for what goes wrong when the
    /// file set is re-derived from an object-store LIST instead, and for why
    /// the catalog cannot be consulted one layer down in `basin-storage`.
    ///
    /// Each entry carries its per-file `column_stats` as well as its path, so
    /// [`TableResolver::open`] can still prune files by predicate before
    /// opening any of them ([`prune_live_files`]) — the prune that used to
    /// happen inside `Storage::read`.
    pub fn register(
        &mut self,
        table: TableId,
        project: ProjectId,
        schema: SchemaRef,
        live_files: Vec<DataFile>,
    ) {
        self.tables.insert(
            table.0,
            TableEntry {
                project,
                schema,
                live_files,
            },
        );
    }
}

impl TableResolver for StorageTableResolver {
    fn open(
        &self,
        table: TableId,
        projection: &[usize],
        filters: &[basin_plan::Expr],
    ) -> Option<(Box<dyn BatchSource>, crate::build::ScanPushdown)> {
        let entry = self.tables.get(&table.0)?;
        // `StorageBatchSource::new` only fails on thread-spawn exhaustion —
        // vanishingly rare, and `TableResolver::open`'s `Option` return type
        // (shared with `MemTableResolver`) has no room for a distinct error,
        // so it collapses to the same "unknown table" `None` a missing
        // registration would produce. A caller that needs to tell the two
        // apart should construct `StorageBatchSource` directly.
        // Build the ReadOptions from what the planner offered. Projection
        // always pushes — it is a column subset the reader honours directly.
        // Filters push only when every one of them translates to a
        // basin-storage Predicate; a partial translation would mean the scan
        // has to re-apply all of them anyway, so there is nothing to gain by
        // pushing some.
        let mut opts = ReadOptions::default();
        let mut pushed = crate::build::ScanPushdown::default();
        // Every requested position must name a real column. A `filter_map`
        // here would quietly hand back a NARROWER column list than the scan
        // asked for while still reporting `projection_applied`, and the scan's
        // identity remap would then read every column one slot off. A
        // registration whose schema disagrees with the plan is a bug either
        // way; declining the pushdown surfaces it as an out-of-range error
        // from `Scan::new` instead of as shifted data.
        let names: Option<Vec<String>> = projection
            .iter()
            .map(|i| entry.schema.fields().get(*i).map(|f| f.name().clone()))
            .collect();
        if let Some(names) = names.filter(|n| !n.is_empty()) {
            opts.projection = Some(names);
            pushed.projection_applied = true;
        }
        let translated: Vec<_> = filters
            .iter()
            .filter_map(|f| expr_to_predicate(f, &entry.schema, projection))
            .collect();
        if !filters.is_empty() && translated.len() == filters.len() {
            opts.filters = translated;
            pushed.filters_applied = true;
        }

        // File-level prune first, over the LIVE set only. `Storage::read` did
        // this inside `resolve_table_read_paths`; the live-paths read cannot,
        // because it is handed the file set rather than resolving one.
        let paths = prune_live_files(&entry.live_files, &opts.filters, &entry.schema);

        StorageBatchSource::new_live_paths(
            self.storage.clone(),
            entry.project,
            paths,
            entry.schema.clone(),
            opts,
        )
        .ok()
        .map(|s| (Box::new(s) as Box<dyn BatchSource>, pushed))
    }
}

/// Translate a planner predicate into a `basin-storage` one, if it is a shape
/// storage can prune on.
///
/// Deliberately narrow. `basin_storage::Predicate` covers equality, ordered
/// comparison, anchored prefix and integer membership — the shapes that map
/// onto file statistics, zone maps and bloom filters. Anything else returns
/// `None` and the scan evaluates it Arrow-side, which is always correct and
/// only costs I/O.
///
/// Storage predicates are keyed by column NAME, so the `ColumnRef` has to be
/// resolved to one. That resolution goes through `projection`, not straight
/// into `schema`: a scan filter's column index is a position within the scan's
/// own projection (see `build.rs`'s `LogicalPlan::Scan` arm for why the
/// logical layer numbers them that way), while `schema` is the whole table.
/// The two coincide only for an unpruned, identity projection.
///
/// Reading `schema` directly instead was a silent wrong answer, not a missed
/// prune: `SELECT n FROM u WHERE n > 7` prunes the scan to `projection=[n]`
/// and renumbers the predicate to position 0, which resolved against the table
/// to the name of the FIRST table column and pushed *that* column's
/// comparison into storage. Which wrong column you got — and so whether the
/// query came back unfiltered, empty, or right by luck — depended on the
/// SELECT list, since that is what decides the projection.
///
/// A position with no entry in `projection` yields `None`, so the predicate is
/// simply not pushed and the scan evaluates it Arrow-side. Correct, and never
/// a guess.
///
/// The operator OIDs are Postgres's own, read from `pg_operator` on a live
/// server rather than remembered: 96/410/416 are `=` for int4/int8/int8-int4,
/// 521/413 are `>`, 97/412 are `<`. Getting one wrong here would push the
/// wrong predicate into storage and silently return wrong rows, so they are
/// spelled out rather than computed.
fn expr_to_predicate(
    e: &basin_plan::Expr,
    schema: &arrow_schema::Schema,
    projection: &[usize],
) -> Option<basin_storage::Predicate> {
    use basin_plan::{Datum, Expr};
    use basin_storage::{Predicate, ScalarValue};

    let Expr::Binary { op, lhs, rhs } = e else {
        return None;
    };
    // Only `column OP literal`. The commuted form would need each operator's
    // negation, which is a table this does not have yet.
    let Expr::Column(c) = lhs.as_ref() else {
        return None;
    };
    let Expr::Literal(d, _) = rhs.as_ref() else {
        return None;
    };
    let table_col = *projection.get(c.index as usize)?;
    let name = schema.fields().get(table_col)?.name().clone();

    let scalar = match d {
        Datum::Int16(v) => ScalarValue::Int64(*v as i64),
        Datum::Int32(v) => ScalarValue::Int64(*v as i64),
        Datum::Int64(v) => ScalarValue::Int64(*v),
        Datum::Float32(v) => ScalarValue::Float64(*v as f64),
        Datum::Float64(v) => ScalarValue::Float64(*v),
        Datum::Utf8(v) => ScalarValue::Utf8(v.clone()),
        Datum::Bool(v) => ScalarValue::Boolean(*v),
        // NULL never compares true in SQL, and a storage predicate has no way
        // to say so — leave it for the Arrow-side evaluator.
        Datum::Null | Datum::Bytes(_) => return None,
    };

    match op.0.get() {
        96 | 410 | 416 | 15 | 532 | 533 => Some(Predicate::Eq(name, scalar)),
        521 | 413 | 419 | 762 | 1756 => Some(Predicate::Gt(name, scalar)),
        97 | 412 | 418 | 761 | 1754 => Some(Predicate::Lt(name, scalar)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field};
    use basin_storage::{
        FileFormat, Predicate, ReadCountersSnapshot, ScalarValue, StorageConfig, WriteOptions,
    };
    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;

    use crate::build::{self, MemTableResolver};

    // ── Fixtures ─────────────────────────────────────────────────────────

    fn schema_id_name() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    fn batch_id_name(ids: &[i64], names: &[&str]) -> RecordBatch {
        RecordBatch::try_new(
            schema_id_name(),
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
            ],
        )
        .unwrap()
    }

    /// Real, on-disk storage rooted at `dir` — a genuine `LocalFileSystem`,
    /// not `object_store::memory::InMemory`, so these tests exercise the
    /// same file-format encode/decode path production traffic does. Caches
    /// off, matching `basin-storage`'s own test convention (`lib.rs`'s
    /// `storage_in` helper), so nothing here is masked by a warm cache.
    fn storage_in(dir: &TempDir) -> Storage {
        let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        Storage::new(StorageConfig {
            object_store: Arc::new(fs),
            root_prefix: None,
            disk_cache: None,
            page_cache: None,
        })
    }

    fn rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    /// Write one batch to storage in `format`, returning the `(project,
    /// table)` pair the write landed under.
    fn write_one(
        storage: &Storage,
        format: FileFormat,
        batch: &RecordBatch,
    ) -> (ProjectId, TableName) {
        let project = ProjectId::new();
        let table = TableName::new("t").unwrap();
        rt().block_on(async {
            storage
                .write_batch_with_options(
                    &project,
                    &table,
                    &basin_common::PartitionKey::default_key(),
                    batch,
                    &WriteOptions {
                        file_format: format,
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
        });
        (project, table)
    }

    /// The live file set for a `StorageTableResolver::register` in these
    /// tests. There is no catalog attached to `storage_in`'s `Storage`, so
    /// there is no `live_data_files()` to ask; every file these tests write is
    /// still live (nothing here supersedes anything), which makes the LIST and
    /// the live set the same set. `_with_stats` because the resolver prunes on
    /// those stats — without a catalog they come from the file footers, which
    /// is where `Storage::read` used to get them too. Production callers must
    /// NOT source a scan's file set this way — see
    /// `StorageBatchSource::new_live_paths`.
    fn all_files_as_live(
        storage: &Storage,
        project: &ProjectId,
        table: &TableName,
    ) -> Vec<DataFile> {
        rt().block_on(async {
            storage
                .list_data_files_with_stats(project, table)
                .await
                .unwrap()
        })
    }

    fn drain(mut source: StorageBatchSource) -> Vec<RecordBatch> {
        let mut out = Vec::new();
        while let Some(b) = source.next_batch().unwrap() {
            out.push(b);
        }
        out
    }

    fn total_rows(batches: &[RecordBatch]) -> usize {
        batches.iter().map(|b| b.num_rows()).sum()
    }

    // ── Round trips ──────────────────────────────────────────────────────

    // Vortex first, deliberately: ADR 0015 makes it the default format, and
    // the task this module exists for says to name and handle it first
    // rather than as an afterthought to Parquet.
    #[test]
    fn vortex_round_trip_reads_back_every_row() {
        let dir = TempDir::new().unwrap();
        let storage = storage_in(&dir);
        let batch = batch_id_name(&[1, 2, 3], &["a", "b", "c"]);
        let (project, table) = write_one(&storage, FileFormat::Vortex, &batch);

        let source = StorageBatchSource::new(
            storage,
            project,
            table,
            schema_id_name(),
            ReadOptions::default(),
        )
        .unwrap();
        assert_eq!(
            source.schema().fields().len(),
            2,
            "full, unprojected schema"
        );
        let batches = drain(source);
        assert_eq!(total_rows(&batches), 3);
        let ids: Vec<i64> = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(
            ids,
            vec![1, 2, 3],
            "Vortex is the default format (ADR 0015)"
        );
    }

    #[test]
    fn parquet_round_trip_reads_back_every_row() {
        let dir = TempDir::new().unwrap();
        let storage = storage_in(&dir);
        let batch = batch_id_name(&[10, 20], &["x", "y"]);
        let (project, table) = write_one(&storage, FileFormat::Parquet, &batch);

        let source = StorageBatchSource::new(
            storage,
            project,
            table,
            schema_id_name(),
            ReadOptions::default(),
        )
        .unwrap();
        let batches = drain(source);
        assert_eq!(total_rows(&batches), 2);
        let names: Vec<String> = batches
            .iter()
            .flat_map(|b| {
                b.column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .map(|s| s.unwrap().to_string())
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(names, vec!["x".to_string(), "y".to_string()]);
    }

    // ── Projection ───────────────────────────────────────────────────────

    #[test]
    fn projection_pushdown_narrows_schema_and_columns() {
        let dir = TempDir::new().unwrap();
        let storage = storage_in(&dir);
        let batch = batch_id_name(&[1, 2], &["a", "b"]);
        let (project, table) = write_one(&storage, FileFormat::Vortex, &batch);

        let opts = ReadOptions {
            projection: Some(vec!["name".to_string()]),
            ..Default::default()
        };
        let source =
            StorageBatchSource::new(storage, project, table, schema_id_name(), opts).unwrap();
        assert_eq!(source.schema().fields().len(), 1);
        assert_eq!(source.schema().field(0).name(), "name");

        let batches = drain(source);
        assert_eq!(total_rows(&batches), 2);
        for b in &batches {
            assert_eq!(b.num_columns(), 1, "only the projected column comes back");
        }
    }

    // ── Empty table ──────────────────────────────────────────────────────

    #[test]
    fn empty_table_yields_no_batches() {
        let dir = TempDir::new().unwrap();
        let storage = storage_in(&dir);
        let project = ProjectId::new();
        let table = TableName::new("empty").unwrap();

        let source = StorageBatchSource::new(
            storage,
            project,
            table,
            schema_id_name(),
            ReadOptions::default(),
        )
        .unwrap();
        let batches = drain(source);
        assert!(
            batches.is_empty(),
            "a table with zero files has zero batches"
        );
    }

    // ── Multiple files ───────────────────────────────────────────────────

    #[test]
    fn multiple_files_are_all_read() {
        let dir = TempDir::new().unwrap();
        let storage = storage_in(&dir);
        let project = ProjectId::new();
        let table = TableName::new("multi").unwrap();
        rt().block_on(async {
            for chunk in [(0i64, 5i64), (5, 10), (10, 15)] {
                let ids: Vec<i64> = (chunk.0..chunk.1).collect();
                let names: Vec<String> = ids.iter().map(|i| format!("n{i}")).collect();
                let name_refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
                let batch = batch_id_name(&ids, &name_refs);
                storage
                    .write_batch(
                        &project,
                        &table,
                        &basin_common::PartitionKey::default_key(),
                        &batch,
                    )
                    .await
                    .unwrap();
            }
        });

        let source = StorageBatchSource::new(
            storage,
            project,
            table,
            schema_id_name(),
            ReadOptions::default(),
        )
        .unwrap();
        let batches = drain(source);
        assert_eq!(
            total_rows(&batches),
            15,
            "all three files' rows must come back"
        );
        let mut ids: Vec<i64> = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        ids.sort_unstable();
        assert_eq!(ids, (0..15).collect::<Vec<i64>>());
    }

    // ── Predicate pushdown actually prunes ──────────────────────────────

    /// Three files with disjoint `id` ranges; a `Gt` predicate that only the
    /// last file can satisfy must (a) return only that file's rows and (b)
    /// leave storage's own counters showing it never opened the files the
    /// stats prune could rule out — the whole point of pushing the filter
    /// down instead of reading every file and filtering in `Scan`.
    #[test]
    fn predicate_pushdown_prunes_files_not_just_rows() {
        let dir = TempDir::new().unwrap();
        let storage = storage_in(&dir);
        let project = ProjectId::new();
        let table = TableName::new("pruned").unwrap();
        rt().block_on(async {
            for chunk in [(0i64, 100i64), (100, 200), (200, 300)] {
                let ids: Vec<i64> = vec![chunk.0, chunk.1 - 1];
                let names: Vec<String> = ids.iter().map(|i| format!("n{i}")).collect();
                let name_refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
                let batch = batch_id_name(&ids, &name_refs);
                storage
                    .write_batch(
                        &project,
                        &table,
                        &basin_common::PartitionKey::default_key(),
                        &batch,
                    )
                    .await
                    .unwrap();
            }
        });

        let before: ReadCountersSnapshot = storage.read_counters().snapshot();

        let opts = ReadOptions {
            filters: vec![Predicate::Gt("id".to_string(), ScalarValue::Int64(250))],
            ..Default::default()
        };
        let source =
            StorageBatchSource::new(storage.clone(), project, table, schema_id_name(), opts)
                .unwrap();
        let batches = drain(source);

        let ids: Vec<i64> = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(ids, vec![299], "only the third file's id=299 row is > 250");

        let after = storage.read_counters().snapshot();
        let delta = after.delta(&before);
        assert_eq!(
            delta.files_opened, 1,
            "file-level stats pruning must skip both files whose id range can't satisfy id > 250"
        );
    }

    // ── Wired into a real `Scan`, through `StorageTableResolver` ────────

    /// The point of widening `TableResolver::open` to carry projection and
    /// filters: a plan built by `build()` must prune files at the storage
    /// layer, not open everything and filter afterwards.
    ///
    /// Reading only the projected columns and skipping files by predicate is
    /// Basin's whole scan advantage over Postgres's heap tuples. Losing it
    /// costs no correctness at all — the answers stay right and only the I/O
    /// gets worse — which is exactly why it needs a counter assertion rather
    /// than a row-count assertion. A row check would pass either way.
    #[test]
    fn pushdown_reaches_storage_through_build() {
        use basin_plan::{ColId, ColumnRef, Datum, Expr, LogicalPlan, OpId, SnapshotId, TableId};

        let dir = TempDir::new().unwrap();
        let storage = storage_in(&dir);
        let project = ProjectId::new();
        let table = TableName::new("pushed").unwrap();
        rt().block_on(async {
            for chunk in [(0i64, 100i64), (100, 200), (200, 300)] {
                let ids: Vec<i64> = vec![chunk.0, chunk.1 - 1];
                let names: Vec<String> = ids.iter().map(|i| format!("n{i}")).collect();
                let name_refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
                storage
                    .write_batch(
                        &project,
                        &table,
                        &basin_common::PartitionKey::default_key(),
                        &batch_id_name(&ids, &name_refs),
                    )
                    .await
                    .unwrap();
            }
        });

        let live = all_files_as_live(&storage, &project, &table);
        let mut resolver = StorageTableResolver::new(storage.clone());
        resolver.register(TableId(7), project, schema_id_name(), live);

        let before = storage.read_counters().snapshot();
        let plan = LogicalPlan::Scan {
            table: TableId(7),
            projection: vec![ColId(0), ColId(1)],
            // id > 250 — OID 521 is int8 `>`, from pg_operator on a live server.
            filters: vec![Expr::Binary {
                op: OpId(basin_pgtype::Oid(521)),
                lhs: Box::new(Expr::Column(ColumnRef {
                    relation: 0,
                    index: 0,
                    name: "id".into(),
                })),
                rhs: Box::new(Expr::Literal(Datum::Int64(250), basin_pgtype::PgType::INT8)),
            }],
            snapshot: SnapshotId(0),
        };

        let mut op = crate::build::build(&plan, &resolver).unwrap();
        let mut rows = 0usize;
        while let Some(b) = op.next_batch().unwrap() {
            rows += b.num_rows();
        }
        assert_eq!(rows, 1, "only id=299 satisfies id > 250");

        let delta = storage.read_counters().snapshot().delta(&before);
        assert_eq!(
            delta.files_opened, 1,
            "the predicate must prune two of three files AT STORAGE, not after decode — \
             a build() that opens everything and filters Arrow-side gives the same rows \
             and throws away the scan advantage silently"
        );
    }

    /// Three columns, chosen so that the same `> 7` predicate gives a
    /// DIFFERENT answer on each: `uid` passes for every row, `k` for none, `n`
    /// for exactly the last two. A predicate pushed against the wrong column
    /// therefore cannot come out right by coincidence.
    fn schema_uid_k_n() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("uid", DataType::Int64, false),
            Field::new("k", DataType::Int32, false),
            Field::new("n", DataType::Int32, false),
        ]))
    }

    fn batch_uid_k_n() -> RecordBatch {
        RecordBatch::try_new(
            schema_uid_k_n(),
            vec![
                Arc::new(Int64Array::from(vec![10i64, 11, 12])),
                Arc::new(arrow_array::Int32Array::from(vec![0, 0, 0])),
                Arc::new(arrow_array::Int32Array::from(vec![7, 8, 9])),
            ],
        )
        .unwrap()
    }

    /// Run `plan` against a freshly written `uid/k/n` table and return the
    /// first output column as text, so a caller can assert on VALUES.
    fn run_uid_k_n(projection: Vec<basin_plan::ColId>, filter_index: u16) -> Vec<String> {
        use basin_plan::{ColumnRef, Datum, Expr, LogicalPlan, OpId, SnapshotId, TableId};

        let dir = TempDir::new().unwrap();
        let storage = storage_in(&dir);
        let project = ProjectId::new();
        let table = TableName::new("ukn").unwrap();
        rt().block_on(async {
            storage
                .write_batch(
                    &project,
                    &table,
                    &basin_common::PartitionKey::default_key(),
                    &batch_uid_k_n(),
                )
                .await
                .unwrap();
        });

        let live = all_files_as_live(&storage, &project, &table);
        let mut resolver = StorageTableResolver::new(storage);
        resolver.register(TableId(3), project, schema_uid_k_n(), live);

        let plan = LogicalPlan::Scan {
            table: TableId(3),
            projection,
            // OID 521 is int4 `>`, from pg_operator on a live server. This is
            // exactly the operator class `expr_to_predicate` pushes, which is
            // why it is the one that produced wrong answers.
            filters: vec![Expr::Binary {
                op: OpId(basin_pgtype::Oid(521)),
                lhs: Box::new(Expr::Column(ColumnRef {
                    relation: 0,
                    index: filter_index,
                    name: "n".into(),
                })),
                rhs: Box::new(Expr::Literal(Datum::Int32(7), basin_pgtype::PgType::INT4)),
            }],
            snapshot: SnapshotId(0),
        };

        let mut op = crate::build::build(&plan, &resolver).unwrap();
        let opts = arrow::util::display::FormatOptions::default();
        let mut out = Vec::new();
        while let Some(b) = op.next_batch().unwrap() {
            let f =
                arrow::util::display::ArrayFormatter::try_new(b.column(0).as_ref(), &opts).unwrap();
            for r in 0..b.num_rows() {
                out.push(f.value(r).to_string());
            }
        }
        out
    }

    /// The scan filter's column index is a position within the scan's own
    /// `projection`, not within the table (see this module's docs). Resolving
    /// it against the table schema pushed a predicate on the wrong COLUMN NAME
    /// into storage, and storage then filtered on that column — silently, with
    /// the right row count shape and the wrong rows.
    ///
    /// This is the plan `SELECT n FROM u WHERE n > 7` optimizes to. Position 0
    /// of `[ColId(2)]` is `n`; read against the table it was `uid`, every
    /// value of which passes `> 7`, so the answer came back UNFILTERED.
    #[test]
    fn a_pushed_predicate_names_the_projected_column_not_the_tables_first() {
        assert_eq!(
            run_uid_k_n(vec![basin_plan::ColId(2)], 0),
            vec!["8".to_string(), "9".to_string()],
            "`n > 7` keeps 8 and 9; `uid > 7` would keep all three and `k > 7` none — \
             only the values tell those apart, which is why a row-count assertion \
             would have passed on the wrong answer"
        );
    }

    /// Same defect, opposite symptom — the half that makes row counts useless
    /// as a check. `SELECT uid FROM u WHERE n > 7` prunes to
    /// `projection=[uid, n]` and renumbers the predicate to position 1; read
    /// against the table that is `k`, whose every value FAILS `> 7`, so this
    /// direction returned zero rows. Same root cause as the test above: which
    /// wrong column you land on depends on the SELECT list.
    #[test]
    fn a_pushed_predicate_is_right_when_the_projection_keeps_two_columns() {
        assert_eq!(
            run_uid_k_n(vec![basin_plan::ColId(0), basin_plan::ColId(2)], 1),
            vec!["11".to_string(), "12".to_string()],
            "uid of the rows where n > 7"
        );
    }

    /// End-to-end: a `StorageTableResolver` feeding a `Scan` built by
    /// `crate::build::build`, proving `storage_source` is not just usable
    /// standalone but actually plugs into the operator tree the rest of the
    /// engine builds. Pushdown now DOES reach storage on this path — see
    /// `pushdown_reaches_storage_through_build` below, which is the test that
    /// proves it rather than assuming it.
    #[test]
    fn storage_table_resolver_feeds_a_real_scan() {
        let dir = TempDir::new().unwrap();
        let storage = storage_in(&dir);
        let batch = batch_id_name(&[1, 2, 3, 4], &["a", "b", "c", "d"]);
        let (project, table_name) = write_one(&storage, FileFormat::Vortex, &batch);

        let live = all_files_as_live(&storage, &project, &table_name);
        let mut resolver = StorageTableResolver::new(storage);
        let table_id = basin_plan::TableId(7);
        resolver.register(table_id, project, schema_id_name(), live);

        let plan = basin_plan::LogicalPlan::Scan {
            table: table_id,
            projection: vec![basin_plan::ColId(0), basin_plan::ColId(1)],
            filters: vec![],
            snapshot: basin_plan::SnapshotId(0),
        };
        let mut op = build::build(&plan, &resolver).unwrap();
        let mut rows = 0;
        while let Some(b) = op.next_batch().unwrap() {
            rows += b.num_rows();
        }
        assert_eq!(rows, 4);
    }

    /// `open` for an unregistered `TableId` behaves like `MemTableResolver`
    /// on an unknown table: `None`, not a panic.
    #[test]
    fn unregistered_table_id_resolves_to_none() {
        let dir = TempDir::new().unwrap();
        let storage = storage_in(&dir);
        let resolver = StorageTableResolver::new(storage);
        assert!(resolver.open(basin_plan::TableId(999), &[], &[]).is_none());
        // `MemTableResolver` is unused directly in this module's tests
        // otherwise; referencing it here just documents the parity claim
        // above without importing it unused elsewhere.
        let _ = MemTableResolver::new();
    }

    // ── Cancellation: one batch per `next_batch` call ───────────────────

    #[test]
    fn next_batch_returns_control_after_one_batch_not_the_whole_table() {
        let dir = TempDir::new().unwrap();
        let storage = storage_in(&dir);
        let project = ProjectId::new();
        let table = TableName::new("paced").unwrap();
        rt().block_on(async {
            for i in 0..3i64 {
                let batch = batch_id_name(&[i], &["r"]);
                storage
                    .write_batch(
                        &project,
                        &table,
                        &basin_common::PartitionKey::default_key(),
                        &batch,
                    )
                    .await
                    .unwrap();
            }
        });

        let mut source = StorageBatchSource::new(
            storage,
            project,
            table,
            schema_id_name(),
            ReadOptions::default(),
        )
        .unwrap();
        // Each call returns after exactly one file's batch (one row here),
        // not all three at once — and there are more calls than files, so a
        // source that secretly drained everything on the first call would
        // fail this by returning `None` too early or all 3 rows on call 1.
        let mut calls = 0;
        let mut total = 0;
        while let Some(b) = source.next_batch().unwrap() {
            calls += 1;
            total += b.num_rows();
            assert!(
                b.num_rows() <= 1,
                "one write_batch call produced one file producing one batch of 1 row"
            );
        }
        assert_eq!(total, 3);
        assert_eq!(
            calls, 3,
            "three files means three next_batch calls to drain them"
        );
    }
}
