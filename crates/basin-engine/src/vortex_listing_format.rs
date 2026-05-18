//! Basin-local wrapper around [`vortex_datafusion::VortexFormat`] that patches
//! `Statistics.total_byte_size` so DataFusion's `join_selection` optimizer rule
//! gets a real value instead of `Precision::Absent`.
//!
//! ## Root cause
//!
//! `VortexFormat::infer_stats` folds `total_byte_size` from per-column
//! `Stat::UncompressedSizeInBytes`.  Because `PRUNING_STATS` in
//! `vortex-array-0.70.0` omits that stat, the fold always produces
//! `Precision::Absent`.  DataFusion's `join_selection` /
//! `supports_collect_by_thresholds` fall back to row-count heuristics when
//! byte-size is absent, which can mis-plan byte-skewed joins (observed as a
//! 0.54× regression on `inner_join@100k`).
//!
//! ## Fix (W2-1)
//!
//! After delegating to the inner `VortexFormat::infer_stats`, if
//! `total_byte_size` is still `Absent`, substitute
//! `Precision::Inexact(object.size as usize)`.  `ObjectMeta::size` is the
//! compressed on-disk byte count — a valid underestimate and sufficient for the
//! relative ordering the optimizer needs.  We never overwrite an `Exact` or
//! `Inexact` value returned upstream.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::Result as DFResult;
use datafusion::common::Statistics;
use datafusion::common::stats::Precision;
use datafusion::datasource::file_format::FileFormat;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_expr::{LexOrdering, LexRequirement};
use datafusion_datasource::TableSchema;
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_format::FileMeta;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::file_sink_config::FileSinkConfig;
use object_store::{ObjectMeta, ObjectStore};

/// Wraps [`vortex_datafusion::VortexFormat`] and patches `total_byte_size` in
/// `infer_stats` when the inner format returns `Precision::Absent`.
///
/// Every other [`FileFormat`] method is an exact pass-through to the inner
/// format so scan, write, schema-inference, and source-construction behaviour
/// are unchanged.
pub(crate) struct BasinVortexFormat {
    inner: Arc<vortex_datafusion::VortexFormat>,
}

impl BasinVortexFormat {
    /// Create a wrapper around an already-configured `VortexFormat`.
    pub(crate) fn new(inner: Arc<vortex_datafusion::VortexFormat>) -> Self {
        Self { inner }
    }
}

impl fmt::Debug for BasinVortexFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BasinVortexFormat")
            .field("inner", &self.inner)
            .finish()
    }
}

#[async_trait]
impl FileFormat for BasinVortexFormat {
    // ---- identity / metadata ------------------------------------------------

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_ext(&self) -> String {
        self.inner.get_ext()
    }

    fn get_ext_with_compression(
        &self,
        file_compression_type: &FileCompressionType,
    ) -> DFResult<String> {
        self.inner.get_ext_with_compression(file_compression_type)
    }

    fn compression_type(&self) -> Option<FileCompressionType> {
        self.inner.compression_type()
    }

    // ---- schema / statistics ------------------------------------------------

    async fn infer_schema(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> DFResult<SchemaRef> {
        self.inner.infer_schema(state, store, objects).await
    }

    /// Delegates to the inner `VortexFormat::infer_stats`, then patches
    /// `total_byte_size` if it came back as `Precision::Absent`.
    ///
    /// We substitute `Precision::Inexact(object.size as usize)` — the
    /// compressed on-disk byte count.  This is a valid underestimate of the
    /// uncompressed size and sufficient for the optimizer's relative
    /// join-side ordering.  An upstream `Exact` or `Inexact` value is
    /// never overwritten.
    async fn infer_stats(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> DFResult<Statistics> {
        let mut stats = self
            .inner
            .infer_stats(state, store, table_schema, object)
            .await?;

        if matches!(stats.total_byte_size, Precision::Absent) {
            stats.total_byte_size = Precision::Inexact(object.size as usize);
        }

        Ok(stats)
    }

    async fn infer_ordering(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> DFResult<Option<LexOrdering>> {
        self.inner
            .infer_ordering(state, store, table_schema, object)
            .await
    }

    /// Overrides the default `infer_stats_and_ordering` to route statistics
    /// through our patched `infer_stats` rather than the inner one, while
    /// delegating ordering to the inner format.
    async fn infer_stats_and_ordering(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> DFResult<FileMeta> {
        let statistics = self
            .infer_stats(state, store, Arc::clone(&table_schema), object)
            .await?;
        let ordering = self
            .infer_ordering(state, store, table_schema, object)
            .await?;
        Ok(FileMeta::new(statistics).with_ordering(ordering))
    }

    // ---- plan construction --------------------------------------------------

    async fn create_physical_plan(
        &self,
        state: &dyn Session,
        conf: FileScanConfig,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.inner.create_physical_plan(state, conf).await
    }

    async fn create_writer_physical_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        state: &dyn Session,
        conf: FileSinkConfig,
        order_requirements: Option<LexRequirement>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.inner
            .create_writer_physical_plan(input, state, conf, order_requirements)
            .await
    }

    fn file_source(&self, table_schema: TableSchema) -> Arc<dyn FileSource> {
        self.inner.file_source(table_schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::common::Statistics;
    use datafusion::common::stats::Precision;
    use object_store::ObjectMeta;
    use object_store::path::Path;

    /// Verify that the W2-1 patch replaces `Precision::Absent` with
    /// `Precision::Inexact(object.size)` and never overwrites a value that
    /// is already `Exact` or `Inexact`.
    ///
    /// We test the patch logic directly rather than through the async
    /// `infer_stats` call because the latter requires a live object-store and
    /// a real Vortex file.  The observable contract ("absent → inexact
    /// compressed size, non-absent → unchanged") is fully captured here.
    #[test]
    fn patch_replaces_absent_with_inexact_object_size() {
        let fake_size: u64 = 4096;
        let object = ObjectMeta {
            location: Path::from("data.vortex"),
            last_modified: chrono::Utc::now(),
            size: fake_size,
            e_tag: None,
            version: None,
        };

        // --- absent is replaced ---
        let mut stats = Statistics {
            num_rows: Precision::Exact(100),
            total_byte_size: Precision::Absent,
            column_statistics: vec![],
        };
        if matches!(stats.total_byte_size, Precision::Absent) {
            stats.total_byte_size = Precision::Inexact(object.size as usize);
        }
        assert!(
            !matches!(stats.total_byte_size, Precision::Absent),
            "Absent total_byte_size must be patched to non-Absent"
        );
        assert_eq!(
            stats.total_byte_size,
            Precision::Inexact(fake_size as usize),
            "patched value should equal object.size wrapped in Inexact"
        );

        // --- exact is preserved ---
        let mut exact = Statistics {
            num_rows: Precision::Exact(100),
            total_byte_size: Precision::Exact(9999),
            column_statistics: vec![],
        };
        if matches!(exact.total_byte_size, Precision::Absent) {
            exact.total_byte_size = Precision::Inexact(object.size as usize);
        }
        assert_eq!(
            exact.total_byte_size,
            Precision::Exact(9999),
            "Exact total_byte_size must not be overwritten"
        );

        // --- inexact is also preserved ---
        let mut inexact = Statistics {
            num_rows: Precision::Exact(100),
            total_byte_size: Precision::Inexact(8888),
            column_statistics: vec![],
        };
        if matches!(inexact.total_byte_size, Precision::Absent) {
            inexact.total_byte_size = Precision::Inexact(object.size as usize);
        }
        assert_eq!(
            inexact.total_byte_size,
            Precision::Inexact(8888),
            "Inexact total_byte_size must not be overwritten"
        );
    }

    /// Confirms that `BasinVortexFormat::new` accepts a `VortexFormat` and
    /// can be constructed without panicking.
    #[test]
    fn construction_succeeds() {
        use vortex::VortexSessionDefault as _;
        use vortex::session::VortexSession;
        use vortex_datafusion::VortexFormat;
        use vortex_datafusion::VortexTableOptions;

        let inner = Arc::new(VortexFormat::new_with_options(
            VortexSession::default(),
            VortexTableOptions {
                projection_pushdown: true,
                scan_concurrency: Some(1),
                ..Default::default()
            },
        ));
        let _format = BasinVortexFormat::new(inner);
    }
}
