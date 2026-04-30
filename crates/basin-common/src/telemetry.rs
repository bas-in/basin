//! Telemetry init.
//!
//! Per-tenant metrics from day one (see TASK.md cross-cutting). This module
//! sets up tracing-subscriber so logs are JSON in prod and pretty-printed in
//! dev. OTLP export is feature-gated so unit tests don't need a collector.

use tracing::Level;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Output format for the tracing subscriber.
#[derive(Clone, Copy, Debug, Default)]
pub enum LogFormat {
    /// Pretty, ANSI-coloured, human-readable. Default for dev.
    #[default]
    Pretty,
    /// One JSON object per line. Use in prod and CI.
    Json,
}

/// Initialise the global tracing subscriber.
///
/// Idempotent only at first call — repeated calls return Err. Tests should
/// call [`try_init_for_tests`] which silently ignores reinit errors.
pub fn init(default_level: Level, format: LogFormat) -> Result<(), tracing_subscriber::util::TryInitError> {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(default_level.to_string()));

    let registry = tracing_subscriber::registry().with(filter);

    match format {
        LogFormat::Pretty => registry
            .with(tracing_subscriber::fmt::layer().with_target(true))
            .try_init(),
        LogFormat::Json => registry
            .with(
                tracing_subscriber::fmt::layer()
                    .json()
                    .with_target(true)
                    .with_current_span(true),
            )
            .try_init(),
    }
}

/// Test-helper. Safe to call from many tests in the same process; the second
/// and subsequent calls are no-ops.
pub fn try_init_for_tests() {
    let _ = tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")))
        .with(tracing_subscriber::fmt::layer().with_test_writer())
        .try_init();
}
