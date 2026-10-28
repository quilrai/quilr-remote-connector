//! Log output setup (text or flat JSON, stdout or a daily-rotated file) and the heartbeat line.

use std::io::IsTerminal;
use std::time::{Duration, Instant};

use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::writer::BoxMakeWriter;

use quilr_remote_connector::telemetry;

use crate::{LogArgs, LogFormat};

const KEEP_LOG_FILES: usize = 7;

/// Installs the global subscriber. Keep the returned guard alive to flush file logs on exit.
pub(crate) fn init(args: &LogArgs) -> anyhow::Result<Option<WorkerGuard>> {
    let level = args.log_level.trim().to_ascii_lowercase();
    if !matches!(
        level.as_str(),
        "error" | "warn" | "info" | "debug" | "trace"
    ) {
        anyhow::bail!(
            "invalid --log-level '{}': use error, warn, info, or debug",
            args.log_level
        );
    }
    // Dependencies only log warnings; RUST_LOG still overrides everything for troubleshooting.
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new(format!(
            "warn,quilr_remote_connector={level},quilr_connector={level}"
        ))
    });

    let (writer, guard, terminal) = match &args.log_file {
        Some(path) => {
            let dir = path
                .parent()
                .filter(|dir| !dir.as_os_str().is_empty())
                .unwrap_or(std::path::Path::new("."));
            let stem = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("connector");
            std::fs::create_dir_all(dir).map_err(|e| {
                anyhow::anyhow!("cannot create log directory {}: {e}", dir.display())
            })?;
            let mut builder = RollingFileAppender::builder()
                .rotation(Rotation::DAILY)
                .filename_prefix(stem)
                .max_log_files(KEEP_LOG_FILES);
            // `path` itself becomes a symlink to the current day's file, for `tail -F`.
            if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                builder = builder.latest_symlink(name);
            }
            if let Some(ext) = path.extension().and_then(|s| s.to_str()) {
                builder = builder.filename_suffix(ext);
            }
            let appender = builder
                .build(dir)
                .map_err(|e| anyhow::anyhow!("cannot open log file {}: {e}", path.display()))?;
            let (writer, guard) = tracing_appender::non_blocking(appender);
            (BoxMakeWriter::new(writer), Some(guard), false)
        }
        None => (
            BoxMakeWriter::new(std::io::stdout),
            None,
            std::io::stdout().is_terminal(),
        ),
    };

    let json = match args.log_format {
        LogFormat::Json => true,
        LogFormat::Text => false,
        LogFormat::Auto => !terminal,
    };
    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(writer)
        .with_target(false);
    if json {
        builder
            .json()
            .flatten_event(true)
            .with_current_span(false)
            .with_span_list(false)
            .init();
    } else {
        builder.with_ansi(terminal).init();
    }
    Ok(guard)
}

/// Logs a status summary every `interval`; request counters cover the interval since the last line.
pub(crate) async fn heartbeat_loop(interval: Duration, shutdown: CancellationToken) {
    let started = Instant::now();
    let mut ticker = tokio::time::interval(interval);
    ticker.tick().await;
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = ticker.tick() => {}
        }
        let s = telemetry::take_snapshot();
        macro_rules! heartbeat {
            ($level:ident, $message:literal) => {
                $level!(
                    event = "heartbeat",
                    connected = s.connected,
                    uptime_secs = started.elapsed().as_secs(),
                    connected_secs = s.connected_secs,
                    active_streams = s.active_streams,
                    requests = s.requests,
                    errors = s.errors,
                    disconnects = s.disconnects,
                    $message
                )
            };
        }
        if s.connected {
            heartbeat!(info, "Connector healthy");
        } else {
            heartbeat!(warn, "Connector not connected to gateway");
        }
    }
}
