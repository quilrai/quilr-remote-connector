//! Process-wide counters reported by the periodic heartbeat log line.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static CONNECTED: AtomicBool = AtomicBool::new(false);
static CONNECTED_SINCE_SECS: AtomicU64 = AtomicU64::new(0);
static ACTIVE_STREAMS: AtomicU64 = AtomicU64::new(0);
static REQUESTS: AtomicU64 = AtomicU64::new(0);
static ERRORS: AtomicU64 = AtomicU64::new(0);
static DISCONNECTS: AtomicU64 = AtomicU64::new(0);

fn unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub(crate) fn set_connected(connected: bool) {
    if connected {
        CONNECTED_SINCE_SECS.store(unix_secs(), Ordering::Relaxed);
    } else if CONNECTED.load(Ordering::Relaxed) {
        DISCONNECTS.fetch_add(1, Ordering::Relaxed);
    }
    CONNECTED.store(connected, Ordering::Relaxed);
}

pub(crate) fn record_request(failed: bool) {
    REQUESTS.fetch_add(1, Ordering::Relaxed);
    if failed {
        ERRORS.fetch_add(1, Ordering::Relaxed);
    }
}

/// Counts a forwarding stream as active until dropped.
pub(crate) struct ActiveStream(());

impl ActiveStream {
    pub(crate) fn begin() -> Self {
        ACTIVE_STREAMS.fetch_add(1, Ordering::Relaxed);
        Self(())
    }
}

impl Drop for ActiveStream {
    fn drop(&mut self) {
        ACTIVE_STREAMS.fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Snapshot {
    pub connected: bool,
    /// Seconds the current gateway session has been up; zero while disconnected.
    pub connected_secs: u64,
    pub active_streams: u64,
    /// Counters below are reset by each snapshot.
    pub requests: u64,
    pub errors: u64,
    pub disconnects: u64,
}

/// Reads the current state and resets the interval counters.
pub fn take_snapshot() -> Snapshot {
    let connected = CONNECTED.load(Ordering::Relaxed);
    Snapshot {
        connected,
        connected_secs: if connected {
            unix_secs().saturating_sub(CONNECTED_SINCE_SECS.load(Ordering::Relaxed))
        } else {
            0
        },
        active_streams: ACTIVE_STREAMS.load(Ordering::Relaxed),
        requests: REQUESTS.swap(0, Ordering::Relaxed),
        errors: ERRORS.swap(0, Ordering::Relaxed),
        disconnects: DISCONNECTS.swap(0, Ordering::Relaxed),
    }
}
