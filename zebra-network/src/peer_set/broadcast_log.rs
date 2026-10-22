//! Bounded diagnostics for block broadcast batches, never transaction or share traffic.

use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex,
    },
    time::{Duration, Instant},
};

use zebra_chain::block;

use crate::Request;

const INTERVAL: Duration = Duration::from_secs(30);

/// A fixed-size limiter; contention drops diagnostics instead of delaying delivery.
struct Gate {
    last: Mutex<Option<Instant>>,
    suppressed: AtomicU64,
}

impl Gate {
    const fn new() -> Self {
        Self {
            last: Mutex::new(None),
            suppressed: AtomicU64::new(0),
        }
    }

    fn admit(&self, now: Instant) -> Option<u64> {
        let Ok(mut last) = self.last.try_lock() else {
            self.suppressed.fetch_add(1, Ordering::Relaxed);
            return None;
        };
        if last.is_some_and(|previous| now.duration_since(previous) < INTERVAL) {
            self.suppressed.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        *last = Some(now);
        Some(self.suppressed.swap(0, Ordering::Relaxed))
    }
}

// Success, failed batch, and no candidates have separate budgets, for each kind.
// A healthy batch cannot hide the first failure. No per-hash or per-peer state.
static GATES: [Gate; 6] = [const { Gate::new() }; 6];

#[derive(Clone, Copy)]
pub(super) struct BlockBroadcast {
    hash: block::Hash,
    all_peers: bool,
}

impl BlockBroadcast {
    pub(super) fn from_request(request: &Request) -> Option<Self> {
        match request {
            Request::AdvertiseBlockToAll(hash) => Some(Self {
                hash: *hash,
                all_peers: true,
            }),
            Request::AdvertiseBlock(hash, _) => Some(Self {
                hash: *hash,
                all_peers: false,
            }),
            _ => None,
        }
    }

    fn kind(self) -> &'static str {
        if self.all_peers {
            "all_peers"
        } else {
            "sampled"
        }
    }

    /// Empty queued passes are expected and must not imply zero candidate peers.
    pub(super) fn completed(self, ok: usize, failed: usize) {
        let gate = usize::from(self.all_peers) * 3 + usize::from(failed > 0);
        self.completed_with_gate(ok, failed, &GATES[gate]);
    }

    fn completed_with_gate(self, ok: usize, failed: usize, gate: &Gate) {
        let Some(outcome) = batch_outcome(ok, failed) else {
            return;
        };
        let Some(suppressed) = gate.admit(Instant::now()) else {
            return;
        };
        if failed > 0 {
            tracing::warn!(hash = ?self.hash, kind = self.kind(), attempted = ok + failed,
                ok, failed, outcome, suppressed, phase = "peer_send_summary",
                "block broadcast peer sends failed; independent receipt is unconfirmed");
        } else {
            tracing::info!(hash = ?self.hash, kind = self.kind(), attempted = ok,
                ok, failed, outcome, suppressed, phase = "peer_send_summary",
                "block broadcast peer requests completed; independent receipt is unconfirmed");
        }
    }

    /// Called only at the all-peer entry, before ready peers become unready.
    pub(super) fn no_candidates(self) {
        self.no_candidates_with_gate(&GATES[usize::from(self.all_peers) * 3 + 2]);
    }

    fn no_candidates_with_gate(self, gate: &Gate) {
        let Some(suppressed) = gate.admit(Instant::now()) else {
            return;
        };
        tracing::warn!(hash = ?self.hash, kind = self.kind(), attempted = 0,
            suppressed, phase = "peer_send_no_candidates",
            "block broadcast has no candidate peers; independent receipt is unconfirmed");
    }
}

fn batch_outcome(ok: usize, failed: usize) -> Option<&'static str> {
    match (ok, failed) {
        (0, 0) => None,
        (_, 0) => Some("requests_completed"),
        (0, _) => Some("all_failed"),
        (_, _) => Some("partial_failure"),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{self, Write},
        sync::Arc,
    };

    use super::*;

    #[derive(Clone, Default)]
    struct Capture(Arc<Mutex<Vec<u8>>>);

    impl Write for Capture {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0
                .lock()
                .map_err(|_| io::Error::other("test log capture poisoned"))?
                .extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn failed_and_partial_batches_are_visible_without_peer_data() {
        let capture = Capture::default();
        let writer = capture.clone();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            let broadcast = BlockBroadcast {
                hash: block::Hash([7; 32]),
                all_peers: true,
            };
            // Each fixture has an independent limiter; no global test ordering.
            broadcast.completed_with_gate(0, 2, &Gate::new());
            broadcast.completed_with_gate(1, 2, &Gate::new());
            broadcast.completed_with_gate(0, 0, &Gate::new());
        });
        let log = String::from_utf8(capture.0.lock().unwrap().clone()).unwrap();
        assert_eq!(log.lines().count(), 2, "empty queued pass must be silent");
        assert!(log.contains("WARN"));
        assert!(log.contains("all_failed"));
        assert!(log.contains("partial_failure"));
        assert!(log.contains("attempted=3"));
        assert!(log.contains("ok=1"));
        assert!(log.contains("failed=2"));
        assert!(log.contains(&format!("{:?}", block::Hash([7; 32]))));
        assert!(!log.contains("127.0.0.1"));
        assert!(!log.contains("request="));
    }

    #[test]
    fn empty_partial_and_failed_batches_are_distinct() {
        assert_eq!(batch_outcome(0, 0), None);
        assert_eq!(batch_outcome(0, 2), Some("all_failed"));
        assert_eq!(batch_outcome(1, 2), Some("partial_failure"));
        assert_eq!(batch_outcome(2, 0), Some("requests_completed"));
        assert!(BlockBroadcast::from_request(&Request::Peers).is_none());
    }

    #[test]
    fn no_candidates_is_visible_but_empty_queued_passes_are_silent() {
        let capture = Capture::default();
        let writer = capture.clone();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            let broadcast =
                BlockBroadcast::from_request(&Request::AdvertiseBlockToAll(block::Hash([8; 32])))
                    .unwrap();
            broadcast.completed_with_gate(0, 0, &Gate::new());
            broadcast.no_candidates_with_gate(&Gate::new());
            broadcast.completed_with_gate(0, 0, &Gate::new());
        });
        let log = String::from_utf8(capture.0.lock().unwrap().clone()).unwrap();
        assert_eq!(log.lines().count(), 1);
        assert!(log.contains("WARN"));
        assert!(log.contains("peer_send_no_candidates"));
        assert!(log.contains("attempted=0"));
        assert!(log.contains("all_peers"));
        assert!(log.contains("independent receipt is unconfirmed"));
        assert!(!log.contains("peer_send_summary"));
        assert!(!log.contains("all_failed"));
        assert!(!log.contains("requests_completed"));
    }

    #[test]
    fn bursts_and_contention_are_nonblocking_and_counted() {
        let gate = Gate::new();
        let now = Instant::now();
        assert_eq!(gate.admit(now), Some(0));
        assert_eq!(gate.admit(now + Duration::from_secs(29)), None);
        let held = gate.last.lock().unwrap();
        assert_eq!(gate.admit(now + INTERVAL), None);
        drop(held);
        assert_eq!(gate.admit(now + INTERVAL), Some(2));
        assert_eq!(gate.admit(now + INTERVAL * 2), Some(0));
    }
}
