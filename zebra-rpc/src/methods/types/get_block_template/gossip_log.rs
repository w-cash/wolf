//! Fixed-size, rate-limited queue-admission diagnostics. No RPC data or error strings.

use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex,
    },
    time::Duration,
};

use tokio::{sync::mpsc::error::TrySendError, time::Instant};
use zebra_chain::block;

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
        if last.is_some_and(|previous| now.duration_since(previous) < Duration::from_secs(30)) {
            self.suppressed.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        *last = Some(now);
        Some(self.suppressed.swap(0, Ordering::Relaxed))
    }
}

// Queue-full, closed, and duplicate success have independent process-wide budgets.
static GATES: [Gate; 3] = [const { Gate::new() }; 3];

pub(super) fn record(
    hash: block::Hash,
    height: block::Height,
    duplicate: bool,
    result: &Result<(), TrySendError<(block::Hash, block::Height)>>,
) {
    record_with_gates(hash, height, duplicate, result, &GATES);
}

fn record_with_gates(
    hash: block::Hash,
    height: block::Height,
    duplicate: bool,
    result: &Result<(), TrySendError<(block::Hash, block::Height)>>,
    gates: &[Gate; 3],
) {
    let kind = if duplicate {
        "canonical_duplicate"
    } else {
        "fresh"
    };
    let (gate, outcome) = match result {
        Err(TrySendError::Full(_)) => (Some(0), "queue_full"),
        Err(TrySendError::Closed(_)) => (Some(1), "queue_closed"),
        Ok(()) if duplicate => (Some(2), "queued"),
        Ok(()) => (None, "queued"),
    };
    let suppressed = match gate {
        Some(gate) => match gates[gate].admit(Instant::now()) {
            Some(suppressed) => suppressed,
            None => return,
        },
        None => 0,
    };
    if result.is_err() {
        tracing::warn!(
            ?hash,
            ?height,
            kind,
            outcome,
            suppressed,
            phase = "rpc_gossip_admission",
            "accepted block gossip admission failed; retry the same submission"
        );
    } else {
        tracing::info!(
            ?hash,
            ?height,
            kind,
            outcome,
            suppressed,
            phase = "rpc_gossip_admission",
            "accepted block queued for gossip; independent receipt is unconfirmed"
        );
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
    fn fresh_duplicate_full_and_closed_admissions_are_distinct_and_payload_free() {
        let capture = Capture::default();
        let writer = capture.clone();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish();
        let hash = block::Hash([6; 32]);
        let height = block::Height(42);
        let full = Err(TrySendError::Full((hash, height)));
        let closed = Err(TrySendError::Closed((hash, height)));
        let fresh = Ok(());
        let duplicate = Ok(());
        tracing::subscriber::with_default(subscriber, || {
            let gates = [const { Gate::new() }; 3];
            record_with_gates(hash, height, false, &full, &gates);
            record_with_gates(hash, height, true, &closed, &gates);
            record_with_gates(hash, height, false, &fresh, &gates);
            record_with_gates(hash, height, true, &duplicate, &gates);
            // A repeated duplicate is paced without suppressing fresh admission.
            record_with_gates(hash, height, true, &duplicate, &gates);
            record_with_gates(hash, height, false, &fresh, &gates);
        });
        let log = String::from_utf8(capture.0.lock().unwrap().clone()).unwrap();
        let lines: Vec<_> = log.lines().collect();
        assert_eq!(lines.len(), 5);
        for (line, level, kind, outcome) in [
            (lines[0], "WARN", "fresh", "queue_full"),
            (lines[1], "WARN", "canonical_duplicate", "queue_closed"),
            (lines[2], "INFO", "fresh", "queued"),
            (lines[3], "INFO", "canonical_duplicate", "queued"),
            (lines[4], "INFO", "fresh", "queued"),
        ] {
            assert!(line.contains(level), "{line}");
            assert!(line.contains(kind), "{line}");
            assert!(line.contains(outcome), "{line}");
            assert!(line.contains("rpc_gossip_admission"), "{line}");
            assert!(line.contains(&format!("{hash:?}")), "{line}");
            assert!(line.contains(&format!("height={height:?}")), "{line}");
        }
        assert!(full.is_err() && closed.is_err() && fresh.is_ok() && duplicate.is_ok());
        assert!(!log.contains("TrySendError"));
        assert!(!log.contains("request="));
        assert!(!log.contains("127.0.0.1"));
        assert!(!log.contains("cookie"));
    }

    #[test]
    fn admission_diagnostics_never_wait_for_the_limiter() {
        let gate = Gate::new();
        let now = Instant::now();
        assert_eq!(gate.admit(now), Some(0));
        assert_eq!(gate.admit(now + Duration::from_secs(29)), None);
        let guard = gate.last.lock().unwrap();
        assert_eq!(gate.admit(now + Duration::from_secs(30)), None);
        drop(guard);
        assert_eq!(gate.admit(now + Duration::from_secs(30)), Some(2));
    }
}
