//! Lossy diagnostics are independent of the authoritative winner journal.
//!
//! Mining threads only copy fixed public metadata into a bounded channel. The
//! single writer formats JSON and may stall without blocking share admission.

use std::{
    collections::VecDeque,
    io::{self, Write},
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc::{self, Receiver, SyncSender},
        Arc, Mutex, OnceLock,
    },
    thread,
    time::{Duration, Instant},
};

use serde_json::json;
use wcash_pool_protocol::{Hex32, MergedChain, WinnerDescriptor};

use crate::{
    pool_backend_journal::PoolBackendJournalError, MinerError, PoolBackendWinnerSnapshot,
    PoolWinnerReconciliationError,
};

const CAPACITY: usize = 128;
const RECENT_KEYS: usize = 128;
const INTERVAL: Duration = Duration::from_secs(30);
const NOISY_BURST: u64 = 32;
static DIAGNOSTICS: OnceLock<Diagnostics> = OnceLock::new();
static STARTUP: Mutex<()> = Mutex::new(());

#[cfg(test)]
thread_local! {
    static CAPTURE: std::cell::RefCell<Option<Vec<Event>>> = const { std::cell::RefCell::new(None) };
}

struct Diagnostics {
    sender: SyncSender<Event>,
    dropped: Arc<AtomicU64>,
}

impl Diagnostics {
    fn channel(capacity: usize) -> (Self, Receiver<Event>) {
        let (sender, receiver) = mpsc::sync_channel(capacity);
        (
            Self {
                sender,
                dropped: Arc::default(),
            },
            receiver,
        )
    }

    fn record(&self, event: Event) {
        if self.sender.try_send(event).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
struct Winner {
    chain: &'static str,
    hash_le: [u8; 32],
    height: u32,
    job: [u8; 32],
    event_seq: u64,
}

#[derive(Clone, Copy)]
struct Observation {
    hash_be: [u8; 32],
    height: u32,
}

impl Winner {
    fn new(winner: &WinnerDescriptor, job: &Hex32, event_seq: u64) -> Self {
        Self {
            chain: match winner.chain {
                MergedChain::Wcash => "Wcash",
                MergedChain::Zcash => "Zcash",
            },
            hash_le: *winner.block_hash_le.as_bytes(),
            height: winner.height,
            job: *job.as_bytes(),
            event_seq,
        }
    }

    fn snapshot(snapshot: &PoolBackendWinnerSnapshot) -> Self {
        Self::new(
            snapshot.winner(),
            snapshot.job_id(),
            snapshot.revision_event_seq(),
        )
    }
}

#[derive(Clone, Copy)]
struct Event {
    winner: Option<Winner>,
    observation: Option<Observation>,
    stage: &'static str,
    result: &'static str,
    warning: bool,
    noisy: bool,
    confirmations: Option<u32>,
    counts: Option<[usize; 6]>,
    submission_blocked: Option<bool>,
    operation: Option<&'static str>,
    os_error: Option<i32>,
    io_kind: Option<&'static str>,
}

impl Event {
    fn winner(
        snapshot: &PoolBackendWinnerSnapshot,
        stage: &'static str,
        result: &'static str,
    ) -> Self {
        Self::new(Some(Winner::snapshot(snapshot)), stage, result)
    }

    fn new(winner: Option<Winner>, stage: &'static str, result: &'static str) -> Self {
        Self {
            winner,
            observation: None,
            stage,
            result,
            warning: false,
            noisy: true,
            confirmations: None,
            counts: None,
            submission_blocked: None,
            operation: None,
            os_error: None,
            io_kind: None,
        }
    }
}

fn record(event: Event) {
    #[cfg(test)]
    if CAPTURE.with(|capture| {
        if let Some(events) = capture.borrow_mut().as_mut() {
            events.push(event);
            true
        } else {
            false
        }
    }) {
        return;
    }
    if let Some(diagnostics) = DIAGNOSTICS.get() {
        diagnostics.record(event);
    }
}

/// Enables the single bounded diagnostic writer for `native-pool-backend` only.
///
/// Diagnostic failures are deliberately independent of journal durability. No
/// other command enables this writer, and shutdown never waits for a slow sink.
pub fn start_native_backend_diagnostics() -> io::Result<()> {
    let _startup = STARTUP
        .lock()
        .map_err(|_| io::Error::other("diagnostic startup unavailable"))?;
    if DIAGNOSTICS.get().is_some() {
        return Ok(());
    }
    let mut writer = diagnostic_writer()?;
    let (diagnostics, receiver) = Diagnostics::channel(CAPACITY);
    let dropped = Arc::clone(&diagnostics.dropped);
    thread::Builder::new()
        .name("wcash-diagnostics".into())
        .spawn(move || {
            let mut reporter = Reporter::new(Instant::now());
            loop {
                match receiver.recv_timeout(Duration::from_secs(1)) {
                    Ok(event) => {
                        if reporter
                            .record(
                                &mut writer,
                                event,
                                Instant::now(),
                                dropped.load(Ordering::Relaxed),
                            )
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                    Err(mpsc::RecvTimeoutError::Disconnected) => break,
                }
                if reporter
                    .summary(&mut writer, Instant::now(), dropped.load(Ordering::Relaxed))
                    .is_err()
                {
                    break;
                }
            }
        })?;
    let _ = DIAGNOSTICS.set(diagnostics);
    let mut started = Event::new(None, "diagnostics_started", "bounded_lossy_writer");
    started.noisy = false;
    record(started);
    Ok(())
}

#[cfg(unix)]
fn diagnostic_writer() -> io::Result<std::fs::File> {
    use std::os::fd::AsFd;
    // A private descriptor avoids holding Rust's process-wide stderr mutex if
    // journald stops reading. Only the diagnostic thread can block on this fd.
    Ok(io::stderr().as_fd().try_clone_to_owned()?.into())
}

#[cfg(not(unix))]
fn diagnostic_writer() -> io::Result<std::fs::File> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "native backend diagnostics require an independent Unix descriptor",
    ))
}

pub(crate) fn lifecycle(
    snapshot: &PoolBackendWinnerSnapshot,
    stage: &'static str,
    event_seq: u64,
    confirmations: Option<u32>,
) {
    let mut event = Event::winner(snapshot, "winner_lifecycle_persisted", stage);
    if let Some(winner) = event.winner.as_mut() {
        winner.event_seq = event_seq;
    }
    event.confirmations = confirmations;
    event.warning = matches!(stage, "orphaned" | "quarantined" | "side_chain");
    event.noisy = false;
    record(event);
}

pub(crate) fn submission(
    snapshot: &PoolBackendWinnerSnapshot,
    stage: &'static str,
    result: &'static str,
    counts: Option<[usize; 6]>,
) {
    let mut event = Event::winner(snapshot, stage, result);
    event.counts = counts;
    event.warning = stage == "winner_submission_result"
        && !matches!(result, "rpc_accepted" | "best_chain_admitted");
    record(event);
}

pub(crate) fn submission_error(snapshot: &PoolBackendWinnerSnapshot, error: &MinerError) {
    submission(
        snapshot,
        "winner_submission_result",
        error_class(error),
        None,
    );
}

pub(crate) fn observer_header_result(
    height: u32,
    expected_hash: &str,
    result: &Result<Option<u32>, MinerError>,
) {
    if let Some(event) = observer_header_event(height, expected_hash, result) {
        record(event);
    }
}

fn observer_header_event(
    height: u32,
    expected_hash: &str,
    result: &Result<Option<u32>, MinerError>,
) -> Option<Event> {
    let Err(error) = result else {
        return None;
    };
    // The existing conservative policy can omit this transport error while
    // other nodes answer positively. Report that omission without changing it.
    if !matches!(error, MinerError::RpcTransport(_) | MinerError::Io(_)) {
        return None;
    }
    let mut hash_be = [0; 32];
    if hex::decode_to_slice(expected_hash, &mut hash_be).is_err() {
        return None;
    }
    let mut event = Event::new(None, "observer_header_failure", "transport_unavailable");
    event.observation = Some(Observation { hash_be, height });
    event.warning = true;
    Some(event)
}

/// Reports a failed persisted lifecycle update without exposing its error text.
pub fn record_native_winner_persistence_failure(snapshot: &PoolBackendWinnerSnapshot) {
    let mut event = Event::winner(snapshot, "winner_persistence_failure", "persistence_failed");
    event.warning = true;
    event.noisy = false;
    record(event);
}

/// Records a safe bounded reconciliation outcome without formatting its error.
pub fn record_native_winner_reconciliation(
    snapshot: &PoolBackendWinnerSnapshot,
    error: Option<&PoolWinnerReconciliationError>,
) {
    let Some(error) = error else {
        return;
    };
    let mut event = Event::winner(
        snapshot,
        "winner_reconciliation",
        error_class(error.miner_error()),
    );
    event.warning = true;
    event.submission_blocked = Some(error.submission_blocked());
    record(event);
}

fn error_class(error: &MinerError) -> &'static str {
    match error {
        MinerError::WinnerSubmissionDeferred { .. } => "submission_deferred",
        MinerError::RpcTransport(_) | MinerError::Io(_) => "transport_unavailable",
        MinerError::RpcError { .. } => "rpc_error",
        MinerError::ParentTipMismatch { .. } => "parent_tip_mismatch",
        _ => "reconciliation_error",
    }
}

pub(crate) fn journal_error(error: &PoolBackendJournalError) {
    if let Some(event) = journal_event(error) {
        record(event);
    }
}

fn journal_event(error: &PoolBackendJournalError) -> Option<Event> {
    let mut event = Event::new(None, "journal_failure", "io_error");
    event.warning = true;
    match error {
        PoolBackendJournalError::Io {
            operation, source, ..
        } => {
            event.result = "io_error";
            event.operation = Some(operation);
            event.os_error = source.raw_os_error();
            event.io_kind = Some(io_kind(source.kind()));
            event.noisy = false;
        }
        PoolBackendJournalError::PathChanged { .. } => {
            event.result = "path_changed";
            event.noisy = false;
        }
        PoolBackendJournalError::Poisoned => event.result = "poisoned",
        PoolBackendJournalError::MutexPoisoned => event.result = "mutex_poisoned",
        PoolBackendJournalError::JournalTooLarge { .. }
        | PoolBackendJournalError::EventCapacity { .. }
        | PoolBackendJournalError::WinnerCapacity { .. } => event.result = "capacity_exhausted",
        PoolBackendJournalError::Allocation(_) => event.result = "allocation_failed",
        PoolBackendJournalError::Overflow => event.result = "capacity_overflow",
        _ => return None,
    }
    Some(event)
}

fn io_kind(kind: io::ErrorKind) -> &'static str {
    match kind {
        io::ErrorKind::NotFound => "not_found",
        io::ErrorKind::PermissionDenied => "permission_denied",
        io::ErrorKind::StorageFull => "storage_full",
        io::ErrorKind::WriteZero => "write_zero",
        io::ErrorKind::Interrupted => "interrupted",
        io::ErrorKind::BrokenPipe => "broken_pipe",
        _ => "other",
    }
}

#[cfg(test)]
pub(crate) fn capture<R>(run: impl FnOnce() -> R) -> (R, Vec<(&'static str, &'static str)>) {
    CAPTURE.with(|capture| *capture.borrow_mut() = Some(Vec::new()));
    let result = run();
    let events = CAPTURE.with(|capture| capture.borrow_mut().take().unwrap());
    (
        result,
        events
            .into_iter()
            .map(|event| (event.stage, event.result))
            .collect(),
    )
}

#[derive(Eq, PartialEq)]
struct Key(Option<(&'static str, [u8; 32])>, &'static str, &'static str);

struct Reporter {
    recent: VecDeque<(Key, Instant, u64)>,
    window: Instant,
    burst: u64,
    suppressed: u64,
    reported_suppressed: u64,
    reported_dropped: u64,
    last_summary: Instant,
}

impl Reporter {
    fn new(now: Instant) -> Self {
        Self {
            recent: VecDeque::new(),
            window: now,
            burst: 0,
            suppressed: 0,
            reported_suppressed: 0,
            reported_dropped: 0,
            last_summary: now,
        }
    }

    fn record(
        &mut self,
        writer: &mut impl Write,
        event: Event,
        now: Instant,
        dropped: u64,
    ) -> io::Result<()> {
        let mut suppressed = 0;
        if event.noisy {
            if now.duration_since(self.window) >= INTERVAL {
                self.window = now;
                self.burst = 0;
            }
            let key = Key(
                event
                    .winner
                    .map(|w| (w.chain, w.hash_le))
                    .or_else(|| event.observation.map(|o| ("Zcash", o.hash_be))),
                event.stage,
                event.result,
            );
            if let Some((_, previous, count)) = self
                .recent
                .iter_mut()
                .find(|(candidate, _, _)| *candidate == key)
            {
                if now.duration_since(*previous) < INTERVAL || self.burst >= NOISY_BURST {
                    *count = count.saturating_add(1);
                    self.suppressed = self.suppressed.saturating_add(1);
                    return Ok(());
                }
                suppressed = *count;
                *count = 0;
                *previous = now;
            } else if self.burst >= NOISY_BURST {
                self.suppressed = self.suppressed.saturating_add(1);
                return Ok(());
            } else {
                if self.recent.len() == RECENT_KEYS {
                    self.recent.pop_front();
                }
                self.recent.push_back((key, now, 0));
            }
            self.burst = self.burst.saturating_add(1);
        }
        let winner = event.winner.map(|w| {
            let mut hash = w.hash_le;
            hash.reverse();
            json!({"chain":w.chain,"hash":hex::encode(hash),"height":w.height,
                "job_id":hex::encode(w.job),"event_seq":w.event_seq})
        });
        let observation = event.observation.map(|o| json!({"chain":"Zcash","role":"observer","hash":hex::encode(o.hash_be),"height":o.height}));
        let value = json!({"component":"native_pool_backend","level":if event.warning {"WARN"} else {"INFO"},
            "stage":event.stage,"result":event.result,"winner":winner,"observation":observation,"confirmations":event.confirmations,
            "node_outcome_counts":event.counts.map(|counts| json!({
                "accepted_best_chain":counts[0],"known_side_chain":counts[1],"unconfirmed":counts[2],
                "rejected":counts[3],"unavailable":counts[4],"invalid_response":counts[5]})),
            "submission_blocked":event.submission_blocked,
            "operation":event.operation,"os_error":event.os_error,"io_kind":event.io_kind,"suppressed_since_previous":suppressed,
            "suppressed_total":self.suppressed,"diagnostic_dropped_total":dropped,
            "asserts_independent_peer_receipt":false});
        write_record(writer, &value)
    }

    fn summary(&mut self, writer: &mut impl Write, now: Instant, dropped: u64) -> io::Result<()> {
        if now.duration_since(self.last_summary) < INTERVAL {
            return Ok(());
        }
        self.last_summary = now;
        if self.reported_suppressed == self.suppressed && self.reported_dropped == dropped {
            return Ok(());
        }
        self.reported_suppressed = self.suppressed;
        self.reported_dropped = dropped;
        write_record(
            writer,
            &json!({"component":"native_pool_backend","level":"WARN",
            "stage":"diagnostic_output_summary","suppressed_total":self.suppressed,"diagnostic_dropped_total":dropped}),
        )
    }
}

fn write_record(writer: &mut impl Write, value: &serde_json::Value) -> io::Result<()> {
    // Formatting and allocation belong only to the sink thread. A complete
    // bounded line is sent together so unrelated stderr messages do not splice
    // into serde's incremental writes.
    let mut encoded = serde_json::to_vec(value)?;
    encoded.push(b'\n');
    writer.write_all(&encoded)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn saturated_sink_does_not_wait_for_writer() {
        let (diagnostics, _receiver) = Diagnostics::channel(1);
        let event = Event::new(None, "fixture", "safe");
        let start = Instant::now();
        for _ in 0..10_000 {
            diagnostics.record(event);
        }
        assert_eq!(diagnostics.dropped.load(Ordering::Relaxed), 9_999);
        assert!(start.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn retry_output_is_bounded_and_reports_suppression() {
        let now = Instant::now();
        let mut reporter = Reporter::new(now);
        let mut output = Vec::new();
        let event = Event::new(None, "winner_reconciliation", "submission_deferred");
        for _ in 0..100 {
            reporter.record(&mut output, event, now, 0).unwrap();
        }
        assert_eq!(output.iter().filter(|byte| **byte == b'\n').count(), 1);
        reporter
            .record(&mut output, event, now + INTERVAL, 5)
            .unwrap();
        let rows: Vec<serde_json::Value> = output
            .split(|b| *b == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice(line).unwrap())
            .collect();
        assert_eq!(rows[1]["suppressed_since_previous"], 99);
        assert_eq!(rows[1]["diagnostic_dropped_total"], 5);
    }

    #[test]
    fn journal_diagnostics_never_format_private_path_or_error() {
        let error = PoolBackendJournalError::Io {
            operation: "fsync",
            path: "PRIVATE_PATH_SENTINEL".into(),
            source: io::Error::other("PRIVATE_RPC_OR_BLOCK_SENTINEL"),
        };
        // The typed event excludes both potentially private strings by construction.
        let event = journal_event(&error).unwrap();
        let mut output = Vec::new();
        Reporter::new(Instant::now())
            .record(&mut output, event, Instant::now(), 0)
            .unwrap();
        let output = String::from_utf8(output).unwrap();
        assert!(!output.contains("SENTINEL"));
        assert!(output.contains("fsync"));
        assert!(output.contains("other"));
    }

    #[test]
    fn public_identity_is_display_order_and_record_is_bounded() {
        let mut hash = [0; 32];
        hash[0] = 0x12;
        hash[31] = 0xab;
        let winner = Winner {
            chain: "Zcash",
            hash_le: hash,
            height: 42,
            job: [0x23; 32],
            event_seq: 7,
        };
        let mut event = Event::new(Some(winner), "winner_durable", "fsync_completed");
        event.noisy = false;
        let mut output = Vec::new();
        Reporter::new(Instant::now())
            .record(&mut output, event, Instant::now(), 0)
            .unwrap();
        assert!(output.len() < 4096);
        let record: serde_json::Value = serde_json::from_slice(&output).unwrap();
        let display = record["winner"]["hash"].as_str().unwrap();
        assert!(display.starts_with("ab") && display.ends_with("12"));
        assert_eq!(record["winner"]["event_seq"], 7);
        assert_eq!(record["asserts_independent_peer_receipt"], false);
    }

    #[test]
    fn noisy_distinct_keys_and_recent_state_remain_bounded() {
        let now = Instant::now();
        let mut reporter = Reporter::new(now);
        let mut output = Vec::new();
        for byte in 0..=255u8 {
            let winner = Winner {
                chain: "Zcash",
                hash_le: [byte; 32],
                height: 42,
                job: [0; 32],
                event_seq: 1,
            };
            let event = Event::new(Some(winner), "winner_submission_result", "incomplete");
            reporter.record(&mut output, event, now, 0).unwrap();
        }
        assert_eq!(
            output.iter().filter(|byte| **byte == b'\n').count(),
            usize::try_from(NOISY_BURST).unwrap()
        );
        assert_eq!(reporter.suppressed, 256 - NOISY_BURST);
        for index in 0..256u32 {
            let mut hash = [0; 32];
            hash[..4].copy_from_slice(&index.to_le_bytes());
            let winner = Winner {
                chain: "Zcash",
                hash_le: hash,
                height: 42,
                job: [0; 32],
                event_seq: 1,
            };
            reporter
                .record(
                    &mut output,
                    Event::new(Some(winner), "fixture", "safe"),
                    now + INTERVAL * (index + 1),
                    0,
                )
                .unwrap();
        }
        assert_eq!(reporter.recent.len(), RECENT_KEYS);
    }

    #[test]
    fn hard_storage_failure_bypasses_retry_budget_and_bad_share_is_silent() {
        assert!(journal_event(&PoolBackendJournalError::ShareConflict {
            share_id: Hex32::new([1; 32])
        })
        .is_none());
        let now = Instant::now();
        let mut reporter = Reporter::new(now);
        reporter.burst = NOISY_BURST;
        let error = PoolBackendJournalError::Io {
            operation: "fsync",
            path: "PRIVATE_PATH".into(),
            source: io::Error::from(io::ErrorKind::StorageFull),
        };
        let mut output = Vec::new();
        reporter
            .record(&mut output, journal_event(&error).unwrap(), now, 0)
            .unwrap();
        let record: serde_json::Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(record["stage"], "journal_failure");
        assert_eq!(record["io_kind"], "storage_full");
    }

    #[test]
    fn observer_unavailable_is_distinct_from_exact_absence_and_positive_audit() {
        let hash = format!("12{}ab", "00".repeat(30));
        assert!(observer_header_event(42, &hash, &Ok(None)).is_none());
        assert!(observer_header_event(42, &hash, &Ok(Some(3))).is_none());
        let event = observer_header_event(
            42,
            &hash,
            &Err(MinerError::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                "PRIVATE_ENDPOINT_OR_BODY",
            ))),
        )
        .unwrap();
        let mut output = Vec::new();
        Reporter::new(Instant::now())
            .record(&mut output, event, Instant::now(), 0)
            .unwrap();
        assert!(!String::from_utf8_lossy(&output).contains("PRIVATE_"));
        let row: serde_json::Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(row["stage"], "observer_header_failure");
        assert_eq!(row["result"], "transport_unavailable");
        assert_eq!(row["observation"]["hash"], hash);
        assert_eq!(row["observation"]["role"], "observer");
        assert!(row["observation"].get("job_id").is_none());
        assert!(row["observation"].get("event_seq").is_none());
        assert!(row["winner"].is_null());
    }

    #[test]
    fn maximum_shape_and_summary_are_bounded_single_write_lines() {
        #[derive(Default)]
        struct Sink {
            writes: usize,
            bytes: Vec<u8>,
        }
        impl Write for Sink {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                self.writes += 1;
                self.bytes.extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let now = Instant::now();
        let winner = Winner {
            chain: "Zcash",
            hash_le: [0xff; 32],
            height: u32::MAX,
            job: [0xff; 32],
            event_seq: u64::MAX,
        };
        let mut event = Event::new(
            Some(winner),
            "winner_lifecycle_persisted",
            "transport_unavailable",
        );
        event.observation = Some(Observation {
            hash_be: [0xff; 32],
            height: u32::MAX,
        });
        event.noisy = false;
        event.warning = true;
        event.confirmations = Some(u32::MAX);
        event.counts = Some([usize::MAX; 6]);
        event.submission_blocked = Some(true);
        event.operation = Some("fsync recovered");
        event.os_error = Some(i32::MIN);
        event.io_kind = Some("permission_denied");
        let mut reporter = Reporter::new(now);
        reporter.suppressed = u64::MAX;
        let mut sink = Sink::default();
        reporter.record(&mut sink, event, now, u64::MAX).unwrap();
        assert_eq!(sink.writes, 1);
        assert!(sink.bytes.len() < 4096);
        serde_json::from_slice::<serde_json::Value>(&sink.bytes).unwrap();
        let first_length = sink.bytes.len();
        reporter
            .summary(&mut sink, now + INTERVAL, u64::MAX)
            .unwrap();
        assert_eq!(sink.writes, 2);
        assert!(sink.bytes.len() - first_length < 4096);
        serde_json::from_slice::<serde_json::Value>(&sink.bytes[first_length..]).unwrap();
    }
}
