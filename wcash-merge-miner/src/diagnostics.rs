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
    pool_backend_journal::PoolBackendJournalError,
    preparation_timing::{PreparationPhase, PreparationReport},
    MinerError, PoolBackendWinnerSnapshot, PoolWinnerReconciliationError,
};

const CAPACITY: usize = 128;
const RECENT_KEYS: usize = 128;
const INTERVAL: Duration = Duration::from_secs(30);
const NOISY_BURST: u64 = 32;
const TIMING_ORIGINS: usize = 128;
static DIAGNOSTICS: OnceLock<Diagnostics> = OnceLock::new();
static STARTUP: Mutex<()> = Mutex::new(());

#[cfg(test)]
thread_local! {
    static CAPTURE: std::cell::RefCell<Option<Vec<Event>>> = const { std::cell::RefCell::new(None) };
    static TIMING_CAPTURE: std::cell::RefCell<Option<Arc<Mutex<Vec<Event>>>>> = const { std::cell::RefCell::new(None) };
}

struct Diagnostics {
    sender: SyncSender<Event>,
    dropped: Arc<AtomicU64>,
    timing_origins: Mutex<VecDeque<ZecOrigin>>,
    timing_dropped: Arc<AtomicU64>,
}

impl Diagnostics {
    fn channel(capacity: usize) -> (Self, Receiver<Event>) {
        let (sender, receiver) = mpsc::sync_channel(capacity);
        (
            Self {
                sender,
                dropped: Arc::default(),
                timing_origins: Mutex::new(VecDeque::with_capacity(TIMING_ORIGINS)),
                timing_dropped: Arc::default(),
            },
            receiver,
        )
    }

    fn record(&self, mut event: Event) {
        event.timing_metadata_dropped_total = self.timing_dropped.load(Ordering::Relaxed);
        if self.sender.try_send(event).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn note_commit(&self, key: ZecOriginKey, committed_at: Instant) {
        let Ok(mut origins) = self.timing_origins.try_lock() else {
            self.timing_dropped.fetch_add(1, Ordering::Relaxed);
            return;
        };
        if origins.iter().any(|origin| origin.key == key) {
            return;
        }
        if origins.len() == TIMING_ORIGINS {
            origins.pop_front();
            self.timing_dropped.fetch_add(1, Ordering::Relaxed);
        }
        origins.push_back(ZecOrigin {
            key,
            committed_at,
            attempts: 0,
        });
    }

    fn begin_attempt(&self, key: ZecOriginKey) -> Option<(Instant, u64)> {
        let Ok(mut origins) = self.timing_origins.try_lock() else {
            self.timing_dropped.fetch_add(1, Ordering::Relaxed);
            return None;
        };
        let origin = origins.iter_mut().find(|origin| origin.key == key)?;
        origin.attempts = origin.attempts.saturating_add(1);
        Some((origin.committed_at, origin.attempts))
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
struct ZecOriginKey {
    share: [u8; 32],
    job: [u8; 32],
    hash: [u8; 32],
}

struct ZecOrigin {
    key: ZecOriginKey,
    committed_at: Instant,
    attempts: u64,
}

/// Captures only a newly durable ZEC winner, without printing or waiting for a sink.
pub(crate) fn note_zec_winner_committed(
    share_id: &Hex32,
    job_id: &Hex32,
    winner: &WinnerDescriptor,
) {
    if winner.chain != MergedChain::Zcash {
        return;
    }
    if let Some(diagnostics) = DIAGNOSTICS.get() {
        diagnostics.note_commit(
            ZecOriginKey {
                share: *share_id.as_bytes(),
                job: *job_id.as_bytes(),
                hash: *winner.block_hash_le.as_bytes(),
            },
            Instant::now(),
        );
    }
}

/// Same-process timings start after durability, never at hardware discovery.
#[derive(Clone)]
pub(crate) struct ZecWinnerAttempt {
    winner: Winner,
    started: Instant,
    fanout_started: Instant,
    committed_at: Option<Instant>,
    attempt_number: Option<u64>,
    redelivery: bool,
    #[cfg(test)]
    capture: Option<Arc<Mutex<Vec<Event>>>>,
}

#[derive(Clone, Copy)]
pub(crate) enum ZecRecipientRole {
    Template,
    Validator,
    Relay,
}

impl ZecRecipientRole {
    fn name(self) -> &'static str {
        match self {
            Self::Template => "template",
            Self::Validator => "validator",
            Self::Relay => "relay",
        }
    }
}

/// Local clocks only; this adds no read RPC or pre-send diagnostic.
pub(crate) struct ZecRecipientTiming {
    attempt: ZecWinnerAttempt,
    role: ZecRecipientRole,
    index: usize,
    started: Instant,
    preflight_finished: Option<Instant>,
    submit_finished: Option<Instant>,
    rpc_result: Option<&'static str>,
}

#[derive(Clone, Copy)]
struct ZecTiming {
    role: Option<&'static str>,
    index: Option<usize>,
    attempt_number: Option<u64>,
    redelivery: bool,
    commit_to_reconciliation_ms: Option<u64>,
    reconciliation_to_submit_start_ms: Option<u64>,
    commit_to_submit_start_ms: Option<u64>,
    reconciliation_to_submit_complete_ms: Option<u64>,
    commit_to_submit_complete_ms: Option<u64>,
    preflight_ms: Option<u64>,
    submit_rpc_ms: Option<u64>,
    classification_ms: Option<u64>,
    elapsed_ms: u64,
    fanout_elapsed_ms: Option<u64>,
    rpc_result: Option<&'static str>,
}

fn elapsed_ms(later: Instant, earlier: Instant) -> u64 {
    u64::try_from(later.saturating_duration_since(earlier).as_millis()).unwrap_or(u64::MAX)
}

impl ZecWinnerAttempt {
    pub(crate) fn for_snapshot(snapshot: &PoolBackendWinnerSnapshot, started: Instant) -> Self {
        let origin = DIAGNOSTICS.get().and_then(|diagnostics| {
            diagnostics.begin_attempt(ZecOriginKey {
                share: *snapshot.share_id().as_bytes(),
                job: *snapshot.job_id().as_bytes(),
                hash: *snapshot.winner().block_hash_le.as_bytes(),
            })
        });
        Self {
            winner: Winner::snapshot(snapshot),
            started,
            fanout_started: Instant::now(),
            committed_at: origin.map(|(at, _)| at),
            attempt_number: origin.map(|(_, number)| number),
            redelivery: !snapshot.lifecycle().requires_submission(),
            #[cfg(test)]
            capture: TIMING_CAPTURE.with(|capture| capture.borrow().clone()),
        }
    }

    pub(crate) fn recipient(self, role: ZecRecipientRole, index: usize) -> ZecRecipientTiming {
        ZecRecipientTiming {
            attempt: self,
            role,
            index,
            started: Instant::now(),
            preflight_finished: None,
            submit_finished: None,
            rpc_result: None,
        }
    }

    fn emit(&self, mut event: Event) {
        // Preserve the first same-process attempt through unrelated retry bursts.
        // The producer's bounded, lossy channel and drop counter still apply.
        event.noisy = self.attempt_number != Some(1);
        #[cfg(test)]
        if let Some(capture) = &self.capture {
            capture.lock().unwrap().push(event);
            return;
        }
        record(event);
    }

    pub(crate) fn completed(&self, result: &'static str, counts: Option<[usize; 6]>) {
        let finished = Instant::now();
        let mut event = Event::new(Some(self.winner), "winner_submission_result", result);
        event.counts = counts;
        event.warning = result != "best_chain_admitted";
        event.timing = Some(self.base_timing(finished));
        self.emit(event);
    }

    pub(crate) fn failed(&self, error: &MinerError) {
        self.completed(error_class(error), None);
    }

    fn base_timing(&self, finished: Instant) -> ZecTiming {
        ZecTiming {
            role: None,
            index: None,
            attempt_number: self.attempt_number,
            redelivery: self.redelivery,
            commit_to_reconciliation_ms: self.committed_at.map(|at| elapsed_ms(self.started, at)),
            reconciliation_to_submit_start_ms: None,
            commit_to_submit_start_ms: None,
            reconciliation_to_submit_complete_ms: None,
            commit_to_submit_complete_ms: None,
            preflight_ms: None,
            submit_rpc_ms: None,
            classification_ms: None,
            elapsed_ms: elapsed_ms(finished, self.started),
            fanout_elapsed_ms: Some(elapsed_ms(finished, self.fanout_started)),
            rpc_result: None,
        }
    }
}

impl ZecRecipientTiming {
    pub(crate) fn preflight_finished(&mut self) {
        self.preflight_finished = Some(Instant::now());
    }

    pub(crate) fn submitted(&mut self, result: &Result<serde_json::Value, MinerError>) {
        self.submit_finished = Some(Instant::now());
        self.rpc_result = Some(match result {
            Ok(serde_json::Value::Null) => "rpc_accepted",
            Ok(serde_json::Value::String(reason)) if reason.eq_ignore_ascii_case("duplicate") => {
                "duplicate"
            }
            Ok(serde_json::Value::String(_)) => "rpc_string_result",
            Ok(_) => "invalid_response",
            Err(error) => error_class(error),
        });
    }

    pub(crate) fn completed(self, result: &'static str) {
        let finished = Instant::now();
        let event = self.result_event(result, finished);
        self.attempt.emit(event);
    }

    fn result_event(&self, result: &'static str, finished: Instant) -> Event {
        let preflight_finished = self.preflight_finished.unwrap_or(finished);
        let mut timing = self.attempt.base_timing(finished);
        timing.role = Some(self.role.name());
        timing.index = Some(self.index);
        timing.preflight_ms = Some(elapsed_ms(preflight_finished, self.started));
        timing.submit_rpc_ms = self
            .submit_finished
            .map(|at| elapsed_ms(at, preflight_finished));
        timing.classification_ms = self.submit_finished.map(|at| elapsed_ms(finished, at));
        timing.reconciliation_to_submit_start_ms = self
            .submit_finished
            .map(|_| elapsed_ms(preflight_finished, self.attempt.started));
        timing.commit_to_submit_start_ms = self.submit_finished.and_then(|_| {
            self.attempt
                .committed_at
                .map(|commit| elapsed_ms(preflight_finished, commit))
        });
        timing.reconciliation_to_submit_complete_ms = self
            .submit_finished
            .map(|at| elapsed_ms(at, self.attempt.started));
        timing.commit_to_submit_complete_ms = self.submit_finished.and_then(|at| {
            self.attempt
                .committed_at
                .map(|commit| elapsed_ms(at, commit))
        });
        timing.elapsed_ms = elapsed_ms(finished, self.started);
        timing.fanout_elapsed_ms = None;
        timing.rpc_result = self.rpc_result;
        let mut event = Event::new(Some(self.attempt.winner), "winner_recipient_result", result);
        event.warning = result != "best_chain";
        event.timing = Some(timing);
        event
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
    queued_at: Instant,
    timing: Option<ZecTiming>,
    preparation: Option<PreparationReport>,
    timing_metadata_dropped_total: u64,
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
            queued_at: Instant::now(),
            timing: None,
            preparation: None,
            timing_metadata_dropped_total: 0,
        }
    }
}

/// Called once after a completed slow or failed preparation attempt.
/// All formatting stays on the existing bounded, lossy writer thread.
pub(crate) fn preparation_result(report: PreparationReport) {
    record(preparation_event(report));
}

fn preparation_event(report: PreparationReport) -> Event {
    let mut event = Event::new(
        None,
        "job_preparation_result",
        report.result.error_class().unwrap_or(report.result.label()),
    );
    event.warning = report.result.error_class().is_some();
    event.preparation = Some(report);
    event
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
    let timing_dropped = Arc::clone(&diagnostics.timing_dropped);
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
                    .summary(
                        &mut writer,
                        Instant::now(),
                        dropped.load(Ordering::Relaxed),
                        timing_dropped.load(Ordering::Relaxed),
                    )
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

#[cfg(test)]
pub(crate) fn capture_zec_timings<R>(run: impl FnOnce() -> R) -> (R, Vec<serde_json::Value>) {
    let captured = Arc::new(Mutex::new(Vec::new()));
    TIMING_CAPTURE.with(|capture| *capture.borrow_mut() = Some(Arc::clone(&captured)));
    let result = run();
    TIMING_CAPTURE.with(|capture| *capture.borrow_mut() = None);
    let mut output = Vec::new();
    let mut reporter = Reporter::new(Instant::now());
    for mut event in captured.lock().unwrap().drain(..) {
        event.noisy = false;
        reporter
            .record(&mut output, event, Instant::now(), 0)
            .unwrap();
    }
    let values = output
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).unwrap())
        .collect();
    (result, values)
}

#[cfg(test)]
pub(crate) fn capture_preparation_timings<R>(
    run: impl FnOnce() -> R,
) -> (R, Vec<serde_json::Value>) {
    CAPTURE.with(|capture| *capture.borrow_mut() = Some(Vec::new()));
    let result = run();
    let events = CAPTURE.with(|capture| capture.borrow_mut().take().unwrap());
    let mut output = Vec::new();
    let mut reporter = Reporter::new(Instant::now());
    for mut event in events
        .into_iter()
        .filter(|event| event.preparation.is_some())
    {
        event.noisy = false;
        reporter
            .record(&mut output, event, Instant::now(), 0)
            .unwrap();
    }
    let values = output
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).unwrap())
        .collect();
    (result, values)
}

#[derive(Eq, PartialEq)]
struct Key(
    Option<(&'static str, [u8; 32])>,
    &'static str,
    &'static str,
    Option<(&'static str, usize)>,
);

struct Reporter {
    recent: VecDeque<(Key, Instant, u64)>,
    window: Instant,
    burst: u64,
    suppressed: u64,
    reported_suppressed: u64,
    reported_dropped: u64,
    reported_timing_dropped: u64,
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
            reported_timing_dropped: 0,
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
                event
                    .timing
                    .and_then(|timing| timing.role.zip(timing.index))
                    .or_else(|| {
                        event
                            .preparation
                            .map(|report| (report.finished_phase.name(), 0))
                    }),
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
        let timing = event.timing.map(|timing| json!({
            "origin":if timing.commit_to_reconciliation_ms.is_some() {"post_durable_commit_same_process"} else {"reconciliation_entry_only"},
            "recipient_role":timing.role,"recipient_index":timing.index,
            "attempt_number":timing.attempt_number,
            "attempt_kind":match timing.attempt_number {Some(1)=>"first_observed_attempt",Some(_)=>"later_observed_attempt",None=>"unknown_origin"},
            "delivery_kind":if timing.redelivery {"observed_redelivery"} else {"required_submission"},
            "commit_to_reconciliation_ms":timing.commit_to_reconciliation_ms,
            "reconciliation_to_submit_start_ms":timing.reconciliation_to_submit_start_ms,
            "commit_to_submit_start_ms":timing.commit_to_submit_start_ms,
            "reconciliation_to_submit_complete_ms":timing.reconciliation_to_submit_complete_ms,
            "commit_to_submit_complete_ms":timing.commit_to_submit_complete_ms,
            "preflight_ms":timing.preflight_ms,"submit_rpc_ms":timing.submit_rpc_ms,
            "classification_ms":timing.classification_ms,"elapsed_ms":timing.elapsed_ms,
            "fanout_elapsed_ms":timing.fanout_elapsed_ms,"submit_attempted":timing.submit_rpc_ms.is_some(),
            "rpc_result":timing.rpc_result,
            "asserts_hardware_discovery_time":false
        }));
        let preparation = event.preparation.map(|report| {
            let phases: serde_json::Map<String, serde_json::Value> = PreparationPhase::ALL
                .into_iter()
                .map(|phase| {
                    (
                        phase.name().to_owned(),
                        json!(report.phases_ms[phase.index()]),
                    )
                })
                .collect();
            json!({
                "scope":"backend_preparation_through_activation",
                "outcome":report.result.label(),
                "elapsed_ms":report.elapsed_ms,
                "phase_ms":phases,
                "finished_phase":report.finished_phase.name(),
                "error_class":report.result.error_class(),
                "wcash_candidate_height":report.wcash_height,
                "zcash_candidate_height":report.zcash_height,
                "phase_accounting":"exclusive_caller_phases",
                "slow_threshold_ms":1000,
                "asserts_asic_notify_time":false,
                "asserts_winner_submission_latency":false
            })
        });
        let value = json!({"component":"native_pool_backend","level":if event.warning {"WARN"} else {"INFO"},
            "stage":event.stage,"result":event.result,"winner":winner,"observation":observation,"confirmations":event.confirmations,
            "node_outcome_counts":event.counts.map(|counts| json!({
                "accepted_best_chain":counts[0],"known_side_chain":counts[1],"unconfirmed":counts[2],
                "rejected":counts[3],"unavailable":counts[4],"invalid_response":counts[5]})),
            "submission_blocked":event.submission_blocked,
            "operation":event.operation,"os_error":event.os_error,"io_kind":event.io_kind,"suppressed_since_previous":suppressed,
            "suppressed_total":self.suppressed,"diagnostic_dropped_total":dropped,
            "timing_metadata_dropped_total":event.timing_metadata_dropped_total,
            "diagnostic_queue_wait_ms":elapsed_ms(now,event.queued_at),"timing":timing,"preparation":preparation,
            "asserts_independent_peer_receipt":false});
        write_record(writer, &value)
    }

    fn summary(
        &mut self,
        writer: &mut impl Write,
        now: Instant,
        dropped: u64,
        timing_dropped: u64,
    ) -> io::Result<()> {
        if now.duration_since(self.last_summary) < INTERVAL {
            return Ok(());
        }
        self.last_summary = now;
        if self.reported_suppressed == self.suppressed
            && self.reported_dropped == dropped
            && self.reported_timing_dropped == timing_dropped
        {
            return Ok(());
        }
        self.reported_suppressed = self.suppressed;
        self.reported_dropped = dropped;
        self.reported_timing_dropped = timing_dropped;
        write_record(
            writer,
            &json!({"component":"native_pool_backend","level":"WARN",
            "stage":"diagnostic_output_summary","suppressed_total":self.suppressed,"diagnostic_dropped_total":dropped,
            "timing_metadata_dropped_total":timing_dropped}),
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
    use crate::preparation_timing::{PreparationResult, PHASE_COUNT};

    fn preparation_fixture() -> PreparationReport {
        PreparationReport {
            elapsed_ms: 3_000,
            phases_ms: [0; PHASE_COUNT],
            finished_phase: PreparationPhase::WcashCandidate,
            wcash_height: Some(38_789),
            zcash_height: None,
            result: PreparationResult::Failed("rpc_error"),
        }
    }

    #[test]
    fn preparation_queue_saturation_and_disconnection_never_wait_for_sink_or_origin_lock() {
        let (diagnostics, receiver) = Diagnostics::channel(1);
        // Preparation reports never acquire the separate winner timing cache.
        let _held = diagnostics.timing_origins.lock().unwrap();
        let event = preparation_event(preparation_fixture());
        let started = Instant::now();
        for _ in 0..10_000 {
            diagnostics.record(event);
        }
        assert_eq!(diagnostics.dropped.load(Ordering::Relaxed), 9_999);
        assert!(receiver.try_recv().unwrap().preparation.is_some());
        assert!(receiver.try_recv().is_err());
        drop(receiver);
        diagnostics.record(event);
        assert_eq!(diagnostics.dropped.load(Ordering::Relaxed), 10_000);
        assert_eq!(diagnostics.timing_dropped.load(Ordering::Relaxed), 0);
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn preparation_retries_are_paced_by_phase_and_error_class_with_drop_visibility() {
        let now = Instant::now();
        let mut reporter = Reporter::new(now);
        let mut output = Vec::new();
        let report = preparation_fixture();
        let event = preparation_event(report);
        for _ in 0..100 {
            reporter.record(&mut output, event, now, 0).unwrap();
        }
        let mut other_phase = report;
        other_phase.finished_phase = PreparationPhase::ZcashProposal;
        reporter
            .record(&mut output, preparation_event(other_phase), now, 0)
            .unwrap();
        let mut other_error = report;
        other_error.result = PreparationResult::Failed("parent_tip_mismatch");
        reporter
            .record(&mut output, preparation_event(other_error), now, 0)
            .unwrap();
        reporter
            .record(&mut output, event, now + INTERVAL, 7)
            .unwrap();
        let rows = output
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(rows.len(), 4);
        assert_eq!(reporter.recent.len(), 3);
        assert_eq!(rows[3]["suppressed_since_previous"], 99);
        assert_eq!(rows[3]["diagnostic_dropped_total"], 7);
        assert_eq!(
            rows[3]["preparation"]["phase_accounting"],
            "exclusive_caller_phases"
        );
        assert_eq!(rows[3]["preparation"]["wcash_candidate_height"], 38_789);
        assert!(rows[3]["preparation"]["zcash_candidate_height"].is_null());
    }

    #[test]
    fn preparation_finish_emits_only_fixed_public_metadata_after_result() {
        let secret = "PRIVATE_ENDPOINT_PAYOUT_OR_RPC_BODY";
        let ((), rows) = capture_preparation_timings(|| {
            let mut timing = crate::NativePreparationTiming::new();
            timing.set_heights(38_789, 3_528_090);
            timing.enter(PreparationPhase::ZcashProposal);
            timing.finish_error(&MinerError::ParentProposalRejected {
                endpoint: secret.into(),
                reason: secret.into(),
            });
            // Abandoned attempts and fast successful attempts emit no event.
            let _abandoned = crate::NativePreparationTiming::new();
            crate::NativePreparationTiming::new().finish_ok();
        });
        assert_eq!(rows.len(), 1);
        assert!(!rows[0].to_string().contains("PRIVATE_"));
        assert_eq!(rows[0]["stage"], "job_preparation_result");
        assert_eq!(rows[0]["result"], "proposal_rejected");
        assert_eq!(rows[0]["level"], "WARN");
        assert_eq!(rows[0]["preparation"]["outcome"], "failed");
        assert_eq!(rows[0]["preparation"]["finished_phase"], "zcash_proposal");
        assert_eq!(
            rows[0]["preparation"]["phase_ms"]
                .as_object()
                .unwrap()
                .len(),
            PHASE_COUNT
        );
        assert_eq!(
            rows[0]["preparation"]["asserts_winner_submission_latency"],
            false
        );
        assert!(rows[0]["winner"].is_null());
    }

    #[test]
    fn maximum_preparation_report_is_bounded_and_separate_from_sink_lag() {
        let now = Instant::now();
        let mut report = preparation_fixture();
        report.elapsed_ms = u64::MAX;
        report.phases_ms = [u64::MAX; PHASE_COUNT];
        report.wcash_height = Some(u32::MAX);
        report.zcash_height = Some(u32::MAX);
        let mut event = preparation_event(report);
        event.queued_at = now;
        let mut output = Vec::new();
        Reporter::new(now)
            .record(
                &mut output,
                event,
                now + Duration::from_millis(80),
                u64::MAX,
            )
            .unwrap();
        assert!(output.len() < 4096);
        assert_eq!(output.iter().filter(|byte| **byte == b'\n').count(), 1);
        let row: serde_json::Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(row["preparation"]["elapsed_ms"], u64::MAX);
        assert_eq!(row["diagnostic_queue_wait_ms"], 80);
        assert!(row["timing"].is_null());
    }

    fn timing_fixture(now: Instant) -> ZecWinnerAttempt {
        ZecWinnerAttempt {
            winner: Winner {
                chain: "Zcash",
                hash_le: [1; 32],
                height: 42,
                job: [2; 32],
                event_seq: 7,
            },
            started: now + Duration::from_millis(30),
            fanout_started: now + Duration::from_millis(35),
            committed_at: Some(now),
            attempt_number: Some(1),
            redelivery: false,
            capture: None,
        }
    }

    #[test]
    fn zec_timing_origin_is_exact_bounded_and_never_reset_by_replay() {
        let (diagnostics, _receiver) = Diagnostics::channel(1);
        let now = Instant::now();
        let key = ZecOriginKey {
            share: [1; 32],
            job: [2; 32],
            hash: [3; 32],
        };
        diagnostics.note_commit(key, now);
        diagnostics.note_commit(key, now + Duration::from_secs(5));
        assert_eq!(diagnostics.begin_attempt(key), Some((now, 1)));
        assert_eq!(diagnostics.begin_attempt(key), Some((now, 2)));
        let different_share = ZecOriginKey {
            share: [4; 32],
            ..key
        };
        assert!(diagnostics.begin_attempt(different_share).is_none());
        for byte in 1..=128u8 {
            diagnostics.note_commit(
                ZecOriginKey {
                    hash: [byte; 32],
                    ..different_share
                },
                now,
            );
        }
        assert_eq!(
            diagnostics.timing_origins.lock().unwrap().len(),
            TIMING_ORIGINS
        );
        assert!(diagnostics.begin_attempt(key).is_none());
        assert_eq!(diagnostics.timing_dropped.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn zec_origin_contention_drops_metadata_without_waiting_or_touching_sink() {
        let (diagnostics, receiver) = Diagnostics::channel(1);
        let now = Instant::now();
        let key = ZecOriginKey {
            share: [1; 32],
            job: [2; 32],
            hash: [3; 32],
        };
        let _held = diagnostics.timing_origins.lock().unwrap();
        diagnostics.note_commit(key, now);
        assert!(diagnostics.begin_attempt(key).is_none());
        assert_eq!(diagnostics.timing_dropped.load(Ordering::Relaxed), 2);
        assert!(
            receiver.try_recv().is_err(),
            "origin capture never prints before submission"
        );
    }

    #[test]
    fn zec_recipient_timings_separate_preflight_rpc_classification_and_sink_lag() {
        let now = Instant::now();
        let recipient = ZecRecipientTiming {
            attempt: timing_fixture(now),
            role: ZecRecipientRole::Validator,
            index: 0,
            started: now + Duration::from_millis(40),
            preflight_finished: Some(now + Duration::from_millis(50)),
            submit_finished: Some(now + Duration::from_millis(80)),
            rpc_result: Some("rpc_accepted"),
        };
        let mut event = recipient.result_event("best_chain", now + Duration::from_millis(100));
        event.queued_at = now + Duration::from_millis(100);
        let mut output = Vec::new();
        Reporter::new(now)
            .record(&mut output, event, now + Duration::from_millis(120), 0)
            .unwrap();
        let row: serde_json::Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(row["timing"]["commit_to_reconciliation_ms"], 30);
        assert_eq!(row["timing"]["preflight_ms"], 10);
        assert_eq!(row["timing"]["submit_rpc_ms"], 30);
        assert_eq!(row["timing"]["classification_ms"], 20);
        assert_eq!(row["timing"]["reconciliation_to_submit_start_ms"], 20);
        assert_eq!(row["timing"]["commit_to_submit_start_ms"], 50);
        assert_eq!(row["timing"]["reconciliation_to_submit_complete_ms"], 50);
        assert_eq!(row["timing"]["commit_to_submit_complete_ms"], 80);
        assert_eq!(row["diagnostic_queue_wait_ms"], 20);
        assert_eq!(row["level"], "INFO");
        assert_eq!(row["timing"]["asserts_hardware_discovery_time"], false);
    }

    #[test]
    fn zec_timing_unknown_origin_and_not_attempted_are_null_not_zero() {
        let now = Instant::now();
        let mut attempt = timing_fixture(now);
        attempt.committed_at = None;
        attempt.attempt_number = None;
        let recipient = attempt.recipient(ZecRecipientRole::Template, 0);
        let event = recipient.result_event("not_eligible", Instant::now());
        let mut output = Vec::new();
        Reporter::new(now)
            .record(&mut output, event, Instant::now(), 0)
            .unwrap();
        let row: serde_json::Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(row["timing"]["origin"], "reconciliation_entry_only");
        assert!(row["timing"]["commit_to_reconciliation_ms"].is_null());
        assert!(row["timing"]["reconciliation_to_submit_start_ms"].is_null());
        assert!(row["timing"]["commit_to_submit_start_ms"].is_null());
        assert!(row["timing"]["commit_to_submit_complete_ms"].is_null());
        assert!(row["timing"]["submit_rpc_ms"].is_null());
        assert!(row["timing"]["classification_ms"].is_null());
        assert!(row["timing"]["rpc_result"].is_null());
    }

    #[test]
    fn zec_recipient_roles_have_separate_bounded_retry_keys() {
        let now = Instant::now();
        let mut reporter = Reporter::new(now);
        let mut output = Vec::new();
        for role in [
            ZecRecipientRole::Template,
            ZecRecipientRole::Validator,
            ZecRecipientRole::Relay,
        ] {
            let mut attempt = timing_fixture(now);
            attempt.attempt_number = Some(2);
            let event = attempt.recipient(role, 0).result_event("best_chain", now);
            reporter.record(&mut output, event, now, 0).unwrap();
            reporter.record(&mut output, event, now, 0).unwrap();
        }
        assert_eq!(output.iter().filter(|byte| **byte == b'\n').count(), 3);
        assert_eq!(reporter.suppressed, 3);
        assert_eq!(reporter.recent.len(), 3);
    }

    #[test]
    fn initial_zec_timings_survive_unrelated_retry_budget_but_later_attempts_are_bounded() {
        let now = Instant::now();
        let mut reporter = Reporter::new(now);
        let mut output = Vec::new();
        for byte in 0..u8::try_from(NOISY_BURST).unwrap() {
            let winner = Winner {
                chain: "Wcash",
                hash_le: [byte; 32],
                ..timing_fixture(now).winner
            };
            reporter
                .record(
                    &mut output,
                    Event::new(Some(winner), "winner_reconciliation", "submission_deferred"),
                    now,
                    0,
                )
                .unwrap();
        }
        assert_eq!(reporter.burst, NOISY_BURST);
        output.clear();

        let captured = Arc::new(Mutex::new(Vec::new()));
        let mut attempt = timing_fixture(now);
        attempt.capture = Some(Arc::clone(&captured));
        for role in [
            ZecRecipientRole::Template,
            ZecRecipientRole::Validator,
            ZecRecipientRole::Relay,
        ] {
            attempt.clone().recipient(role, 0).completed("best_chain");
        }
        attempt.completed("best_chain_admitted", Some([2, 0, 0, 0, 0, 0]));
        for attempt_number in [Some(2), None] {
            attempt.attempt_number = attempt_number;
            if attempt_number.is_none() {
                attempt.committed_at = None;
            }
            attempt
                .clone()
                .recipient(ZecRecipientRole::Template, 0)
                .completed("best_chain");
            attempt.completed("best_chain_admitted", Some([2, 0, 0, 0, 0, 0]));
        }
        for event in captured.lock().unwrap().drain(..) {
            reporter
                .record(&mut output, event, Instant::now(), 0)
                .unwrap();
        }
        let rows = output
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(rows.len(), 4);
        assert!(rows.iter().all(|row| row["timing"]["attempt_number"] == 1));
        assert_eq!(
            rows.iter()
                .filter(|row| row["stage"] == "winner_recipient_result")
                .count(),
            3
        );
        assert_eq!(rows[3]["stage"], "winner_submission_result");
        assert_eq!(reporter.suppressed, 4);
        assert_eq!(reporter.burst, NOISY_BURST);
    }

    #[test]
    fn zec_timing_full_or_broken_sink_preserves_nonblocking_drop_counters() {
        struct Broken;
        impl Write for Broken {
            fn write(&mut self, _bytes: &[u8]) -> io::Result<usize> {
                Err(io::ErrorKind::BrokenPipe.into())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let now = Instant::now();
        let event = timing_fixture(now)
            .recipient(ZecRecipientRole::Template, 0)
            .result_event("best_chain", now);
        let (diagnostics, receiver) = Diagnostics::channel(1);
        for _ in 0..10_000 {
            diagnostics.record(event);
        }
        assert_eq!(diagnostics.dropped.load(Ordering::Relaxed), 9_999);
        let event = receiver.recv().unwrap();
        assert_eq!(
            Reporter::new(now)
                .record(&mut Broken, event, now, 0)
                .unwrap_err()
                .kind(),
            io::ErrorKind::BrokenPipe
        );
        drop(receiver);
        diagnostics.record(event);
        assert_eq!(diagnostics.dropped.load(Ordering::Relaxed), 10_000);
    }

    #[test]
    fn zec_timing_rpc_error_details_are_never_formatted() {
        let now = Instant::now();
        let mut recipient = timing_fixture(now).recipient(ZecRecipientRole::Template, 0);
        recipient.preflight_finished();
        recipient.submitted(&Err(MinerError::RpcProtocol(
            "PRIVATE_ENDPOINT_OR_RESPONSE".into(),
        )));
        let event = recipient.result_event("invalid_response", Instant::now());
        let mut output = Vec::new();
        Reporter::new(now)
            .record(&mut output, event, Instant::now(), 0)
            .unwrap();
        assert!(!String::from_utf8(output).unwrap().contains("PRIVATE_"));
    }

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
        event.timing_metadata_dropped_total = u64::MAX;
        event.timing = Some(ZecTiming {
            role: Some("validator"),
            index: Some(usize::MAX),
            attempt_number: Some(u64::MAX),
            redelivery: true,
            commit_to_reconciliation_ms: Some(u64::MAX),
            reconciliation_to_submit_start_ms: Some(u64::MAX),
            commit_to_submit_start_ms: Some(u64::MAX),
            reconciliation_to_submit_complete_ms: Some(u64::MAX),
            commit_to_submit_complete_ms: Some(u64::MAX),
            preflight_ms: Some(u64::MAX),
            submit_rpc_ms: Some(u64::MAX),
            classification_ms: Some(u64::MAX),
            elapsed_ms: u64::MAX,
            fanout_elapsed_ms: Some(u64::MAX),
            rpc_result: Some("transport_unavailable"),
        });
        let mut reporter = Reporter::new(now);
        reporter.suppressed = u64::MAX;
        let mut sink = Sink::default();
        reporter.record(&mut sink, event, now, u64::MAX).unwrap();
        assert_eq!(sink.writes, 1);
        assert!(sink.bytes.len() < 4096);
        serde_json::from_slice::<serde_json::Value>(&sink.bytes).unwrap();
        let first_length = sink.bytes.len();
        reporter
            .summary(&mut sink, now + INTERVAL, u64::MAX, 0)
            .unwrap();
        assert_eq!(sink.writes, 2);
        assert!(sink.bytes.len() - first_length < 4096);
        serde_json::from_slice::<serde_json::Value>(&sink.bytes[first_length..]).unwrap();
    }
}
