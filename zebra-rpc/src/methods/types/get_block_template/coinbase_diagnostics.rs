//! Bounded, non-authoritative coinbase diagnostics, separate from RPC and proof work.
//!
//! Producers only attempt a bounded send. The sole logger thread applies fixed
//! process-wide budgets before fixed JSON serialization and fallible stdout I/O.
//! A blocked sink cannot block a proof, its waiter, or candidate publication.
//! I/O errors disable the logger; panic isolation is not assumed in abort builds.

use std::{
    cell::Cell,
    io::{self, Write},
    marker::PhantomData,
    panic::{catch_unwind, AssertUnwindSafe},
    rc::Rc,
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc::{sync_channel, Receiver, SyncSender, TrySendError},
        Arc, OnceLock,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const QUEUE_CAPACITY: usize = 64;

/// The calling stage, never derived from an address or a transaction.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) enum Phase {
    #[default]
    Unknown,
    Template,
    Sizing,
    SelectedCoinbase,
    Prewarm,
}

impl Phase {
    fn label(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Template => "template",
            Self::Sizing => "sizing",
            Self::SelectedCoinbase => "selected_coinbase",
            Self::Prewarm => "prewarm",
        }
    }
}

/// Copy this context explicitly when dispatching work to another thread.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct Context {
    pub(super) request_id: u64,
    pub(super) phase: Phase,
}

thread_local! {
    static CONTEXT: Cell<Context> = const { Cell::new(Context {
        request_id: 0,
        phase: Phase::Unknown,
    }) };
}

/// Restores the current thread's prior context, including during unwinding.
#[must_use = "keep the guard alive for the diagnostic scope"]
pub(super) struct ContextGuard {
    previous: Context,
    // A thread-local restoration guard must never migrate to another thread.
    _same_thread: PhantomData<Rc<()>>,
}

impl Drop for ContextGuard {
    fn drop(&mut self) {
        CONTEXT.with(|context| context.set(self.previous));
    }
}

pub(super) fn current() -> Context {
    CONTEXT.with(Cell::get)
}

pub(super) fn enter(request_id: u64, phase: Phase) -> ContextGuard {
    let previous = current();
    CONTEXT.with(|context| context.set(Context { request_id, phase }));
    ContextGuard {
        previous,
        _same_thread: PhantomData,
    }
}

pub(super) fn phase(phase: Phase) -> ContextGuard {
    enter(current().request_id, phase)
}

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

/// Process-local correlation only. Zero means unknown after ID exhaustion.
/// Never wraps or reuses a previously issued nonzero identity.
pub(super) fn next_id() -> u64 {
    take_id(&NEXT_ID)
}

fn take_id(counter: &AtomicU64) -> u64 {
    counter
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            value.checked_add(1)
        })
        .unwrap_or(0)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Event {
    CacheLookup,
    CacheCall,
    Constructor,
    ReserveRegistration,
    ReserveWork,
    TemplateWorker,
}

impl Event {
    fn label(self) -> &'static str {
        match self {
            Self::CacheLookup => "cache_lookup",
            Self::CacheCall => "cache_call",
            Self::Constructor => "constructor",
            Self::ReserveRegistration => "reserve_registration",
            Self::ReserveWork => "reserve_work",
            Self::TemplateWorker => "template_worker",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Outcome {
    ForegroundReady,
    ReserveReady,
    JoinBuilding,
    NewBuild,
    Ready,
    Ok,
    Error,
    Panic,
    Accepted,
    StateContended,
    CacheContended,
    OutsideWindow,
    ExistingForeground,
    ExistingReserve,
    Capacity,
    Stale,
    Skipped,
    Started,
    Finished,
    Busy,
    Deadline,
    NoRuntime,
    HeightLimit,
}

impl Outcome {
    fn label(self) -> &'static str {
        match self {
            Self::ForegroundReady => "foreground_ready",
            Self::ReserveReady => "reserve_ready",
            Self::JoinBuilding => "join_building",
            Self::NewBuild => "new_build",
            Self::Ready => "ready",
            Self::Ok => "ok",
            Self::Error => "error",
            Self::Panic => "panic",
            Self::Accepted => "accepted",
            Self::StateContended => "state_contended",
            Self::CacheContended => "cache_contended",
            Self::OutsideWindow => "outside_window",
            Self::ExistingForeground => "existing_foreground",
            Self::ExistingReserve => "existing_reserve",
            Self::Capacity => "capacity",
            Self::Stale => "stale",
            Self::Skipped => "skipped",
            Self::Started => "started",
            Self::Finished => "finished",
            Self::Busy => "busy",
            Self::Deadline => "deadline",
            Self::NoRuntime => "no_runtime",
            Self::HeightLimit => "height_limit",
        }
    }
}

/// A fixed-size numeric record. There is no payload or free-form string field.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Record {
    pub(super) observed_at_unix_ms: u64,
    enqueued_at: Instant,
    pub(super) request_id: u64,
    pub(super) cache_id: u64,
    pub(super) entry_id: u64,
    pub(super) height: Option<u32>,
    pub(super) fee_zat: Option<u64>,
    pub(super) phase: Phase,
    pub(super) event: Event,
    pub(super) outcome: Outcome,
    pub(super) elapsed_ms: u64,
    pub(super) dispatch_ms: Option<u64>,
    pub(super) selected_txs: Option<u64>,
    pub(super) reserve_start: Option<u32>,
    pub(super) reserve_end: Option<u32>,
    pub(super) observed_fee_zat: Option<u64>,
    pub(super) lookup_key_reserved: Option<bool>,
}

impl Record {
    pub(super) fn new(event: Event, outcome: Outcome) -> Self {
        let context = current();
        Self {
            observed_at_unix_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
                .unwrap_or(0),
            enqueued_at: Instant::now(),
            request_id: context.request_id,
            phase: context.phase,
            event,
            outcome,
            cache_id: 0,
            entry_id: 0,
            height: None,
            fee_zat: None,
            elapsed_ms: 0,
            dispatch_ms: None,
            selected_txs: None,
            reserve_start: None,
            reserve_end: None,
            observed_fee_zat: None,
            lookup_key_reserved: None,
        }
    }

    fn important(self) -> bool {
        matches!(self.event, Event::Constructor | Event::TemplateWorker)
            || matches!(
                self.outcome,
                Outcome::JoinBuilding
                    | Outcome::NewBuild
                    | Outcome::Error
                    | Outcome::Panic
                    | Outcome::StateContended
                    | Outcome::CacheContended
                    | Outcome::Busy
                    | Outcome::Deadline
                    | Outcome::NoRuntime
                    | Outcome::HeightLimit
            )
            || (self.event == Event::ReserveWork
                && matches!(self.outcome, Outcome::Started | Outcome::Finished))
            || (self.event == Event::CacheCall && self.elapsed_ms >= 1_000)
    }
}

#[derive(Default)]
struct Counters {
    dropped: AtomicU64,
    suppressed: AtomicU64,
}

fn increment(counter: &AtomicU64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
        Some(value.saturating_add(1))
    });
}

struct Logger {
    sender: SyncSender<Record>,
    counters: Arc<Counters>,
}

impl Logger {
    fn emit(&self, record: Record) {
        match self.sender.try_send(record) {
            Ok(()) => {}
            Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) => {
                increment(&self.counters.dropped);
            }
        }
    }
}

static LOGGER: OnceLock<Option<Logger>> = OnceLock::new();
static DISABLED_DROPPED: AtomicU64 = AtomicU64::new(0);

/// Call from handler/cache initialization, before admitting proof work.
/// Failure disables diagnostics for this process; producers never retry spawning.
pub(super) fn initialize() {
    LOGGER.get_or_init(|| {
        start_logger(write_record)
            .map(|(logger, worker)| {
                // This dedicated thread never owns RPC, proof, cache or runtime handles.
                drop(worker);
                logger
            })
            .ok()
    });
}

/// Never initializes, waits for a limiter, or calls tracing on the producer.
pub(super) fn emit(record: Record) {
    #[cfg(test)]
    if capture_record(record) {
        return;
    }
    if let Some(Some(logger)) = LOGGER.get() {
        logger.emit(record);
    } else {
        increment(&DISABLED_DROPPED);
    }
}

/// Fixed token budgets: important 1/s with burst 4; routine 1/5s with burst 2.
/// In a 300s interval this admits at most 304 + 62 records. Discarding a
/// fractional refill interval is conservative and cannot increase that bound.
struct Gate {
    tokens: u64,
    burst: u64,
    refill_seconds: u64,
    refilled_at: Duration,
}

impl Gate {
    fn new(refill_seconds: u64, burst: u64) -> Self {
        Self {
            tokens: burst,
            burst,
            refill_seconds,
            refilled_at: Duration::ZERO,
        }
    }

    fn admit(&mut self, now: Duration) -> bool {
        let refill = now.saturating_sub(self.refilled_at).as_secs() / self.refill_seconds;
        if refill != 0 {
            self.tokens = self.tokens.saturating_add(refill).min(self.burst);
            self.refilled_at = now;
        }
        if self.tokens == 0 {
            return false;
        }
        self.tokens -= 1;
        true
    }
}

struct Limits {
    important: Gate,
    routine: Gate,
}

impl Limits {
    fn new() -> Self {
        Self {
            important: Gate::new(1, 4),
            routine: Gate::new(5, 2),
        }
    }

    fn admit(&mut self, record: Record, now: Duration, counters: &Counters) -> bool {
        let admitted = if record.important() {
            self.important.admit(now)
        } else {
            self.routine.admit(now)
        };
        if !admitted {
            increment(&counters.suppressed);
        }
        admitted
    }
}

type Job = Box<dyn FnOnce() + Send + 'static>;

fn start_logger<F>(sink: F) -> io::Result<(Logger, JoinHandle<()>)>
where
    F: FnMut(Record, u64, u64) -> io::Result<()> + Send + 'static,
{
    start_logger_with_spawn(sink, |job| {
        thread::Builder::new()
            .name("aux-coinbase-log".to_owned())
            .spawn(job)
    })
}

fn start_logger_with_spawn<F, S>(sink: F, spawn: S) -> io::Result<(Logger, JoinHandle<()>)>
where
    F: FnMut(Record, u64, u64) -> io::Result<()> + Send + 'static,
    S: FnOnce(Job) -> io::Result<JoinHandle<()>>,
{
    let (sender, receiver) = sync_channel(QUEUE_CAPACITY);
    let counters = Arc::new(Counters::default());
    let observed = Arc::clone(&counters);
    let worker = spawn(Box::new(move || run(receiver, observed, sink)))?;
    Ok((Logger { sender, counters }, worker))
}

fn run<F>(receiver: Receiver<Record>, counters: Arc<Counters>, mut sink: F)
where
    F: FnMut(Record, u64, u64) -> io::Result<()>,
{
    let started = Instant::now();
    let mut limits = Limits::new();
    while let Ok(record) = receiver.recv() {
        if !limits.admit(record, started.elapsed(), &counters) {
            continue;
        }
        let dropped = counters.dropped.load(Ordering::Relaxed);
        let suppressed = counters.suppressed.load(Ordering::Relaxed);
        // Additional defense in unwind builds only. Production panic=abort
        // requires the fixed formatter and fallible I/O below, not catching panics.
        if !matches!(
            catch_unwind(AssertUnwindSafe(|| sink(record, dropped, suppressed))),
            Ok(Ok(()))
        ) {
            increment(&counters.dropped);
            // Dropping the receiver disconnects every producer without waiting.
            return;
        }
    }
}

/// Fixed primitives and static labels only; serialization never invokes user formatting.
#[derive(serde::Serialize)]
struct DiagnosticLine {
    level: &'static str,
    target: &'static str,
    event: &'static str,
    observed_at_unix_ms: u64,
    queue_wait_ms: u64,
    request_id: u64,
    cache_id: u64,
    entry_id: u64,
    height: Option<u32>,
    fee_zat: Option<u64>,
    phase: &'static str,
    diagnostic_event: &'static str,
    outcome: &'static str,
    elapsed_ms: u64,
    dispatch_ms: Option<u64>,
    selected_txs: Option<u64>,
    reserve_start: Option<u32>,
    reserve_end: Option<u32>,
    observed_fee_zat: Option<u64>,
    lookup_key_reserved: Option<bool>,
    dropped_total: u64,
    suppressed_total: u64,
    process_local_only: bool,
    non_authoritative: bool,
}

fn write_record(record: Record, dropped: u64, suppressed: u64) -> io::Result<()> {
    write_record_to(&mut io::stdout().lock(), record, dropped, suppressed)
}

fn write_record_to(
    writer: &mut impl Write,
    record: Record,
    dropped: u64,
    suppressed: u64,
) -> io::Result<()> {
    let line = DiagnosticLine {
        level: "INFO",
        target: "zebra_rpc::auxiliary_coinbase_diagnostics",
        event: "auxiliary_coinbase_diagnostic",
        observed_at_unix_ms: record.observed_at_unix_ms,
        // Record age at drain; the journal timestamp may be later if the sink stalls.
        queue_wait_ms: u64::try_from(record.enqueued_at.elapsed().as_millis()).unwrap_or(u64::MAX),
        request_id: record.request_id,
        cache_id: record.cache_id,
        entry_id: record.entry_id,
        height: record.height,
        fee_zat: record.fee_zat,
        phase: record.phase.label(),
        diagnostic_event: record.event.label(),
        outcome: record.outcome.label(),
        elapsed_ms: record.elapsed_ms,
        dispatch_ms: record.dispatch_ms,
        selected_txs: record.selected_txs,
        reserve_start: record.reserve_start,
        reserve_end: record.reserve_end,
        observed_fee_zat: record.observed_fee_zat,
        lookup_key_reserved: record.lookup_key_reserved,
        dropped_total: dropped.saturating_add(DISABLED_DROPPED.load(Ordering::Relaxed)),
        suppressed_total: suppressed,
        process_local_only: true,
        non_authoritative: true,
    };
    let mut bytes = serde_json::to_vec(&line).map_err(io::Error::other)?;
    bytes.push(b'\n');
    writer.write_all(&bytes)
}

#[cfg(test)]
thread_local! {
    static CAPTURED: std::cell::RefCell<Option<Vec<Record>>> = const {
        std::cell::RefCell::new(None)
    };
}

#[cfg(test)]
fn capture_record(record: Record) -> bool {
    CAPTURED.with(|captured| {
        let Ok(mut captured) = captured.try_borrow_mut() else {
            return false;
        };
        if let Some(records) = captured.as_mut() {
            records.push(record);
            return true;
        }
        false
    })
}

/// Captures this thread's producer records without touching the process logger.
/// Other dispatched threads need their own explicit scoped capture.
#[cfg(test)]
pub(super) fn capture_records<F, T>(capture: F) -> Vec<Record>
where
    F: FnOnce() -> T,
{
    struct RestoreCapture {
        previous: Option<Vec<Record>>,
        active: bool,
    }
    impl Drop for RestoreCapture {
        fn drop(&mut self) {
            if self.active {
                let _ = CAPTURED.with(|captured| captured.replace(self.previous.take()));
            }
        }
    }
    let mut restore = RestoreCapture {
        previous: CAPTURED.with(|captured| captured.replace(Some(Vec::new()))),
        active: true,
    };
    let _ = capture();
    let records = CAPTURED.with(|captured| captured.replace(restore.previous.take()));
    restore.active = false;
    records.unwrap_or_default()
}

#[cfg(test)]
mod tests;
