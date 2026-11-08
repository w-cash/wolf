use std::{
    io::Write,
    sync::{mpsc, Condvar, Mutex},
};

use super::*;

#[test]
fn context_restores_nested_phases_on_return_and_unwind_and_does_not_cross_threads() {
    let original = current();
    {
        let _request = enter(71, Phase::Template);
        assert_eq!(
            current(),
            Context {
                request_id: 71,
                phase: Phase::Template
            }
        );
        {
            let _sizing = phase(Phase::Sizing);
            let record = Record::new(Event::CacheLookup, Outcome::ReserveReady);
            assert_eq!((record.request_id, record.phase), (71, Phase::Sizing));
        }
        assert_eq!(current().phase, Phase::Template);
        let failed = catch_unwind(|| {
            let _selected = phase(Phase::SelectedCoinbase);
            panic!("synthetic diagnostic-scope unwind");
        });
        assert!(failed.is_err());
        assert_eq!(current().phase, Phase::Template);
        let copied = current();
        let child = thread::spawn(move || {
            assert_eq!(current(), Context::default());
            let _context = enter(copied.request_id, Phase::Prewarm);
            current()
        })
        .join()
        .unwrap();
        assert_eq!(
            child,
            Context {
                request_id: 71,
                phase: Phase::Prewarm
            }
        );
        assert_eq!(current().phase, Phase::Template);
    }
    assert_eq!(current(), original);
}

#[test]
fn exhausted_process_local_ids_become_unknown_and_counters_saturate() {
    let counter = AtomicU64::new(u64::MAX - 1);
    assert_eq!(take_id(&counter), u64::MAX - 1);
    assert_eq!(take_id(&counter), 0);
    assert_eq!(take_id(&counter), 0);
    increment(&counter);
    assert_eq!(counter.load(Ordering::Relaxed), u64::MAX);
    let first = next_id();
    assert_ne!(first, 0);
    assert!(next_id() >= first);
}

#[test]
fn independent_fixed_budgets_count_suppression_and_refill_without_sleeping() {
    let counters = Counters::default();
    let mut limits = Limits::new();
    let important = Record::new(Event::Constructor, Outcome::Started);
    let routine = Record::new(Event::CacheLookup, Outcome::ForegroundReady);
    for _ in 0..4 {
        assert!(limits.admit(important, Duration::ZERO, &counters));
    }
    assert!(!limits.admit(important, Duration::from_millis(999), &counters));
    for _ in 0..2 {
        assert!(limits.admit(routine, Duration::ZERO, &counters));
    }
    assert!(!limits.admit(routine, Duration::from_secs(4), &counters));
    assert!(limits.admit(important, Duration::from_secs(1), &counters));
    assert!(limits.admit(routine, Duration::from_secs(5), &counters));
    assert_eq!(counters.suppressed.load(Ordering::Relaxed), 2);

    let mut gates = Limits::new();
    let mut admitted = 0;
    for millis in 0..=300_000 {
        let now = Duration::from_millis(millis);
        admitted += usize::from(gates.admit(important, now, &counters));
        admitted += usize::from(gates.admit(routine, now, &counters));
    }
    assert!(
        admitted <= 366,
        "fixed process-wide budgets bound every key together"
    );
    assert_eq!(
        counters.suppressed.load(Ordering::Relaxed),
        2 + 600_002 - u64::try_from(admitted).unwrap()
    );
}

#[test]
fn diagnostic_classes_keep_failures_and_ownership_separate_from_routine_hits() {
    for (event, outcome) in [
        (Event::CacheLookup, Outcome::NewBuild),
        (Event::CacheLookup, Outcome::JoinBuilding),
        (Event::TemplateWorker, Outcome::Deadline),
        (Event::TemplateWorker, Outcome::Busy),
        (Event::Constructor, Outcome::Finished),
        (Event::Constructor, Outcome::Panic),
        (Event::ReserveRegistration, Outcome::StateContended),
        (Event::ReserveRegistration, Outcome::CacheContended),
        (Event::ReserveWork, Outcome::Error),
    ] {
        assert!(Record::new(event, outcome).important());
    }
    for (event, outcome) in [
        (Event::CacheLookup, Outcome::ReserveReady),
        (Event::CacheLookup, Outcome::ForegroundReady),
        (Event::ReserveRegistration, Outcome::Accepted),
        (Event::ReserveWork, Outcome::ExistingReserve),
        (Event::ReserveWork, Outcome::OutsideWindow),
    ] {
        assert!(!Record::new(event, outcome).important());
    }
}

#[derive(Default)]
struct SinkGate {
    released: Mutex<bool>,
    changed: Condvar,
}

impl SinkGate {
    fn wait(&self) {
        let released = self.released.lock().unwrap();
        let (released, timeout) = self
            .changed
            .wait_timeout_while(released, Duration::from_secs(5), |released| !*released)
            .unwrap();
        assert!(
            !timeout.timed_out() || *released,
            "test releases the isolated sink"
        );
    }

    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.changed.notify_all();
    }
}

#[test]
fn blocked_sink_leaves_one_bounded_queue_and_producers_never_wait_for_it() {
    let gate = Arc::new(SinkGate::default());
    let blocked = Arc::clone(&gate);
    let (entered_tx, entered_rx) = mpsc::channel();
    let mut first = true;
    let (logger, worker) = start_logger(move |_, _, _| {
        if first {
            first = false;
            entered_tx.send(()).unwrap();
            blocked.wait();
        }
        Ok(())
    })
    .unwrap();
    let record = Record::new(Event::Constructor, Outcome::Started);
    logger.emit(record);
    let entered = entered_rx.recv_timeout(Duration::from_secs(2));
    let logger = Arc::new(logger);
    let producing = Arc::clone(&logger);
    let (completed_tx, completed_rx) = mpsc::channel();
    let producer = thread::spawn(move || {
        for _ in 0..QUEUE_CAPACITY + 17 {
            producing.emit(record);
        }
        completed_tx
            .send(producing.counters.dropped.load(Ordering::Relaxed))
            .unwrap();
    });
    let completed_before_release = completed_rx.recv_timeout(Duration::from_secs(2));
    // Always release before assertions/join, including either timeout path.
    gate.release();
    producer.join().unwrap();
    drop(logger);
    worker.join().unwrap();
    assert!(entered.is_ok());
    assert_eq!(completed_before_release.unwrap(), 17);
}

struct BrokenPipe;

impl Write for BrokenPipe {
    fn write(&mut self, _: &[u8]) -> io::Result<usize> {
        Err(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "synthetic closed sink",
        ))
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn failing_sink_exits_its_worker_and_disconnects_producers() {
    let (logger, worker) = start_logger(|record, dropped, suppressed| {
        write_record_to(&mut BrokenPipe, record, dropped, suppressed)
    })
    .unwrap();
    logger.emit(Record::new(Event::Constructor, Outcome::Started));
    assert!(
        worker.join().is_ok(),
        "fallible sink failure stops only its logger thread"
    );
    logger.emit(Record::new(Event::Constructor, Outcome::Finished));
    assert_eq!(logger.counters.dropped.load(Ordering::Relaxed), 2);
}

#[test]
fn spawn_failure_is_disabled_once_without_starting_a_worker() {
    let disabled = OnceLock::<Option<Logger>>::new();
    let attempts = AtomicU64::new(0);
    for _ in 0..2 {
        assert!(disabled
            .get_or_init(|| {
                increment(&attempts);
                start_logger_with_spawn(
                    |_, _, _| Ok(()),
                    |_| Err(io::Error::other("synthetic thread spawn failure")),
                )
                .ok()
                .map(|(logger, worker)| {
                    drop(worker);
                    logger
                })
            })
            .is_none());
    }
    assert_eq!(attempts.load(Ordering::Relaxed), 1);
}

#[test]
fn structured_info_contains_numeric_provenance_and_only_fixed_labels() {
    let _scope = enter(123, Phase::SelectedCoinbase);
    let mut record = Record::new(Event::CacheLookup, Outcome::NewBuild);
    record.cache_id = 456;
    record.entry_id = 789;
    record.height = Some(54_289);
    record.fee_zat = Some(19);
    record.elapsed_ms = 2_005;
    record.dispatch_ms = Some(4);
    record.selected_txs = Some(2);
    record.reserve_start = Some(54_289);
    record.reserve_end = Some(54_290);
    record.observed_fee_zat = Some(17);
    record.lookup_key_reserved = Some(false);
    let mut bytes = Vec::new();
    assert!(write_record_to(&mut bytes, record, 3, 5).is_ok());
    let log = String::from_utf8(bytes).unwrap();
    assert_eq!(log.lines().count(), 1);
    let value: serde_json::Value = serde_json::from_str(&log).unwrap();
    for (field, expected) in [
        ("request_id", 123),
        ("cache_id", 456),
        ("entry_id", 789),
        ("height", 54_289),
        ("fee_zat", 19),
        ("elapsed_ms", 2_005),
        ("dispatch_ms", 4),
        ("selected_txs", 2),
        ("reserve_start", 54_289),
        ("reserve_end", 54_290),
        ("observed_fee_zat", 17),
        ("suppressed_total", 5),
    ] {
        assert_eq!(value[field].as_u64(), Some(expected), "{field}: {log}");
    }
    assert_eq!(value["level"], "INFO");
    assert_eq!(value["target"], "zebra_rpc::auxiliary_coinbase_diagnostics");
    assert_eq!(value["event"], "auxiliary_coinbase_diagnostic");
    assert_eq!(value["phase"], "selected_coinbase");
    assert_eq!(value["diagnostic_event"], "cache_lookup");
    assert_eq!(value["outcome"], "new_build");
    assert_eq!(value["process_local_only"], true);
    assert_eq!(value["non_authoritative"], true);
    assert_eq!(value["lookup_key_reserved"], false);
    for excluded in [
        "address",
        "recipient",
        "cookie",
        "transaction",
        "proof_bytes",
        "retire_token",
        "0x",
        "request=",
    ] {
        assert!(
            !log.contains(excluded),
            "unexpected payload field {excluded}: {log}"
        );
    }
}

#[test]
fn scoped_capture_is_nested_thread_local_and_restores_after_unwind() {
    let mut inner = Vec::new();
    let outer = capture_records(|| {
        let _scope = enter(1, Phase::Template);
        emit(Record::new(Event::TemplateWorker, Outcome::Started));
        inner = capture_records(|| {
            let _scope = enter(2, Phase::Sizing);
            emit(Record::new(Event::CacheLookup, Outcome::JoinBuilding));
        });
        let failed = catch_unwind(|| {
            capture_records(|| {
                let _scope = enter(3, Phase::SelectedCoinbase);
                emit(Record::new(Event::Constructor, Outcome::Started));
                panic!("synthetic scoped capture unwind");
            })
        });
        assert!(failed.is_err());
        let child = thread::spawn(|| {
            capture_records(|| {
                emit(Record::new(Event::ReserveWork, Outcome::Skipped));
            })
        })
        .join()
        .unwrap();
        assert_eq!(child.len(), 1);
        assert_eq!(child[0].request_id, 0);
        emit(Record::new(Event::TemplateWorker, Outcome::Finished));
    });
    assert_eq!(inner.len(), 1);
    assert_eq!((inner[0].request_id, inner[0].phase), (2, Phase::Sizing));
    assert_eq!(outer.len(), 2);
    assert!(outer.iter().all(|record| record.request_id == 1));
    assert_eq!(outer[0].outcome, Outcome::Started);
    assert_eq!(outer[1].outcome, Outcome::Finished);
}

#[test]
fn queued_record_preserves_producer_timestamp_and_reports_drain_age() {
    let mut record = Record::new(Event::TemplateWorker, Outcome::Deadline);
    record.observed_at_unix_ms = 1_794_145_118_639;
    record.enqueued_at = Instant::now()
        .checked_sub(Duration::from_millis(250))
        .unwrap();
    let mut bytes = Vec::new();
    assert!(write_record_to(&mut bytes, record, 0, 0).is_ok());
    let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        value["observed_at_unix_ms"].as_u64(),
        Some(1_794_145_118_639)
    );
    assert!(
        value["queue_wait_ms"].as_u64().unwrap() >= 250,
        "drain age is separate from producer observation time"
    );
    assert!(std::mem::size_of::<Record>() <= 192);
    assert!(
        std::mem::size_of::<Record>() * QUEUE_CAPACITY <= 16 * 1024,
        "bounded queue stores at most16KiB of fixed-size record payloads"
    );
}

#[test]
fn maximum_numeric_diagnostic_is_one_bounded_fallible_json_line() {
    let mut record = Record::new(Event::Constructor, Outcome::Ok);
    record.observed_at_unix_ms = u64::MAX;
    record.request_id = u64::MAX;
    record.cache_id = u64::MAX;
    record.entry_id = u64::MAX;
    record.height = Some(u32::MAX);
    record.fee_zat = Some(u64::MAX);
    record.elapsed_ms = u64::MAX;
    record.dispatch_ms = Some(u64::MAX);
    record.selected_txs = Some(u64::MAX);
    record.reserve_start = Some(u32::MAX);
    record.reserve_end = Some(u32::MAX);
    record.observed_fee_zat = Some(u64::MAX);
    record.lookup_key_reserved = Some(true);
    let mut bytes = Vec::new();
    assert!(write_record_to(&mut bytes, record, u64::MAX, u64::MAX).is_ok());
    assert!(
        bytes.len() <= 2_048,
        "numeric-only line remains bounded at maximum values"
    );
    assert_eq!(bytes.last(), Some(&b'\n'));
    let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(value["dropped_total"].as_u64(), Some(u64::MAX));
    assert_eq!(value["lookup_key_reserved"], true);
    let error = write_record_to(&mut BrokenPipe, record, 0, 0).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
}
