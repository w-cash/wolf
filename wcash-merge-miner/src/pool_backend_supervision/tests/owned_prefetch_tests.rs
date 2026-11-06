use super::*;

#[derive(Debug)]
struct OwnedPrefetchMarker {
    id: u8,
    dropped: Arc<AtomicUsize>,
}

impl Drop for OwnedPrefetchMarker {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::AcqRel);
    }
}

#[test]
fn owned_prefetch_rejected_handoff_returns_the_exact_request_owner() {
    let dropped = Arc::new(AtomicUsize::new(0));
    let marker = |id| OwnedPrefetchMarker {
        id,
        dropped: Arc::clone(&dropped),
    };
    let (cleanup, cleaned) = mpsc::channel();
    let abandoned_request = cleanup.clone();
    let mut worker = PoolBackendControlWorker::spawn_with_owned_requests(
        "owned-prefetch-return-fixture",
        |request| request,
        move |result| cleanup.send(result).unwrap(),
        move |request| abandoned_request.send(request).unwrap(),
    )
    .unwrap();
    assert!(worker.request_owned(marker(1)).is_ok());
    let Err((returned, _)) = worker.request_owned(marker(2)) else {
        panic!("a busy one-request worker accepted another owned child");
    };
    assert_eq!(returned.id, 2);
    assert_eq!(dropped.load(Ordering::Acquire), 0);
    drop(returned);
    assert_eq!(dropped.load(Ordering::Acquire), 1);

    worker.request_shutdown().unwrap();
    let retained = cleaned.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(retained.id, 1);
    assert_eq!(dropped.load(Ordering::Acquire), 1);
    drop(retained);
    assert_eq!(dropped.load(Ordering::Acquire), 2);

    let Err((returned, _)) = worker.request_owned(marker(3)) else {
        panic!("a stopped worker accepted an owned child");
    };
    assert_eq!(returned.id, 3);
    assert_eq!(dropped.load(Ordering::Acquire), 2);
    drop(returned);
    assert_eq!(dropped.load(Ordering::Acquire), 3);
}

#[test]
fn owned_prefetch_stopped_before_work_is_returned_to_request_cleanup() {
    let dropped = Arc::new(AtomicUsize::new(0));
    let worked = Arc::new(AtomicUsize::new(0));
    let worker_calls = Arc::clone(&worked);
    let (cleanup, cleaned) = mpsc::channel();
    let abandoned_request = cleanup.clone();
    let mut worker = PoolBackendControlWorker::spawn_with_owned_requests(
        "owned-prefetch-stop-fixture",
        move |request| {
            worker_calls.fetch_add(1, Ordering::AcqRel);
            request
        },
        move |result| cleanup.send((false, result)).unwrap(),
        move |request| abandoned_request.send((true, request)).unwrap(),
    )
    .unwrap();
    // Deterministically exercise the worker's state after shutdown was
    // published but before it starts an accepted request. Its queue still owns
    // the exact child, so skipping work must hand it to the cleanup callback.
    worker.shutdown.store(true, Ordering::Release);
    assert!(worker
        .request_owned(OwnedPrefetchMarker {
            id: 7,
            dropped: Arc::clone(&dropped),
        })
        .is_ok());
    worker.request_shutdown().unwrap();
    let (request_cleanup, retained) = cleaned.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(request_cleanup);
    assert_eq!(retained.id, 7);
    assert_eq!(worked.load(Ordering::Acquire), 0);
    assert_eq!(dropped.load(Ordering::Acquire), 0);
    drop(retained);
    assert_eq!(dropped.load(Ordering::Acquire), 1);
}
