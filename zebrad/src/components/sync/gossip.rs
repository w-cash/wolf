//! A task that gossips newly verified [`block::Hash`]es to peers.
//!
//! [`block::Hash`]: zebra_chain::block::Hash

use std::{
    collections::{HashSet, VecDeque},
    time::Duration,
};

use futures::{future::BoxFuture, stream::FuturesUnordered, FutureExt, StreamExt};
use thiserror::Error;
use tokio::{
    sync::{mpsc, watch},
    time::{sleep_until, timeout, Instant},
};
use tower::{Service, ServiceExt};
use tracing::Instrument;

use zebra_chain::{block, chain_tip::ChainTip};
use zebra_network as zn;
use zebra_state::ChainTipChange;

use crate::{
    components::sync::{SyncStatus, PEER_GOSSIP_DELAY, TIPS_RESPONSE_TIMEOUT},
    BoxError,
};

use BlockGossipError::*;

#[cfg(test)]
mod tests;

/// Mined notifications retain their own delivery obligation; ordinary tip changes coalesce.
const MAX_PENDING_MINED_BLOCKS: usize = 64;
const FRESH_ATTEMPTS_PER_RETRY: u8 = 3;
const MINED_ATTEMPTS_PER_COMMITTED: u8 = 4;
const GOSSIP_RETRY_DELAY: Duration = Duration::from_secs(1);
const GOSSIP_WARNING_INTERVAL: Duration = Duration::from_secs(30);

/// Errors that can occur when gossiping committed blocks.
#[derive(Error, Debug)]
pub enum BlockGossipError {
    #[error("chain tip sender was dropped")]
    TipChange(watch::error::RecvError),

    #[error("sync status sender was dropped")]
    SyncStatus(watch::error::RecvError),

    #[error("permanent peer set failure")]
    PeerSetReadiness(zn::BoxError),
}

#[derive(Clone, Copy, Debug)]
struct Advertisement {
    hash: block::Hash,
    height: block::Height,
    mined: bool,
    attempts: u32,
    queued_at: Instant,
    eligible_at: Instant,
}

impl Advertisement {
    fn request(self) -> zn::Request {
        if self.mined {
            zn::Request::AdvertiseBlockToAll(self.hash)
        } else {
            zn::Request::AdvertiseBlock(self.hash, None)
        }
    }

    fn kind(self) -> &'static str {
        if self.mined {
            "mined"
        } else {
            "committed"
        }
    }
}

#[derive(Default)]
struct PendingGossip {
    fresh: VecDeque<Advertisement>,
    retries: VecDeque<(Advertisement, Instant)>,
    mined_hashes: HashSet<block::Hash>,
    committed: Option<(Advertisement, Instant)>,
    fresh_since_retry: u8,
    mined_since_committed: u8,
}

impl PendingGossip {
    fn has_capacity(&self) -> bool {
        self.mined_hashes.len() < MAX_PENDING_MINED_BLOCKS
    }

    fn enqueue_mined(&mut self, hash: block::Hash, height: block::Height, now: Instant) {
        if self.mined_hashes.contains(&hash) {
            metrics::counter!("sync.gossip.mined.duplicates").increment(1);
            return;
        }
        assert!(
            self.has_capacity(),
            "the receiver is gated by pending mined capacity"
        );
        self.mined_hashes.insert(hash);
        self.fresh.push_back(Advertisement {
            hash,
            height,
            mined: true,
            attempts: 0,
            queued_at: now,
            eligible_at: now,
        });
        metrics::counter!("sync.gossip.mined.enqueued").increment(1);
    }

    fn enqueue_committed(&mut self, hash: block::Hash, height: block::Height, now: Instant) {
        self.committed = Some((
            Advertisement {
                hash,
                height,
                mined: false,
                attempts: 0,
                queued_at: now,
                eligible_at: now,
            },
            now,
        ));
    }

    fn take_ready(&mut self, now: Instant) -> Option<Advertisement> {
        let retry_ready = self
            .retries
            .front()
            .is_some_and(|(_, ready_at)| *ready_at <= now);
        let committed_ready = self
            .committed
            .as_ref()
            .is_some_and(|(_, ready_at)| *ready_at <= now);
        let mut advertisement =
            if committed_ready && self.mined_since_committed >= MINED_ATTEMPTS_PER_COMMITTED {
                self.committed
                    .take()
                    .map(|(advertisement, _)| advertisement)
            } else if !self.fresh.is_empty()
                && (!retry_ready || self.fresh_since_retry < FRESH_ATTEMPTS_PER_RETRY)
            {
                return self.take_fresh();
            } else if retry_ready {
                self.fresh_since_retry = 0;
                self.retries
                    .pop_front()
                    .map(|(advertisement, _)| advertisement)
            } else if committed_ready {
                self.committed
                    .take()
                    .map(|(advertisement, _)| advertisement)
            } else {
                None
            }?;
        if advertisement.mined {
            self.mined_since_committed = self.mined_since_committed.saturating_add(1);
        } else {
            self.mined_since_committed = 0;
        }
        advertisement.attempts = advertisement.attempts.saturating_add(1);
        Some(advertisement)
    }

    fn take_fresh(&mut self) -> Option<Advertisement> {
        let mut advertisement = self.fresh.pop_front()?;
        self.fresh_since_retry = self.fresh_since_retry.saturating_add(1);
        self.mined_since_committed = self.mined_since_committed.saturating_add(1);
        advertisement.attempts = advertisement.attempts.saturating_add(1);
        Some(advertisement)
    }

    fn retry(&mut self, mut advertisement: Advertisement, now: Instant) {
        if advertisement.mined {
            advertisement.eligible_at = now + GOSSIP_RETRY_DELAY;
            self.retries
                .push_back((advertisement, advertisement.eligible_at));
        }
        // Ordinary tip advertisements remain best-effort, as before the dispatcher.
        // Retrying one could race the delayed tip reader and advertise an older tip.
    }

    fn complete(&mut self, advertisement: Advertisement) {
        if advertisement.mined {
            self.mined_hashes.remove(&advertisement.hash);
            if self
                .committed
                .as_ref()
                .is_some_and(|(tip, _)| tip.hash == advertisement.hash)
            {
                self.committed = None;
            }
        }
    }

    fn next_deadline(&self) -> Option<Instant> {
        self.retries
            .front()
            .map(|(_, deadline)| *deadline)
            .into_iter()
            .chain(self.committed.as_ref().map(|(_, deadline)| *deadline))
            .min()
    }

    fn record_metrics(&self, in_flight: u8) {
        // The queue is bounded at 64 entries, exactly representable by f64.
        metrics::gauge!("sync.gossip.pending.mined").set(self.mined_hashes.len() as f64);
        metrics::gauge!("sync.gossip.request.in_flight").set(f64::from(in_flight));
        metrics::gauge!("sync.gossip.mined.backpressure").set(if self.has_capacity() {
            0.0
        } else {
            1.0
        });
    }
}

enum AttemptOutcome {
    Completed,
    Incomplete(&'static str),
    PermanentReadiness(BoxError),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AttemptTiming {
    queue_wait_ms: u64,
    readiness_wait_ms: u64,
    request_elapsed_ms: u64,
    total_elapsed_ms: u64,
}

impl AttemptTiming {
    fn capture(
        advertisement: Advertisement,
        started: Instant,
        request_started: Option<Instant>,
        finished: Instant,
    ) -> Self {
        let millis = |duration: Duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
        Self {
            queue_wait_ms: millis(started.saturating_duration_since(advertisement.eligible_at)),
            readiness_wait_ms: millis(
                request_started
                    .unwrap_or(finished)
                    .saturating_duration_since(started),
            ),
            request_elapsed_ms: request_started
                .map(|called| millis(finished.saturating_duration_since(called)))
                .unwrap_or(0),
            total_elapsed_ms: millis(finished.saturating_duration_since(advertisement.queued_at)),
        }
    }
}

struct AttemptReport {
    outcome: AttemptOutcome,
    timing: AttemptTiming,
}

enum AttemptFailure {
    Readiness(BoxError),
    Request,
}

#[derive(Clone, Copy)]
enum AttemptLane {
    General,
    FreshMined,
}

impl AttemptLane {
    fn name(self) -> &'static str {
        match self {
            Self::General => "general",
            Self::FreshMined => "fresh_mined",
        }
    }
}

fn broadcast_attempt<ZN>(
    mut network: ZN,
    advertisement: Advertisement,
    lane: AttemptLane,
) -> BoxFuture<'static, AttemptReport>
where
    ZN: Service<zn::Request, Response = zn::Response, Error = BoxError> + Send + 'static,
    ZN::Future: Send,
{
    async move {
        let started = Instant::now();
        let mut request_started = None;
        // Tower's Timeout only bounds call(), so readiness must share this deadline.
        let result = timeout(TIPS_RESPONSE_TIMEOUT, async {
            let ready = network.ready().await.map_err(AttemptFailure::Readiness)?;
            // This can be a buffered service call, not physical peer readiness.
            request_started = Some(Instant::now());
            let request = advertisement.request();
            ready
                .call(request)
                .await
                .map_err(|_| AttemptFailure::Request)
        })
        .await;
        let timing = AttemptTiming::capture(advertisement, started, request_started, Instant::now());
        let outcome = match result {
            Ok(Ok(zn::Response::MinedBlockAdvertisement(report))) if advertisement.mined => {
                if report.non_local_sent > 0 {
                    info!(hash = ?advertisement.hash, height = ?advertisement.height,
                        attempt = advertisement.attempts, attempted = report.attempted(),
                        non_local_sent = report.non_local_sent, local_sent = report.local_sent,
                        failed = report.failed, lane = lane.name(),
                        queue_wait_ms = timing.queue_wait_ms, readiness_wait_ms = timing.readiness_wait_ms,
                        request_elapsed_ms = timing.request_elapsed_ms, total_elapsed_ms = timing.total_elapsed_ms,
                        phase = "mined_transport_result",
                        "RPC-submitted block inventory transport completed; independent peer receipt is unconfirmed");
                    AttemptOutcome::Completed
                } else {
                    if advertisement.attempts == 1 {
                        warn!(hash = ?advertisement.hash, height = ?advertisement.height,
                            attempt = advertisement.attempts, attempted = report.attempted(),
                            non_local_sent = report.non_local_sent, local_sent = report.local_sent,
                            failed = report.failed, lane = lane.name(),
                            queue_wait_ms = timing.queue_wait_ms, readiness_wait_ms = timing.readiness_wait_ms,
                            request_elapsed_ms = timing.request_elapsed_ms, total_elapsed_ms = timing.total_elapsed_ms,
                            phase = "mined_transport_result",
                            "RPC-submitted block has no non-local transport success; retained for retry");
                    }
                    let reason = if report.local_sent > 0 {
                        "local_only_transport"
                    } else if report.failed > 0 {
                        "all_peer_sends_failed"
                    } else {
                        "no_peer_candidates"
                    };
                    AttemptOutcome::Incomplete(reason)
                }
            },
            // Wcash and ordinary tip advertisements retain their best-effort contract.
            Ok(Ok(zn::Response::Nil)) => AttemptOutcome::Completed,
            Ok(Ok(_)) => AttemptOutcome::Incomplete("unexpected_network_response"),
            Ok(Err(AttemptFailure::Readiness(error))) => AttemptOutcome::PermanentReadiness(error),
            Ok(Err(AttemptFailure::Request)) => AttemptOutcome::Incomplete("request_error"),
            Err(_) if request_started.is_some() => AttemptOutcome::Incomplete("network_request_timeout"),
            Err(_) => AttemptOutcome::Incomplete("service_readiness_timeout"),
        };
        AttemptReport { outcome, timing }
    }
    .in_current_span()
    .boxed()
}

async fn next_committed_tip(
    mut sync_status: SyncStatus,
    mut chain_tip: ChainTipChange,
) -> Result<((block::Hash, block::Height), ChainTipChange), BlockGossipError> {
    const WAIT_FOR_BLOCK_SUBMISSION_DELAY: Duration = Duration::from_micros(100);
    tokio::time::sleep(PEER_GOSSIP_DELAY).await;
    let tip_action = chain_tip.wait_for_tip_change().await.map_err(TipChange)?;
    tokio::time::sleep(WAIT_FOR_BLOCK_SUBMISSION_DELAY).await;
    sync_status
        .wait_until_close_to_tip()
        .await
        .map_err(SyncStatus)?;
    let best_tip = chain_tip
        .last_tip_change()
        .unwrap_or(tip_action)
        .best_tip_hash_and_height();
    Ok((best_tip, chain_tip))
}

/// Gossips committed tips and retains successfully received mined notifications for retry.
///
/// A completed network request is not proof that an independent peer received the block.
/// Readiness and request waits are bounded together; no detached broadcast tasks survive
/// a timeout. If pending mined storage fills, the bounded RPC channel applies backpressure.
/// Ordinary tip changes coalesce while every distinct pending mined hash is retained.
/// Zcash mined attempts also require a transport success beyond configured sidecars
/// and loopback peers; this is not evidence of independent receipt or adoption.
/// Failed ordinary tip advertisements are best-effort and are not retried.
/// These in-memory obligations do not survive a process restart and do not replace
/// durable winner submission or independent observation by the pool backend.
/// When `prioritize_fresh_mined` is enabled for Zcash, a second owned attempt lane
/// lets a fresh RPC-origin notification start while an earlier attempt is pending.
/// Both lanes retain their futures until completion or their bounded deadlines;
/// at most two attempts run, and the existing general lane still schedules retries
/// and ordinary tips fairly. Wcash keeps its single-lane policy.
pub async fn gossip_best_tip_block_hashes<ZN>(
    sync_status: SyncStatus,
    mut chain_state: ChainTipChange,
    broadcast_network: ZN,
    mut mined_block_receiver: Option<mpsc::Receiver<(block::Hash, block::Height)>>,
    prioritize_fresh_mined: bool,
) -> Result<(), BlockGossipError>
where
    ZN: Service<zn::Request, Response = zn::Response, Error = BoxError> + Send + Clone + 'static,
    ZN::Future: Send,
{
    info!("initializing block gossip task");
    let mut pending = PendingGossip::default();
    let mut committed_tip = next_committed_tip(sync_status.clone(), chain_state.clone()).boxed();
    let mut attempts = FuturesUnordered::new();
    let mut general_active = false;
    let mut fresh_mined_active = false;
    let mut last_warning: Option<Instant> = None;
    let mut suppressed_warnings = 0_u64;
    let mut last_ordinary_warning: Option<Instant> = None;
    let mut suppressed_ordinary_warnings = 0_u64;

    loop {
        if !general_active {
            if let Some(advertisement) = pending.take_ready(Instant::now()) {
                metrics::counter!("sync.gossip.request.attempts", "kind" => advertisement.kind())
                    .increment(1);
                attempts.push(
                    broadcast_attempt(
                        broadcast_network.clone(),
                        advertisement,
                        AttemptLane::General,
                    )
                    .map(move |outcome| (AttemptLane::General, advertisement, outcome))
                    .boxed(),
                );
                general_active = true;
            }
        }
        if prioritize_fresh_mined && !fresh_mined_active {
            if let Some(advertisement) = pending.take_fresh() {
                metrics::counter!("sync.gossip.request.attempts", "kind" => advertisement.kind())
                    .increment(1);
                attempts.push(
                    broadcast_attempt(
                        broadcast_network.clone(),
                        advertisement,
                        AttemptLane::FreshMined,
                    )
                    .map(move |outcome| (AttemptLane::FreshMined, advertisement, outcome))
                    .boxed(),
                );
                fresh_mined_active = true;
            }
        }
        pending.record_metrics(u8::from(general_active) + u8::from(fresh_mined_active));
        let retry_deadline = if !general_active {
            pending.next_deadline()
        } else {
            None
        };
        tokio::select! {
            result = &mut committed_tip => {
                let ((hash, height), updated_chain_state) = result.inspect_err(|error| {
                    let reason = match error {
                        TipChange(_) => "tip_change_closed",
                        SyncStatus(_) => "sync_status_closed",
                        PeerSetReadiness(_) => "peer_set_readiness",
                    };
                    error!(phase = "gossip_task_exit", reason,
                        "block gossip task stopped; independent receipt is unconfirmed");
                })?;
                chain_state = updated_chain_state;
                pending.enqueue_committed(hash, height, Instant::now());
                committed_tip = next_committed_tip(sync_status.clone(), chain_state.clone()).boxed();
            },
            mined = async {
                mined_block_receiver.as_mut().expect("the branch requires a receiver").recv().await
            }, if mined_block_receiver.is_some() && pending.has_capacity() => {
                if let Some((hash, height)) = mined {
                    pending.enqueue_mined(hash, height, Instant::now());
                } else {
                    mined_block_receiver = None;
                }
            },
            completed = attempts.next(), if !attempts.is_empty() => {
                let (lane, advertisement, AttemptReport { outcome, timing }) = completed.expect("an active lane owns its attempt");
                match lane {
                    AttemptLane::General => general_active = false,
                    AttemptLane::FreshMined => fresh_mined_active = false,
                }
                match outcome {
                    AttemptOutcome::Completed => {
                        pending.complete(advertisement);
                        metrics::counter!("sync.gossip.request.completed", "kind" => advertisement.kind()).increment(1);
                        info!(hash = ?advertisement.hash, height = ?advertisement.height,
                            kind = advertisement.kind(), attempt = advertisement.attempts, lane = lane.name(),
                            queue_wait_ms = timing.queue_wait_ms, readiness_wait_ms = timing.readiness_wait_ms,
                            request_elapsed_ms = timing.request_elapsed_ms, total_elapsed_ms = timing.total_elapsed_ms,
                            phase = "network_request_completed",
                            "block gossip network request completed; independent peer receipt is not confirmed");
                        if advertisement.mined
                            && mined_block_receiver.as_ref().is_none_or(|rx| rx.is_empty())
                            && chain_state.latest_chain_tip().best_tip_hash() == Some(advertisement.hash)
                        {
                            chain_state.mark_last_change_hash(advertisement.hash);
                            committed_tip = next_committed_tip(sync_status.clone(), chain_state.clone()).boxed();
                        }
                    },
                    AttemptOutcome::Incomplete(reason) if advertisement.mined => {
                        let now = Instant::now();
                        pending.retry(advertisement, now);
                        metrics::counter!("sync.gossip.request.retries", "kind" => advertisement.kind(), "reason" => reason).increment(1);
                        if advertisement.attempts == 1 || last_warning.is_none_or(|previous| now.duration_since(previous) >= GOSSIP_WARNING_INTERVAL) {
                            last_warning = Some(now);
                            warn!(hash = ?advertisement.hash, height = ?advertisement.height,
                                attempt = advertisement.attempts, lane = lane.name(),
                                queue_wait_ms = timing.queue_wait_ms, readiness_wait_ms = timing.readiness_wait_ms,
                                request_elapsed_ms = timing.request_elapsed_ms, total_elapsed_ms = timing.total_elapsed_ms,
                                pending_seconds = now.duration_since(advertisement.queued_at).as_secs(),
                                pending_mined = pending.mined_hashes.len(), reason,
                                suppressed = suppressed_warnings, phase = "retry_queued",
                                "block gossip request incomplete; pending RPC-submitted hashes retained for retry");
                            suppressed_warnings = 0;
                        } else {
                            suppressed_warnings = suppressed_warnings.saturating_add(1);
                        }
                    },
                    AttemptOutcome::Incomplete(reason) => {
                        metrics::counter!("sync.gossip.request.dropped", "kind" => advertisement.kind(), "reason" => reason).increment(1);
                        let now = Instant::now();
                        if last_ordinary_warning.is_none_or(|previous| now.duration_since(previous) >= GOSSIP_WARNING_INTERVAL) {
                            last_ordinary_warning = Some(now);
                            warn!(hash = ?advertisement.hash, height = ?advertisement.height,
                                attempt = advertisement.attempts, reason, lane = lane.name(),
                                queue_wait_ms = timing.queue_wait_ms, readiness_wait_ms = timing.readiness_wait_ms,
                                request_elapsed_ms = timing.request_elapsed_ms, total_elapsed_ms = timing.total_elapsed_ms,
                                suppressed = suppressed_ordinary_warnings, phase = "attempt_dropped",
                                "ordinary tip gossip request incomplete; best-effort attempt dropped");
                            suppressed_ordinary_warnings = 0;
                        } else {
                            suppressed_ordinary_warnings = suppressed_ordinary_warnings.saturating_add(1);
                        }
                    },
                    AttemptOutcome::PermanentReadiness(error) => {
                        error!(hash = ?advertisement.hash, height = ?advertisement.height,
                            lane = lane.name(),
                            queue_wait_ms = timing.queue_wait_ms, readiness_wait_ms = timing.readiness_wait_ms,
                            request_elapsed_ms = timing.request_elapsed_ms, total_elapsed_ms = timing.total_elapsed_ms,
                            phase = "gossip_task_exit", reason = "peer_set_readiness",
                            "block gossip task stopped; independent receipt is unconfirmed");
                        return Err(PeerSetReadiness(error));
                    },
                }
            },
            _ = async {
                if let Some(deadline) = retry_deadline {
                    sleep_until(deadline).await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => {},
        }
    }
}
