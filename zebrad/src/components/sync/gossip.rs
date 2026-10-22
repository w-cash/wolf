//! A task that gossips newly verified [`block::Hash`]es to peers.
//!
//! [`block::Hash`]: zebra_chain::block::Hash

use std::{
    collections::{HashSet, VecDeque},
    time::Duration,
};

use futures::{future::BoxFuture, FutureExt};
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
        });
        metrics::counter!("sync.gossip.mined.enqueued").increment(1);
        debug!(
            ?hash,
            ?height,
            phase = "queued",
            "mined block gossip queued"
        );
    }

    fn enqueue_committed(&mut self, hash: block::Hash, height: block::Height, now: Instant) {
        self.committed = Some((
            Advertisement {
                hash,
                height,
                mined: false,
                attempts: 0,
                queued_at: now,
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
                self.fresh_since_retry = self.fresh_since_retry.saturating_add(1);
                self.fresh.pop_front()
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

    fn retry(&mut self, advertisement: Advertisement, now: Instant) {
        let ready_at = now + GOSSIP_RETRY_DELAY;
        if advertisement.mined {
            self.retries.push_back((advertisement, ready_at));
        } else if self.committed.is_none() {
            // A newer ordinary tip supersedes a failed ordinary advertisement.
            self.committed = Some((advertisement, ready_at));
        }
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

    fn record_metrics(&self, in_flight: bool) {
        // The queue is bounded at 64 entries, exactly representable by f64.
        metrics::gauge!("sync.gossip.pending.mined").set(self.mined_hashes.len() as f64);
        metrics::gauge!("sync.gossip.request.in_flight").set(if in_flight { 1.0 } else { 0.0 });
        metrics::gauge!("sync.gossip.mined.backpressure").set(if self.has_capacity() {
            0.0
        } else {
            1.0
        });
    }
}

enum AttemptOutcome {
    Completed,
    Retry(&'static str),
    PermanentReadiness(BoxError),
}

enum AttemptFailure {
    Readiness(BoxError),
    Request,
}

fn broadcast_attempt<ZN>(
    mut network: ZN,
    advertisement: Advertisement,
) -> BoxFuture<'static, AttemptOutcome>
where
    ZN: Service<zn::Request, Response = zn::Response, Error = BoxError> + Send + 'static,
    ZN::Future: Send,
{
    async move {
        let mut request_started = false;
        // Tower's Timeout only bounds call(), so readiness must share this deadline.
        let result = timeout(TIPS_RESPONSE_TIMEOUT, async {
            let ready = network.ready().await.map_err(AttemptFailure::Readiness)?;
            // This can be a buffered service call, not physical peer readiness.
            request_started = true;
            let request = advertisement.request();
            let log_msg = if advertisement.mined {
                "sending mined block broadcast"
            } else {
                "sending committed block broadcast"
            };
            info!(height = ?advertisement.height, ?request, attempt = advertisement.attempts,
                phase = "request_started", log_msg);
            ready
                .call(request)
                .await
                .map_err(|_| AttemptFailure::Request)
        })
        .await;
        match result {
            Ok(Ok(_)) => AttemptOutcome::Completed,
            Ok(Err(AttemptFailure::Readiness(error))) => AttemptOutcome::PermanentReadiness(error),
            Ok(Err(AttemptFailure::Request)) => AttemptOutcome::Retry("request_error"),
            Err(_) if request_started => AttemptOutcome::Retry("network_request_timeout"),
            Err(_) => AttemptOutcome::Retry("service_readiness_timeout"),
        }
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
/// These in-memory obligations do not survive a process restart and do not replace
/// durable winner submission or independent observation by the pool backend.
pub async fn gossip_best_tip_block_hashes<ZN>(
    sync_status: SyncStatus,
    mut chain_state: ChainTipChange,
    broadcast_network: ZN,
    mut mined_block_receiver: Option<mpsc::Receiver<(block::Hash, block::Height)>>,
) -> Result<(), BlockGossipError>
where
    ZN: Service<zn::Request, Response = zn::Response, Error = BoxError> + Send + Clone + 'static,
    ZN::Future: Send,
{
    info!("initializing block gossip task");
    let mut pending = PendingGossip::default();
    let mut committed_tip = next_committed_tip(sync_status.clone(), chain_state.clone()).boxed();
    let mut attempt: Option<BoxFuture<'static, AttemptOutcome>> = None;
    let mut active: Option<Advertisement> = None;
    let mut last_warning: Option<Instant> = None;

    loop {
        if attempt.is_none() {
            if let Some(advertisement) = pending.take_ready(Instant::now()) {
                metrics::counter!("sync.gossip.request.attempts", "kind" => advertisement.kind())
                    .increment(1);
                attempt = Some(broadcast_attempt(broadcast_network.clone(), advertisement));
                active = Some(advertisement);
            }
        }
        pending.record_metrics(attempt.is_some());
        let retry_deadline = if attempt.is_none() {
            pending.next_deadline()
        } else {
            None
        };
        tokio::select! {
            result = &mut committed_tip => {
                let ((hash, height), updated_chain_state) = result?;
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
            outcome = async {
                attempt.as_mut().expect("the branch requires an active attempt").await
            }, if attempt.is_some() => {
                let advertisement = active.take().expect("an active attempt owns its advertisement");
                attempt = None;
                match outcome {
                    AttemptOutcome::Completed => {
                        pending.complete(advertisement);
                        metrics::counter!("sync.gossip.request.completed", "kind" => advertisement.kind()).increment(1);
                        debug!(hash = ?advertisement.hash, height = ?advertisement.height,
                            attempt = advertisement.attempts, phase = "network_request_completed",
                            "block gossip network request completed; independent peer receipt is not confirmed");
                        if advertisement.mined
                            && mined_block_receiver.as_ref().is_none_or(|rx| rx.is_empty())
                            && chain_state.latest_chain_tip().best_tip_hash() == Some(advertisement.hash)
                        {
                            chain_state.mark_last_change_hash(advertisement.hash);
                            committed_tip = next_committed_tip(sync_status.clone(), chain_state.clone()).boxed();
                        }
                    },
                    AttemptOutcome::Retry(reason) => {
                        let now = Instant::now();
                        pending.retry(advertisement, now);
                        metrics::counter!("sync.gossip.request.retries", "kind" => advertisement.kind(), "reason" => reason).increment(1);
                        if last_warning.is_none_or(|previous| now.duration_since(previous) >= GOSSIP_WARNING_INTERVAL) {
                            last_warning = Some(now);
                            warn!(hash = ?advertisement.hash, height = ?advertisement.height,
                                attempt = advertisement.attempts,
                                pending_seconds = now.duration_since(advertisement.queued_at).as_secs(),
                                pending_mined = pending.mined_hashes.len(), reason, phase = "retry_queued",
                                "block gossip request incomplete; pending mined hashes retained for retry");
                        }
                    },
                    AttemptOutcome::PermanentReadiness(error) => return Err(PeerSetReadiness(error)),
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
