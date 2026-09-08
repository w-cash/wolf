//! Internal native-Zcash and Wcash AuxPoW mining in Zebra.
//!
//! # TODO
//! - pause mining if we have no peers, like `zcashd` does,
//!   and add a developer config that mines regardless of how many peers we have.
//!   <https://github.com/zcash/zcash/blob/6fdd9f1b81d3b228326c9826fa10696fc516444b/src/miner.cpp#L865-L880>
//! - move common code into zebra-chain or zebra-node-services and remove the RPC dependency.

use std::{cmp::min, sync::Arc, thread::available_parallelism, time::Duration};

use color_eyre::Report;
use futures::{stream::FuturesUnordered, StreamExt};
use thread_priority::{ThreadBuilder, ThreadPriority};
use tokio::{select, sync::watch, task::JoinHandle, time::sleep};
use tower::Service;
use tracing::{Instrument, Span};

use zebra_chain::{
    block::{self, Block},
    chain_sync_status::ChainSyncStatus,
    chain_tip::ChainTip,
    diagnostic::task::WaitForPanics,
    serialization::{AtLeastOne, ZcashSerialize},
    shutdown::is_shutting_down,
    work::equihash::{Solution, SolverCancelled},
};
use zebra_network::AddressBookPeers;
use zebra_node_services::mempool;
use zebra_rpc::{
    client::{
        BlockTemplateTimeSource,
        GetBlockTemplateCapability::{CoinbaseTxn, LongPoll},
        GetBlockTemplateParameters,
        GetBlockTemplateRequestMode::Template,
        HexData,
    },
    methods::{RpcImpl, RpcServer},
    proposal_block_from_template,
};
use zebra_state::WatchReceiver;

use crate::components::metrics::Config;

/// Number of synthetic Zcash parent nonces searched before the Wcash internal
/// miner starts another bounded batch.
const WCASH_AUXPOW_NONCES_PER_BATCH: u64 = 8;

/// The amount of time we wait between block template retries.
pub const BLOCK_TEMPLATE_WAIT_TIME: Duration = Duration::from_secs(20);

/// A rate-limit for block template refreshes.
pub const BLOCK_TEMPLATE_REFRESH_LIMIT: Duration = Duration::from_secs(2);

/// How long we wait after mining a block, before expecting a new template.
///
/// This should be slightly longer than `BLOCK_TEMPLATE_REFRESH_LIMIT` to allow for template
/// generation.
pub const BLOCK_MINING_WAIT_TIME: Duration = Duration::from_secs(3);

/// Initialize the miner based on its config, and spawn a task for it.
///
/// This method is CPU and memory-intensive. It uses 144 MB of RAM and one CPU core per configured
/// mining thread.
///
/// See [`run_mining_solver()`] for more details.
pub fn spawn_init<Mempool, State, ReadState, Tip, AddressBook, BlockVerifierRouter, SyncStatus>(
    config: &Config,
    rpc: RpcImpl<Mempool, State, ReadState, Tip, AddressBook, BlockVerifierRouter, SyncStatus>,
) -> JoinHandle<Result<(), Report>>
// TODO: simplify or avoid repeating these generics (how?)
where
    Mempool: Service<
            mempool::Request,
            Response = mempool::Response,
            Error = zebra_node_services::BoxError,
        > + Clone
        + Send
        + Sync
        + 'static,
    Mempool::Future: Send,
    State: Service<
            zebra_state::Request,
            Response = zebra_state::Response,
            Error = zebra_state::BoxError,
        > + Clone
        + Send
        + Sync
        + 'static,
    <State as Service<zebra_state::Request>>::Future: Send,
    ReadState: Service<
            zebra_state::ReadRequest,
            Response = zebra_state::ReadResponse,
            Error = zebra_state::BoxError,
        > + Clone
        + Send
        + Sync
        + 'static,
    <ReadState as Service<zebra_state::ReadRequest>>::Future: Send,
    Tip: ChainTip + Clone + Send + Sync + 'static,
    BlockVerifierRouter: Service<zebra_consensus::Request, Response = block::Hash, Error = zebra_consensus::BoxError>
        + Clone
        + Send
        + Sync
        + 'static,
    <BlockVerifierRouter as Service<zebra_consensus::Request>>::Future: Send,
    SyncStatus: ChainSyncStatus + Clone + Send + Sync + 'static,
    AddressBook: AddressBookPeers + Clone + Send + Sync + 'static,
{
    // TODO: spawn an entirely new executor here, so mining is isolated from higher priority tasks.
    tokio::spawn(init(config.clone(), rpc).in_current_span())
}

/// Initialize the miner based on its config.
///
/// This method is CPU and memory-intensive. It uses 144 MB of RAM and one CPU core per configured
/// mining thread.
///
/// See [`run_mining_solver()`] for more details.
pub async fn init<Mempool, State, ReadState, Tip, BlockVerifierRouter, SyncStatus, AddressBook>(
    _config: Config,
    rpc: RpcImpl<Mempool, State, ReadState, Tip, AddressBook, BlockVerifierRouter, SyncStatus>,
) -> Result<(), Report>
where
    Mempool: Service<
            mempool::Request,
            Response = mempool::Response,
            Error = zebra_node_services::BoxError,
        > + Clone
        + Send
        + Sync
        + 'static,
    Mempool::Future: Send,
    State: Service<
            zebra_state::Request,
            Response = zebra_state::Response,
            Error = zebra_state::BoxError,
        > + Clone
        + Send
        + Sync
        + 'static,
    <State as Service<zebra_state::Request>>::Future: Send,
    ReadState: Service<
            zebra_state::ReadRequest,
            Response = zebra_state::ReadResponse,
            Error = zebra_state::BoxError,
        > + Clone
        + Send
        + Sync
        + 'static,
    <ReadState as Service<zebra_state::ReadRequest>>::Future: Send,
    Tip: ChainTip + Clone + Send + Sync + 'static,
    BlockVerifierRouter: Service<zebra_consensus::Request, Response = block::Hash, Error = zebra_consensus::BoxError>
        + Clone
        + Send
        + Sync
        + 'static,
    <BlockVerifierRouter as Service<zebra_consensus::Request>>::Future: Send,
    SyncStatus: ChainSyncStatus + Clone + Send + Sync + 'static,
    AddressBook: AddressBookPeers + Clone + Send + Sync + 'static,
{
    // TODO: change this to `config.internal_miner_threads` once mining tasks are cancelled when the best tip changes (#8797)
    let configured_threads = 1;
    // If we can't detect the number of cores, use the configured number.
    let available_threads = available_parallelism()
        .map(usize::from)
        .unwrap_or(configured_threads);

    // Use the minimum of the configured and available threads.
    let solver_count = min(configured_threads, available_threads);

    info!(
        ?solver_count,
        "launching mining tasks with parallel solvers"
    );

    let (template_sender, template_receiver) = watch::channel(None);
    let template_receiver = WatchReceiver::new(template_receiver);

    // Spawn these tasks, to avoid blocked cooperative futures, and improve shutdown responsiveness.
    // This is particularly important when there are a large number of solver threads.
    let mut abort_handles = Vec::new();

    let template_generator = tokio::task::spawn(
        generate_block_templates(rpc.clone(), template_sender).in_current_span(),
    );
    abort_handles.push(template_generator.abort_handle());
    let template_generator = template_generator.wait_for_panics();

    let mut mining_solvers = FuturesUnordered::new();
    for solver_id in 0..solver_count {
        // Assume there are less than 256 cores. If there are more, only run 256 tasks.
        let solver_id = min(solver_id, usize::from(u8::MAX))
            .try_into()
            .expect("just limited to u8::MAX");

        let solver = tokio::task::spawn(
            run_mining_solver(solver_id, template_receiver.clone(), rpc.clone()).in_current_span(),
        );
        abort_handles.push(solver.abort_handle());

        mining_solvers.push(solver.wait_for_panics());
    }

    // These tasks run forever unless there is a fatal error or shutdown.
    // When that happens, the first task to error returns, and the other JoinHandle futures are
    // cancelled.
    let first_result;
    select! {
        result = template_generator => { first_result = result; }
        result = mining_solvers.next() => {
            first_result = result
                .expect("stream never terminates because there is at least one solver task");
        }
    }

    // But the spawned async tasks keep running, so we need to abort them here.
    for abort_handle in abort_handles {
        abort_handle.abort();
    }

    // Any spawned blocking threads will keep running. When this task returns and drops the
    // `template_sender`, it cancels all the spawned miner threads. This works because we've
    // aborted the `template_generator` task, which owns the `template_sender`. (And it doesn't
    // spawn any blocking threads.)
    first_result
}

/// Generates block templates using `rpc`, and sends them to mining threads using `template_sender`.
#[instrument(skip(rpc, template_sender))]
pub async fn generate_block_templates<
    Mempool,
    State,
    ReadState,
    Tip,
    BlockVerifierRouter,
    SyncStatus,
    AddressBook,
>(
    rpc: RpcImpl<Mempool, State, ReadState, Tip, AddressBook, BlockVerifierRouter, SyncStatus>,
    template_sender: watch::Sender<Option<Arc<Block>>>,
) -> Result<(), Report>
where
    Mempool: Service<
            mempool::Request,
            Response = mempool::Response,
            Error = zebra_node_services::BoxError,
        > + Clone
        + Send
        + Sync
        + 'static,
    Mempool::Future: Send,
    State: Service<
            zebra_state::Request,
            Response = zebra_state::Response,
            Error = zebra_state::BoxError,
        > + Clone
        + Send
        + Sync
        + 'static,
    <State as Service<zebra_state::Request>>::Future: Send,
    ReadState: Service<
            zebra_state::ReadRequest,
            Response = zebra_state::ReadResponse,
            Error = zebra_state::BoxError,
        > + Clone
        + Send
        + Sync
        + 'static,
    <ReadState as Service<zebra_state::ReadRequest>>::Future: Send,
    Tip: ChainTip + Clone + Send + Sync + 'static,
    BlockVerifierRouter: Service<zebra_consensus::Request, Response = block::Hash, Error = zebra_consensus::BoxError>
        + Clone
        + Send
        + Sync
        + 'static,
    <BlockVerifierRouter as Service<zebra_consensus::Request>>::Future: Send,
    SyncStatus: ChainSyncStatus + Clone + Send + Sync + 'static,
    AddressBook: AddressBookPeers + Clone + Send + Sync + 'static,
{
    // Pass the correct arguments, even if Zebra currently ignores them.
    let mut parameters =
        GetBlockTemplateParameters::new(Template, None, vec![LongPoll, CoinbaseTxn], None, None);

    // Shut down the task when all the template receivers are dropped, or Zebra shuts down.
    while !template_sender.is_closed() && !is_shutting_down() {
        let template: Result<_, _> = rpc.get_block_template(Some(parameters.clone())).await;

        // Wait for the chain to sync so we get a valid template.
        let Ok(template) = template else {
            warn!(
                ?BLOCK_TEMPLATE_WAIT_TIME,
                ?template,
                "waiting for a valid block template",
            );

            // Skip the wait if we got an error because we are shutting down.
            if !is_shutting_down() {
                sleep(BLOCK_TEMPLATE_WAIT_TIME).await;
            }

            continue;
        };

        // Convert from RPC GetBlockTemplate to Block
        let template = template
            .try_into_template()
            .expect("invalid RPC response: proposal in response to a template request");

        info!(
            height = ?template.height(),
            transactions = ?template.transactions().len(),
            "mining with an updated block template",
        );

        // Tell the next get_block_template() call to wait until the template has changed.
        parameters = GetBlockTemplateParameters::new(
            Template,
            None,
            vec![LongPoll, CoinbaseTxn],
            Some(template.long_poll_id()),
            None,
        );

        let block = proposal_block_from_template(
            &template,
            BlockTemplateTimeSource::CurTime,
            rpc.network(),
        )?;

        // If the template has actually changed, send an updated template.
        template_sender.send_if_modified(|old_block| {
            if old_block.as_ref().map(|b| b.header.clone()) == Some(block.header.clone()) {
                return false;
            }
            *old_block = Some(Arc::new(block));
            true
        });

        // If the blockchain is changing rapidly, limit how often we'll update the template.
        // But if we're shutting down, do that immediately.
        if !template_sender.is_closed() && !is_shutting_down() {
            sleep(BLOCK_TEMPLATE_REFRESH_LIMIT).await;
        }
    }

    Ok(())
}

/// Runs a single mining thread that gets blocks from the `template_receiver`, calculates native
/// Equihash or Zcash-parent AuxPoW solutions with nonces based on `solver_id`, and submits valid
/// blocks to Zebra's block validator.
///
/// This method is CPU and memory-intensive. It uses 144 MB of RAM and one CPU core while running.
/// It can run for minutes or hours if the network difficulty is high. Mining uses a thread with
/// low CPU priority.
#[instrument(skip(template_receiver, rpc))]
pub async fn run_mining_solver<
    Mempool,
    State,
    ReadState,
    Tip,
    BlockVerifierRouter,
    SyncStatus,
    AddressBook,
>(
    solver_id: u8,
    mut template_receiver: WatchReceiver<Option<Arc<Block>>>,
    rpc: RpcImpl<Mempool, State, ReadState, Tip, AddressBook, BlockVerifierRouter, SyncStatus>,
) -> Result<(), Report>
where
    Mempool: Service<
            mempool::Request,
            Response = mempool::Response,
            Error = zebra_node_services::BoxError,
        > + Clone
        + Send
        + Sync
        + 'static,
    Mempool::Future: Send,
    State: Service<
            zebra_state::Request,
            Response = zebra_state::Response,
            Error = zebra_state::BoxError,
        > + Clone
        + Send
        + Sync
        + 'static,
    <State as Service<zebra_state::Request>>::Future: Send,
    ReadState: Service<
            zebra_state::ReadRequest,
            Response = zebra_state::ReadResponse,
            Error = zebra_state::BoxError,
        > + Clone
        + Send
        + Sync
        + 'static,
    <ReadState as Service<zebra_state::ReadRequest>>::Future: Send,
    Tip: ChainTip + Clone + Send + Sync + 'static,
    BlockVerifierRouter: Service<zebra_consensus::Request, Response = block::Hash, Error = zebra_consensus::BoxError>
        + Clone
        + Send
        + Sync
        + 'static,
    <BlockVerifierRouter as Service<zebra_consensus::Request>>::Future: Send,
    SyncStatus: ChainSyncStatus + Clone + Send + Sync + 'static,
    AddressBook: AddressBookPeers + Clone + Send + Sync + 'static,
{
    // Shut down the task when the template sender is dropped, or Zebra shuts down.
    while template_receiver.has_changed().is_ok() && !is_shutting_down() {
        // Get the latest block template, and mark the current value as seen.
        // We mark the value first to avoid missed updates.
        template_receiver.mark_as_seen();
        let template = template_receiver.cloned_watch_data();

        let Some(template) = template else {
            if solver_id == 0 {
                info!(
                    ?solver_id,
                    ?BLOCK_TEMPLATE_WAIT_TIME,
                    "solver waiting for initial block template"
                );
            } else {
                debug!(
                    ?solver_id,
                    ?BLOCK_TEMPLATE_WAIT_TIME,
                    "solver waiting for initial block template"
                );
            }

            // Skip the wait if we didn't get a template because we are shutting down.
            if !is_shutting_down() {
                sleep(BLOCK_TEMPLATE_WAIT_TIME).await;
            }

            continue;
        };

        let height = template.coinbase_height().expect("template is valid");

        // Set up the cancellation conditions for the miner.
        let mut cancel_receiver = template_receiver.clone();
        let old_header = template.header.as_ref().clone();
        let cancel_fn = move || match cancel_receiver.has_changed() {
            // Guard against get_block_template() providing an identical header. This could happen
            // if something irrelevant to the block data changes, the time was within 1 second, or
            // there is a spurious channel change.
            Ok(has_changed) => {
                cancel_receiver.mark_as_seen();

                // We only need to check header equality, because the block data is bound to the
                // header.
                if has_changed
                    && Some(old_header.clone())
                        != cancel_receiver
                            .cloned_watch_data()
                            .map(|b| b.header.as_ref().clone())
                {
                    Err(SolverCancelled)
                } else {
                    Ok(())
                }
            }
            // If the sender was dropped, we're likely shutting down, so cancel the solver.
            Err(_sender_dropped) => Err(SolverCancelled),
        };

        // Mine at least one block using the equihash solver.
        let blocks = match mine_a_block(solver_id, template, cancel_fn).await {
            Ok(blocks) => blocks,
            Err(InternalMiningError::Cancelled(_)) => {
                // If the solver was cancelled, we're either shutting down, or we have a new template.
                if solver_id == 0 {
                    info!(
                        ?height,
                        ?solver_id,
                        new_template = ?template_receiver.has_changed(),
                        shutting_down = ?is_shutting_down(),
                        "solver cancelled: getting a new block template or shutting down"
                    );
                } else {
                    debug!(
                        ?height,
                        ?solver_id,
                        new_template = ?template_receiver.has_changed(),
                        shutting_down = ?is_shutting_down(),
                        "solver cancelled: getting a new block template or shutting down"
                    );
                }

                // If the blockchain is changing rapidly, limit how often we'll update the template.
                // But if we're shutting down, do that immediately.
                if template_receiver.has_changed().is_ok() && !is_shutting_down() {
                    sleep(BLOCK_TEMPLATE_REFRESH_LIMIT).await;
                }

                continue;
            }
            Err(error) => return Err(error.into()),
        };

        // Submit the newly mined blocks to the verifiers.
        //
        // TODO: if there is a new template (`cancel_fn().is_err()`), and
        //       GetBlockTemplate.submit_old is false, return immediately, and skip submitting the
        //       blocks.
        let mut any_success = false;
        for block in blocks {
            let data = block
                .zcash_serialize_to_vec()
                .expect("serializing to Vec never fails");

            match rpc.submit_block(HexData(data), None).await {
                Ok(success) => {
                    info!(
                        ?height,
                        hash = ?block.hash(),
                        ?solver_id,
                        ?success,
                        "successfully mined a new block",
                    );
                    any_success = true;
                }
                Err(error) => info!(
                    ?height,
                    hash = ?block.hash(),
                    ?solver_id,
                    ?error,
                    "validating a newly mined block failed, trying again",
                ),
            }
        }

        // Start re-mining quickly after a failed solution.
        // If there's a new template, we'll use it, otherwise the existing one is ok.
        if !any_success {
            // If the blockchain is changing rapidly, limit how often we'll update the template.
            // But if we're shutting down, do that immediately.
            if template_receiver.has_changed().is_ok() && !is_shutting_down() {
                sleep(BLOCK_TEMPLATE_REFRESH_LIMIT).await;
            }
            continue;
        }

        // Wait for the new block to verify, and the RPC task to pick up a new template.
        // But don't wait too long, we could have mined on a fork.
        tokio::select! {
            shutdown_result = template_receiver.changed() => shutdown_result?,
            _ = sleep(BLOCK_MINING_WAIT_TIME) => {}

        }
    }

    Ok(())
}

/// Mines one or more blocks based on `template`. Native templates use Zebra's Equihash solver;
/// Wcash templates use a real Equihash synthetic Zcash parent and attach its canonical AuxPoW
/// proof. Returns as soon as it has at least one block and uses a different nonce range for each
/// `solver_id`.
///
/// If `cancel_fn()` returns an error, returns early with
/// [`InternalMiningError::Cancelled`].
///
/// See [`run_mining_solver()`] for more details.
pub async fn mine_a_block<F>(
    solver_id: u8,
    template: Arc<Block>,
    cancel_fn: F,
) -> Result<AtLeastOne<Block>, InternalMiningError>
where
    F: FnMut() -> Result<(), SolverCancelled> + Send + Sync + 'static,
{
    let height = template.coinbase_height().expect("template is valid");

    // TODO: Replace with Arc::unwrap_or_clone() when it stabilises:
    // https://github.com/rust-lang/rust/issues/93610
    let mut header = template.header.as_ref().clone();

    // Use a different nonce for each solver thread.
    // Change both the first and last bytes, so we don't have to care if the nonces are incremented in
    // big-endian or little-endian order. And we can see the thread that mined a block from the nonce.
    *header.nonce.first_mut().unwrap() = solver_id;
    *header.nonce.last_mut().unwrap() = solver_id;

    // Mine one or more blocks using the solver, in a low-priority blocking thread.
    let span = Span::current();
    let solved_headers = tokio::task::spawn_blocking(move || {
        span.in_scope(move || {
            let miner_thread_handle = ThreadBuilder::default().name("zebra-miner").priority(ThreadPriority::Min).spawn(move |priority_result| {
                if let Err(error) = priority_result {
                    info!(?error, "could not set miner to run at a low priority: running at default priority");
                }

                if header.solution.as_wcash().is_some() {
                    solve_wcash_header(header, height, solver_id, cancel_fn)
                } else {
                    Solution::solve(header, cancel_fn).map_err(InternalMiningError::from)
                }
            }).expect("unable to spawn miner thread");

            miner_thread_handle.wait_for_panics()
        })
    })
    .wait_for_panics()
    .await?;

    // Modify the template into solved blocks.

    // TODO: Replace with Arc::unwrap_or_clone() when it stabilises
    let block = (*template).clone();

    let solved_blocks: Vec<Block> = solved_headers
        .into_iter()
        .map(|header| {
            let mut block = block.clone();
            block.header = Arc::new(header);
            block
        })
        .collect();

    Ok(solved_blocks
        .try_into()
        .expect("a 1:1 mapping of AtLeastOne produces at least one block"))
}

/// Errors produced by the built-in native or Wcash AuxPoW solver.
#[derive(Debug, thiserror::Error)]
pub enum InternalMiningError {
    /// A new template or node shutdown cancelled the current work.
    #[error(transparent)]
    Cancelled(#[from] SolverCancelled),

    /// The local Zcash-parent harness rejected the Wcash template or work.
    #[error(transparent)]
    Wcash(#[from] wcash_merge_miner::MinerError),

    /// A generated template timestamp cannot be serialized into a block header.
    #[error("Wcash block template timestamp {0} does not fit in u32")]
    InvalidTemplateTimestamp(i64),

    /// A solved Wcash header could not be represented as a non-empty result.
    #[error("Wcash solver unexpectedly produced an empty result set")]
    EmptySolutionSet,
}

/// Mines a real Equihash `(200, 9)` synthetic Zcash parent and attaches its
/// canonical proof to `header`.
///
/// This is a local integration harness. Its parent coinbase commits to Wcash
/// but does not contain the payouts or other transactions required by a live
/// Zcash node. Production pools need a parent template producer or complete
/// coinbase builder that includes the commitment before shielded proving and
/// authorization, recomputes all affected parent commitments, and submits
/// parent-chain winners independently.
fn solve_wcash_header<F>(
    header: block::Header,
    height: block::Height,
    solver_id: u8,
    mut cancel_fn: F,
) -> Result<AtLeastOne<block::Header>, InternalMiningError>
where
    F: FnMut() -> Result<(), SolverCancelled>,
{
    let timestamp = header
        .time
        .timestamp()
        .try_into()
        .map_err(|_| InternalMiningError::InvalidTemplateTimestamp(header.time.timestamp()))?;
    let config = wcash_merge_miner::JobConfig {
        parent_height: height.0,
        timestamp,
        auxiliary_nonce: u32::from(solver_id),
        ..Default::default()
    };
    let job = wcash_merge_miner::PreparedJob::from_wcash_header(&header, config)?;

    // Give each configured solver a disjoint initial range. A single worker is
    // currently launched, but this remains correct when parallel mining is
    // re-enabled.
    let mut start_nonce = u64::from(solver_id) << 56;
    loop {
        cancel_fn()?;
        match job.solve_with_cancel(start_nonce, WCASH_AUXPOW_NONCES_PER_BATCH, || {
            cancel_fn().is_err()
        }) {
            Ok(solved) => {
                let solved_header = job.attach_to_header(header, &solved)?;
                return vec![solved_header]
                    .try_into()
                    .map_err(|_| InternalMiningError::EmptySolutionSet);
            }
            Err(wcash_merge_miner::MinerError::SolverExhausted { attempted }) => {
                start_nonce = start_nonce
                    .checked_add(attempted)
                    .ok_or(wcash_merge_miner::MinerError::NonceRangeOverflow)?;
            }
            Err(wcash_merge_miner::MinerError::SolverCancelled { .. }) => {
                return Err(SolverCancelled.into());
            }
            Err(error) => return Err(error.into()),
        }
    }
}
