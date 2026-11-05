//! Per-request Wcash candidates built without invalidating the coinbase cache.

use std::sync::Arc;

use jsonrpsee::core::RpcResult;
use jsonrpsee_types::{ErrorCode, ErrorObject};
use rand::{rngs::OsRng, RngCore};
use zebra_chain::{block::Block, parameters::Network};

use super::types::get_block_template::{
    proposal::proposal_block_from_template, BlockTemplateResponse,
};

/// Gives each request a fresh child ID before validation and publication.
///
/// The nonce is part of the proof-independent Wcash hash. Changing it leaves
/// the cached coinbase proof, transaction commitments and timestamp intact.
/// Published candidates are never modified, so late solutions keep their ID.
pub(super) fn from_template(
    template: &BlockTemplateResponse,
    network: &Network,
) -> RpcResult<Block> {
    let mut candidate = proposal_block_from_template(template, None, network).map_err(|error| {
        ErrorObject::owned(
            ErrorCode::InternalError.code(),
            format!("failed to construct Wcash auxiliary block: {error}"),
            None::<()>,
        )
    })?;
    let mut nonce = [0; 32];
    OsRng.try_fill_bytes(&mut nonce).map_err(|_| {
        ErrorObject::borrowed(
            ErrorCode::InternalError.code(),
            "operating system randomness unavailable for Wcash auxiliary candidate",
            None,
        )
    })?;
    Arc::make_mut(&mut candidate.header).nonce = nonce.into();
    Ok(candidate)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{HashSet, VecDeque},
        thread,
    };

    use zebra_chain::{
        block::Height, history_tree::HistoryTree, serialization::DateTime32,
        serialization::ZcashSerialize, work::difficulty::ParameterDifficulty,
    };

    use crate::{
        config::mining::{default_miner_address_for_network, Config, MinerAddressType},
        methods::{
            types::{
                get_block_template::{CoinbaseCache, MinerParams},
                long_poll::LongPollId,
            },
            wcash_aux_block::AuxBlockCandidateCache,
        },
    };

    use super::*;

    fn frozen_template() -> (Network, BlockTemplateResponse) {
        let network = Network::new_wcash_regtest();
        let address = default_miner_address_for_network(&network, &MinerAddressType::Transparent)
            .expect("Wcash Regtest has a test mining address");
        let miner = MinerParams::new(
            &network,
            Config {
                miner_address: Some(address.parse().expect("the test address is valid")),
                ..Default::default()
            },
        )
        .expect("the transparent test recipient is supported");
        let time = DateTime32::now();
        let chain = zebra_state::GetBlockTemplateChainInfo {
            tip_hash: network.genesis_hash(),
            tip_height: Height::MIN,
            chain_history_root: HistoryTree::default().hash(),
            expected_difficulty: network.target_difficulty_limit().to_compact(),
            cur_time: time,
            min_time: time,
            max_time: time,
        };
        let template = BlockTemplateResponse::new_internal(
            &network,
            &CoinbaseCache::default(),
            &miner,
            None,
            &chain,
            LongPollId::new(0, 0, 0, 1, 0),
            vec![],
            None,
        )
        .expect("the frozen Wcash template is valid");
        (network, template)
    }

    #[test]
    fn same_template_candidates_only_change_the_child_nonce() {
        let (network, template) = frozen_template();
        let base = proposal_block_from_template(&template, None, &network).unwrap();
        let base_bytes = base.zcash_serialize_to_vec().unwrap();
        let mut ids = HashSet::new();
        for _ in 0..64 {
            let mut candidate = from_template(&template, &network).unwrap();
            assert!(ids.insert(candidate.hash()), "each request gets fresh work");
            zebra_consensus::block::check::wcash_auxpow_proposal_is_valid(
                &candidate.header,
                &network,
                &Height(1),
                &candidate.hash(),
            )
            .expect("a random child nonce preserves the proposal header rules");
            Arc::make_mut(&mut candidate.header).nonce = base.header.nonce;
            assert_eq!(candidate.zcash_serialize_to_vec().unwrap(), base_bytes);
        }
        // The shared proposal path and its cached template remain untouched.
        assert_eq!(
            proposal_block_from_template(&template, None, &network)
                .unwrap()
                .zcash_serialize_to_vec()
                .unwrap(),
            base_bytes
        );
    }

    #[test]
    fn retiring_unique_jobs_allows_turnover_beyond_the_cache_bound() {
        let (network, template) = frozen_template();
        let cache = AuxBlockCandidateCache::default();
        let mut live = VecDeque::new();
        for _ in 0..64 {
            let lease = cache
                .insert(Arc::new(from_template(&template, &network).unwrap()))
                .expect("retirement keeps six overlapping jobs within the cache bound");
            cache.publish(lease);
            live.push_back(lease);
            if live.len() == 6 {
                let retired = live.pop_front().unwrap();
                cache.retire(retired.id(), retired.retire_token()).unwrap();
            }
        }
        for lease in live {
            assert!(cache.begin_submission(lease.id()).is_ok());
        }
    }

    #[test]
    fn concurrent_same_template_requests_preserve_previously_issued_candidates() {
        let (network, template) = frozen_template();
        let first = Arc::new(from_template(&template, &network).unwrap());
        let first_bytes = first.zcash_serialize_to_vec().unwrap();
        let cache = AuxBlockCandidateCache::default();
        let lease = cache.insert(Arc::clone(&first)).unwrap();
        cache.publish(lease);

        let ids = thread::scope(|scope| {
            let handles: Vec<_> = (0..16)
                .map(|_| scope.spawn(|| from_template(&template, &network).unwrap().hash()))
                .collect();
            handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .collect::<HashSet<_>>()
        });
        assert_eq!(ids.len(), 16);
        assert!(!ids.contains(&first.hash()));
        let retained = cache.begin_submission(lease.id()).unwrap();
        assert!(Arc::ptr_eq(&first, &retained));
        assert_eq!(retained.zcash_serialize_to_vec().unwrap(), first_bytes);
    }
}
