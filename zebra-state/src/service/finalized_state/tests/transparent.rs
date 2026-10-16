//! Fixed-vector tests for the finalized transparent address balance writer.
//!
//! Regression coverage for the credit-before-debit overflow panic: applying a
//! same-address transparent self-spend chain in one block used to push the
//! intermediate address balance above `MAX_MONEY` and panic the writer, even
//! though the final consensus balance was valid.
//!
//! This test drives `DiskWriteBatch::prepare_transparent_transaction_batch`
//! (the writer's public entry point, whose signature is unchanged by the fix)
//! so the same source compiles against both the buggy and the fixed revision:
//! it panics on the buggy revision and passes on the fixed one.

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use zebra_chain::{
    amount::{
        Amount, DeferredPoolBalanceChange, NonNegative, MAX_MONEY, MAX_SINGLE_TRANSACTION_VALUE,
    },
    block::{self, Block, Height},
    parameters::{Network, NetworkKind},
    serialization::ZcashDeserializeInto,
    transaction::{self, LockTime, Transaction},
    transparent::{
        self, new_ordered_outputs_with_height, Address, Input, OutPoint, Output, Script,
    },
};

use crate::{
    constants::{state_database_format_version_in_code, STATE_DATABASE_KIND},
    request::{FinalizedBlock, SemanticallyVerifiedBlock, Treestate},
    service::finalized_state::{
        disk_db::DiskWriteBatch,
        disk_format::transparent::{
            AddressBalanceLocation, AddressBalanceLocationUpdates, OutputLocation,
        },
        ZebraDb, STATE_COLUMN_FAMILIES_IN_CODE,
    },
    CheckpointVerifiedBlock, Config,
};

fn new_ephemeral_zebra_db(network: &Network) -> ZebraDb {
    ZebraDb::new(
        &Config::ephemeral(),
        STATE_DATABASE_KIND,
        &state_database_format_version_in_code(),
        network,
        // The raw database accesses in this test create invalid database formats.
        true,
        STATE_COLUMN_FAMILIES_IN_CODE
            .iter()
            .map(ToString::to_string),
        false,
    )
    .expect("opening an ephemeral database should succeed")
}

/// Cross-version regression test for the credit-before-debit overflow panic.
///
/// Builds a synthetic block whose two transactions form a same-address self-spend
/// chain that re-creates a transaction-representable UTXO of value `V` twice, and drives it
/// through [`DiskWriteBatch::prepare_transparent_transaction_batch`].
///
/// - On the **buggy** revision the writer credits both new outputs before debiting
///   the matching spends, so the intermediate balance reaches `MAX_MONEY + V`
///   and panics with `"balance overflow already checked"`.
/// - On the **fixed** revision the writer processes each transaction in block order
///   (debit-then-credit), so the per-address running balance stays in
///   `[0, MAX_MONEY]` and the final on-disk balance equals the original
///   `MAX_MONEY - V`. Other unspent outputs account for the rest of that address balance.
#[test]
fn intra_block_self_spend_chain_in_finalized_state() {
    let _init_guard = zebra_test::init();

    let network = Network::Mainnet;
    let height = Height(1);
    let address = Address::from_script_hash(NetworkKind::Mainnet, [0x42; 20]);
    // Wcash widens aggregate address balances, while individual serialized outputs still
    // use the inherited transaction-library bound. Keep the overflow regression meaningful
    // under either profile by placing the initial aggregate balance one output below its bound.
    let value_atoms = MAX_MONEY.min(MAX_SINGLE_TRANSACTION_VALUE) / 2;
    let value = Amount::<NonNegative>::try_from(value_atoms)
        .expect("half the smaller value bound fits in a transaction output");
    let initial_balance = Amount::<NonNegative>::try_from(MAX_MONEY - value_atoms)
        .expect("the initial address balance is below the aggregate value bound");
    let after_first_credit = (initial_balance + value).unwrap();
    assert_eq!(after_first_credit.zatoshis(), MAX_MONEY);
    assert!(
        (after_first_credit + value).is_err(),
        "credit-before-debit must still overflow"
    );

    // T0 spends a pre-existing on-chain UTXO of value V to address A and re-creates V to A.
    let existing_outpoint = OutPoint {
        hash: transaction::Hash([0x00; 32]),
        index: 0,
    };
    let t0 = Arc::new(Transaction::test_v1(
        vec![Input::PrevOut {
            outpoint: existing_outpoint,
            unlock_script: Script::new(&[]),
            sequence: 0xffff_ffff,
        }],
        vec![Output::new(value, address.script())],
        LockTime::unlocked(),
    ));
    let t0_hash = t0.hash();

    // T1 spends T0's output and creates V back to A.
    let t0_output_outpoint = OutPoint {
        hash: t0_hash,
        index: 0,
    };
    let t1 = Arc::new(Transaction::test_v1(
        vec![Input::PrevOut {
            outpoint: t0_output_outpoint,
            unlock_script: Script::new(&[]),
            sequence: 0xffff_ffff,
        }],
        vec![Output::new(value, address.script())],
        LockTime::unlocked(),
    ));

    // Synthetic block. The header is a dummy from zebra-test (round-tripped); the writer
    // doesn't validate the header — only the transactions and the block height matter.
    let header: block::Header = zebra_test::vectors::DUMMY_HEADER
        .as_slice()
        .zcash_deserialize_into()
        .expect("DUMMY_HEADER deserializes");
    let block = Arc::new(Block {
        header: Arc::new(header),
        transactions: vec![t0.clone(), t1.clone()],
    });
    let transaction_hashes: Arc<[_]> = block.transactions.iter().map(|tx| tx.hash()).collect();
    let new_outputs = new_ordered_outputs_with_height(&block, height, &transaction_hashes);

    let semantically_verified = SemanticallyVerifiedBlock {
        block: block.clone(),
        hash: block::Hash([0x00; 32]),
        height,
        new_outputs,
        transaction_hashes,
    };
    let finalized = FinalizedBlock::from_checkpoint_verified(
        CheckpointVerifiedBlock(semantically_verified),
        Treestate::default(),
        DeferredPoolBalanceChange::zero(),
    );

    // Inputs to `prepare_transparent_transaction_batch`, prepared the way `write_block` does.
    let existing_output_location = OutputLocation::from_usize(Height(0), 0, 0);
    let t0_output_location = OutputLocation::from_usize(height, 0, 0);
    let t1_output_location = OutputLocation::from_usize(height, 1, 0);

    let make_utxo =
        |h: Height| transparent::Utxo::new(Output::new(value, address.script()), h, false);
    let existing_utxo = make_utxo(Height(0));
    let t0_output_utxo = make_utxo(height);
    let t1_output_utxo = make_utxo(height);

    let new_outputs_by_out_loc: BTreeMap<OutputLocation, transparent::Utxo> = BTreeMap::from([
        (t0_output_location, t0_output_utxo.clone()),
        (t1_output_location, t1_output_utxo),
    ]);
    let spent_utxos_by_outpoint: HashMap<OutPoint, transparent::Utxo> = HashMap::from([
        (existing_outpoint, existing_utxo.clone()),
        (t0_output_outpoint, t0_output_utxo.clone()),
    ]);
    let spent_utxos_by_out_loc: BTreeMap<OutputLocation, transparent::Utxo> = BTreeMap::from([
        (existing_output_location, existing_utxo),
        (t0_output_location, t0_output_utxo),
    ]);

    // Pre-populate `address_balances` with A's pre-block on-chain balance, the way
    // `block.rs` does via `read_addr_locs`.
    let mut existing_abl = AddressBalanceLocation::new(existing_output_location);
    *existing_abl.balance_mut() = initial_balance;
    *existing_abl.received_mut() = u64::from(initial_balance);
    let address_balances =
        AddressBalanceLocationUpdates::Insert(HashMap::from([(address, existing_abl)]));

    let zebra_db = new_ephemeral_zebra_db(&network);
    let mut batch = DiskWriteBatch::new();

    // On the buggy revision this call panics with "balance overflow already checked" during
    // the credit-first batch (intermediate balance reaches MAX_MONEY + V). On the fixed
    // revision it completes cleanly.
    batch.prepare_transparent_transaction_batch(
        &zebra_db,
        &network,
        &finalized,
        &new_outputs_by_out_loc,
        &spent_utxos_by_outpoint,
        &spent_utxos_by_out_loc,
        #[cfg(feature = "indexer")]
        &HashMap::new(),
        address_balances,
    );

    // Write the batch and confirm the final on-disk balance matches the consensus value
    // (existing balance − 2*V debits + 2*V credits = existing balance).
    zebra_db
        .write_batch(batch)
        .expect("ephemeral db accepts the batch");

    let (balance, received) = zebra_db
        .address_balance(&address)
        .expect("address balance is present after writing the batch");
    assert_eq!(
        balance, initial_balance,
        "final balance equals the existing balance"
    );
    assert_eq!(
        received,
        u64::from(initial_balance)
            .checked_add(u64::from(value).checked_mul(2).unwrap())
            .unwrap(),
        "received counts the initial balance plus two intra-block credits of V",
    );
}
