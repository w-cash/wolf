//! Exact finalized-chain coverage for the Wcash foundation consensus rule.

use zebra_chain::block::{Hash, Height};

use crate::{
    service::{
        finalized_state::{DiskDb, DiskWriteBatch, RawBytes, ReadDisk, TypedColumnFamily},
        foundation_validation::coverage,
    },
    StateInitError,
};

use super::ZebraDb;

pub(crate) const FOUNDATION_VALIDATION: &str = "wcash_foundation_validation";
type FoundationValidationCf<'cf> = TypedColumnFamily<'cf, (), RawBytes>;

/// Check a read-only snapshot before opening or moving a legacy database for writing.
pub(in crate::service::finalized_state) fn check_disk_coverage(
    db: &DiskDb,
) -> Result<(), StateInitError> {
    let Some(cf) = db.cf_handle("hash_by_height") else {
        return Ok(());
    };
    let Some((height, hash)) = db.zs_last_key_value(&cf) else {
        return Ok(());
    };
    let Some(expected) = coverage(&db.network(), height, hash) else {
        return Ok(());
    };
    let actual =
        FoundationValidationCf::new(db, FOUNDATION_VALIDATION).and_then(|cf| cf.zs_get(&()));
    if actual.as_ref().map(RawBytes::raw_bytes) != Some(&expected) {
        return Err(StateInitError::UncoveredFoundationHistory {
            path: db.path().to_path_buf(),
            height,
            hash,
        });
    }
    Ok(())
}

impl ZebraDb {
    pub(super) fn check_foundation_validation_coverage(&self) -> Result<(), StateInitError> {
        check_disk_coverage(&self.db)
    }

    /// Include coverage in the same transaction as the finalized tip it covers.
    pub(super) fn prepare_foundation_validation_coverage(
        &self,
        batch: &mut DiskWriteBatch,
        height: Height,
        hash: Hash,
    ) {
        if let Some(bytes) = coverage(&self.network(), height, hash) {
            let cf = FoundationValidationCf::new(&self.db, FOUNDATION_VALIDATION)
                .expect("foundation validation column is registered when opening writable state");
            let _ = cf
                .with_batch_for_writing(batch)
                .zs_insert(&(), &RawBytes::new_raw_bytes(bytes));
        }
    }
}

#[cfg(test)]
mod tests {
    use zebra_chain::parameters::{
        subsidy::{wcash_foundation_activation_height, wcash_foundation_expiry_height},
        Network,
    };

    use crate::{
        constants::{state_database_format_version_in_code, STATE_DATABASE_KIND},
        service::finalized_state::{
            DiskWriteBatch, FinalizedState, WriteDisk, STATE_COLUMN_FAMILIES_IN_CODE,
        },
        tests::setup::{test_genesis, uses_wcash_consensus, wcash_fake_children},
        Config,
    };

    use super::*;

    fn network() -> Network {
        let fixture = Network::new_wcash_regtest();
        if wcash_foundation_activation_height(&fixture).is_some() {
            fixture
        } else {
            Network::try_new_wcash_mainnet().expect("Mainnet parameters are enabled")
        }
    }

    fn open(config: &Config, network: &Network) -> Result<ZebraDb, StateInitError> {
        ZebraDb::new(
            config,
            STATE_DATABASE_KIND,
            &state_database_format_version_in_code(),
            network,
            true,
            STATE_COLUMN_FAMILIES_IN_CODE
                .iter()
                .map(ToString::to_string),
            false,
        )
    }

    /// Emulate an old executable's atomic tip update, without new-rule coverage.
    fn append_legacy_tip(db: &ZebraDb, height: Height, hash: Hash, covered: bool) {
        let mut batch = DiskWriteBatch::new();
        let cf = db.db.cf_handle("hash_by_height").unwrap();
        batch.zs_insert(&cf, height, hash);
        if covered {
            db.prepare_foundation_validation_coverage(&mut batch, height, hash);
        }
        db.write_batch(batch).unwrap();
    }

    /// Independently encode coverage with a missing or explicit allocation ratio.
    fn rule_marker_fixture(
        network: &Network,
        height: Height,
        hash: Hash,
        ratio: Option<(u64, u64)>,
        activation: Height,
    ) -> Vec<u8> {
        let mut marker = if let Some((numerator, denominator)) = ratio {
            let mut marker = b"wcash-foundation-fund-validation-v2\0".to_vec();
            marker.extend_from_slice(&numerator.to_be_bytes());
            marker.extend_from_slice(&denominator.to_be_bytes());
            marker
        } else {
            b"wcash-foundation-fund-validation-v1\0".to_vec()
        };
        marker.extend_from_slice(&activation.0.to_be_bytes());
        marker.extend_from_slice(
            &wcash_foundation_expiry_height(network)
                .unwrap()
                .0
                .to_be_bytes(),
        );
        marker.extend_from_slice(&height.0.to_be_bytes());
        marker.extend_from_slice(&hash.0);
        marker
    }

    #[test]
    fn foundation_finalized_rejects_incompatible_rule_coverage_on_reopen() {
        let _init_guard = zebra_test::init();
        if !uses_wcash_consensus() {
            return;
        }
        let network = network();
        let activation = wcash_foundation_activation_height(&network).unwrap();
        let expiry = wcash_foundation_expiry_height(&network).unwrap();
        let hash = Hash([0x30; 32]);

        for height in [activation, expiry] {
            for ratio in [None, Some((2, 10)), Some((3, 5))] {
                let dir = tempfile::tempdir().unwrap();
                let config = Config {
                    cache_dir: dir.path().to_path_buf(),
                    ephemeral: false,
                    delete_old_database: false,
                    ..Config::default()
                };
                let marker = rule_marker_fixture(&network, height, hash, ratio, activation);
                let db = open(&config, &network).unwrap();
                append_legacy_tip(&db, height, hash, false);
                let cf = FoundationValidationCf::new(&db.db, FOUNDATION_VALIDATION).unwrap();
                FoundationValidationCf::new(&db.db, FOUNDATION_VALIDATION)
                    .unwrap()
                    .new_batch_for_writing()
                    .zs_insert(&(), &RawBytes::new_raw_bytes(marker.clone()))
                    .write_batch()
                    .unwrap();
                assert!(matches!(
                    db.check_foundation_validation_coverage(),
                    Err(StateInitError::UncoveredFoundationHistory { .. })
                ));
                assert_eq!(cf.zs_get(&()).unwrap().raw_bytes(), &marker);
                drop(cf);
                let path = db.path().to_path_buf();
                drop(db);
                let error = open(&config, &network).unwrap_err();
                assert!(matches!(
                    error,
                    StateInitError::UncoveredFoundationHistory { .. }
                ));
                assert!(error.to_string().contains("NEW, empty state cache_dir"));
                assert!(path.is_dir());
            }
        }

        // Before activation, even an obsolete marker does not discard the original history.
        let dir = tempfile::tempdir().unwrap();
        let config = Config {
            cache_dir: dir.path().to_path_buf(),
            ephemeral: false,
            delete_old_database: false,
            ..Config::default()
        };
        let height = activation.previous().unwrap();
        let db = open(&config, &network).unwrap();
        append_legacy_tip(&db, height, hash, false);
        let cf = FoundationValidationCf::new(&db.db, FOUNDATION_VALIDATION).unwrap();
        cf.new_batch_for_writing()
            .zs_insert(
                &(),
                &RawBytes::new_raw_bytes(rule_marker_fixture(
                    &network, height, hash, None, activation,
                )),
            )
            .write_batch()
            .unwrap();
        let path = db.path().to_path_buf();
        drop(db);
        let db = open(&config, &network).unwrap();
        assert_eq!(db.tip(), Some((height, hash)));
        assert_eq!(db.path(), path);
        db.check_foundation_validation_coverage().unwrap();
        append_legacy_tip(&db, activation, hash, true);
        drop(db);
        let db = open(&config, &network).unwrap();
        assert_eq!(db.tip(), Some((activation, hash)));
        db.check_foundation_validation_coverage().unwrap();
        let cf = FoundationValidationCf::new(&db.db, FOUNDATION_VALIDATION).unwrap();
        assert_eq!(
            cf.zs_get(&()).unwrap().raw_bytes(),
            &rule_marker_fixture(&network, activation, hash, Some((3, 10)), activation)
        );
    }

    #[test]
    fn foundation_finalized_rejects_mismatched_activation_on_reopen() {
        let _init_guard = zebra_test::init();
        if !uses_wcash_consensus() {
            return;
        }
        let network = Network::try_new_wcash_mainnet().unwrap();
        assert_eq!(
            wcash_foundation_activation_height(&network),
            Some(Height(33_000))
        );
        let incorrect_activation = Height(40_001);
        let expiry = wcash_foundation_expiry_height(&network).unwrap();
        let hash = Hash([0x33; 32]);

        for height in [incorrect_activation, expiry] {
            let dir = tempfile::tempdir().unwrap();
            let config = Config {
                cache_dir: dir.path().to_path_buf(),
                ephemeral: false,
                delete_old_database: false,
                ..Config::default()
            };
            let db = open(&config, &network).unwrap();
            append_legacy_tip(&db, height, hash, false);
            let marker =
                rule_marker_fixture(&network, height, hash, Some((3, 10)), incorrect_activation);
            assert_ne!(Some(marker.clone()), coverage(&network, height, hash));
            FoundationValidationCf::new(&db.db, FOUNDATION_VALIDATION)
                .unwrap()
                .new_batch_for_writing()
                .zs_insert(&(), &RawBytes::new_raw_bytes(marker.clone()))
                .write_batch()
                .unwrap();
            assert_eq!(
                FoundationValidationCf::new(&db.db, FOUNDATION_VALIDATION)
                    .unwrap()
                    .zs_get(&())
                    .unwrap()
                    .raw_bytes(),
                &marker
            );
            let path = db.path().to_path_buf();
            drop(db);
            let error = open(&config, &network).unwrap_err();
            assert!(matches!(
                error,
                StateInitError::UncoveredFoundationHistory { .. }
            ));
            assert!(error.to_string().contains("NEW, empty state cache_dir"));
            assert!(path.is_dir());
        }
    }

    #[test]
    fn foundation_finalized_legacy_upgrade_and_downgrade_guard() {
        let _init_guard = zebra_test::init();
        if !uses_wcash_consensus() {
            return;
        }

        let network = network();
        let activation = wcash_foundation_activation_height(&network).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let config = Config {
            cache_dir: dir.path().to_path_buf(),
            ephemeral: false,
            delete_old_database: false,
            ..Config::default()
        };
        let db = open(&config, &network).unwrap();
        append_legacy_tip(&db, activation.previous().unwrap(), Hash([1; 32]), false);
        drop(db);

        // Upgrading before activation keeps the same persistent database and tip.
        let db = open(&config, &network).unwrap();
        assert_eq!(
            db.tip(),
            Some((activation.previous().unwrap(), Hash([1; 32])))
        );
        append_legacy_tip(&db, activation, Hash([2; 32]), false);
        let path = db.path().to_path_buf();
        drop(db);
        let error = open(&config, &network).unwrap_err();
        assert!(matches!(
            error,
            StateInitError::UncoveredFoundationHistory { .. }
        ));
        assert!(error.to_string().contains("NEW, empty state cache_dir"));
        assert!(
            path.is_dir(),
            "refusing unsafe history must preserve the old database"
        );

        // A separate covered database exercises new -> old -> new without repairing history.
        let covered_dir = tempfile::tempdir().unwrap();
        let config = Config {
            cache_dir: covered_dir.path().to_path_buf(),
            ..config
        };
        let db = open(&config, &network).unwrap();
        append_legacy_tip(&db, activation, Hash([2; 32]), true);
        drop(db);
        let db = open(&config, &network).unwrap();
        assert_eq!(db.tip(), Some((activation, Hash([2; 32]))));
        append_legacy_tip(&db, activation.next().unwrap(), Hash([3; 32]), false);
        drop(db);
        assert!(matches!(
            open(&config, &network),
            Err(StateInitError::UncoveredFoundationHistory { .. })
        ));
        assert!(path.is_dir());
    }

    #[test]
    fn foundation_guard_preserves_previous_major_directory_before_reuse() {
        let _init_guard = zebra_test::init();
        if !uses_wcash_consensus() {
            return;
        }
        let network = network();
        let activation = wcash_foundation_activation_height(&network).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let config = Config {
            cache_dir: dir.path().to_path_buf(),
            ephemeral: false,
            delete_old_database: false,
            ..Config::default()
        };
        let old_version = semver::Version::new(27, 0, 0);
        let db = DiskDb::new(
            &config,
            STATE_DATABASE_KIND,
            &old_version,
            &network,
            STATE_COLUMN_FAMILIES_IN_CODE
                .iter()
                .filter(|name| **name != FOUNDATION_VALIDATION)
                .map(ToString::to_string),
            false,
        )
        .unwrap();
        let cf = db.cf_handle("hash_by_height").unwrap();
        let mut batch = DiskWriteBatch::new();
        batch.zs_insert(&cf, activation, Hash([9; 32]));
        db.write(batch).unwrap();
        crate::write_database_format_version_to_disk(
            &config,
            STATE_DATABASE_KIND,
            27,
            &old_version,
            &network,
        )
        .unwrap();
        let old_path = db.path().to_path_buf();
        drop(db);
        assert!(matches!(
            open(&config, &network),
            Err(StateInitError::UncoveredFoundationHistory { .. })
        ));
        assert!(old_path.is_dir());
        assert!(!config.db_path(STATE_DATABASE_KIND, 28, &network).exists());
        assert_eq!(
            crate::database_format_version_on_disk(&config, STATE_DATABASE_KIND, 27, &network)
                .unwrap(),
            Some(old_version)
        );
    }

    #[test]
    fn foundation_finalized_marker_requires_exact_rule_tip_and_expired_history() {
        let _init_guard = zebra_test::init();
        if !uses_wcash_consensus() {
            return;
        }

        let network = network();
        let expiry =
            zebra_chain::parameters::subsidy::wcash_foundation_expiry_height(&network).unwrap();
        let db = open(&Config::ephemeral(), &network).unwrap();
        append_legacy_tip(&db, expiry, Hash([4; 32]), false);
        assert!(db.check_foundation_validation_coverage().is_err());
        append_legacy_tip(&db, expiry, Hash([4; 32]), true);
        db.check_foundation_validation_coverage().unwrap();
        append_legacy_tip(&db, expiry, Hash([5; 32]), false);
        assert!(db.check_foundation_validation_coverage().is_err());
        append_legacy_tip(&db, expiry, Hash([4; 32]), false);
        let cf = FoundationValidationCf::new(&db.db, FOUNDATION_VALIDATION).unwrap();
        let mut marker = cf.zs_get(&()).unwrap().raw_bytes().clone();
        marker[0] ^= 1;
        cf.new_batch_for_writing()
            .zs_insert(&(), &RawBytes::new_raw_bytes(marker))
            .write_batch()
            .unwrap();
        assert!(db.check_foundation_validation_coverage().is_err());
    }

    #[test]
    fn foundation_fixture_finalized_writes_survive_reopening() {
        let _init_guard = zebra_test::init();
        let network = Network::new_wcash_regtest();
        if !uses_wcash_consensus() || wcash_foundation_activation_height(&network).is_none() {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let config = Config {
            cache_dir: dir.path().to_path_buf(),
            ephemeral: false,
            delete_old_database: false,
            ..Config::default()
        };
        let children = wcash_fake_children(&network, 21);
        let mut state = FinalizedState::new_with_debug(
            &config,
            &network,
            true,
            #[cfg(feature = "elasticsearch")]
            false,
            false,
        )
        .unwrap();
        state
            .commit_finalized_direct(
                test_genesis(&network).into(),
                None,
                "foundation persistence test",
            )
            .unwrap();
        for child in children.iter().take(9) {
            state
                .commit_finalized_direct(child.clone().into(), None, "foundation persistence test")
                .unwrap();
        }
        drop(state);
        let mut state = FinalizedState::new_with_debug(
            &config,
            &network,
            true,
            #[cfg(feature = "elasticsearch")]
            false,
            false,
        )
        .unwrap();
        assert_eq!(state.db.tip(), Some((Height(9), children[8].hash())));
        for child in children.iter().skip(9) {
            state
                .commit_finalized_direct(child.clone().into(), None, "foundation persistence test")
                .unwrap();
        }
        drop(state);
        let db = open(&config, &network).unwrap();
        assert_eq!(db.tip(), Some((Height(21), children[20].hash())));
        db.check_foundation_validation_coverage().unwrap();
    }
}
