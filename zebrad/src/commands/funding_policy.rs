//! Inspect the policy actually compiled into an executable, without node startup.

use abscissa_core::{Command, Runnable};
use clap::Parser;
use zebra_chain::parameters::{
    subsidy::{
        block_subsidy, wcash_foundation_activation_height, wcash_foundation_expiry_height,
        wcash_foundation_fund, WCASH_FOUNDATION_DENOMINATOR, WCASH_FOUNDATION_NUMERATOR,
        WCASH_FOUNDATION_TEST_FIXTURE_ENABLED,
    },
    Network,
};

/// Print compiled funding parameters for release and local-fixture verification.
#[derive(Command, Debug, Default, Parser)]
pub struct FundingPolicyCmd {}

fn policy_report() -> serde_json::Value {
    let fixture = WCASH_FOUNDATION_TEST_FIXTURE_ENABLED;
    if !cfg!(feature = "wcash-consensus") {
        return serde_json::json!({"consensus_profile": "zcash", "fixture": fixture});
    }
    let network = if fixture {
        Network::new_wcash_regtest()
    } else {
        Network::try_new_wcash_mainnet().expect("the built-in Mainnet anchor is frozen")
    };
    let start = wcash_foundation_activation_height(&network)
        .expect("the inspected Wcash profile has a fixed foundation term");
    let end = wcash_foundation_expiry_height(&network)
        .expect("the inspected Wcash profile has a fixed foundation expiry");
    let gross = block_subsidy(start, &network).expect("the fixed activation subsidy is valid");
    let allocation = wcash_foundation_fund(start, &network, gross)
        .expect("the fixed activation height is funded");
    serde_json::json!({
        "consensus_profile": "wcash",
        "fixture": fixture,
        "network": if network.is_wcash_regtest() { "WcashRegtest" } else { "WcashMainnet" },
        "start_height": start.0,
        "end_height_exclusive": end.0,
        "recipient": allocation.address.encode_wcash(&network)
            .expect("the fixed recipient belongs to the inspected network"),
        "script_pub_key": hex::encode(allocation.output().lock_script.as_raw_bytes()),
        "numerator": WCASH_FOUNDATION_NUMERATOR,
        "denominator": WCASH_FOUNDATION_DENOMINATOR,
    })
}

impl Runnable for FundingPolicyCmd {
    #[allow(clippy::print_stdout)]
    fn run(&self) {
        println!("{}", policy_report());
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn funding_policy_identifies_compiled_artifact() {
        let report = super::policy_report();
        assert_eq!(
            report["fixture"],
            zebra_chain::parameters::subsidy::WCASH_FOUNDATION_TEST_FIXTURE_ENABLED
        );
        if cfg!(feature = "wcash-consensus") {
            assert_eq!(report["consensus_profile"], "wcash");
            assert_eq!(report["numerator"], 3);
            assert_eq!(report["denominator"], 10);
            assert!(report.get("divisor").is_none());
            if zebra_chain::parameters::subsidy::WCASH_FOUNDATION_TEST_FIXTURE_ENABLED {
                assert_eq!(report["start_height"], 10);
                assert_eq!(report["end_height_exclusive"], 20);
                assert_eq!(report["recipient"], "WRHUq9CTLZFa52NmAyHH5usN21Q8jZVskN1");
            } else {
                assert_eq!(report["start_height"], 33_000);
                assert_eq!(report["end_height_exclusive"], 621_455);
                assert_eq!(report["recipient"], "W1ZRudbQKz7jQdKmAarbaJsegscnZzXjpu2");
                assert_eq!(
                    report["script_pub_key"],
                    "76a9148708e78a0203f93b2691057281e1773fd263702088ac"
                );
            }
        } else {
            assert_eq!(report["consensus_profile"], "zcash");
        }
    }
}
