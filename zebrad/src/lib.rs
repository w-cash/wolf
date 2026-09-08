//! # Wcash node
//!
//! Wcash is a privacy-focused proof-of-work cryptocurrency node written in Rust
//! and based on the Zebra codebase. Its mined coinbase value is created in the
//! Ironwood shielded pool, and its child blocks are authorized by Zcash-parent
//! Equihash AuxPoW.
//!
//! This source tree enables a public engineering `WcashTestnet` and an isolated
//! `WcashRegtest`. Testnet has a frozen identity and genesis for AuxPoW and pool
//! interoperability. Post-genesis transactions are V6-only and use an exact
//! Wcash-specific signature domain; inherited Zcash domains are rejected.
//! Mainnet remains disabled.
//!
//! Wcash retains Zebra's modular crates and much of its Zcash validation code.
//! The upstream architecture and crate names are preserved where changing them
//! would reduce reviewability or compatibility.
//!
//! ## Configuration
//!
//! The command below places the generated `wcash.toml` config file in the default preferences directory of Linux:
//!
//! ```console
//! zebrad generate -o ~/.config/wcash.toml
//! ```
//!
//! See [`config::ZebradConfig`] for other operating systems' default locations
//! and more information about configuring Wcash.
//!
//! ## Wcash feature flags
//!
//! The following [Cargo
//! features](https://doc.rust-lang.org/cargo/reference/features.html#command-line-feature-options)
//! are available at compile time:
//!
//! ### Metrics
//!
//! * configuring a `tracing.progress_bar`: shows key metrics in the terminal using progress bars,
//!   and automatically configures Wcash to send logs to a file.
//!   (The `progress-bar` feature is activated by default.)
//! * `prometheus`: export metrics to prometheus.
//!
//! Read the [metrics](https://zebra.zfnd.org/user/metrics.html) section of the book
//! for more details.
//!
//! ### Tracing
//!
//! Sending traces to different subscribers:
//! * configuring a `tracing.log_file`: appends traces to a file on disk.
//! * `journald`: send tracing spans and events to `systemd-journald`.
//! * `sentry`: send crash and panic events to sentry.io.
//! * `flamegraph`: generate a flamegraph of tracing spans.
//!
//! Changing the traces that are collected:
//! * `filter-reload`: dynamically reload tracing filters at runtime.
//! * `error-debug`: enable extra debugging in release builds.
//! * `tokio-console`: enable tokio's `console-subscriber` (needs [specific compiler flags])
//! * A set of features that [skip verbose tracing].
//!   The default features ignore `debug` and `trace` logs in release builds.
//!
//! Read the [tracing](https://zebra.zfnd.org/user/tracing.html) section of the book
//! for more details.
//!
//! [skip verbose tracing]: https://docs.rs/tracing/0.1.35/tracing/level_filters/index.html#compile-time-filters
//! [specific compiler flags]: https://zebra.zfnd.org/dev/tokio-console.html#setup
//!
//! ### Testing
//!
//! * `proptest-impl`: enable randomised test data generation.
//! * `lightwalletd-grpc-tests`: enable inherited JSON-RPC tests that query `lightwalletd` using gRPC.
//!
//! ### Experimental
//!
//! * `elasticsearch`: save block data into elasticsearch database. Read the [elasticsearch](https://zebra.zfnd.org/user/elasticsearch.html)
//!   section of the book for more details.
//! * `internal-miner`: enable the local Wcash development miner. It constructs a
//!   synthetic Zcash parent and is not a live dual-chain production pool.
//!
//! ## Zebra crates
//!
//! [The Zebra monorepo](https://github.com/ZcashFoundation/zebra) is a collection of the following
//! crates:
//!
//! - [tower-batch-control](https://docs.rs/tower-batch-control/latest/tower_batch_control/)
//! - [tower-fallback](https://docs.rs/tower-fallback/latest/tower_fallback/)
//! - [zebra-chain](https://docs.rs/zebra-chain/latest/zebra_chain/)
//! - [zebra-consensus](https://docs.rs/zebra-consensus/latest/zebra_consensus/)
//! - [zebra-network](https://docs.rs/zebra-network/latest/zebra_network/)
//! - [zebra-node-services](https://docs.rs/zebra-node-services/latest/zebra_node_services/)
//! - [zebra-rpc](https://docs.rs/zebra-rpc/latest/zebra_rpc/)
//! - [zebra-script](https://docs.rs/zebra-script/latest/zebra_script/)
//! - [zebra-state](https://docs.rs/zebra-state/latest/zebra_state/)
//! - [zebra-test](https://docs.rs/zebra-test/latest/zebra_test/)
//! - [zebra-utils](https://docs.rs/zebra-utils/latest/zebra_utils/)
//! - [zebrad](https://docs.rs/zebrad/latest/zebrad/)
//!
//! The links in the list above point to the documentation of the public APIs of the crates. For
//! the documentation of the internal APIs, follow <https://zebra.zfnd.org/internal> that lists
//! all Zebra crates as well in the left sidebar.

#![doc(html_favicon_url = "https://zfnd.org/wp-content/uploads/2022/03/zebra-favicon-128.png")]
#![doc(html_logo_url = "https://zfnd.org/wp-content/uploads/2022/03/zebra-icon.png")]
#![doc(html_root_url = "https://docs.rs/zebrad")]
// Tracing causes false positives on this lint:
// https://github.com/tokio-rs/tracing/issues/553
#![allow(clippy::cognitive_complexity)]

#[macro_use]
extern crate tracing;

/// Error type alias to make working with tower traits easier.
///
/// Note: the 'static lifetime bound means that the *type* cannot have any
/// non-'static lifetimes, (e.g., when a type contains a borrow and is
/// parameterized by 'a), *not* that the object itself has 'static lifetime.
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

pub mod application;
pub mod commands;
pub mod components;
pub mod config;
pub mod prelude;

#[cfg(feature = "sentry")]
pub(crate) mod sentry;
