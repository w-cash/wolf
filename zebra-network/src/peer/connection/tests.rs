//! Tests for peer connections

#![allow(clippy::unwrap_in_result)]

use std::{
    io,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    sync::Arc,
};

use chrono::Utc;
use futures::{channel::mpsc, sink::SinkMapErr, SinkExt};

use zebra_chain::{
    block::{CountedHeader, Height},
    serialization::{
        SerializationError, ZcashSerialize, MAX_HEADERS_PER_MESSAGE, MAX_PROTOCOL_MESSAGE_LEN,
    },
    work::equihash::Solution,
};
use zebra_test::mock_service::MockService;

use crate::{
    constants::CURRENT_NETWORK_PROTOCOL_VERSION,
    peer::{ClientRequest, ConnectedAddr, Connection, ConnectionInfo, ErrorSlot},
    peer_set::ActiveConnectionCounter,
    protocol::{
        external::{AddrInVersion, Message},
        types::{Nonce, PeerServices},
    },
    Request, Response, VersionMessage,
};

mod prop;
mod vectors;

#[test]
fn large_wcash_headers_are_limited_by_exact_wire_size_and_count() {
    let mut header = zebra_chain::block::genesis::wcash_regtest_genesis_block()
        .header
        .as_ref()
        .clone();
    header.solution = Solution::for_wcash(vec![0x5a; 128 * 1024])
        .expect("the large fixture is below the Wcash witness limit");
    let large_header = CountedHeader {
        header: Arc::new(header),
    };

    let oversized = vec![large_header.clone(); MAX_HEADERS_PER_MESSAGE];
    assert!(oversized.zcash_serialized_size() > MAX_PROTOCOL_MESSAGE_LEN);

    let limited = super::headers_within_message_limits(oversized);
    assert_eq!(limited.len(), 15, "the sixteenth large header does not fit");
    assert!(limited.zcash_serialized_size() <= MAX_PROTOCOL_MESSAGE_LEN);

    let mut one_too_many = limited.clone();
    one_too_many.push(large_header.clone());
    assert!(one_too_many.zcash_serialized_size() > MAX_PROTOCOL_MESSAGE_LEN);

    let count_limited = super::headers_within_message_limits(vec![
        CountedHeader {
            header: zebra_chain::block::genesis::wcash_regtest_genesis_block()
                .header
                .clone(),
        };
        MAX_HEADERS_PER_MESSAGE + 1
    ]);
    assert_eq!(count_limited.len(), MAX_HEADERS_PER_MESSAGE);
    assert!(count_limited.zcash_serialized_size() <= MAX_PROTOCOL_MESSAGE_LEN);
}

/// Creates a new [`Connection`] instance for testing.
fn new_test_connection<A>() -> (
    Connection<
        MockService<Request, Response, A>,
        SinkMapErr<mpsc::Sender<Message>, fn(mpsc::SendError) -> SerializationError>,
    >,
    mpsc::Sender<ClientRequest>,
    MockService<Request, Response, A>,
    mpsc::Receiver<Message>,
    ErrorSlot,
) {
    let mock_inbound_service = MockService::build().finish();
    let (client_tx, client_rx) = mpsc::channel(0);
    let shared_error_slot = ErrorSlot::default();

    // Normally the network has more capacity than the sender's single implicit slot,
    // but the smaller capacity makes some tests easier.
    let (peer_tx, peer_rx) = mpsc::channel(0);

    let error_converter: fn(mpsc::SendError) -> SerializationError = |_| {
        io::Error::new(
            io::ErrorKind::BrokenPipe,
            "peer outbound message stream was closed",
        )
        .into()
    };
    let peer_tx = peer_tx.sink_map_err(error_converter);

    let fake_addr: SocketAddr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 4).into();
    let fake_version = CURRENT_NETWORK_PROTOCOL_VERSION;
    let fake_services = PeerServices::default();

    let remote = VersionMessage {
        version: fake_version,
        services: fake_services,
        timestamp: Utc::now(),
        address_recv: AddrInVersion::new(fake_addr, fake_services),
        address_from: AddrInVersion::new(fake_addr, fake_services),
        nonce: Nonce::default(),
        user_agent: "connection test".to_string(),
        start_height: Height(0),
        relay: true,
    };

    let connection_info = ConnectionInfo {
        connected_addr: ConnectedAddr::Isolated,
        remote,
        negotiated_version: fake_version,
    };

    let connection = Connection::new(
        mock_inbound_service.clone(),
        client_rx,
        shared_error_slot.clone(),
        peer_tx,
        ActiveConnectionCounter::new_counter().track_connection(),
        Arc::new(connection_info),
        Vec::new(),
    );

    (
        connection,
        client_tx,
        mock_inbound_service,
        peer_rx,
        shared_error_slot,
    )
}
