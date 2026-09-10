//! Connection-local request sequencing for the private pool backend.
//!
//! This state machine deliberately contains no socket or mining logic. A
//! transport validates one decoded request here before handing it to the
//! durable backend actor, so reconnects cannot reset journal state and a
//! connection cannot return to historical replay after entering live mode.

use thiserror::Error;
use wcash_pool_protocol::{BackendErrorCode, BackendRequest, ProtocolError};

/// Backend connection role established by the first post-hello request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BackendConnectionRole {
    /// The connection has not completed its hello exchange.
    AwaitingHello,
    /// Hello completed, but the connection has not selected a role.
    Negotiated,
    /// The connection subscribed to the atomic job snapshot and live events.
    Live,
    /// The connection reads durable historical events for projection recovery.
    Replay,
}

/// A validated request accepted for dispatch by the backend actor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackendRequestKind {
    /// Initial protocol and authority negotiation.
    Hello,
    /// Atomic job snapshot and live subscription.
    SubscribeJobs,
    /// Consensus validation and durable share commit.
    SubmitShare,
    /// Bounded historical event page.
    ReadEvents,
    /// Bounded health snapshot.
    Health,
}

/// Request-order failure that must close the local backend connection.
#[derive(Debug, Error)]
pub(crate) enum BackendConnectionStateError {
    /// A syntactically decoded request violated the shared protocol contract.
    #[error("invalid backend request")]
    InvalidProtocol(#[source] ProtocolError),
    /// Every connection must negotiate before issuing another operation.
    #[error("hello must be the first backend request")]
    HelloRequired,
    /// Negotiation occurs exactly once on a connection.
    #[error("hello was already completed on this backend connection")]
    DuplicateHello,
    /// Correlation identifiers are connection-local and strictly monotonic.
    #[error("backend request IDs must increase strictly")]
    NonIncreasingRequestId,
    /// A share cannot arrive before the edge has obtained an atomic snapshot.
    #[error("share submission requires a live job subscription")]
    SubscriptionRequired,
    /// A live connection cannot also serve historical projection replay.
    #[error("live backend connections cannot read historical event pages")]
    ReadEventsOnLiveConnection,
    /// A replay connection cannot submit miner work.
    #[error("replay backend connections cannot submit shares")]
    SubmitOnReplayConnection,
    /// Only one live subscription is permitted per connection.
    #[error("the backend connection is already subscribed to live jobs")]
    DuplicateSubscription,
}

impl BackendConnectionStateError {
    /// Returns the stable wire error category for this failure.
    pub(crate) const fn code(&self) -> BackendErrorCode {
        BackendErrorCode::InvalidRequest
    }
}

/// Strict connection-local ordering state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BackendConnectionState {
    role: BackendConnectionRole,
    last_request_id: u64,
}

impl Default for BackendConnectionState {
    fn default() -> Self {
        Self::new()
    }
}

impl BackendConnectionState {
    /// Creates a connection that accepts only a protocol hello.
    pub(crate) const fn new() -> Self {
        Self {
            role: BackendConnectionRole::AwaitingHello,
            last_request_id: 0,
        }
    }

    /// Returns the role already established for this connection.
    pub(crate) const fn role(&self) -> BackendConnectionRole {
        self.role
    }

    /// Returns the last accepted correlation identifier in tests.
    #[cfg(test)]
    const fn last_request_id(&self) -> u64 {
        self.last_request_id
    }

    /// Validates request syntax and sequencing, then advances this state.
    ///
    /// Failures leave the state unchanged. The transport must still close the
    /// connection after returning a bounded protocol error; retrying malformed
    /// input on the same byte stream is deliberately unsupported.
    pub(crate) fn accept(
        &mut self,
        request: &BackendRequest,
    ) -> Result<BackendRequestKind, BackendConnectionStateError> {
        request
            .validate()
            .map_err(BackendConnectionStateError::InvalidProtocol)?;

        let id = request.id();
        if id <= self.last_request_id {
            return Err(BackendConnectionStateError::NonIncreasingRequestId);
        }

        let (next_role, kind) = match (self.role, request) {
            (BackendConnectionRole::AwaitingHello, BackendRequest::Hello { .. }) => {
                (BackendConnectionRole::Negotiated, BackendRequestKind::Hello)
            }
            (BackendConnectionRole::AwaitingHello, _) => {
                return Err(BackendConnectionStateError::HelloRequired)
            }
            (_, BackendRequest::Hello { .. }) => {
                return Err(BackendConnectionStateError::DuplicateHello)
            }
            (BackendConnectionRole::Negotiated, BackendRequest::SubscribeJobs { .. }) => (
                BackendConnectionRole::Live,
                BackendRequestKind::SubscribeJobs,
            ),
            (BackendConnectionRole::Negotiated, BackendRequest::ReadEvents { .. }) => (
                BackendConnectionRole::Replay,
                BackendRequestKind::ReadEvents,
            ),
            (BackendConnectionRole::Negotiated, BackendRequest::Health { .. }) => (
                BackendConnectionRole::Negotiated,
                BackendRequestKind::Health,
            ),
            (BackendConnectionRole::Negotiated, BackendRequest::SubmitShare { .. }) => {
                return Err(BackendConnectionStateError::SubscriptionRequired)
            }
            (BackendConnectionRole::Live, BackendRequest::SubmitShare { .. }) => {
                (BackendConnectionRole::Live, BackendRequestKind::SubmitShare)
            }
            (BackendConnectionRole::Live, BackendRequest::Health { .. }) => {
                (BackendConnectionRole::Live, BackendRequestKind::Health)
            }
            (BackendConnectionRole::Live, BackendRequest::ReadEvents { .. }) => {
                return Err(BackendConnectionStateError::ReadEventsOnLiveConnection)
            }
            (BackendConnectionRole::Live, BackendRequest::SubscribeJobs { .. }) => {
                return Err(BackendConnectionStateError::DuplicateSubscription)
            }
            (BackendConnectionRole::Replay, BackendRequest::ReadEvents { .. }) => (
                BackendConnectionRole::Replay,
                BackendRequestKind::ReadEvents,
            ),
            (BackendConnectionRole::Replay, BackendRequest::Health { .. }) => {
                (BackendConnectionRole::Replay, BackendRequestKind::Health)
            }
            (BackendConnectionRole::Replay, BackendRequest::SubscribeJobs { .. }) => (
                BackendConnectionRole::Live,
                BackendRequestKind::SubscribeJobs,
            ),
            (BackendConnectionRole::Replay, BackendRequest::SubmitShare { .. }) => {
                return Err(BackendConnectionStateError::SubmitOnReplayConnection)
            }
        };

        self.role = next_role;
        self.last_request_id = id;
        Ok(kind)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use wcash_pool_protocol::{
        BackendRequest, CanonicalUuid, BACKEND_PROTOCOL_VERSION, MAX_EVENT_PAGE_ITEMS,
    };

    use super::{
        BackendConnectionRole, BackendConnectionState, BackendConnectionStateError,
        BackendRequestKind,
    };

    fn pool_instance() -> CanonicalUuid {
        serde_json::from_value(json!("2d8fdaf6-2499-4e16-9be4-69f79b0f24f5"))
            .expect("the test UUID uses canonical lowercase syntax")
    }

    fn hello(id: u64) -> BackendRequest {
        BackendRequest::Hello {
            version: BACKEND_PROTOCOL_VERSION,
            id,
            pool_instance: pool_instance(),
            last_event_seq: 0,
        }
    }

    fn subscribe(id: u64) -> BackendRequest {
        BackendRequest::SubscribeJobs {
            version: BACKEND_PROTOCOL_VERSION,
            id,
            after_event_seq: 0,
        }
    }

    fn read_events(id: u64) -> BackendRequest {
        BackendRequest::ReadEvents {
            version: BACKEND_PROTOCOL_VERSION,
            id,
            after_event_seq: 0,
            limit: MAX_EVENT_PAGE_ITEMS,
        }
    }

    fn health(id: u64) -> BackendRequest {
        BackendRequest::Health {
            version: BACKEND_PROTOCOL_VERSION,
            id,
        }
    }

    #[test]
    fn hello_is_first_and_exactly_once() {
        let mut state = BackendConnectionState::new();
        assert!(matches!(
            state.accept(&health(1)),
            Err(BackendConnectionStateError::HelloRequired)
        ));
        assert_eq!(state.role(), BackendConnectionRole::AwaitingHello);
        assert_eq!(state.last_request_id(), 0);

        assert_eq!(state.accept(&hello(1)).unwrap(), BackendRequestKind::Hello);
        assert_eq!(state.role(), BackendConnectionRole::Negotiated);
        assert!(matches!(
            state.accept(&hello(2)),
            Err(BackendConnectionStateError::DuplicateHello)
        ));
        assert_eq!(state.last_request_id(), 1);
    }

    #[test]
    fn request_ids_increase_without_advancing_on_failure() {
        let mut state = BackendConnectionState::new();
        state.accept(&hello(7)).unwrap();
        assert!(matches!(
            state.accept(&health(7)),
            Err(BackendConnectionStateError::NonIncreasingRequestId)
        ));
        assert!(matches!(
            state.accept(&health(6)),
            Err(BackendConnectionStateError::NonIncreasingRequestId)
        ));
        assert_eq!(state.last_request_id(), 7);
        assert_eq!(
            state.accept(&health(8)).unwrap(),
            BackendRequestKind::Health
        );
        assert_eq!(state.last_request_id(), 8);
    }

    #[test]
    fn subscription_selects_a_live_only_connection() {
        let mut state = BackendConnectionState::new();
        state.accept(&hello(1)).unwrap();
        assert_eq!(
            state.accept(&subscribe(2)).unwrap(),
            BackendRequestKind::SubscribeJobs
        );
        assert_eq!(state.role(), BackendConnectionRole::Live);
        assert_eq!(
            state.accept(&health(3)).unwrap(),
            BackendRequestKind::Health
        );
        assert!(matches!(
            state.accept(&read_events(4)),
            Err(BackendConnectionStateError::ReadEventsOnLiveConnection)
        ));
        assert!(matches!(
            state.accept(&subscribe(4)),
            Err(BackendConnectionStateError::DuplicateSubscription)
        ));
    }

    #[test]
    fn replay_must_complete_before_the_one_way_live_transition() {
        let mut state = BackendConnectionState::new();
        state.accept(&hello(1)).unwrap();
        assert_eq!(
            state.accept(&read_events(2)).unwrap(),
            BackendRequestKind::ReadEvents
        );
        assert_eq!(state.role(), BackendConnectionRole::Replay);
        assert_eq!(
            state.accept(&read_events(3)).unwrap(),
            BackendRequestKind::ReadEvents
        );
        assert_eq!(
            state.accept(&health(4)).unwrap(),
            BackendRequestKind::Health
        );
        assert_eq!(
            state.accept(&subscribe(5)).unwrap(),
            BackendRequestKind::SubscribeJobs
        );
        assert_eq!(state.role(), BackendConnectionRole::Live);
        assert!(matches!(
            state.accept(&read_events(6)),
            Err(BackendConnectionStateError::ReadEventsOnLiveConnection)
        ));
    }

    #[test]
    fn submit_requires_the_live_role() {
        let mut state = BackendConnectionState::new();
        state.accept(&hello(1)).unwrap();

        let request: BackendRequest = serde_json::from_value(json!({
            "type": "submit_share",
            "v": BACKEND_PROTOCOL_VERSION,
            "id": 2,
            "job_id": "01".repeat(32),
            "identity": {
                "account_id": "3074ff46-f289-46ca-b4c3-c9d452c575b6",
                "worker_id": "a4c10f5d-6da7-4674-86d5-72c68d5ecfe7",
                "label": "worker-1"
            },
            "target_le": "ff".repeat(32),
            "time": "01000000",
            "nonce": "00".repeat(32),
            "solution": "00".repeat(1344)
        }))
        .expect("the test share request is syntactically valid");

        assert!(matches!(
            state.accept(&request),
            Err(BackendConnectionStateError::SubscriptionRequired)
        ));
        let BackendRequest::SubmitShare {
            version,
            job_id,
            identity,
            target_le,
            time,
            nonce,
            solution,
            ..
        } = request
        else {
            unreachable!("the JSON above selects submit_share")
        };
        let request = BackendRequest::SubmitShare {
            version,
            id: 4,
            job_id,
            identity,
            target_le,
            time,
            nonce,
            solution,
        };
        state.accept(&subscribe(3)).unwrap();
        assert_eq!(
            state.accept(&request).unwrap(),
            BackendRequestKind::SubmitShare
        );
    }

    #[test]
    fn protocol_validation_precedes_state_changes() {
        let mut state = BackendConnectionState::new();
        let invalid = BackendRequest::Hello {
            version: BACKEND_PROTOCOL_VERSION + 1,
            id: 1,
            pool_instance: pool_instance(),
            last_event_seq: 0,
        };
        assert!(matches!(
            state.accept(&invalid),
            Err(BackendConnectionStateError::InvalidProtocol(_))
        ));
        assert_eq!(state, BackendConnectionState::new());
    }

    #[test]
    fn ordering_errors_map_to_a_stable_wire_code() {
        assert_eq!(
            BackendConnectionStateError::HelloRequired.code(),
            wcash_pool_protocol::BackendErrorCode::InvalidRequest
        );
    }
}
