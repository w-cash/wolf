//! Permission-restricted Unix listener for the private pool backend.

use std::{
    fs::{self, File},
    io,
    os::unix::{
        ffi::OsStrExt,
        fs::{FileTypeExt, MetadataExt, PermissionsExt},
        net::{UnixListener, UnixStream},
    },
    path::{Component, Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use thiserror::Error;
use uuid::Uuid;
use wcash_pool_protocol::{
    canonical_attribution_id, canonical_share_id, BackendErrorCode, BackendEvent, BackendMessage,
    BackendRequest, CanonicalUuid, BACKEND_PROTOCOL_VERSION, MAX_EVENT_PAGE_ITEMS,
};

use crate::{
    pool_backend_connection::{BackendConnectionRole, BackendConnectionState, BackendRequestKind},
    pool_backend_transport::{
        read_unix_backend_request, write_unix_backend_messages, BackendTransportError,
    },
};

const MAX_SOCKET_PATH_BYTES: usize = 100;
const DEFAULT_MAXIMUM_CONNECTIONS: usize = 16;
const MAXIMUM_CONNECTIONS: usize = 64;
const DEFAULT_HELLO_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_FRAME_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(90);
const DEFAULT_WRITE_TIMEOUT: Duration = Duration::from_secs(5);
const MAXIMUM_TIMEOUT: Duration = Duration::from_secs(300);
const MAXIMUM_RESPONSE_MESSAGES: usize = MAX_EVENT_PAGE_ITEMS as usize + 1;

/// Validated listener policy for one private pool-backend socket.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PoolBackendListenerConfig {
    socket_path: PathBuf,
    expected_peer_uid: u32,
    expected_socket_gid: u32,
    maximum_connections: usize,
    hello_timeout: Duration,
    frame_timeout: Duration,
    idle_timeout: Duration,
    write_timeout: Duration,
}

impl PoolBackendListenerConfig {
    /// Creates a conservative local-socket policy.
    pub fn new(
        socket_path: impl Into<PathBuf>,
        expected_peer_uid: u32,
        expected_socket_gid: u32,
    ) -> Result<Self, PoolBackendListenerError> {
        let config = Self {
            socket_path: socket_path.into(),
            expected_peer_uid,
            expected_socket_gid,
            maximum_connections: DEFAULT_MAXIMUM_CONNECTIONS,
            hello_timeout: DEFAULT_HELLO_TIMEOUT,
            frame_timeout: DEFAULT_FRAME_TIMEOUT,
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
            write_timeout: DEFAULT_WRITE_TIMEOUT,
        };
        config.validate()?;
        Ok(config)
    }

    /// Sets the simultaneous connection cap.
    pub fn with_maximum_connections(
        mut self,
        maximum_connections: usize,
    ) -> Result<Self, PoolBackendListenerError> {
        self.maximum_connections = maximum_connections;
        self.validate()?;
        Ok(self)
    }

    /// Sets complete hello, frame, idle, and write deadlines.
    pub fn with_timeouts(
        mut self,
        hello_timeout: Duration,
        frame_timeout: Duration,
        idle_timeout: Duration,
        write_timeout: Duration,
    ) -> Result<Self, PoolBackendListenerError> {
        self.hello_timeout = hello_timeout;
        self.frame_timeout = frame_timeout;
        self.idle_timeout = idle_timeout;
        self.write_timeout = write_timeout;
        self.validate()?;
        Ok(self)
    }

    fn validate(&self) -> Result<(), PoolBackendListenerError> {
        validate_socket_path(&self.socket_path)?;
        if !(1..=MAXIMUM_CONNECTIONS).contains(&self.maximum_connections) {
            return Err(PoolBackendListenerError::InvalidConfiguration(
                "maximum connections must be in 1..=64",
            ));
        }
        for (name, timeout) in [
            ("hello timeout", self.hello_timeout),
            ("frame timeout", self.frame_timeout),
            ("idle timeout", self.idle_timeout),
            ("write timeout", self.write_timeout),
        ] {
            if timeout.is_zero() || timeout > MAXIMUM_TIMEOUT {
                return Err(PoolBackendListenerError::InvalidTimeout { name });
            }
        }
        Ok(())
    }
}

/// Stable identities and peer facts for one authorized connection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PoolBackendSession {
    backend_session: CanonicalUuid,
    peer_uid: u32,
}

/// Immutable chain, payout, and journal authority exposed by one backend.
///
/// Private fields prevent a listener implementation from accidentally
/// validating only a subset of the authority advertised in `HelloOk`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PoolBackendAuthority {
    backend_instance: CanonicalUuid,
    journal_stream: CanonicalUuid,
    wcash_genesis: wcash_pool_protocol::Hex32,
    zcash_genesis: wcash_pool_protocol::Hex32,
    wcash_payout_commitment: wcash_pool_protocol::Hex32,
    zcash_payout_commitment: wcash_pool_protocol::Hex32,
    chain_id: u32,
}

impl PoolBackendAuthority {
    /// Creates one complete authority after checking every nonzero identity.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        backend_instance: CanonicalUuid,
        journal_stream: CanonicalUuid,
        wcash_genesis: wcash_pool_protocol::Hex32,
        zcash_genesis: wcash_pool_protocol::Hex32,
        wcash_payout_commitment: wcash_pool_protocol::Hex32,
        zcash_payout_commitment: wcash_pool_protocol::Hex32,
        chain_id: u32,
    ) -> Result<Self, PoolBackendListenerError> {
        if backend_instance.is_nil()
            || journal_stream.is_nil()
            || backend_instance == journal_stream
            || wcash_genesis.is_zero()
            || zcash_genesis.is_zero()
            || wcash_payout_commitment.is_zero()
            || zcash_payout_commitment.is_zero()
            || chain_id == 0
        {
            return Err(PoolBackendListenerError::InvalidConfiguration(
                "backend authority identities, chains, and payout commitments must be nonzero and distinct where required",
            ));
        }
        Ok(Self {
            backend_instance,
            journal_stream,
            wcash_genesis,
            zcash_genesis,
            wcash_payout_commitment,
            zcash_payout_commitment,
            chain_id,
        })
    }

    /// Returns the stable backend installation identity.
    pub const fn backend_instance(&self) -> CanonicalUuid {
        self.backend_instance
    }

    /// Returns the stable journal sequence namespace.
    pub const fn journal_stream(&self) -> CanonicalUuid {
        self.journal_stream
    }
}

impl PoolBackendSession {
    /// Returns the fresh identity of this connection.
    pub const fn backend_session(&self) -> CanonicalUuid {
        self.backend_session
    }

    /// Returns the operating-system-authenticated pool UID.
    pub const fn peer_uid(&self) -> u32 {
        self.peer_uid
    }
}

/// Redacted request failure returned by the durable backend actor.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("pool backend request failed with {code:?}")]
pub struct PoolBackendHandlerError {
    code: BackendErrorCode,
    message: &'static str,
    fatal: bool,
}

impl PoolBackendHandlerError {
    /// Creates a stable, secret-free wire failure.
    pub const fn new(code: BackendErrorCode, message: &'static str, fatal: bool) -> Self {
        Self {
            code,
            message,
            fatal,
        }
    }
}

/// Synchronous durable request actor used by the private listener.
///
/// Returned messages must contain zero or more live `Event` messages followed
/// by exactly one correlated response. The listener validates that ordering,
/// which makes the response flush barrier explicit at the socket boundary.
pub trait PoolBackendRequestHandler: Send + Sync {
    /// Returns the immutable authority that every Hello response must match.
    fn persistent_authority(&self) -> PoolBackendAuthority;

    /// Handles one already-authorized and correctly sequenced request.
    fn handle(
        &self,
        session: &PoolBackendSession,
        kind: BackendRequestKind,
        request: BackendRequest,
    ) -> Result<Vec<BackendMessage>, PoolBackendHandlerError>;
}

/// Outcome of accepting and fully serving one local connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PoolBackendConnectionOutcome {
    /// An authorized connection closed cleanly or after a bounded protocol error.
    Served,
    /// The peer UID did not match the configured pool service account.
    Unauthorized,
    /// The connection cap was reached before any frame buffer was allocated.
    Overloaded,
}

/// Listener setup or connection failure.
#[derive(Debug, Error)]
pub enum PoolBackendListenerError {
    /// A static configuration invariant was violated.
    #[error("invalid pool backend listener configuration: {0}")]
    InvalidConfiguration(&'static str),
    /// A configured timeout was zero or unreasonably large.
    #[error("invalid pool backend {name}; expected a duration in 1ns..=300s")]
    InvalidTimeout {
        /// Name of the invalid timeout.
        name: &'static str,
    },
    /// A filesystem object or permission did not satisfy local trust policy.
    #[error("unsafe pool backend socket path: {0}")]
    UnsafePath(String),
    /// Socket setup or accept failed.
    #[error("pool backend listener I/O failed")]
    Io(#[source] io::Error),
    /// Safe operating-system peer credential lookup failed.
    #[error("could not authenticate pool backend peer credentials")]
    PeerCredentials(#[source] nix::Error),
    /// The connection violated framing or a finite transport deadline.
    #[error("pool backend connection transport failed")]
    Transport(#[from] BackendTransportError),
    /// The handler returned invalid ordering or an invalid message.
    #[error("pool backend handler violated its response contract")]
    InvalidHandlerResponse,
    /// A fresh session UUID could not be represented safely.
    #[error("could not create a pool backend session identity")]
    SessionIdentity,
}

/// Bound private Unix listener with inode-safe cleanup.
pub struct PoolBackendListener {
    listener: UnixListener,
    config: PoolBackendListenerConfig,
    socket_device: u64,
    socket_inode: u64,
    active_connections: Arc<AtomicUsize>,
}

impl PoolBackendListener {
    /// Binds a new socket without replacing any existing filesystem object.
    pub fn bind(config: PoolBackendListenerConfig) -> Result<Self, PoolBackendListenerError> {
        config.validate()?;
        validate_runtime_directory(&config.socket_path)?;
        reject_existing_socket_path(&config.socket_path)?;

        let listener =
            UnixListener::bind(&config.socket_path).map_err(PoolBackendListenerError::Io)?;
        let initial_metadata =
            fs::symlink_metadata(&config.socket_path).map_err(PoolBackendListenerError::Io)?;
        if !initial_metadata.file_type().is_socket() {
            return Err(PoolBackendListenerError::UnsafePath(
                "bind did not create a filesystem socket".to_string(),
            ));
        }
        let mut cleanup = BoundSocketGuard::new(
            config.socket_path.clone(),
            initial_metadata.dev(),
            initial_metadata.ino(),
        );
        fs::set_permissions(&config.socket_path, fs::Permissions::from_mode(0o660))
            .map_err(PoolBackendListenerError::Io)?;
        let metadata = validate_bound_socket(&config)?;
        if metadata.dev() != initial_metadata.dev() || metadata.ino() != initial_metadata.ino() {
            return Err(PoolBackendListenerError::UnsafePath(
                "socket path changed while the listener was being bound".to_string(),
            ));
        }
        sync_parent_directory(&config.socket_path)?;
        cleanup.disarm();

        Ok(Self {
            listener,
            config,
            socket_device: metadata.dev(),
            socket_inode: metadata.ino(),
            active_connections: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Returns the bound filesystem path.
    pub fn socket_path(&self) -> &Path {
        &self.config.socket_path
    }

    /// Accepts and serves exactly one connection on the current thread.
    ///
    /// Callers may invoke this concurrently through an `Arc`; the listener's
    /// atomic cap is acquired before request framing allocates a payload.
    pub fn serve_one(
        &self,
        handler: &dyn PoolBackendRequestHandler,
    ) -> Result<PoolBackendConnectionOutcome, PoolBackendListenerError> {
        let (mut stream, _) = self
            .listener
            .accept()
            .map_err(PoolBackendListenerError::Io)?;
        let peer_uid = peer_uid(&stream)?;
        if peer_uid != self.config.expected_peer_uid {
            return Ok(PoolBackendConnectionOutcome::Unauthorized);
        }
        let Some(_permit) = ConnectionPermit::acquire(
            Arc::clone(&self.active_connections),
            self.config.maximum_connections,
        ) else {
            return Ok(PoolBackendConnectionOutcome::Overloaded);
        };

        let authority = handler.persistent_authority();
        let session = PoolBackendSession {
            backend_session: fresh_session_uuid(
                authority.backend_instance(),
                authority.journal_stream(),
            )?,
            peer_uid,
        };
        serve_authorized_connection(&mut stream, &self.config, handler, &session, &authority)?;
        Ok(PoolBackendConnectionOutcome::Served)
    }
}

struct BoundSocketGuard {
    path: PathBuf,
    device: u64,
    inode: u64,
    armed: bool,
}

impl BoundSocketGuard {
    fn new(path: PathBuf, device: u64, inode: u64) -> Self {
        Self {
            path,
            device,
            inode,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for BoundSocketGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let Ok(metadata) = fs::symlink_metadata(&self.path) else {
            return;
        };
        if metadata.file_type().is_socket()
            && metadata.dev() == self.device
            && metadata.ino() == self.inode
            && fs::remove_file(&self.path).is_ok()
        {
            let _ = sync_parent_directory(&self.path);
        }
    }
}

impl Drop for PoolBackendListener {
    fn drop(&mut self) {
        let Ok(metadata) = fs::symlink_metadata(&self.config.socket_path) else {
            return;
        };
        if metadata.file_type().is_socket()
            && metadata.dev() == self.socket_device
            && metadata.ino() == self.socket_inode
            && fs::remove_file(&self.config.socket_path).is_ok()
        {
            let _ = sync_parent_directory(&self.config.socket_path);
        }
    }
}

struct ConnectionPermit {
    active_connections: Arc<AtomicUsize>,
}

impl ConnectionPermit {
    fn acquire(active_connections: Arc<AtomicUsize>, maximum: usize) -> Option<Self> {
        active_connections
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                (active < maximum).then(|| active + 1)
            })
            .ok()?;
        Some(Self { active_connections })
    }
}

impl Drop for ConnectionPermit {
    fn drop(&mut self) {
        let previous = self.active_connections.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "a connection permit increments before drop");
    }
}

fn serve_authorized_connection(
    stream: &mut UnixStream,
    config: &PoolBackendListenerConfig,
    handler: &dyn PoolBackendRequestHandler,
    session: &PoolBackendSession,
    authority: &PoolBackendAuthority,
) -> Result<(), PoolBackendListenerError> {
    let mut state = BackendConnectionState::new();
    let mut live_event_cursor = None;
    loop {
        let idle_timeout = if state.role() == BackendConnectionRole::AwaitingHello {
            config.hello_timeout
        } else {
            config.idle_timeout
        };
        let request = match read_unix_backend_request(stream, idle_timeout, config.frame_timeout)? {
            Some(request) => request,
            None => return Ok(()),
        };
        let request_id = request.id();
        let request_contract = request.clone();
        let kind = match state.accept(&request) {
            Ok(kind) => kind,
            Err(error) => {
                write_error(
                    stream,
                    config.write_timeout,
                    request_id,
                    error.code(),
                    error.to_string(),
                )?;
                return Ok(());
            }
        };

        match handler.handle(session, kind, request) {
            Ok(messages) => {
                let next_live_event_cursor = validate_handler_messages(
                    kind,
                    state.role(),
                    &request_contract,
                    session,
                    authority,
                    live_event_cursor,
                    &messages,
                )?;
                write_unix_backend_messages(stream, &messages, config.write_timeout)?;
                live_event_cursor = next_live_event_cursor;
            }
            Err(error) => {
                write_error(
                    stream,
                    config.write_timeout,
                    request_id,
                    error.code,
                    error.message.to_string(),
                )?;
                if error.fatal
                    || matches!(
                        kind,
                        BackendRequestKind::Hello | BackendRequestKind::SubscribeJobs
                    )
                {
                    return Ok(());
                }
            }
        }
    }
}

fn validate_handler_messages(
    kind: BackendRequestKind,
    role: BackendConnectionRole,
    request: &BackendRequest,
    session: &PoolBackendSession,
    authority: &PoolBackendAuthority,
    previous_live_event_cursor: Option<u64>,
    messages: &[BackendMessage],
) -> Result<Option<u64>, PoolBackendListenerError> {
    if messages.len() > MAXIMUM_RESPONSE_MESSAGES {
        return Err(PoolBackendListenerError::InvalidHandlerResponse);
    }
    let Some((response, events)) = messages.split_last() else {
        return Err(PoolBackendListenerError::InvalidHandlerResponse);
    };
    let request_id = request.id();
    if events
        .iter()
        .any(|message| !matches!(message, BackendMessage::Event { .. }))
        || events.iter().any(|message| message.validate().is_err())
        || response.validate().is_err()
        || response.correlation_id() != Some(request_id)
    {
        return Err(PoolBackendListenerError::InvalidHandlerResponse);
    }
    if matches!(
        kind,
        BackendRequestKind::Hello
            | BackendRequestKind::SubscribeJobs
            | BackendRequestKind::ReadEvents
    ) && !events.is_empty()
    {
        return Err(PoolBackendListenerError::InvalidHandlerResponse);
    }
    if role != BackendConnectionRole::Live && !events.is_empty() {
        return Err(PoolBackendListenerError::InvalidHandlerResponse);
    }

    let mut delivered_live_cursor = previous_live_event_cursor;
    for message in events {
        let BackendMessage::Event { event, .. } = message else {
            return Err(PoolBackendListenerError::InvalidHandlerResponse);
        };
        let previous =
            delivered_live_cursor.ok_or(PoolBackendListenerError::InvalidHandlerResponse)?;
        let expected = previous
            .checked_add(1)
            .ok_or(PoolBackendListenerError::InvalidHandlerResponse)?;
        if event.event_seq() != expected {
            return Err(PoolBackendListenerError::InvalidHandlerResponse);
        }
        delivered_live_cursor = Some(expected);
    }

    match (kind, request, response) {
        (
            BackendRequestKind::Hello,
            BackendRequest::Hello { last_event_seq, .. },
            BackendMessage::HelloOk {
                backend_session,
                backend_instance,
                journal_stream,
                wcash_genesis,
                zcash_genesis,
                wcash_payout_commitment,
                zcash_payout_commitment,
                chain_id,
                current_event_seq,
                ..
            },
        ) if *backend_session == session.backend_session
            && *backend_instance == authority.backend_instance
            && *journal_stream == authority.journal_stream
            && *wcash_genesis == authority.wcash_genesis
            && *zcash_genesis == authority.zcash_genesis
            && *wcash_payout_commitment == authority.wcash_payout_commitment
            && *zcash_payout_commitment == authority.zcash_payout_commitment
            && *chain_id == authority.chain_id
            && *current_event_seq >= *last_event_seq =>
        {
            Ok(None)
        }
        (
            BackendRequestKind::SubscribeJobs,
            BackendRequest::SubscribeJobs {
                after_event_seq, ..
            },
            BackendMessage::JobSnapshot { event_seq, .. },
        ) if *event_seq >= *after_event_seq => Ok(Some(*event_seq)),
        (
            BackendRequestKind::ReadEvents,
            BackendRequest::ReadEvents {
                after_event_seq,
                limit,
                ..
            },
            BackendMessage::EventsPage {
                after_event_seq: response_after,
                events: response_events,
                ..
            },
        ) if response_after == after_event_seq && response_events.len() <= usize::from(*limit) => {
            Ok(previous_live_event_cursor)
        }
        (
            BackendRequestKind::Health,
            BackendRequest::Health { .. },
            BackendMessage::HealthStatus { .. },
        ) if role != BackendConnectionRole::Live && events.is_empty() => {
            Ok(previous_live_event_cursor)
        }
        (
            BackendRequestKind::Health,
            BackendRequest::Health { .. },
            BackendMessage::HealthStatus { event_seq, .. },
        ) if delivered_live_cursor == Some(*event_seq) => Ok(delivered_live_cursor),
        (
            BackendRequestKind::SubmitShare,
            BackendRequest::SubmitShare {
                job_id,
                identity,
                target_le,
                time,
                nonce,
                solution,
                ..
            },
            BackendMessage::ShareCommitted {
                receipt, replayed, ..
            },
        ) => {
            let expected_attribution = canonical_attribution_id(identity, target_le)
                .map_err(|_| PoolBackendListenerError::InvalidHandlerResponse)?;
            if receipt.job_id != *job_id
                || receipt.share_id != canonical_share_id(job_id, time, nonce, solution)
                || receipt.attribution_id != expected_attribution
            {
                return Err(PoolBackendListenerError::InvalidHandlerResponse);
            }

            let matching_commit_event = events.iter().any(|message| {
                matches!(
                    message,
                    BackendMessage::Event {
                        event: BackendEvent::ShareCommitted {
                            receipt: event_receipt,
                            job_id: event_job_id,
                            identity: event_identity,
                            target_le: event_target,
                        },
                        ..
                    } if event_receipt == receipt
                        && event_job_id == job_id
                        && event_identity == identity
                        && event_target == target_le
                )
            });
            let previous = previous_live_event_cursor
                .ok_or(PoolBackendListenerError::InvalidHandlerResponse)?;
            if (!*replayed || receipt.event_seq > previous)
                && (!matching_commit_event || receipt.event_seq <= previous)
            {
                return Err(PoolBackendListenerError::InvalidHandlerResponse);
            }
            Ok(delivered_live_cursor)
        }
        _ => Err(PoolBackendListenerError::InvalidHandlerResponse),
    }
}

fn write_error(
    stream: &mut UnixStream,
    timeout: Duration,
    id: u64,
    code: BackendErrorCode,
    message: String,
) -> Result<(), PoolBackendListenerError> {
    let bounded = if message.is_empty() || message.len() > 512 || !message.is_ascii() {
        "invalid backend request".to_string()
    } else {
        message
    };
    let response = BackendMessage::Error {
        version: BACKEND_PROTOCOL_VERSION,
        id,
        code,
        message: bounded,
    };
    write_unix_backend_messages(stream, std::slice::from_ref(&response), timeout)
        .map_err(Into::into)
}

fn fresh_session_uuid(
    backend_instance: CanonicalUuid,
    journal_stream: CanonicalUuid,
) -> Result<CanonicalUuid, PoolBackendListenerError> {
    for _ in 0..8 {
        let session = CanonicalUuid::new(Uuid::new_v4());
        if session != backend_instance && session != journal_stream {
            return Ok(session);
        }
    }
    Err(PoolBackendListenerError::SessionIdentity)
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn peer_uid(stream: &UnixStream) -> Result<u32, PoolBackendListenerError> {
    let credentials =
        nix::sys::socket::getsockopt(stream, nix::sys::socket::sockopt::PeerCredentials)
            .map_err(PoolBackendListenerError::PeerCredentials)?;
    Ok(credentials.uid())
}

#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd",
    target_os = "dragonfly"
))]
fn peer_uid(stream: &UnixStream) -> Result<u32, PoolBackendListenerError> {
    let (uid, _) =
        nix::unistd::getpeereid(stream).map_err(PoolBackendListenerError::PeerCredentials)?;
    Ok(uid.as_raw())
}

#[cfg(not(any(
    target_os = "linux",
    target_os = "android",
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd",
    target_os = "dragonfly"
)))]
fn peer_uid(_stream: &UnixStream) -> Result<u32, PoolBackendListenerError> {
    Err(PoolBackendListenerError::InvalidConfiguration(
        "this Unix target has no supported peer-credential API",
    ))
}

fn validate_socket_path(path: &Path) -> Result<(), PoolBackendListenerError> {
    if !path.is_absolute() {
        return Err(PoolBackendListenerError::UnsafePath(
            "socket path must be absolute".to_string(),
        ));
    }
    if path
        .components()
        .any(|component| !matches!(component, Component::RootDir | Component::Normal(_)))
    {
        return Err(PoolBackendListenerError::UnsafePath(
            "socket path must not contain '.' or '..' components".to_string(),
        ));
    }
    let bytes = path.as_os_str().as_bytes();
    if bytes.contains(&0) {
        return Err(PoolBackendListenerError::UnsafePath(
            "socket path contains a NUL byte".to_string(),
        ));
    }
    if bytes.len() > MAX_SOCKET_PATH_BYTES {
        return Err(PoolBackendListenerError::UnsafePath(format!(
            "socket path is {} bytes; maximum is {MAX_SOCKET_PATH_BYTES}",
            bytes.len()
        )));
    }
    if path.file_name().is_none() {
        return Err(PoolBackendListenerError::UnsafePath(
            "socket path has no file name".to_string(),
        ));
    }
    Ok(())
}

fn validate_runtime_directory(path: &Path) -> Result<(), PoolBackendListenerError> {
    let parent = path.parent().ok_or_else(|| {
        PoolBackendListenerError::UnsafePath("socket path has no parent directory".to_string())
    })?;
    let metadata = fs::symlink_metadata(parent).map_err(PoolBackendListenerError::Io)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(PoolBackendListenerError::UnsafePath(
            "socket parent must be a real directory".to_string(),
        ));
    }
    let canonical_parent = fs::canonicalize(parent).map_err(PoolBackendListenerError::Io)?;
    if canonical_parent.as_os_str() != parent.as_os_str() {
        return Err(PoolBackendListenerError::UnsafePath(
            "socket parent must be canonical and contain no symbolic-link ancestors".to_string(),
        ));
    }
    if metadata.uid() != nix::unistd::geteuid().as_raw() {
        return Err(PoolBackendListenerError::UnsafePath(
            "socket parent must be owned by the backend UID".to_string(),
        ));
    }
    if metadata.permissions().mode() & 0o022 != 0 {
        return Err(PoolBackendListenerError::UnsafePath(
            "socket parent must not be writable by group or other users".to_string(),
        ));
    }
    Ok(())
}

fn reject_existing_socket_path(path: &Path) -> Result<(), PoolBackendListenerError> {
    match fs::symlink_metadata(path) {
        Ok(_) => Err(PoolBackendListenerError::UnsafePath(
            "socket path already exists; remove stale state explicitly".to_string(),
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(PoolBackendListenerError::Io(error)),
    }
}

fn validate_bound_socket(
    config: &PoolBackendListenerConfig,
) -> Result<fs::Metadata, PoolBackendListenerError> {
    let metadata =
        fs::symlink_metadata(&config.socket_path).map_err(PoolBackendListenerError::Io)?;
    if !metadata.file_type().is_socket()
        || metadata.uid() != nix::unistd::geteuid().as_raw()
        || metadata.gid() != config.expected_socket_gid
        || metadata.permissions().mode() & 0o777 != 0o660
    {
        return Err(PoolBackendListenerError::UnsafePath(
            "bound socket type, ownership, group, or mode is not the configured policy".to_string(),
        ));
    }
    Ok(metadata)
}

fn sync_parent_directory(path: &Path) -> Result<(), PoolBackendListenerError> {
    let parent = path.parent().ok_or_else(|| {
        PoolBackendListenerError::UnsafePath("socket path has no parent directory".to_string())
    })?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(PoolBackendListenerError::Io)
}

#[cfg(test)]
mod tests {
    use std::{io::Write, os::unix::net::UnixStream, sync::Mutex, thread};

    use serde_json::json;
    use wcash_pool_protocol::{
        decode_backend_message, encode_backend_request, Hex1344, Hex32, Hex4, ShareReceipt,
        TargetLe, WorkerIdentity, REQUIRED_BACKEND_CAPABILITIES,
    };

    use super::*;

    fn canonical_uuid(value: &str) -> CanonicalUuid {
        serde_json::from_value(json!(value)).expect("canonical test UUID")
    }

    fn private_directory() -> tempfile::TempDir {
        let directory = tempfile::tempdir().expect("temporary directory");
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700))
            .expect("private test directory");
        directory
    }

    fn socket_path(directory: &tempfile::TempDir) -> PathBuf {
        fs::canonicalize(directory.path())
            .expect("temporary directory has a canonical path")
            .join("backend.sock")
    }

    fn config(path: PathBuf) -> PoolBackendListenerConfig {
        PoolBackendListenerConfig::new(
            path,
            nix::unistd::geteuid().as_raw(),
            nix::unistd::getegid().as_raw(),
        )
        .unwrap()
        .with_timeouts(
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::from_secs(1),
        )
        .unwrap()
    }

    #[test]
    fn rejects_relative_existing_and_unsafe_parent_paths() {
        assert!(PoolBackendListenerConfig::new("relative.sock", 1, 1).is_err());
        let directory = private_directory();
        let path = socket_path(&directory);
        fs::write(&path, b"occupied").unwrap();
        assert!(PoolBackendListener::bind(config(path)).is_err());

        let unsafe_directory = private_directory();
        fs::set_permissions(unsafe_directory.path(), fs::Permissions::from_mode(0o770)).unwrap();
        let unsafe_path = socket_path(&unsafe_directory);
        assert!(PoolBackendListener::bind(config(unsafe_path)).is_err());
    }

    #[test]
    fn bound_socket_has_exact_mode_and_is_removed_by_drop() {
        let directory = private_directory();
        let path = socket_path(&directory);
        let listener = PoolBackendListener::bind(config(path.clone())).unwrap();
        assert_eq!(listener.socket_path(), path);
        let metadata = fs::symlink_metadata(&path).unwrap();
        assert!(metadata.file_type().is_socket());
        assert_eq!(metadata.permissions().mode() & 0o777, 0o660);
        drop(listener);
        assert!(!path.exists());
    }

    #[test]
    fn failed_post_bind_validation_removes_only_the_new_socket() {
        let directory = private_directory();
        let path = socket_path(&directory);
        let unexpected_gid = nix::unistd::getegid().as_raw().wrapping_add(1);
        let invalid = PoolBackendListenerConfig::new(
            path.clone(),
            nix::unistd::geteuid().as_raw(),
            unexpected_gid,
        )
        .unwrap();

        assert!(PoolBackendListener::bind(invalid).is_err());
        assert!(!path.exists());
    }

    #[test]
    fn drop_never_removes_a_replacement_path() {
        let directory = private_directory();
        let path = socket_path(&directory);
        let listener = PoolBackendListener::bind(config(path.clone())).unwrap();
        fs::remove_file(&path).unwrap();
        fs::write(&path, b"replacement").unwrap();
        drop(listener);
        assert_eq!(fs::read(&path).unwrap(), b"replacement");
    }

    #[test]
    fn current_uid_round_trip_enforces_hello_and_response_shape() {
        let directory = private_directory();
        let path = socket_path(&directory);
        let listener = Arc::new(PoolBackendListener::bind(config(path.clone())).unwrap());
        let handler = Arc::new(TestHandler::default());
        let server = {
            let listener = Arc::clone(&listener);
            let handler = Arc::clone(&handler);
            thread::spawn(move || listener.serve_one(handler.as_ref()).unwrap())
        };

        let mut stream = UnixStream::connect(&path).unwrap();
        let hello = BackendRequest::Hello {
            version: BACKEND_PROTOCOL_VERSION,
            id: 1,
            pool_instance: canonical_uuid("f0a56cbd-c01b-4e7e-ab01-51ff7de71695"),
            last_event_seq: 0,
        };
        let frame = encode_backend_request(&hello).unwrap();
        for byte in frame {
            stream.write_all(&[byte]).unwrap();
        }
        let response = read_message(&mut stream);
        assert!(matches!(response, BackendMessage::HelloOk { id: 1, .. }));
        drop(stream);
        assert_eq!(server.join().unwrap(), PoolBackendConnectionOutcome::Served);
        assert_eq!(
            handler.requests.lock().unwrap().as_slice(),
            &[BackendRequestKind::Hello]
        );
    }

    #[test]
    fn wrong_peer_uid_is_rejected_before_handler_dispatch() {
        let directory = private_directory();
        let path = socket_path(&directory);
        let wrong_uid = nix::unistd::geteuid().as_raw().wrapping_add(1);
        let listener = Arc::new(
            PoolBackendListener::bind(
                PoolBackendListenerConfig::new(
                    path.clone(),
                    wrong_uid,
                    nix::unistd::getegid().as_raw(),
                )
                .unwrap(),
            )
            .unwrap(),
        );
        let handler = Arc::new(TestHandler::default());
        let server = {
            let listener = Arc::clone(&listener);
            let handler = Arc::clone(&handler);
            thread::spawn(move || listener.serve_one(handler.as_ref()).unwrap())
        };
        let _stream = UnixStream::connect(&path).unwrap();
        assert_eq!(
            server.join().unwrap(),
            PoolBackendConnectionOutcome::Unauthorized
        );
        assert!(handler.requests.lock().unwrap().is_empty());
    }

    fn read_message(stream: &mut UnixStream) -> BackendMessage {
        use std::io::Read;
        let mut prefix = [0; 4];
        stream.read_exact(&mut prefix).unwrap();
        let length = u32::from_be_bytes(prefix) as usize;
        let mut frame = Vec::from(prefix);
        frame.resize(4 + length, 0);
        stream.read_exact(&mut frame[4..]).unwrap();
        decode_backend_message(&frame).unwrap()
    }

    struct TestHandler {
        requests: Mutex<Vec<BackendRequestKind>>,
        backend_instance: CanonicalUuid,
        journal_stream: CanonicalUuid,
    }

    impl Default for TestHandler {
        fn default() -> Self {
            Self {
                requests: Mutex::new(Vec::new()),
                backend_instance: canonical_uuid("90bd8da9-9b49-4114-9aa8-2ca35aee013e"),
                journal_stream: canonical_uuid("c4756682-e84b-4b0b-927f-0af145ae9826"),
            }
        }
    }

    impl PoolBackendRequestHandler for TestHandler {
        fn persistent_authority(&self) -> PoolBackendAuthority {
            test_authority(self.backend_instance, self.journal_stream)
        }

        fn handle(
            &self,
            session: &PoolBackendSession,
            kind: BackendRequestKind,
            request: BackendRequest,
        ) -> Result<Vec<BackendMessage>, PoolBackendHandlerError> {
            self.requests.lock().unwrap().push(kind);
            let BackendRequest::Hello { id, .. } = request else {
                return Err(PoolBackendHandlerError::new(
                    BackendErrorCode::InvalidRequest,
                    "test handler accepts only hello",
                    true,
                ));
            };
            Ok(vec![BackendMessage::HelloOk {
                version: BACKEND_PROTOCOL_VERSION,
                id,
                backend_session: session.backend_session(),
                backend_instance: self.backend_instance,
                journal_stream: self.journal_stream,
                capabilities: REQUIRED_BACKEND_CAPABILITIES.to_vec(),
                wcash_genesis: Hex32::new([1; 32]),
                zcash_genesis: Hex32::new([2; 32]),
                wcash_payout_commitment: Hex32::new([3; 32]),
                zcash_payout_commitment: Hex32::new([4; 32]),
                chain_id: 1,
                current_event_seq: 0,
            }])
        }
    }

    #[test]
    fn handler_response_contract_requires_exact_correlation_and_kind() {
        let handler = TestHandler::default();
        let session = PoolBackendSession {
            backend_session: canonical_uuid("599ec097-b4e1-4f6d-a127-ff66e5262a52"),
            peer_uid: nix::unistd::geteuid().as_raw(),
        };
        let request = BackendRequest::Hello {
            version: BACKEND_PROTOCOL_VERSION,
            id: 3,
            pool_instance: canonical_uuid("6dc72fbf-7e29-4a58-ad4a-8aa25fc24c40"),
            last_event_seq: 0,
        };
        let wrong = BackendMessage::HealthStatus {
            version: BACKEND_PROTOCOL_VERSION,
            id: 3,
            event_seq: 0,
            healthy: true,
            pending_wcash: 0,
            quarantined_wcash: 0,
            pending_zcash: 0,
        };
        assert!(validate_handler_messages(
            BackendRequestKind::Hello,
            BackendConnectionRole::Negotiated,
            &request,
            &session,
            &handler.persistent_authority(),
            None,
            &[wrong],
        )
        .is_err());
    }

    #[test]
    fn handler_cannot_return_a_wire_error_as_success() {
        let (handler, session, request) = validation_context();
        let response = BackendMessage::Error {
            version: BACKEND_PROTOCOL_VERSION,
            id: request.id(),
            code: BackendErrorCode::InvalidRequest,
            message: "rejected".to_string(),
        };

        assert!(validate_handler_messages(
            BackendRequestKind::Hello,
            BackendConnectionRole::Negotiated,
            &request,
            &session,
            &handler.persistent_authority(),
            None,
            &[response],
        )
        .is_err());
    }

    #[test]
    fn hello_response_must_bind_the_complete_fresh_and_persistent_authority() {
        let (handler, session, request) = validation_context();
        let valid = hello_response(&handler, &session, request.id(), 7);
        assert_eq!(
            validate_handler_messages(
                BackendRequestKind::Hello,
                BackendConnectionRole::Negotiated,
                &request,
                &session,
                &handler.persistent_authority(),
                None,
                std::slice::from_ref(&valid),
            )
            .unwrap(),
            None
        );

        let mut wrong_payout = valid.clone();
        let BackendMessage::HelloOk {
            wcash_payout_commitment,
            ..
        } = &mut wrong_payout
        else {
            unreachable!("test helper returns hello_ok");
        };
        *wcash_payout_commitment = Hex32::new([9; 32]);
        assert!(validate_handler_messages(
            BackendRequestKind::Hello,
            BackendConnectionRole::Negotiated,
            &request,
            &session,
            &handler.persistent_authority(),
            None,
            &[wrong_payout],
        )
        .is_err());

        let BackendMessage::HelloOk {
            version,
            id,
            backend_instance,
            journal_stream,
            capabilities,
            wcash_genesis,
            zcash_genesis,
            wcash_payout_commitment,
            zcash_payout_commitment,
            chain_id,
            current_event_seq,
            ..
        } = valid
        else {
            unreachable!("test helper returns hello_ok");
        };
        let wrong_session = BackendMessage::HelloOk {
            version,
            id,
            backend_session: canonical_uuid("8d5fa8c1-b329-4aaa-afc1-00fd817f4744"),
            backend_instance,
            journal_stream,
            capabilities,
            wcash_genesis,
            zcash_genesis,
            wcash_payout_commitment,
            zcash_payout_commitment,
            chain_id,
            current_event_seq,
        };
        assert!(validate_handler_messages(
            BackendRequestKind::Hello,
            BackendConnectionRole::Negotiated,
            &request,
            &session,
            &handler.persistent_authority(),
            None,
            &[wrong_session],
        )
        .is_err());
    }

    #[test]
    fn event_pages_must_echo_the_exact_cursor_and_requested_bound() {
        let (_, session, _) = validation_context();
        let request = BackendRequest::ReadEvents {
            version: BACKEND_PROTOCOL_VERSION,
            id: 11,
            after_event_seq: 9,
            limit: 2,
        };
        let wrong_cursor = BackendMessage::EventsPage {
            version: BACKEND_PROTOCOL_VERSION,
            id: 11,
            after_event_seq: 8,
            next_event_seq: 8,
            complete: true,
            events: Vec::new(),
        };
        assert!(validate_handler_messages(
            BackendRequestKind::ReadEvents,
            BackendConnectionRole::Replay,
            &request,
            &session,
            &persistent_authority(),
            None,
            &[wrong_cursor],
        )
        .is_err());
    }

    #[test]
    fn live_events_are_contiguous_and_health_reports_the_flush_watermark() {
        let (_, session, _) = validation_context();
        let request = BackendRequest::Health {
            version: BACKEND_PROTOCOL_VERSION,
            id: 12,
        };
        let event = |event_seq| BackendMessage::Event {
            version: BACKEND_PROTOCOL_VERSION,
            event: BackendEvent::GenerationClosed {
                event_seq,
                job_id: Hex32::new([3; 32]),
            },
        };
        let health = |event_seq| BackendMessage::HealthStatus {
            version: BACKEND_PROTOCOL_VERSION,
            id: 12,
            event_seq,
            healthy: true,
            pending_wcash: 0,
            quarantined_wcash: 0,
            pending_zcash: 0,
        };

        assert_eq!(
            validate_handler_messages(
                BackendRequestKind::Health,
                BackendConnectionRole::Live,
                &request,
                &session,
                &persistent_authority(),
                Some(20),
                &[event(21), health(21)],
            )
            .unwrap(),
            Some(21)
        );
        assert!(validate_handler_messages(
            BackendRequestKind::Health,
            BackendConnectionRole::Live,
            &request,
            &session,
            &persistent_authority(),
            Some(20),
            &[event(22), health(22)],
        )
        .is_err());
        assert!(validate_handler_messages(
            BackendRequestKind::Health,
            BackendConnectionRole::Live,
            &request,
            &session,
            &persistent_authority(),
            Some(20),
            &[event(21), health(20)],
        )
        .is_err());
    }

    #[test]
    fn fresh_share_ack_requires_the_exact_attribution_event_before_flush() {
        let (_, session, _) = validation_context();
        let identity = WorkerIdentity {
            account_id: canonical_uuid("77b9fb5b-2e4e-4ed2-b781-1ecef4d867af"),
            worker_id: canonical_uuid("643caa33-4b16-4406-b4d9-1c682d442a9e"),
            label: "rig-1".to_string(),
        };
        let job_id = Hex32::new([4; 32]);
        let target_le = TargetLe::new([0xff; 32]);
        let time = Hex4::new([1, 2, 3, 4]);
        let nonce = Hex32::new([5; 32]);
        let solution = Box::new(Hex1344::new([6; 1344]));
        let request = BackendRequest::SubmitShare {
            version: BACKEND_PROTOCOL_VERSION,
            id: 13,
            job_id: job_id.clone(),
            identity: identity.clone(),
            target_le: target_le.clone(),
            time: time.clone(),
            nonce: nonce.clone(),
            solution: solution.clone(),
        };
        let receipt = ShareReceipt {
            event_seq: 31,
            job_id: job_id.clone(),
            share_id: canonical_share_id(&job_id, &time, &nonce, &solution),
            attribution_id: canonical_attribution_id(&identity, &target_le).unwrap(),
            parent_hash_le: Hex32::new([7; 32]),
            winners: Vec::new(),
        };
        let event = BackendMessage::Event {
            version: BACKEND_PROTOCOL_VERSION,
            event: BackendEvent::ShareCommitted {
                receipt: receipt.clone(),
                job_id: job_id.clone(),
                identity: identity.clone(),
                target_le: target_le.clone(),
            },
        };
        let response = BackendMessage::ShareCommitted {
            version: BACKEND_PROTOCOL_VERSION,
            id: 13,
            receipt: receipt.clone(),
            replayed: false,
        };

        assert_eq!(
            validate_handler_messages(
                BackendRequestKind::SubmitShare,
                BackendConnectionRole::Live,
                &request,
                &session,
                &persistent_authority(),
                Some(30),
                &[event.clone(), response.clone()],
            )
            .unwrap(),
            Some(31)
        );
        assert!(validate_handler_messages(
            BackendRequestKind::SubmitShare,
            BackendConnectionRole::Live,
            &request,
            &session,
            &persistent_authority(),
            Some(30),
            std::slice::from_ref(&response),
        )
        .is_err());

        let conflicting_event = BackendMessage::Event {
            version: BACKEND_PROTOCOL_VERSION,
            event: BackendEvent::ShareCommitted {
                receipt,
                job_id,
                identity: WorkerIdentity {
                    label: "rig-2".to_string(),
                    ..identity
                },
                target_le,
            },
        };
        assert!(validate_handler_messages(
            BackendRequestKind::SubmitShare,
            BackendConnectionRole::Live,
            &request,
            &session,
            &persistent_authority(),
            Some(30),
            &[conflicting_event, response],
        )
        .is_err());
    }

    fn validation_context() -> (TestHandler, PoolBackendSession, BackendRequest) {
        let handler = TestHandler::default();
        let session = PoolBackendSession {
            backend_session: canonical_uuid("599ec097-b4e1-4f6d-a127-ff66e5262a52"),
            peer_uid: nix::unistd::geteuid().as_raw(),
        };
        let request = BackendRequest::Hello {
            version: BACKEND_PROTOCOL_VERSION,
            id: 3,
            pool_instance: canonical_uuid("6dc72fbf-7e29-4a58-ad4a-8aa25fc24c40"),
            last_event_seq: 0,
        };
        (handler, session, request)
    }

    fn persistent_authority() -> PoolBackendAuthority {
        TestHandler::default().persistent_authority()
    }

    fn test_authority(
        backend_instance: CanonicalUuid,
        journal_stream: CanonicalUuid,
    ) -> PoolBackendAuthority {
        PoolBackendAuthority::new(
            backend_instance,
            journal_stream,
            Hex32::new([1; 32]),
            Hex32::new([2; 32]),
            Hex32::new([3; 32]),
            Hex32::new([4; 32]),
            1,
        )
        .expect("valid test authority")
    }

    fn hello_response(
        handler: &TestHandler,
        session: &PoolBackendSession,
        id: u64,
        current_event_seq: u64,
    ) -> BackendMessage {
        BackendMessage::HelloOk {
            version: BACKEND_PROTOCOL_VERSION,
            id,
            backend_session: session.backend_session(),
            backend_instance: handler.backend_instance,
            journal_stream: handler.journal_stream,
            capabilities: REQUIRED_BACKEND_CAPABILITIES.to_vec(),
            wcash_genesis: Hex32::new([1; 32]),
            zcash_genesis: Hex32::new([2; 32]),
            wcash_payout_commitment: Hex32::new([3; 32]),
            zcash_payout_commitment: Hex32::new([4; 32]),
            chain_id: 1,
            current_event_seq,
        }
    }
}
