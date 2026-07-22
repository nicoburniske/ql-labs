use std::{
    collections::HashSet,
    fmt, fs,
    io::{self, ErrorKind},
    os::unix::fs::PermissionsExt,
    str::FromStr,
    sync::Arc,
    thread,
    time::Duration,
};

use anyhow::{Context, Result};
use async_channel::{Sender, TrySendError};
use dashmap::DashMap;
use ql_codec::{Decode, Encode};
use ql_common::QID;
use ql_router::{DEFAULT_ADDRESS, Frame, receive_frame, send};
use ql_wire::{
    HandshakeId, PeerBundle, PeerChallenge, QL_WIRE_VERSION, QlIdentity, RecordHeader,
    SoftwareCrypto, generate_identity,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot},
    time::{Instant, timeout, timeout_at},
};
use tracing::{Instrument, Level, debug, debug_span, info};

const MAX_ROUTES_PER_CONNECTION: usize = 64;
const OUTBOUND_QUEUE_SIZE: usize = 16;
const HANDSHAKE_QUEUE_SIZE: usize = 32;
const MAX_PENDING_HANDSHAKES: usize = 1024;
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

type ConnectionId = u64;
type PendingChallenge = PeerChallenge<Arc<QlIdentity>>;
type ConnectionResult<T> = std::result::Result<T, ConnectionError>;
type HandshakeJob = Box<dyn FnOnce() + Send>;

#[repr(usize)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectionError {
    Io,
    Codec,
    Wire,
    AttachRequired,
    ChallengeActive,
    ChallengeMissing,
    ChallengeTimedOut,
    UnsupportedVersion,
    RouteLimit,
    HandshakeOverloaded,
    HandshakeWorkerStopped,
    WriterStopped,
}

struct ActiveChallenge {
    pending: PendingChallenge,
    deadline: Instant,
    _admission: OwnedSemaphorePermit,
}

struct Router {
    identity: Arc<QlIdentity>,
    handshakes: HandshakeExecutor,
    challenge_capacity: Arc<Semaphore>,
    connections: DashMap<ConnectionId, mpsc::Sender<Vec<u8>>>,
    routes: DashMap<QID, ConnectionId>,
}

struct HandshakeExecutor {
    tx: Sender<HandshakeJob>,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let log_level = std::env::var("QL_ROUTER_LOG")
        .ok()
        .and_then(|level| Level::from_str(&level).ok())
        .unwrap_or(Level::INFO);
    tracing_subscriber::fmt().with_max_level(log_level).init();

    let address = std::env::var("QL_ROUTER_ADDRESS").unwrap_or_else(|_| DEFAULT_ADDRESS.into());
    let listener = TcpListener::bind(&address).await?;
    let crypto = SoftwareCrypto;
    let identity_path = std::env::var("QL_ROUTER_IDENTITY_PATH")
        .unwrap_or_else(|_| "ql-router/identity.bin".into());
    let identity = match fs::read(&identity_path) {
        Ok(bytes) => {
            QlIdentity::decode_bytes(bytes.as_slice()).context("decoding router identity")?
        }
        Err(error) if error.kind() == ErrorKind::NotFound => {
            let identity = generate_identity(&crypto, "QL Router");
            fs::write(&identity_path, identity.encode_vec())
                .context("persisting router identity")?;
            identity
        }
        Err(error) => return Err(error).context("reading router identity"),
    };
    fs::set_permissions(&identity_path, fs::Permissions::from_mode(0o600))
        .context("securing router identity")?;
    let bundle_path =
        std::env::var("QL_ROUTER_BUNDLE_PATH").unwrap_or_else(|_| "ql-router/bundle.bin".into());
    fs::write(&bundle_path, identity.bundle().encode_vec())
        .context("writing router peer bundle")?;
    info!(qid = %hex::encode(identity.qid.0), "QL router identity ready");
    info!(path = %bundle_path, "QL router peer bundle ready");

    let identity = Arc::new(identity);
    let handshake_workers = std::thread::available_parallelism()
        .map_or(1, |parallelism| (parallelism.get() / 2).clamp(1, 4));
    let router = Arc::new(Router {
        identity: identity.clone(),
        handshakes: HandshakeExecutor::new(handshake_workers),
        challenge_capacity: Arc::new(Semaphore::new(MAX_PENDING_HANDSHAKES)),
        connections: DashMap::new(),
        routes: DashMap::new(),
    });
    let mut next_connection: ConnectionId = 1;
    info!(
        workers = handshake_workers,
        queue = HANDSHAKE_QUEUE_SIZE,
        pending = MAX_PENDING_HANDSHAKES,
        routes_per_connection = MAX_ROUTES_PER_CONNECTION,
        "QL router limits configured"
    );
    info!(address = %listener.local_addr()?, "QL router listening on");

    loop {
        let (stream, remote) = listener.accept().await?;
        let connection = next_connection;
        next_connection = next_connection.wrapping_add(1);
        let router = router.clone();
        tokio::spawn(
            async move {
                debug!("connection opened");
                if let Err(error) = serve(connection, stream, &router).await {
                    debug!(%error, "connection failed");
                }
                debug!("connection closed");
            }
            .instrument(debug_span!("connection", id = connection, %remote)),
        );
    }
}

async fn serve(
    connection: ConnectionId,
    stream: TcpStream,
    router: &Router,
) -> ConnectionResult<()> {
    let (mut reader, mut writer) = stream.into_split();
    let (outbound, mut outbound_rx) = mpsc::channel(OUTBOUND_QUEUE_SIZE);
    let mut challenge: Option<ActiveChallenge> = None;
    let mut next_handshake_id = 1;
    let mut routes = HashSet::new();
    router.connections.insert(connection, outbound.clone());

    let writer = tokio::spawn(
        async move {
            while let Some(record) = outbound_rx.recv().await {
                if let Err(error) = send(&mut writer, &record).await {
                    debug!(%error, "connection write failed");
                    break;
                }
            }
        }
        .in_current_span(),
    );

    let result = async {
        loop {
            let frame = if let Some(challenge) = challenge.as_ref() {
                timeout_at(challenge.deadline, receive_frame(&mut reader))
                    .await
                    .map_err(|_| ConnectionError::ChallengeTimedOut)??
            } else {
                receive_frame(&mut reader).await?
            };
            let Some(frame) = frame else { break };
            if routes.is_empty() && challenge.is_none() && !matches!(&frame, Frame::Attach(_)) {
                return Err(ConnectionError::AttachRequired);
            }
            handle_inbound(
                frame,
                connection,
                router,
                &outbound,
                &mut challenge,
                &mut next_handshake_id,
                &mut routes,
            )
            .await?;
        }
        Ok(())
    }
    .await;

    writer.abort();
    router.connections.remove(&connection);
    for qid in routes {
        router
            .routes
            .remove_if(&qid, |_, owner| *owner == connection);
    }
    result
}

async fn handle_inbound(
    frame: Frame,
    connection: ConnectionId,
    router: &Router,
    outbound: &mpsc::Sender<Vec<u8>>,
    challenge: &mut Option<ActiveChallenge>,
    next_handshake_id: &mut u32,
    routes: &mut HashSet<QID>,
) -> ConnectionResult<()> {
    let record = match frame {
        Frame::Attach(bundle) => {
            if challenge.is_some() {
                return Err(ConnectionError::ChallengeActive);
            }
            if routes.len() == MAX_ROUTES_PER_CONNECTION {
                return Err(ConnectionError::RouteLimit);
            }
            let handshake_id = HandshakeId(*next_handshake_id);
            *next_handshake_id = next_handshake_id.wrapping_add(1);
            let admission = router
                .challenge_capacity
                .clone()
                .try_acquire_owned()
                .map_err(|_| ConnectionError::HandshakeOverloaded)?;
            let identity = router.identity.clone();
            let (qid, pending, request) = timeout(
                HANDSHAKE_TIMEOUT,
                router.handshakes.run(move || {
                    let crypto = SoftwareCrypto;
                    let bundle = PeerBundle::decode_bytes(bundle.as_slice())?;
                    bundle.validate(&crypto)?;
                    let qid = bundle.qid;
                    let (pending, request) =
                        PeerChallenge::new(&crypto, identity, bundle, handshake_id)?;
                    Ok((qid, pending, request))
                }),
            )
            .await
            .map_err(|_| ConnectionError::ChallengeTimedOut)??;
            outbound
                .send(request)
                .await
                .map_err(|_| ConnectionError::WriterStopped)?;
            *challenge = Some(ActiveChallenge {
                pending,
                deadline: Instant::now() + HANDSHAKE_TIMEOUT,
                _admission: admission,
            });
            debug!(qid = %hex::encode(qid.0), "challenging QID");
            return Ok(());
        }
        Frame::Record(record) => record,
    };
    let header = RecordHeader::decode_bytes(record.as_slice())?;
    if header.version != QL_WIRE_VERSION {
        return Err(ConnectionError::UnsupportedVersion);
    }
    if header.route.recipient == router.identity.qid {
        let challenge = challenge.take().ok_or(ConnectionError::ChallengeMissing)?;
        let pending = challenge.pending;
        let _admission = challenge._admission;
        let (qid, accepted) = timeout(
            HANDSHAKE_TIMEOUT,
            router
                .handshakes
                .run(move || Ok(pending.verify(&SoftwareCrypto, &record)?)),
        )
        .await
        .map_err(|_| ConnectionError::ChallengeTimedOut)??;
        routes.insert(qid);
        router.routes.insert(qid, connection);
        outbound
            .send(accepted)
            .await
            .map_err(|_| ConnectionError::WriterStopped)?;
        info!(connection, qid = %hex::encode(qid.0), "authenticated QID");
        return Ok(());
    }

    if router
        .routes
        .get(&header.route.sender)
        .is_none_or(|owner| *owner != connection)
    {
        debug!(
            sender = %hex::encode(header.route.sender.0),
            recipient = %hex::encode(header.route.recipient.0),
            "dropping record from unauthenticated sender"
        );
        return Ok(());
    }
    let Some(egress) = router
        .routes
        .get(&header.route.recipient)
        .map(|owner| *owner)
    else {
        debug!(
            sender = %hex::encode(header.route.sender.0),
            recipient = %hex::encode(header.route.recipient.0),
            "no route for recipient"
        );
        return Ok(());
    };
    if let Some(outbound) = router.connections.get(&egress)
        && let Err(mpsc::error::TrySendError::Full(_)) = outbound.try_send(record)
    {
        debug!(connection = egress, "outbound queue full; dropping record");
    }
    Ok(())
}

impl HandshakeExecutor {
    fn new(workers: usize) -> Self {
        let (tx, rx) = async_channel::bounded::<HandshakeJob>(HANDSHAKE_QUEUE_SIZE);
        for _ in 0..workers {
            let rx = rx.clone();
            thread::spawn(move || {
                while let Ok(job) = rx.recv_blocking() {
                    job();
                }
            });
        }
        Self { tx }
    }

    async fn run<T>(
        &self,
        work: impl FnOnce() -> ConnectionResult<T> + Send + 'static,
    ) -> ConnectionResult<T>
    where
        T: Send + 'static,
    {
        let (result_tx, result_rx) = oneshot::channel();
        self.tx
            .try_send(Box::new(move || {
                let _ = result_tx.send(work());
            }))
            .map_err(|error| match error {
                TrySendError::Full(_) => ConnectionError::HandshakeOverloaded,
                TrySendError::Closed(_) => ConnectionError::HandshakeWorkerStopped,
            })?;
        result_rx
            .await
            .map_err(|_| ConnectionError::HandshakeWorkerStopped)?
    }
}

impl From<io::Error> for ConnectionError {
    fn from(_: io::Error) -> Self {
        Self::Io
    }
}

impl From<ql_codec::Error> for ConnectionError {
    fn from(_: ql_codec::Error) -> Self {
        Self::Codec
    }
}

impl From<ql_wire::Error> for ConnectionError {
    fn from(_: ql_wire::Error) -> Self {
        Self::Wire
    }
}

impl fmt::Display for ConnectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io => f.write_str("I/O error"),
            Self::Codec => f.write_str("codec error"),
            Self::Wire => f.write_str("wire error"),
            Self::AttachRequired => f.write_str("peer must attach before routing records"),
            Self::ChallengeActive => f.write_str("route challenge already active"),
            Self::ChallengeMissing => f.write_str("no active route challenge"),
            Self::ChallengeTimedOut => f.write_str("route challenge timed out"),
            Self::UnsupportedVersion => f.write_str("unsupported QL record version"),
            Self::RouteLimit => f.write_str("connection route limit reached"),
            Self::HandshakeOverloaded => f.write_str("handshake capacity exhausted"),
            Self::HandshakeWorkerStopped => f.write_str("handshake worker stopped"),
            Self::WriterStopped => f.write_str("connection writer stopped"),
        }
    }
}
