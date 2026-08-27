use std::{
    collections::HashSet,
    fmt, fs,
    io::{self, ErrorKind},
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use dashmap::DashMap;
use figment::{
    Figment,
    providers::{Env, Serialized},
};
use futures_lite::future;
use ql_codec::{Decode, Encode, Reader};
use ql_common::QID;
use ql_router::{
    DEFAULT_ADDRESS,
    protocol::{self, FrameDecoder, PacketKind, SecureReceiver, SecureSender},
    tokio as router_io,
};
use ql_wire::{
    HandshakeId, PeerBundle, PeerChallenge, QlIdentity, RecordHeader, SoftwareCrypto,
    generate_identity,
};
use serde::{Deserialize, Serialize};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Semaphore, SemaphorePermit, mpsc},
    time::{Instant, timeout, timeout_at},
};
use tracing::{Instrument, Level, debug, debug_span, info};

const MAX_ROUTES_PER_CONNECTION: usize = 64;
const OUTBOUND_QUEUE_SIZE: usize = 4;
const MAX_PENDING_HANDSHAKES: usize = 1024;
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

type ConnectionId = u64;

#[derive(Deserialize, Serialize)]
struct Config {
    log: String,
    address: String,
    identity_path: PathBuf,
    bundle_path: PathBuf,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            log: "INFO".into(),
            address: DEFAULT_ADDRESS.into(),
            identity_path: "ql-router/identity.bin".into(),
            bundle_path: "ql-router/bundle.bin".into(),
        }
    }
}

struct ActiveChallenge {
    pending: PeerChallenge<&'static QlIdentity>,
    deadline: Instant,
    _admission: SemaphorePermit<'static>,
}

struct Router {
    identity: QlIdentity,
    handshake_capacity: Semaphore,
    challenge_capacity: Semaphore,
    connections: DashMap<ConnectionId, Arc<Connection>>,
    routes: DashMap<QID, ConnectionId>,
}

impl Router {
    async fn handshake<T>(&self, work: impl FnOnce() -> T) -> T {
        let _permit = self.handshake_capacity.acquire().await.unwrap();
        work()
    }
}

struct Connection {
    outbound: mpsc::Sender<Vec<u8>>,
}

struct Inbound {
    secure: SecureReceiver,
    challenge: Option<ActiveChallenge>,
    next_handshake_id: u32,
    routes: HashSet<QID>,
}

struct NegotiatedTransport {
    outbound: SecureSender,
    inbound: SecureReceiver,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let config: Config = Figment::from(Serialized::defaults(Config::default()))
        .merge(Env::prefixed("QL_ROUTER_"))
        .extract()?;
    tracing_subscriber::fmt()
        .with_max_level(Level::from_str(&config.log)?)
        .init();

    let listener = TcpListener::bind(&config.address).await?;

    let identity = match fs::read(&config.identity_path) {
        Ok(bytes) => QlIdentity::decode_bytes(bytes.as_slice())?,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            let identity = generate_identity(&SoftwareCrypto, "QL Router");
            fs::write(&config.identity_path, identity.encode_vec())?;
            identity
        }
        Err(error) => return Err(error.into()),
    };
    fs::set_permissions(&config.identity_path, fs::Permissions::from_mode(0o600))?;
    fs::write(&config.bundle_path, identity.bundle().encode_vec())?;
    info!(qid = %hex::encode(identity.qid.0), "QL router identity ready");
    info!(path = %config.bundle_path.display(), "QL router peer bundle ready");

    let handshake_workers = std::thread::available_parallelism()
        .map_or(1, |parallelism| (parallelism.get() / 2).clamp(1, 4));
    let router: &'static Router = Box::leak(Box::new(Router {
        identity,
        handshake_capacity: Semaphore::new(handshake_workers),
        challenge_capacity: Semaphore::new(MAX_PENDING_HANDSHAKES),
        connections: DashMap::new(),
        routes: DashMap::new(),
    }));
    info!(
        workers = handshake_workers,
        pending = MAX_PENDING_HANDSHAKES,
        routes_per_connection = MAX_ROUTES_PER_CONNECTION,
        "QL router limits configured"
    );
    info!(address = %listener.local_addr()?, "QL router listening on");

    run(router, listener).await
}

async fn run(router: &'static Router, listener: TcpListener) -> anyhow::Result<()> {
    loop {
        let (stream, remote) = listener.accept().await?;
        let connection = loop {
            let mut random = [0; 8];
            getrandom::getrandom(&mut random).unwrap();
            let connection = ConnectionId::from_le_bytes(random);
            if connection != 0 && !router.connections.contains_key(&connection) {
                break connection;
            }
        };
        tokio::spawn(
            async move {
                debug!("connection opened");
                if let Err(error) = serve(router, connection, stream).await {
                    debug!(%error, "connection failed");
                }
                debug!("connection closed");
            }
            .instrument(debug_span!("connection", id = connection, %remote)),
        );
    }
}

async fn serve(
    router: &'static Router,
    connection: ConnectionId,
    mut stream: TcpStream,
) -> Result<(), Error> {
    let mut frames = FrameDecoder::new();
    let NegotiatedTransport {
        outbound: mut secure_outbound,
        inbound,
    } = timeout(HANDSHAKE_TIMEOUT, async {
        let request = router_io::read_frame(&mut frames, &mut stream)
            .await?
            .ok_or(Error::Protocol)?;
        negotiate_transport(router, request, &mut frames, &mut stream).await
    })
    .await
    .map_err(|_| Error::HandshakeTimedOut)??;
    let (mut reader, mut writer) = stream.into_split();
    let (outbound, mut outbound_rx) = mpsc::channel(OUTBOUND_QUEUE_SIZE);

    let connection_state = Arc::new(Connection {
        outbound: outbound.clone(),
    });
    router
        .connections
        .insert(connection, connection_state.clone());

    let mut inbound = Inbound {
        secure: inbound,
        challenge: None,
        next_handshake_id: 1,
        routes: HashSet::new(),
    };
    let read = async {
        loop {
            let packet = if let Some(challenge) = inbound.challenge.as_ref() {
                timeout_at(
                    challenge.deadline,
                    router_io::read_frame(&mut frames, &mut reader),
                )
                .await
                .map_err(|_| Error::ChallengeTimedOut)??
            } else {
                router_io::read_frame(&mut frames, &mut reader).await?
            };
            let Some(packet) = packet else { break };
            handle_inbound(router, packet, connection, &outbound, &mut inbound).await?;
        }
        Ok(())
    };
    let write = async move {
        let mut frame = Vec::new();
        while let Some(message) = outbound_rx.recv().await {
            secure_outbound.seal(&mut frame, PacketKind::Record, &message)?;
            tokio::io::AsyncWriteExt::write_all(&mut writer, &frame).await?;
        }
        Ok(())
    };
    let result = future::race(read, write).await;

    router.connections.remove_if(&connection, |_, current| {
        Arc::ptr_eq(current, &connection_state)
    });
    for qid in inbound.routes {
        router
            .routes
            .remove_if(&qid, |_, owner| *owner == connection);
    }
    result
}

async fn negotiate_transport(
    router: &'static Router,
    request: protocol::Frame,
    frames: &mut FrameDecoder,
    stream: &mut TcpStream,
) -> Result<NegotiatedTransport, Error> {
    let (response, mut inbound, outbound) = router
        .handshake(|| protocol::accept_handshake(&router.identity, &request))
        .await?;

    tokio::io::AsyncWriteExt::write_all(stream, &response).await?;

    let confirmation = router_io::read_frame(frames, stream)
        .await?
        .ok_or(Error::Protocol)?;
    let (kind, payload) = inbound.open(confirmation)?;
    if kind != PacketKind::Confirm || !payload.is_empty() {
        return Err(Error::Protocol);
    }

    Ok(NegotiatedTransport { outbound, inbound })
}

async fn handle_inbound(
    router: &'static Router,
    packet: protocol::Frame,
    connection: ConnectionId,
    outbound: &mpsc::Sender<Vec<u8>>,
    inbound: &mut Inbound,
) -> Result<(), Error> {
    let (kind, payload) = inbound.secure.open(packet)?;

    let record = match kind {
        PacketKind::Attach => {
            if inbound.challenge.is_some() {
                return Err(Error::ChallengeActive);
            }
            if inbound.routes.len() == MAX_ROUTES_PER_CONNECTION {
                return Err(Error::RouteLimit);
            }
            let handshake_id = HandshakeId(inbound.next_handshake_id);
            inbound.next_handshake_id = inbound
                .next_handshake_id
                .checked_add(1)
                .ok_or(Error::Protocol)?;
            let admission = router
                .challenge_capacity
                .try_acquire()
                .map_err(|_| Error::HandshakeOverloaded)?;
            let (qid, pending, request) = router
                .handshake(|| -> Result<_, Error> {
                    let mut payload = Reader::new(payload.as_slice());
                    let bundle = payload.decode::<PeerBundle>()?;
                    if !payload.is_empty() {
                        return Err(Error::Protocol);
                    }
                    bundle.validate(&SoftwareCrypto)?;
                    let qid = bundle.qid;
                    let (pending, request) = PeerChallenge::new(
                        &SoftwareCrypto,
                        &router.identity,
                        bundle,
                        handshake_id,
                    )?;
                    Ok((qid, pending, request))
                })
                .await?;
            outbound
                .send(request)
                .await
                .map_err(|_| Error::WriterStopped)?;
            inbound.challenge = Some(ActiveChallenge {
                pending,
                deadline: Instant::now() + HANDSHAKE_TIMEOUT,
                _admission: admission,
            });
            debug!(qid = %hex::encode(qid.0), "challenging QID");
            return Ok(());
        }
        PacketKind::Record => {
            if inbound.routes.is_empty() && inbound.challenge.is_none() {
                return Err(Error::AttachRequired);
            }
            payload
        }
        _ => return Err(Error::Protocol),
    };
    let header = RecordHeader::decode_bytes(record.as_slice())?;
    if header.route.recipient == router.identity.qid {
        let challenge = inbound.challenge.take().ok_or(Error::ChallengeMissing)?;
        let pending = challenge.pending;
        let _admission = challenge._admission;
        let (qid, accepted) = router
            .handshake(|| pending.verify(&SoftwareCrypto, &record))
            .await?;
        outbound
            .send(accepted)
            .await
            .map_err(|_| Error::WriterStopped)?;
        inbound.routes.insert(qid);
        router.routes.insert(qid, connection);
        info!(connection, qid = %hex::encode(qid.0), "authenticated QID");
        return Ok(());
    }

    route_record(router, record, header, connection).await
}

async fn route_record(
    router: &Router,
    record: Vec<u8>,
    header: RecordHeader,
    ingress: ConnectionId,
) -> Result<(), Error> {
    if router
        .routes
        .get(&header.route.sender)
        .is_none_or(|owner| *owner != ingress)
    {
        debug!(
            sender = %hex::encode(header.route.sender.0),
            recipient = %hex::encode(header.route.recipient.0),
            "dropping record from unauthenticated sender"
        );
        return Ok(());
    }
    let Some(egress_id) = router
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
    let Some(egress) = router
        .connections
        .get(&egress_id)
        .map(|connection| connection.clone())
    else {
        return Ok(());
    };

    let _ = egress.outbound.send(record).await;
    Ok(())
}

#[repr(usize)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Error {
    Io,
    Codec,
    Wire,
    AttachRequired,
    ChallengeActive,
    ChallengeMissing,
    ChallengeTimedOut,
    HandshakeTimedOut,
    RouteLimit,
    HandshakeOverloaded,
    WriterStopped,
    Protocol,
}

impl From<io::Error> for Error {
    fn from(_: io::Error) -> Self {
        Self::Io
    }
}

impl From<ql_codec::Error> for Error {
    fn from(_: ql_codec::Error) -> Self {
        Self::Codec
    }
}

impl From<ql_wire::Error> for Error {
    fn from(_: ql_wire::Error) -> Self {
        Self::Wire
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io => f.write_str("I/O error"),
            Self::Codec => f.write_str("codec error"),
            Self::Wire => f.write_str("wire error"),
            Self::AttachRequired => f.write_str("peer must attach before routing records"),
            Self::ChallengeActive => f.write_str("route challenge already active"),
            Self::ChallengeMissing => f.write_str("no active route challenge"),
            Self::ChallengeTimedOut => f.write_str("route challenge timed out"),
            Self::HandshakeTimedOut => f.write_str("transport handshake timed out"),
            Self::RouteLimit => f.write_str("connection route limit reached"),
            Self::HandshakeOverloaded => f.write_str("handshake capacity exhausted"),
            Self::WriterStopped => f.write_str("connection writer stopped"),
            Self::Protocol => f.write_str("transport protocol error"),
        }
    }
}
