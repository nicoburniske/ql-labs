use std::{
    fmt, fs,
    io::{self, ErrorKind, Write},
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::PathBuf,
    str::FromStr,
    time::Duration,
};

use dashmap::DashMap;
use figment::{
    Figment,
    providers::{Env, Serialized},
};
use futures_lite::future;
use ql_codec::{Decode, Encode};
use ql_common::QID;
use ql_router::{
    DEFAULT_ADDRESS,
    protocol::{
        self, FrameDecoder, MAX_ROUTES_PER_CONNECTION, PacketKind, RouterAction, RouterConnection,
    },
    tokio as router_io,
};
use ql_wire::{QlIdentity, SoftwareCrypto, generate_identity};
use serde::{Deserialize, Serialize};
use socket2::{SockRef, TcpKeepalive};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Semaphore, SemaphorePermit, mpsc},
    time::{Instant, sleep, timeout, timeout_at},
};
use tracing::{Instrument, Level, debug, debug_span, info, warn};

const OUTBOUND_QUEUE_SIZE: usize = 4;
const MAX_PENDING_HANDSHAKES: usize = 1024;
const MAX_PENDING_TRANSPORTS: usize = 512;
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
const FRAME_ASSEMBLY_TIMEOUT: Duration = Duration::from_secs(5);
const WRITE_TIMEOUT: Duration = Duration::from_secs(30);
const KEEPALIVE_IDLE: Duration = Duration::from_secs(60);
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);
const ACCEPT_RETRY_DELAY: Duration = Duration::from_millis(100);

type ConnectionId = u64;

#[derive(Deserialize, Serialize)]
struct Config {
    log: String,
    address: String,
    identity_path: PathBuf,
    bundle_path: PathBuf,
    max_connections: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            log: "INFO".into(),
            address: DEFAULT_ADDRESS.into(),
            identity_path: "ql-router/identity.bin".into(),
            bundle_path: "ql-router/bundle.bin".into(),
            max_connections: 16 * 1024,
        }
    }
}

struct ActiveChallenge {
    deadline: Instant,
    _admission: SemaphorePermit<'static>,
}

struct Router {
    identity: QlIdentity,
    handshake_capacity: Capacity,
    challenge_capacity: Capacity,
    connection_capacity: Capacity,
    transport_capacity: Capacity,
    routes: DashMap<QID, Route>,
}

impl Router {
    async fn handshake<T>(&self, work: impl FnOnce() -> T) -> T {
        let _permit = self.handshake_capacity.acquire().await;
        work()
    }
}

struct Route {
    connection: ConnectionId,
    outbound: mpsc::Sender<Vec<u8>>,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let config: Config = Figment::from(Serialized::defaults(Config::default()))
        .merge(Env::prefixed("QL_ROUTER_"))
        .extract()?;
    anyhow::ensure!(
        (1..=Semaphore::MAX_PERMITS).contains(&config.max_connections),
        "max connections is out of range"
    );
    tracing_subscriber::fmt()
        .with_max_level(Level::from_str(&config.log)?)
        .init();

    let listener = TcpListener::bind(&config.address).await?;

    let identity = match fs::read(&config.identity_path) {
        Ok(bytes) => QlIdentity::decode_bytes(bytes.as_slice())?,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            let identity = generate_identity(&SoftwareCrypto, "QL Router");
            fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&config.identity_path)?
                .write_all(&identity.encode_vec())?;
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
        handshake_capacity: Capacity::new(handshake_workers),
        challenge_capacity: Capacity::new(MAX_PENDING_HANDSHAKES),
        connection_capacity: Capacity::new(config.max_connections),
        transport_capacity: Capacity::new(MAX_PENDING_TRANSPORTS),
        routes: DashMap::new(),
    }));
    info!(
        workers = handshake_workers,
        connections = config.max_connections,
        pending_transports = MAX_PENDING_TRANSPORTS,
        pending_challenges = MAX_PENDING_HANDSHAKES,
        routes_per_connection = MAX_ROUTES_PER_CONNECTION,
        "QL router limits configured"
    );
    info!(address = %listener.local_addr()?, "QL router listening on");

    let mut next_connection: ConnectionId = 1;
    loop {
        let connection_permit = router.connection_capacity.acquire().await;
        let transport_permit = router.transport_capacity.acquire().await;
        let (stream, remote) = match listener.accept().await {
            Ok(connection) => connection,
            Err(error) => {
                warn!(%error, "connection accept failed");
                sleep(ACCEPT_RETRY_DELAY).await;
                continue;
            }
        };
        let connection = next_connection;
        next_connection = next_connection
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("connection ID exhausted"))?;
        tokio::spawn(
            async move {
                let _connection_permit = connection_permit;
                debug!("connection opened");
                if let Err(error) = serve(router, connection, stream, transport_permit).await {
                    debug!(%error, "connection failed");
                }
                debug!("connection closed");
            }
            .instrument(debug_span!("connection", connection, %remote)),
        );
    }
}

async fn serve(
    router: &'static Router,
    connection: ConnectionId,
    mut stream: TcpStream,
    transport_permit: SemaphorePermit<'static>,
) -> Result<(), Error> {
    stream.set_nodelay(true)?;
    SockRef::from(&stream).set_tcp_keepalive(
        &TcpKeepalive::new()
            .with_time(KEEPALIVE_IDLE)
            .with_interval(KEEPALIVE_INTERVAL)
            .with_retries(3),
    )?;
    let mut frames = FrameDecoder::new();
    let (mut secure_outbound, inbound) = timeout(HANDSHAKE_TIMEOUT, async {
        let request = router_io::read_frame(&mut frames, &mut stream)
            .await?
            .ok_or(Error::Protocol)?;
        let (response, mut inbound, outbound) = router
            .handshake(|| protocol::accept_handshake(&router.identity, &request))
            .await?;
        tokio::io::AsyncWriteExt::write_all(&mut stream, &response).await?;

        let confirmation = router_io::read_frame(&mut frames, &mut stream)
            .await?
            .ok_or(Error::Protocol)?;
        let (kind, payload) = inbound.open(confirmation)?;
        if kind != PacketKind::Confirm || !payload.is_empty() {
            return Err(Error::Protocol);
        }
        Ok((outbound, inbound))
    })
    .await
    .map_err(|_| Error::HandshakeTimedOut)??;
    drop(transport_permit);
    let (mut reader, mut writer) = stream.into_split();
    let (outbound, mut outbound_rx) = mpsc::channel(OUTBOUND_QUEUE_SIZE);

    let mut inbound = RouterConnection::new(&router.identity, inbound);
    let mut challenge: Option<ActiveChallenge> = None;
    let read = async {
        loop {
            let packet = if let Some(challenge) = challenge.as_ref() {
                timeout_at(
                    challenge.deadline,
                    router_io::read_frame(&mut frames, &mut reader),
                )
                .await
                .map_err(|_| Error::ChallengeTimedOut)??
            } else {
                router_io::read_frame_with_timeout(&mut frames, &mut reader, FRAME_ASSEMBLY_TIMEOUT)
                    .await?
            };
            let Some(packet) = packet else { break };
            let deadline = challenge.as_ref().map(|challenge| challenge.deadline);
            let handled = handle_inbound(
                router,
                packet,
                connection,
                &outbound,
                &mut inbound,
                &mut challenge,
            );
            if let Some(deadline) = deadline {
                timeout_at(deadline, handled)
                    .await
                    .map_err(|_| Error::ChallengeTimedOut)??;
            } else {
                handled.await?;
            }
        }
        Ok(())
    };
    let write = async move {
        let mut frame = Vec::new();
        while let Some(message) = outbound_rx.recv().await {
            secure_outbound.seal(&mut frame, PacketKind::Record, &message)?;
            timeout(
                WRITE_TIMEOUT,
                tokio::io::AsyncWriteExt::write_all(&mut writer, &frame),
            )
            .await
            .map_err(|_| Error::WriteTimedOut)??;
        }
        Ok(())
    };
    let result = future::race(read, write).await;

    for qid in inbound.routes() {
        // a superseded connection must not remove the newer route
        router
            .routes
            .remove_if(&qid, |_, route| route.connection == connection);
    }
    result
}

async fn handle_inbound(
    router: &'static Router,
    packet: protocol::Frame,
    connection: ConnectionId,
    outbound: &mpsc::Sender<Vec<u8>>,
    inbound: &mut RouterConnection<'static>,
    challenge: &mut Option<ActiveChallenge>,
) -> Result<(), Error> {
    match inbound.receive(packet)? {
        RouterAction::Attach(bundle) => {
            let admission = router
                .challenge_capacity
                .try_acquire()
                .ok_or(Error::HandshakeOverloaded)?;
            let deadline = Instant::now() + HANDSHAKE_TIMEOUT;
            let handshake = router.handshake(|| inbound.begin_challenge(bundle));
            let (qid, request) = timeout_at(deadline, handshake)
                .await
                .map_err(|_| Error::ChallengeTimedOut)??;
            *challenge = Some(ActiveChallenge {
                deadline,
                _admission: admission,
            });
            timeout_at(deadline, outbound.send(request))
                .await
                .map_err(|_| Error::ChallengeTimedOut)?
                .map_err(|_| Error::WriterStopped)?;
            debug!(qid = %hex::encode(qid.0), "challenging QID");
            Ok(())
        }
        RouterAction::Authenticate(record) => {
            let active = challenge.take().ok_or(Error::Protocol)?;
            let _admission = active._admission;
            let accepted = router.handshake(|| inbound.verify_route(&record)).await?;
            outbound
                .send(accepted)
                .await
                .map_err(|_| Error::WriterStopped)?;
            let qid = inbound.commit_route()?;
            router.routes.insert(
                qid,
                Route {
                    connection,
                    outbound: outbound.clone(),
                },
            );
            info!(connection, qid = %hex::encode(qid.0), "authenticated QID");
            Ok(())
        }
        RouterAction::Forward {
            sender,
            recipient,
            record,
        } => {
            let Some(egress) = router
                .routes
                .get(&recipient)
                .map(|route| route.outbound.clone())
            else {
                debug!(
                    sender = %hex::encode(sender.0),
                    recipient = %hex::encode(recipient.0),
                    "no route for recipient"
                );
                return Ok(());
            };
            let _ = egress.send(record).await;
            Ok(())
        }
        RouterAction::Unauthenticated { sender, recipient } => {
            debug!(
                sender = %hex::encode(sender.0),
                recipient = %hex::encode(recipient.0),
                "dropping record from unauthenticated sender"
            );
            Ok(())
        }
    }
}

#[derive(Debug)]
enum Error {
    Io(io::Error),
    ChallengeTimedOut,
    HandshakeTimedOut,
    HandshakeOverloaded,
    WriterStopped,
    WriteTimedOut,
    Protocol,
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "I/O error: {error}"),
            Self::ChallengeTimedOut => f.write_str("route challenge timed out"),
            Self::HandshakeTimedOut => f.write_str("transport handshake timed out"),
            Self::HandshakeOverloaded => f.write_str("handshake capacity exhausted"),
            Self::WriterStopped => f.write_str("connection writer stopped"),
            Self::WriteTimedOut => f.write_str("connection write timed out"),
            Self::Protocol => f.write_str("transport protocol error"),
        }
    }
}

struct Capacity(Semaphore);

impl Capacity {
    fn new(permits: usize) -> Self {
        Self(Semaphore::new(permits))
    }

    async fn acquire(&self) -> SemaphorePermit<'_> {
        match self.0.acquire().await {
            Ok(permit) => permit,
            Err(_) => unreachable!("capacity semaphore cannot be closed"),
        }
    }

    fn try_acquire(&self) -> Option<SemaphorePermit<'_>> {
        self.0.try_acquire().ok()
    }
}
