use std::{
    borrow::Cow,
    collections::HashSet,
    fmt, fs,
    io::{self, ErrorKind},
    net::SocketAddr,
    os::unix::fs::PermissionsExt,
    str::FromStr,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use anyhow::{Context, Result};
use async_channel::{Sender, TrySendError};
use dashmap::DashMap;
use ql_codec::{Decode, Encode};
use ql_common::QID;
use ql_router::{
    DEFAULT_ADDRESS, MAX_RECORD_SIZE,
    protocol::{self, Frame, PacketKind, ReplayWindow, TransportKeys},
};
use ql_wire::{
    HandshakeId, IkHandshake, PeerBundle, PeerChallenge, QL_WIRE_VERSION, QlHandshakeRecord,
    QlIdentity, RecordHeader, RecordType, RouteHeader, SessionKey, SoftwareCrypto, TransportParams,
    generate_identity,
};
use tokio::{
    net::{TcpListener, TcpStream, UdpSocket},
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
    UnexpectedFrame,
    InvalidControl,
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
    connections: DashMap<ConnectionId, Arc<Connection>>,
    routes: DashMap<QID, ConnectionId>,
    udp: UdpSocket,
    udp_max_payload: usize,
}

struct Connection {
    outbound: mpsc::Sender<Outbound>,
    udp: UdpSession,
}

struct UdpSession {
    rx_key: SessionKey,
    tx_key: SessionKey,
    rx_replay: Mutex<ReplayWindow>,
    egress: Mutex<UdpEgress>,
    peer_max_payload: usize,
}

struct UdpEgress {
    next_packet: u64,
    address: Option<SocketAddr>,
    packet: Vec<u8>,
}

struct ControlReceiver {
    session_id: u64,
    key: SessionKey,
    replay: ReplayWindow,
}

struct Inbound {
    control: ControlReceiver,
    challenge: Option<ActiveChallenge>,
    next_handshake_id: u32,
    routes: HashSet<QID>,
}

struct NegotiatedTransport {
    control_tx: SessionKey,
    control_rx: ControlReceiver,
    udp: UdpSession,
}

enum Outbound {
    Record(Vec<u8>),
    Control(PacketKind),
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
    let udp = UdpSocket::bind(listener.local_addr()?).await?;
    let udp_max_payload = std::env::var("QL_ROUTER_UDP_MAX_PAYLOAD")
        .map_or(Ok(1200), |value| value.parse::<usize>())
        .context("parsing QL_ROUTER_UDP_MAX_PAYLOAD")?;
    if !(protocol::PACKET_OVERHEAD + RecordHeader::WIRE_SIZE
        ..=MAX_RECORD_SIZE + protocol::PACKET_OVERHEAD)
        .contains(&udp_max_payload)
    {
        anyhow::bail!("invalid QL_ROUTER_UDP_MAX_PAYLOAD");
    }

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
        identity,
        handshakes: HandshakeExecutor::new(handshake_workers),
        challenge_capacity: Arc::new(Semaphore::new(MAX_PENDING_HANDSHAKES)),
        connections: DashMap::new(),
        routes: DashMap::new(),
        udp,
        udp_max_payload,
    });
    info!(
        workers = handshake_workers,
        queue = HANDSHAKE_QUEUE_SIZE,
        pending = MAX_PENDING_HANDSHAKES,
        routes_per_connection = MAX_ROUTES_PER_CONNECTION,
        udp_max_payload,
        "QL router limits configured"
    );
    info!(address = %listener.local_addr()?, "QL router listening on");

    run(listener, router).await
}

async fn run(listener: TcpListener, router: Arc<Router>) -> Result<()> {
    let udp_router = router.clone();
    tokio::spawn(async move {
        if let Err(error) = serve_udp(&udp_router).await {
            tracing::error!(%error, "UDP listener stopped");
        }
    });

    let mut next_connection: ConnectionId = 1;
    loop {
        let (stream, remote) = listener.accept().await?;
        let connection = next_connection;
        next_connection = next_connection
            .checked_add(1)
            .context("connection ID exhausted")?;
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
    mut stream: TcpStream,
    router: &Router,
) -> ConnectionResult<()> {
    let first = protocol::receive_frame(&mut stream)
        .await?
        .ok_or(ConnectionError::UnexpectedFrame)?;
    let Frame::TransportInit(request) = first else {
        return Err(ConnectionError::UnexpectedFrame);
    };
    let NegotiatedTransport {
        control_tx,
        control_rx,
        udp,
    } = negotiate_transport(connection, request, &mut stream, router).await?;
    let (mut reader, mut writer) = stream.into_split();
    let (outbound, mut outbound_rx) = mpsc::channel(OUTBOUND_QUEUE_SIZE);

    let connection_state = Arc::new(Connection {
        outbound: outbound.clone(),
        udp,
    });
    router
        .connections
        .insert(connection, connection_state.clone());

    let writer = tokio::spawn(
        async move {
            let mut control_packet: u64 = 0;
            while let Some(message) = outbound_rx.recv().await {
                let frame = match message {
                    Outbound::Record(record) => Frame::Record(record),
                    Outbound::Control(kind) => {
                        let Some(next_packet) = control_packet.checked_add(1) else {
                            break;
                        };
                        let packet = protocol::seal_packet(
                            &control_tx,
                            kind,
                            connection,
                            control_packet,
                            &[],
                        );
                        control_packet = next_packet;
                        Frame::Authenticated(packet)
                    }
                };
                if let Err(error) = protocol::send_frame(&mut writer, &frame).await {
                    debug!(%error, "connection write failed");
                    break;
                }
            }
        }
        .in_current_span(),
    );

    let mut inbound = Inbound {
        control: control_rx,
        challenge: None,
        next_handshake_id: 1,
        routes: HashSet::new(),
    };
    let result = async {
        loop {
            let frame = if let Some(challenge) = inbound.challenge.as_ref() {
                timeout_at(challenge.deadline, protocol::receive_frame(&mut reader))
                    .await
                    .map_err(|_| ConnectionError::ChallengeTimedOut)??
            } else {
                protocol::receive_frame(&mut reader).await?
            };
            let Some(frame) = frame else { break };
            handle_inbound(frame, connection, router, &outbound, &mut inbound).await?;
        }
        Ok(())
    }
    .await;

    writer.abort();
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
    connection: ConnectionId,
    request: Vec<u8>,
    stream: &mut TcpStream,
    router: &Router,
) -> ConnectionResult<NegotiatedTransport> {
    let identity = router.identity.clone();
    let (response, keys) = timeout(
        HANDSHAKE_TIMEOUT,
        router.handshakes.run(move || {
            let crypto = SoftwareCrypto;
            let (header, request) =
                ql_wire::decode_record::<QlHandshakeRecord, _>(request.as_slice())?;
            if header.version != QL_WIRE_VERSION
                || header.route.recipient != identity.qid
                || header.record_type != RecordType::Handshake
            {
                return Err(ConnectionError::InvalidControl);
            }
            let QlHandshakeRecord::Ik1(request) = request else {
                return Err(ConnectionError::InvalidControl);
            };
            let mut handshake = IkHandshake::new_ik_responder(
                &crypto,
                identity.clone(),
                None,
                TransportParams::default(),
            );
            handshake.read_1(&crypto, header.route, &request)?;
            let response = handshake.write_2(&crypto, request.handshake_id)?;
            let response = ql_wire::encode_record_vec(
                RecordHeader::new(
                    RouteHeader {
                        sender: header.route.recipient,
                        recipient: header.route.sender,
                    },
                    RecordType::Handshake,
                ),
                &QlHandshakeRecord::Ik2(response),
            );
            let finalized = handshake.finalize(&crypto)?;
            Ok((response, protocol::derive_keys(&finalized)))
        }),
    )
    .await
    .map_err(|_| ConnectionError::ChallengeTimedOut)??;

    let session_id = connection;
    let mut payload = Vec::with_capacity(12 + response.len());
    payload.extend_from_slice(&session_id.to_be_bytes());
    payload.extend_from_slice(&(router.udp_max_payload as u32).to_be_bytes());
    payload.extend_from_slice(&response);
    protocol::send_frame(stream, &Frame::TransportResponse(payload)).await?;

    let confirmation = timeout(HANDSHAKE_TIMEOUT, protocol::receive_frame(stream))
        .await
        .map_err(|_| ConnectionError::ChallengeTimedOut)??
        .ok_or(ConnectionError::UnexpectedFrame)?;
    let Frame::Authenticated(confirmation) = confirmation else {
        return Err(ConnectionError::UnexpectedFrame);
    };
    let confirmation = protocol::open_packet(&keys.control_rx, session_id, &confirmation)?;
    let mut replay = ReplayWindow::default();
    if confirmation.kind != PacketKind::Confirm
        || confirmation.payload.len() != 4
        || !replay.accept(confirmation.number)
    {
        return Err(ConnectionError::InvalidControl);
    }
    let peer_max_payload = u32::from_be_bytes(confirmation.payload.try_into().unwrap()) as usize;
    if !(protocol::PACKET_OVERHEAD + RecordHeader::WIRE_SIZE
        ..=MAX_RECORD_SIZE + protocol::PACKET_OVERHEAD)
        .contains(&peer_max_payload)
    {
        return Err(ConnectionError::InvalidControl);
    }

    let TransportKeys {
        control_tx,
        control_rx,
        udp_tx,
        udp_rx,
    } = keys;
    Ok(NegotiatedTransport {
        control_tx,
        control_rx: ControlReceiver {
            session_id,
            key: control_rx,
            replay,
        },
        udp: UdpSession {
            rx_key: udp_rx,
            tx_key: udp_tx,
            rx_replay: Mutex::new(ReplayWindow::default()),
            egress: Mutex::new(UdpEgress {
                next_packet: 0,
                address: None,
                packet: Vec::with_capacity(peer_max_payload),
            }),
            peer_max_payload,
        },
    })
}

async fn serve_udp(router: &Router) -> io::Result<()> {
    let mut buffer = vec![0; router.udp_max_payload];
    loop {
        let (len, source) = router.udp.recv_from(&mut buffer).await?;
        let packet = &buffer[..len];
        if packet.len() < protocol::PACKET_OVERHEAD {
            continue;
        }
        let session_id = u64::from_be_bytes(packet[1..9].try_into().unwrap());
        let Some(connection) = router
            .connections
            .get(&session_id)
            .map(|connection| connection.clone())
        else {
            continue;
        };
        let session = &connection.udp;
        let Ok(packet) = protocol::open_packet(&session.rx_key, session_id, packet) else {
            continue;
        };
        if !session.rx_replay.lock().unwrap().accept(packet.number) {
            continue;
        }
        match packet.kind {
            PacketKind::Bind if packet.payload.is_empty() => {
                session.egress.lock().unwrap().address = Some(source);
                let _ = connection
                    .outbound
                    .try_send(Outbound::Control(PacketKind::UdpReady));
            }
            PacketKind::Record => {
                let source_is_bound = session.egress.lock().unwrap().address == Some(source);
                if !source_is_bound || packet.payload.len() > MAX_RECORD_SIZE {
                    continue;
                }
                let Ok(header) = RecordHeader::decode_bytes(packet.payload) else {
                    continue;
                };
                if header.record_type != RecordType::Session
                    || header.route.recipient == router.identity.qid
                {
                    continue;
                }
                let _ = route_record(Cow::Borrowed(packet.payload), header, session_id, router);
            }
            _ => {}
        }
    }
}

async fn handle_inbound(
    frame: Frame,
    connection: ConnectionId,
    router: &Router,
    outbound: &mpsc::Sender<Outbound>,
    inbound: &mut Inbound,
) -> ConnectionResult<()> {
    let record = match frame {
        Frame::Authenticated(packet) => {
            let packet =
                protocol::open_packet(&inbound.control.key, inbound.control.session_id, &packet)?;
            if packet.kind != PacketKind::Attach || !inbound.control.replay.accept(packet.number) {
                return Err(ConnectionError::InvalidControl);
            }
            if inbound.challenge.is_some() {
                return Err(ConnectionError::ChallengeActive);
            }
            if inbound.routes.len() == MAX_ROUTES_PER_CONNECTION {
                return Err(ConnectionError::RouteLimit);
            }
            let handshake_id = HandshakeId(inbound.next_handshake_id);
            inbound.next_handshake_id = inbound
                .next_handshake_id
                .checked_add(1)
                .ok_or(ConnectionError::InvalidControl)?;
            let admission = router
                .challenge_capacity
                .clone()
                .try_acquire_owned()
                .map_err(|_| ConnectionError::HandshakeOverloaded)?;
            let identity = router.identity.clone();
            let bundle = packet.payload.to_vec();
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
                .send(Outbound::Record(request))
                .await
                .map_err(|_| ConnectionError::WriterStopped)?;
            inbound.challenge = Some(ActiveChallenge {
                pending,
                deadline: Instant::now() + HANDSHAKE_TIMEOUT,
                _admission: admission,
            });
            debug!(qid = %hex::encode(qid.0), "challenging QID");
            return Ok(());
        }
        Frame::Record(record) => {
            if inbound.routes.is_empty() && inbound.challenge.is_none() {
                return Err(ConnectionError::AttachRequired);
            }
            record
        }
        Frame::TransportInit(_) | Frame::TransportResponse(_) => {
            return Err(ConnectionError::UnexpectedFrame);
        }
    };
    let header = RecordHeader::decode_bytes(record.as_slice())?;
    if header.version != QL_WIRE_VERSION {
        return Err(ConnectionError::UnsupportedVersion);
    }
    if header.route.recipient == router.identity.qid {
        let challenge = inbound
            .challenge
            .take()
            .ok_or(ConnectionError::ChallengeMissing)?;
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
        inbound.routes.insert(qid);
        router.routes.insert(qid, connection);
        outbound
            .send(Outbound::Record(accepted))
            .await
            .map_err(|_| ConnectionError::WriterStopped)?;
        info!(connection, qid = %hex::encode(qid.0), "authenticated QID");
        return Ok(());
    }

    route_record(Cow::Owned(record), header, connection, router)
}

fn route_record<'a>(
    record: Cow<'a, [u8]>,
    header: RecordHeader,
    ingress: ConnectionId,
    router: &Router,
) -> ConnectionResult<()> {
    if header.version != QL_WIRE_VERSION {
        return Err(ConnectionError::UnsupportedVersion);
    }
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

    if header.record_type == RecordType::Session
        && record.len() + protocol::PACKET_OVERHEAD <= egress.udp.peer_max_payload
    {
        let udp = &egress.udp;
        let mut udp_egress = udp.egress.lock().unwrap();
        if let Some(address) = udp_egress.address {
            let Some(next_packet) = udp_egress.next_packet.checked_add(1) else {
                udp_egress.address = None;
                return Ok(());
            };
            let number = udp_egress.next_packet;
            udp_egress.next_packet = next_packet;
            protocol::seal_packet_into(
                &mut udp_egress.packet,
                &udp.tx_key,
                PacketKind::Record,
                egress_id,
                number,
                record.as_ref(),
            );
            if let Err(error) = router.udp.try_send_to(&udp_egress.packet, address)
                && error.kind() != io::ErrorKind::WouldBlock
            {
                udp_egress.address = None;
            }
            return Ok(());
        }
    }

    if let Err(mpsc::error::TrySendError::Full(_)) = egress
        .outbound
        .try_send(Outbound::Record(record.into_owned()))
    {
        debug!(
            connection = egress_id,
            "outbound queue full; dropping record"
        );
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
            Self::UnexpectedFrame => f.write_str("unexpected router frame"),
            Self::InvalidControl => f.write_str("invalid authenticated control frame"),
        }
    }
}
