use std::{
    borrow::Cow,
    collections::HashSet,
    fmt, fs,
    io::{self, ErrorKind},
    net::SocketAddr,
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    str::FromStr,
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};

use dashmap::DashMap;
use figment::{
    Figment,
    providers::{Env, Serialized},
};
use ql_codec::{Decode, Encode};
use ql_common::QID;
use ql_router::{
    DEFAULT_ADDRESS, DEFAULT_UDP_PAYLOAD, MAX_RECORD_SIZE,
    protocol::{self, PacketKind},
};
use ql_wire::{
    HandshakeId, IkHandshake, PeerBundle, PeerChallenge, QlHandshakeRecord, QlIdentity,
    RecordHeader, RecordType, RouteHeader, SessionKey, SoftwareCrypto, TransportParams,
    generate_identity,
};
use serde::{Deserialize, Serialize};
use tokio::{
    net::{TcpListener, TcpStream, UdpSocket},
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
    udp_max_payload: usize,
    identity_path: PathBuf,
    bundle_path: PathBuf,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            log: "INFO".into(),
            address: DEFAULT_ADDRESS.into(),
            udp_max_payload: DEFAULT_UDP_PAYLOAD,
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
    udp: UdpSocket,
    udp_max_payload: usize,
}

impl Router {
    async fn handshake<T>(&self, work: impl FnOnce() -> T) -> T {
        let _permit = self.handshake_capacity.acquire().await.unwrap();
        work()
    }
}

struct Connection {
    outbound: mpsc::Sender<Vec<u8>>,
    udp: UdpSession,
}

struct UdpSession {
    rx_key: SessionKey,
    tx_key: SessionKey,
    address: OnceLock<SocketAddr>,
    sender: Mutex<UdpSender>,
    peer_max_payload: usize,
}

struct UdpSender {
    next_packet: u64,
    packet: Vec<u8>,
}

struct Inbound {
    tcp_key: SessionKey,
    next_tcp_packet: u64,
    challenge: Option<ActiveChallenge>,
    next_handshake_id: u32,
    routes: HashSet<QID>,
}

struct NegotiatedTransport {
    tcp_tx: SessionKey,
    tcp_rx: SessionKey,
    udp: UdpSession,
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
    let udp = UdpSocket::bind(listener.local_addr()?).await?;
    let udp_max_payload = config.udp_max_payload;
    if udp_max_payload > protocol::MAX_UDP_PAYLOAD {
        anyhow::bail!("invalid QL_ROUTER_UDP_MAX_PAYLOAD");
    }

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
        udp,
        udp_max_payload,
    }));
    info!(
        workers = handshake_workers,
        pending = MAX_PENDING_HANDSHAKES,
        routes_per_connection = MAX_ROUTES_PER_CONNECTION,
        udp_max_payload,
        "QL router limits configured"
    );
    info!(address = %listener.local_addr()?, "QL router listening on");

    run(router, listener).await
}

async fn run(router: &'static Router, listener: TcpListener) -> anyhow::Result<()> {
    tokio::spawn(async move {
        if let Err(error) = serve_udp(router).await {
            tracing::error!(%error, "UDP listener stopped");
        }
    });

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
    let NegotiatedTransport {
        tcp_tx,
        tcp_rx,
        udp,
    } = timeout(HANDSHAKE_TIMEOUT, async {
        let request = protocol::read_frame(&mut stream)
            .await?
            .ok_or(Error::Protocol)?;
        negotiate_transport(router, connection, request, &mut stream).await
    })
    .await
    .map_err(|_| Error::HandshakeTimedOut)??;
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
            let mut packet_number: u64 = 0;
            let mut packet = Vec::new();
            while let Some(message) = outbound_rx.recv().await {
                let Some(number) = protocol::next_packet_number(&mut packet_number) else {
                    break;
                };
                protocol::seal_packet_into(
                    &mut packet,
                    &tcp_tx,
                    PacketKind::Record,
                    connection,
                    number,
                    &message,
                );
                if let Err(error) = protocol::write_frame(&mut writer, &packet).await {
                    debug!(%error, "connection write failed");
                    break;
                }
            }
        }
        .in_current_span(),
    );

    let mut inbound = Inbound {
        tcp_key: tcp_rx,
        next_tcp_packet: 1,
        challenge: None,
        next_handshake_id: 1,
        routes: HashSet::new(),
    };
    let result = async {
        loop {
            let packet = if let Some(challenge) = inbound.challenge.as_ref() {
                timeout_at(challenge.deadline, protocol::read_frame(&mut reader))
                    .await
                    .map_err(|_| Error::ChallengeTimedOut)??
            } else {
                protocol::read_frame(&mut reader).await?
            };
            let Some(packet) = packet else { break };
            handle_inbound(router, packet, connection, &outbound, &mut inbound).await?;
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
    router: &'static Router,
    connection: ConnectionId,
    request: Vec<u8>,
    stream: &mut TcpStream,
) -> Result<NegotiatedTransport, Error> {
    let (response, tcp_tx, tcp_rx, udp_keys) = router
        .handshake(|| {
            let (header, request) =
                ql_wire::decode_record::<QlHandshakeRecord, _>(request.as_slice())?;
            if header.route.recipient != router.identity.qid
                || header.record_type != RecordType::Handshake
            {
                return Err(Error::Protocol);
            }
            let QlHandshakeRecord::Ik1(request) = request else {
                return Err(Error::Protocol);
            };
            let mut handshake = IkHandshake::new_ik_responder(
                &SoftwareCrypto,
                &router.identity,
                None,
                TransportParams::default(),
            );
            handshake.read_1(&SoftwareCrypto, header.route, &request)?;
            let response = handshake.write_2(&SoftwareCrypto, request.handshake_id)?;
            let response = protocol::TransportResponse {
                session_id: connection,
                max_udp_payload: router.udp_max_payload as u32,
                header: RecordHeader::new(
                    RouteHeader {
                        sender: header.route.recipient,
                        recipient: header.route.sender,
                    },
                    RecordType::Handshake,
                ),
                handshake: QlHandshakeRecord::Ik2(response),
            };
            let finalized = handshake.finalize(&SoftwareCrypto)?;
            let udp_keys = protocol::derive_udp_keys(&finalized);
            Ok((response, finalized.tx_key, finalized.rx_key, udp_keys))
        })
        .await?;

    let session_id = response.session_id;
    protocol::write_frame(stream, &response.encode_vec()).await?;

    let confirmation = protocol::read_frame(stream).await?.ok_or(Error::Protocol)?;
    let confirmation = protocol::open_packet(&tcp_rx, session_id, &confirmation)?;
    if confirmation.kind != PacketKind::Confirm || confirmation.number != 0 {
        return Err(Error::Protocol);
    }
    let peer_max_payload = protocol::TransportConfirmation::decode_bytes(confirmation.payload)?
        .max_udp_payload as usize;
    if peer_max_payload > protocol::MAX_UDP_PAYLOAD {
        return Err(Error::Protocol);
    }

    Ok(NegotiatedTransport {
        tcp_tx,
        tcp_rx,
        udp: UdpSession {
            rx_key: udp_keys.rx,
            tx_key: udp_keys.tx,
            address: OnceLock::new(),
            sender: Mutex::new(UdpSender {
                next_packet: 0,
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
        let Ok(header) = protocol::PacketHeader::decode_bytes(packet) else {
            continue;
        };
        let session_id = header.session_id;
        let Some(connection) = router
            .connections
            .get(&session_id)
            .map(|connection| connection.clone())
        else {
            continue;
        };
        let session = &connection.udp;
        let bound = session.address.get();
        if bound.is_some_and(|address| *address != source) {
            continue;
        }
        let Ok(packet) = protocol::open_packet(&session.rx_key, session_id, packet) else {
            continue;
        };
        if packet.kind == PacketKind::Record {
            if packet.payload.len() > MAX_RECORD_SIZE {
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
            if bound.is_none() {
                session.address.set(source).unwrap();
            }
            let _ = route_record(router, Cow::Borrowed(packet.payload), header, session_id);
        }
    }
}

async fn handle_inbound(
    router: &'static Router,
    packet: Vec<u8>,
    connection: ConnectionId,
    outbound: &mpsc::Sender<Vec<u8>>,
    inbound: &mut Inbound,
) -> Result<(), Error> {
    let (kind, number, payload) =
        protocol::open_packet_owned(&inbound.tcp_key, connection, packet)?;
    if protocol::next_packet_number(&mut inbound.next_tcp_packet) != Some(number) {
        return Err(Error::Protocol);
    }

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
                    let bundle = PeerBundle::decode_bytes(payload.as_slice())?;
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
        inbound.routes.insert(qid);
        router.routes.insert(qid, connection);
        outbound
            .send(accepted)
            .await
            .map_err(|_| Error::WriterStopped)?;
        info!(connection, qid = %hex::encode(qid.0), "authenticated QID");
        return Ok(());
    }

    route_record(router, Cow::Owned(record), header, connection)
}

fn route_record<'a>(
    router: &Router,
    record: Cow<'a, [u8]>,
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

    if header.record_type == RecordType::Session
        && record.len() + protocol::PACKET_OVERHEAD <= egress.udp.peer_max_payload
    {
        let udp = &egress.udp;
        if let Some(&address) = udp.address.get() {
            let mut sender = udp.sender.lock().unwrap();
            let Some(number) = protocol::next_packet_number(&mut sender.next_packet) else {
                return Ok(());
            };
            protocol::seal_packet_into(
                &mut sender.packet,
                &udp.tx_key,
                PacketKind::Record,
                egress_id,
                number,
                record.as_ref(),
            );
            let _ = router.udp.try_send_to(&sender.packet, address);
            return Ok(());
        }
    }

    if let Err(mpsc::error::TrySendError::Full(_)) = egress.outbound.try_send(record.into_owned()) {
        debug!(
            connection = egress_id,
            "outbound queue full; dropping record"
        );
    }
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
