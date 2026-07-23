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
    DEFAULT_ADDRESS, DEFAULT_UDP_PAYLOAD, MAX_RECORD_SIZE,
    protocol::{self, PacketKind, UdpKeys},
};
use ql_wire::{
    HandshakeId, IkHandshake, PeerBundle, PeerChallenge, QlHandshakeRecord, QlIdentity,
    RecordHeader, RecordType, RouteHeader, SessionKey, SoftwareCrypto, TransportParams,
    generate_identity,
};
use tokio::{
    net::{TcpListener, TcpStream, UdpSocket},
    sync::{Semaphore, SemaphorePermit, mpsc, oneshot},
    time::{Instant, timeout, timeout_at},
};
use tracing::{Instrument, Level, debug, debug_span, info};

const MAX_ROUTES_PER_CONNECTION: usize = 64;
const OUTBOUND_QUEUE_SIZE: usize = 16;
const HANDSHAKE_QUEUE_SIZE: usize = 32;
const MAX_PENDING_HANDSHAKES: usize = 1024;
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

type ConnectionId = u64;

struct ActiveChallenge {
    pending: PeerChallenge<&'static QlIdentity>,
    deadline: Instant,
    _admission: SemaphorePermit<'static>,
}

struct Router {
    identity: QlIdentity,
    handshakes: HandshakeExecutor,
    challenge_capacity: Semaphore,
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
    egress: Mutex<UdpEgress>,
    peer_max_payload: usize,
}

struct UdpEgress {
    next_packet: u64,
    address: Option<SocketAddr>,
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

enum Outbound {
    Record(Vec<u8>),
    UdpReady,
}

struct HandshakeExecutor {
    tx: Sender<Box<dyn FnOnce() + Send>>,
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
        .map_or(Ok(DEFAULT_UDP_PAYLOAD), |value| value.parse::<usize>())
        .context("parsing QL_ROUTER_UDP_MAX_PAYLOAD")?;
    if udp_max_payload > protocol::MAX_UDP_PAYLOAD {
        anyhow::bail!("invalid QL_ROUTER_UDP_MAX_PAYLOAD");
    }

    let identity_path = std::env::var("QL_ROUTER_IDENTITY_PATH")
        .unwrap_or_else(|_| "ql-router/identity.bin".into());
    let identity = match fs::read(&identity_path) {
        Ok(bytes) => {
            QlIdentity::decode_bytes(bytes.as_slice()).context("decoding router identity")?
        }
        Err(error) if error.kind() == ErrorKind::NotFound => {
            let identity = generate_identity(&SoftwareCrypto, "QL Router");
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

    let handshake_workers = std::thread::available_parallelism()
        .map_or(1, |parallelism| (parallelism.get() / 2).clamp(1, 4));
    let router: &'static Router = Box::leak(Box::new(Router {
        identity,
        handshakes: HandshakeExecutor::new(handshake_workers),
        challenge_capacity: Semaphore::new(MAX_PENDING_HANDSHAKES),
        connections: DashMap::new(),
        routes: DashMap::new(),
        udp,
        udp_max_payload,
    }));
    info!(
        workers = handshake_workers,
        queue = HANDSHAKE_QUEUE_SIZE,
        pending = MAX_PENDING_HANDSHAKES,
        routes_per_connection = MAX_ROUTES_PER_CONNECTION,
        udp_max_payload,
        "QL router limits configured"
    );
    info!(address = %listener.local_addr()?, "QL router listening on");

    run(router, listener).await
}

async fn run(router: &'static Router, listener: TcpListener) -> Result<()> {
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
            let connection = ConnectionId::decode_bytes(random.as_slice()).unwrap();
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
    let request = protocol::read_frame(&mut stream)
        .await?
        .ok_or(Error::Protocol)?;
    let NegotiatedTransport {
        tcp_tx,
        tcp_rx,
        udp,
    } = negotiate_transport(router, connection, request, &mut stream).await?;
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
                match message {
                    Outbound::Record(record) => protocol::seal_packet_into(
                        &mut packet,
                        &tcp_tx,
                        PacketKind::Record,
                        connection,
                        number,
                        &record,
                    ),
                    Outbound::UdpReady => protocol::seal_packet_into(
                        &mut packet,
                        &tcp_tx,
                        PacketKind::UdpReady,
                        connection,
                        number,
                        &[],
                    ),
                }
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
    let (response, tcp_tx, tcp_rx, udp_keys) = timeout(
        HANDSHAKE_TIMEOUT,
        router.handshakes.run(move || {
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
        }),
    )
    .await
    .map_err(|_| Error::ChallengeTimedOut)??;

    let session_id = response.session_id;
    protocol::write_frame(stream, &response.encode_vec()).await?;

    let confirmation = timeout(HANDSHAKE_TIMEOUT, protocol::read_frame(stream))
        .await
        .map_err(|_| Error::ChallengeTimedOut)??
        .ok_or(Error::Protocol)?;
    let confirmation = protocol::open_packet(&tcp_rx, session_id, &confirmation)?;
    if confirmation.kind != PacketKind::Confirm || confirmation.number != 0 {
        return Err(Error::Protocol);
    }
    let peer_max_payload = protocol::TransportConfirmation::decode_bytes(confirmation.payload)?
        .max_udp_payload as usize;
    if peer_max_payload > protocol::MAX_UDP_PAYLOAD {
        return Err(Error::Protocol);
    }

    let UdpKeys {
        tx: udp_tx,
        rx: udp_rx,
    } = udp_keys;
    Ok(NegotiatedTransport {
        tcp_tx,
        tcp_rx,
        udp: UdpSession {
            rx_key: udp_rx,
            tx_key: udp_tx,
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
        let bound_address = session.egress.lock().unwrap().address;
        if bound_address.is_some_and(|address| address != source) {
            continue;
        }
        let Ok(packet) = protocol::open_packet(&session.rx_key, session_id, packet) else {
            continue;
        };
        match packet.kind {
            PacketKind::Bind if packet.payload.is_empty() => {
                session.egress.lock().unwrap().address = Some(source);
                let _ = connection.outbound.try_send(Outbound::UdpReady);
            }
            PacketKind::Record => {
                if bound_address.is_none() || packet.payload.len() > MAX_RECORD_SIZE {
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
                let _ = route_record(router, Cow::Borrowed(packet.payload), header, session_id);
            }
            _ => {}
        }
    }
}

async fn handle_inbound(
    router: &'static Router,
    packet: Vec<u8>,
    connection: ConnectionId,
    outbound: &mpsc::Sender<Outbound>,
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
            let (qid, pending, request) = timeout(
                HANDSHAKE_TIMEOUT,
                router.handshakes.run(move || {
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
                }),
            )
            .await
            .map_err(|_| Error::ChallengeTimedOut)??;
            outbound
                .send(Outbound::Record(request))
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
        let (qid, accepted) = timeout(
            HANDSHAKE_TIMEOUT,
            router
                .handshakes
                .run(move || Ok(pending.verify(&SoftwareCrypto, &record)?)),
        )
        .await
        .map_err(|_| Error::ChallengeTimedOut)??;
        inbound.routes.insert(qid);
        router.routes.insert(qid, connection);
        outbound
            .send(Outbound::Record(accepted))
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
        let mut udp_egress = udp.egress.lock().unwrap();
        if let Some(address) = udp_egress.address {
            let Some(number) = protocol::next_packet_number(&mut udp_egress.next_packet) else {
                udp_egress.address = None;
                return Ok(());
            };
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
        let (tx, rx) = async_channel::bounded::<Box<dyn FnOnce() + Send>>(HANDSHAKE_QUEUE_SIZE);
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
        work: impl FnOnce() -> Result<T, Error> + Send + 'static,
    ) -> Result<T, Error>
    where
        T: Send + 'static,
    {
        let (result_tx, result_rx) = oneshot::channel();
        self.tx
            .try_send(Box::new(move || {
                let _ = result_tx.send(work());
            }))
            .map_err(|error| match error {
                TrySendError::Full(_) => Error::HandshakeOverloaded,
                TrySendError::Closed(_) => Error::HandshakeWorkerStopped,
            })?;
        result_rx.await.map_err(|_| Error::HandshakeWorkerStopped)?
    }
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
    RouteLimit,
    HandshakeOverloaded,
    HandshakeWorkerStopped,
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
            Self::RouteLimit => f.write_str("connection route limit reached"),
            Self::HandshakeOverloaded => f.write_str("handshake capacity exhausted"),
            Self::HandshakeWorkerStopped => f.write_str("handshake worker stopped"),
            Self::WriterStopped => f.write_str("connection writer stopped"),
            Self::Protocol => f.write_str("transport protocol error"),
        }
    }
}
