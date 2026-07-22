use std::{io, sync::Arc, time::Duration};

use ql_codec::{Decode, Encode};
use ql_wire::{
    HandshakeId, IkHandshake, PeerBundle, QlHandshakeRecord, RecordHeader, RecordType, RouteHeader,
    SoftwareCrypto, TransportParams, generate_identity,
};
use tokio::{
    net::{TcpStream, ToSocketAddrs, UdpSocket, tcp::OwnedReadHalf, tcp::OwnedWriteHalf},
    time::{Instant, timeout},
};

use crate::protocol::{Frame, PacketKind, ReplayWindow};

pub const DEFAULT_ADDRESS: &str = "127.0.0.1:7447";
pub const MAX_RECORD_SIZE: usize = 8 * 1024;

#[derive(Debug, Clone, Copy)]
pub struct UdpConfig {
    pub max_payload: usize,
}

impl Default for UdpConfig {
    fn default() -> Self {
        Self { max_payload: 1200 }
    }
}

pub struct Receiver {
    tcp: OwnedReadHalf,
    udp: UdpReceiver,
    control: ControlReceiver,
}

pub struct Sender {
    tcp: OwnedWriteHalf,
    udp: UdpSender,
    control: ControlSender,
}

struct UdpReceiver {
    socket: Arc<UdpSocket>,
    session_id: u64,
    key: ql_wire::SessionKey,
    replay: ReplayWindow,
    buffer: Vec<u8>,
}

struct UdpSender {
    socket: Arc<UdpSocket>,
    session_id: u64,
    key: ql_wire::SessionKey,
    next_packet: u64,
    max_payload: usize,
    active: bool,
    buffer: Vec<u8>,
}

struct ControlSender {
    session_id: u64,
    key: ql_wire::SessionKey,
    next_packet: u64,
}

struct ControlReceiver {
    session_id: u64,
    key: ql_wire::SessionKey,
    replay: ReplayWindow,
}

pub async fn connect_udp(
    address: impl ToSocketAddrs,
    router: &PeerBundle,
) -> io::Result<(Receiver, Sender)> {
    connect_udp_with_config(address, router, UdpConfig::default()).await
}

pub async fn connect_udp_with_config(
    address: impl ToSocketAddrs,
    router: &PeerBundle,
    config: UdpConfig,
) -> io::Result<(Receiver, Sender)> {
    if !(protocol::PACKET_OVERHEAD + RecordHeader::WIRE_SIZE
        ..=MAX_RECORD_SIZE + protocol::PACKET_OVERHEAD)
        .contains(&config.max_payload)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid UDP payload limit",
        ));
    }

    let crypto = SoftwareCrypto;
    router.validate(&crypto).map_err(invalid_data)?;
    let mut tcp = TcpStream::connect(address).await?;
    tcp.set_nodelay(true)?;
    let peer = tcp.peer_addr()?;
    let udp = Arc::new(
        UdpSocket::bind(if peer.is_ipv4() {
            "0.0.0.0:0"
        } else {
            "[::]:0"
        })
        .await?,
    );
    udp.connect(peer).await?;

    let identity = generate_identity(&crypto, "QL router transport");
    let route = RouteHeader {
        sender: identity.qid,
        recipient: router.qid,
    };
    let mut handshake = IkHandshake::new_ik_initiator(
        &crypto,
        identity,
        router.clone(),
        TransportParams::default(),
    );
    let mut random = [0; 4];
    ql_wire::QlRandom::fill_random_bytes(&crypto, &mut random);
    let handshake_id = HandshakeId(u32::from_be_bytes(random));
    let request = ql_wire::encode_record_vec(
        RecordHeader::new(route, RecordType::Handshake),
        &QlHandshakeRecord::Ik1(
            handshake
                .write_1(&crypto, handshake_id)
                .map_err(invalid_data)?,
        ),
    );
    protocol::send_frame(&mut tcp, &Frame::TransportInit(request)).await?;

    let Frame::TransportResponse(response) = protocol::receive_frame(&mut tcp)
        .await?
        .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "router disconnected"))?
    else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unexpected transport handshake frame",
        ));
    };
    if response.len() < 12 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "truncated transport response",
        ));
    }
    let session_id = u64::from_be_bytes(response[..8].try_into().unwrap());
    let router_max_payload = u32::from_be_bytes(response[8..12].try_into().unwrap()) as usize;
    if !(protocol::PACKET_OVERHEAD + RecordHeader::WIRE_SIZE
        ..=MAX_RECORD_SIZE + protocol::PACKET_OVERHEAD)
        .contains(&router_max_payload)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid router UDP payload limit",
        ));
    }
    let (header, response) =
        ql_wire::decode_record::<QlHandshakeRecord, _>(&response[12..]).map_err(invalid_data)?;
    let QlHandshakeRecord::Ik2(response) = response else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unexpected transport handshake record",
        ));
    };
    handshake
        .read_2(&crypto, header.route, &response)
        .map_err(invalid_data)?;
    let finalized = handshake.finalize(&crypto).map_err(invalid_data)?;
    if finalized.remote_bundle != *router {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "transport peer does not match router",
        ));
    }
    let keys = protocol::derive_keys(&finalized);

    let confirmation = protocol::seal_packet(
        &keys.control_tx,
        PacketKind::Confirm,
        session_id,
        0,
        &(config.max_payload as u32).to_be_bytes(),
    );
    protocol::send_frame(&mut tcp, &Frame::Authenticated(confirmation)).await?;

    let mut udp_packet: u64 = 0;
    let mut control_replay = ReplayWindow::default();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let next_packet = udp_packet
            .checked_add(1)
            .ok_or_else(|| io::Error::other("UDP packet number exhausted during activation"))?;
        let bind =
            protocol::seal_packet(&keys.udp_tx, PacketKind::Bind, session_id, udp_packet, &[]);
        udp_packet = next_packet;
        udp.send(&bind).await?;

        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "UDP activation timed out",
            ));
        }
        match timeout(
            remaining.min(Duration::from_millis(250)),
            protocol::receive_frame(&mut tcp),
        )
        .await
        {
            Ok(Ok(Some(Frame::Authenticated(packet)))) => {
                let packet = protocol::open_packet(&keys.control_rx, session_id, &packet)?;
                if packet.kind == PacketKind::UdpReady
                    && packet.payload.is_empty()
                    && control_replay.accept(packet.number)
                {
                    break;
                }
            }
            Ok(Ok(Some(_))) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unexpected frame during UDP activation",
                ));
            }
            Ok(Ok(None)) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "router disconnected during UDP activation",
                ));
            }
            Ok(Err(error)) => return Err(error),
            Err(_) => {}
        }
    }

    let (tcp, writer) = tcp.into_split();
    Ok((
        Receiver {
            tcp,
            udp: UdpReceiver {
                socket: udp.clone(),
                session_id,
                key: keys.udp_rx,
                replay: ReplayWindow::default(),
                buffer: vec![0; config.max_payload],
            },
            control: ControlReceiver {
                session_id,
                key: keys.control_rx,
                replay: control_replay,
            },
        },
        Sender {
            tcp: writer,
            udp: UdpSender {
                socket: udp,
                session_id,
                key: keys.udp_tx,
                next_packet: udp_packet,
                max_payload: router_max_payload,
                active: true,
                buffer: Vec::with_capacity(router_max_payload),
            },
            control: ControlSender {
                session_id,
                key: keys.control_tx,
                next_packet: 1,
            },
        },
    ))
}

pub async fn receive(receiver: &mut Receiver) -> io::Result<Option<Vec<u8>>> {
    let Receiver { tcp, udp, control } = receiver;
    loop {
        tokio::select! {
            frame = protocol::receive_frame(tcp) => match frame? {
                Some(Frame::Record(record)) => return Ok(Some(record)),
                Some(Frame::Authenticated(packet)) => {
                    let packet = protocol::open_packet(&control.key, control.session_id, &packet)?;
                    if packet.kind == PacketKind::UdpReady
                        && packet.payload.is_empty()
                        && control.replay.accept(packet.number)
                    {
                        continue;
                    }
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "unexpected router control"));
                }
                Some(_) => return Err(io::Error::new(io::ErrorKind::InvalidData, "unexpected router frame")),
                None => return Ok(None),
            },
            received = udp.socket.recv(&mut udp.buffer) => {
                let len = received?;
                let Ok(packet) = protocol::open_packet(&udp.key, udp.session_id, &udp.buffer[..len]) else {
                    continue;
                };
                if packet.kind == PacketKind::Record && udp.replay.accept(packet.number) {
                    return Ok(Some(packet.payload.to_vec()));
                }
            }
        }
    }
}

pub async fn send(sender: &mut Sender, record: &[u8]) -> io::Result<()> {
    let udp = &mut sender.udp;
    if udp.active
        && record.len() + protocol::PACKET_OVERHEAD <= udp.max_payload
        && RecordHeader::decode_bytes(record)
            .is_ok_and(|header| header.record_type == RecordType::Session)
    {
        if let Some(next_packet) = udp.next_packet.checked_add(1) {
            protocol::seal_packet_into(
                &mut udp.buffer,
                &udp.key,
                PacketKind::Record,
                udp.session_id,
                udp.next_packet,
                record,
            );
            udp.next_packet = next_packet;
            match udp.socket.try_send(&udp.buffer) {
                Ok(_) => return Ok(()),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(()),
                Err(_) => {
                    udp.active = false;
                    return Ok(());
                }
            }
        }
        udp.active = false;
    }
    protocol::send_frame(&mut sender.tcp, &Frame::Record(record.to_vec())).await
}

pub async fn attach(sender: &mut Sender, bundle: &PeerBundle) -> io::Result<()> {
    let payload = bundle.encode_vec();
    let control = &mut sender.control;
    let next_packet = control
        .next_packet
        .checked_add(1)
        .ok_or_else(|| io::Error::other("control packet number exhausted"))?;
    let packet = protocol::seal_packet(
        &control.key,
        PacketKind::Attach,
        control.session_id,
        control.next_packet,
        &payload,
    );
    control.next_packet = next_packet;
    protocol::send_frame(&mut sender.tcp, &Frame::Authenticated(packet)).await
}

fn invalid_data(error: impl std::error::Error + Send + Sync + 'static) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

pub mod protocol {
    use std::io;

    use hkdf::Hkdf;
    use ql_wire::{
        ENCRYPTED_MESSAGE_AUTH_SIZE, FinalizedHandshake, Nonce, QlAead, SessionKey, SoftwareCrypto,
    };
    use sha2::Sha256;
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

    use crate::MAX_RECORD_SIZE;

    pub const PACKET_OVERHEAD: usize = 1 + 8 + 8 + ENCRYPTED_MESSAGE_AUTH_SIZE;

    const DATA_FRAME: u8 = 1;
    const TRANSPORT_INIT_FRAME: u8 = 3;
    const TRANSPORT_RESPONSE_FRAME: u8 = 4;
    const AUTHENTICATED_FRAME: u8 = 5;
    const FRAME_HEADER_SIZE: usize = 5;
    const PACKET_VERSION: u8 = 1;
    const PACKET_HEADER_SIZE: usize = PACKET_OVERHEAD - ENCRYPTED_MESSAGE_AUTH_SIZE;
    const CONTROL_KEY_INFO: &[u8] = b"ql-router:transport-keys:v1:control";
    const UDP_KEY_INFO: &[u8] = b"ql-router:transport-keys:v1:udp";

    pub enum Frame {
        Record(Vec<u8>),
        TransportInit(Vec<u8>),
        TransportResponse(Vec<u8>),
        Authenticated(Vec<u8>),
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    #[repr(u8)]
    pub enum PacketKind {
        Confirm = 1,
        Attach = 2,
        UdpReady = 3,
        Bind = 4,
        Record = 5,
    }

    pub struct Packet<'a> {
        pub kind: PacketKind,
        pub number: u64,
        pub payload: &'a [u8],
    }

    pub struct TransportKeys {
        pub control_tx: SessionKey,
        pub control_rx: SessionKey,
        pub udp_tx: SessionKey,
        pub udp_rx: SessionKey,
    }

    #[derive(Default)]
    pub struct ReplayWindow {
        largest: Option<u64>,
        bitmap: u64,
    }

    impl ReplayWindow {
        pub fn accept(&mut self, number: u64) -> bool {
            let Some(largest) = self.largest else {
                self.largest = Some(number);
                self.bitmap = 1;
                return true;
            };
            if number > largest {
                let shift = number - largest;
                self.bitmap = if shift >= 64 {
                    1
                } else {
                    (self.bitmap << shift) | 1
                };
                self.largest = Some(number);
                return true;
            }
            let shift = largest - number;
            if shift >= 64 || self.bitmap & (1 << shift) != 0 {
                return false;
            }
            self.bitmap |= 1 << shift;
            true
        }
    }

    pub fn derive_keys(handshake: &FinalizedHandshake) -> TransportKeys {
        let derive = |key: &SessionKey, info: &[u8]| {
            let hkdf = Hkdf::<Sha256>::new(Some(&handshake.handshake_hash), key.as_bytes());
            let mut out = [0; SessionKey::SIZE];
            hkdf.expand(info, &mut out)
                .expect("fixed transport key size");
            SessionKey(out)
        };
        TransportKeys {
            control_tx: derive(&handshake.tx_key, CONTROL_KEY_INFO),
            control_rx: derive(&handshake.rx_key, CONTROL_KEY_INFO),
            udp_tx: derive(&handshake.tx_key, UDP_KEY_INFO),
            udp_rx: derive(&handshake.rx_key, UDP_KEY_INFO),
        }
    }

    pub fn seal_packet(
        key: &SessionKey,
        kind: PacketKind,
        session_id: u64,
        number: u64,
        payload: &[u8],
    ) -> Vec<u8> {
        let mut packet = Vec::with_capacity(PACKET_OVERHEAD + payload.len());
        seal_packet_into(&mut packet, key, kind, session_id, number, payload);
        packet
    }

    pub fn seal_packet_into(
        packet: &mut Vec<u8>,
        key: &SessionKey,
        kind: PacketKind,
        session_id: u64,
        number: u64,
        payload: &[u8],
    ) {
        packet.clear();
        packet.reserve(PACKET_OVERHEAD + payload.len());
        packet.push(PACKET_VERSION << 4 | kind as u8);
        packet.extend_from_slice(&session_id.to_be_bytes());
        packet.extend_from_slice(&number.to_be_bytes());
        packet.extend_from_slice(payload);
        let tag = SoftwareCrypto.aes256_gcm_encrypt(
            key,
            &Nonce::from_counter(number),
            packet.as_slice(),
            &mut [],
        );
        packet.extend_from_slice(&tag);
    }

    pub fn open_packet<'a>(
        key: &SessionKey,
        expected_session_id: u64,
        packet: &'a [u8],
    ) -> io::Result<Packet<'a>> {
        if packet.len() < PACKET_OVERHEAD
            || packet[0] >> 4 != PACKET_VERSION
            || u64::from_be_bytes(packet[1..9].try_into().unwrap()) != expected_session_id
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid authenticated packet header",
            ));
        }
        let kind = match packet[0] & 0x0f {
            1 => PacketKind::Confirm,
            2 => PacketKind::Attach,
            3 => PacketKind::UdpReady,
            4 => PacketKind::Bind,
            5 => PacketKind::Record,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unsupported authenticated packet kind",
                ));
            }
        };
        let number = u64::from_be_bytes(packet[9..17].try_into().unwrap());
        let tag_at = packet.len() - ENCRYPTED_MESSAGE_AUTH_SIZE;
        let mut tag = [0; ENCRYPTED_MESSAGE_AUTH_SIZE];
        tag.copy_from_slice(&packet[tag_at..]);
        if !SoftwareCrypto.aes256_gcm_decrypt(
            key,
            &Nonce::from_counter(number),
            &packet[..tag_at],
            &mut [],
            &tag,
        ) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "authenticated packet tag mismatch",
            ));
        }
        Ok(Packet {
            kind,
            number,
            payload: &packet[PACKET_HEADER_SIZE..tag_at],
        })
    }

    pub async fn receive_frame(reader: &mut (impl AsyncRead + Unpin)) -> io::Result<Option<Frame>> {
        let mut header = [0; FRAME_HEADER_SIZE];
        if reader.read(&mut header[..1]).await? == 0 {
            return Ok(None);
        }
        reader.read_exact(&mut header[1..]).await?;
        let length = u32::from_be_bytes(header[1..].try_into().unwrap()) as usize;
        if length > MAX_RECORD_SIZE + 12 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "frame exceeds the transport limit",
            ));
        }
        let mut payload = vec![0; length];
        reader.read_exact(&mut payload).await?;
        match header[0] {
            DATA_FRAME => Ok(Some(Frame::Record(payload))),
            TRANSPORT_INIT_FRAME => Ok(Some(Frame::TransportInit(payload))),
            TRANSPORT_RESPONSE_FRAME => Ok(Some(Frame::TransportResponse(payload))),
            AUTHENTICATED_FRAME => Ok(Some(Frame::Authenticated(payload))),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unsupported frame type",
            )),
        }
    }

    pub async fn send_frame(
        writer: &mut (impl AsyncWrite + Unpin),
        frame: &Frame,
    ) -> io::Result<()> {
        let (kind, payload) = match frame {
            Frame::Record(payload) => (DATA_FRAME, payload),
            Frame::TransportInit(payload) => (TRANSPORT_INIT_FRAME, payload),
            Frame::TransportResponse(payload) => (TRANSPORT_RESPONSE_FRAME, payload),
            Frame::Authenticated(payload) => (AUTHENTICATED_FRAME, payload),
        };
        if payload.len() > MAX_RECORD_SIZE + 12 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "frame exceeds the transport limit",
            ));
        }
        let mut header = [0; FRAME_HEADER_SIZE];
        header[0] = kind;
        header[1..].copy_from_slice(&(payload.len() as u32).to_be_bytes());
        writer.write_all(&header).await?;
        writer.write_all(payload).await
    }

    #[cfg(test)]
    mod tests {
        use ql_wire::SessionKey;

        use super::{PacketKind, ReplayWindow, open_packet, seal_packet};

        #[test]
        fn packet_authentication_covers_header_and_payload() {
            let key = SessionKey([7; SessionKey::SIZE]);
            let mut packet = seal_packet(&key, PacketKind::Record, 9, 4, b"record");
            let opened = open_packet(&key, 9, &packet).unwrap();
            assert_eq!(opened.kind, PacketKind::Record);
            assert_eq!(opened.number, 4);
            assert_eq!(opened.payload, b"record");

            packet[17] ^= 1;
            assert!(open_packet(&key, 9, &packet).is_err());
        }

        #[test]
        fn replay_window_accepts_reordering_once() {
            let mut window = ReplayWindow::default();
            assert!(window.accept(70));
            assert!(window.accept(68));
            assert!(!window.accept(68));
            assert!(!window.accept(6));
            assert!(window.accept(71));
        }
    }
}
