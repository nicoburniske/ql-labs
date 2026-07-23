use std::{io, sync::Arc, time::Duration};

use ql_codec::{Decode, Encode};
use ql_wire::{
    HandshakeId, IkHandshake, PeerBundle, QlHandshakeRecord, RecordHeader, RecordType, RouteHeader,
    SessionKey, SoftwareCrypto, TransportParams, generate_identity,
};
use tokio::{
    net::{TcpStream, ToSocketAddrs, UdpSocket, tcp::OwnedReadHalf, tcp::OwnedWriteHalf},
    time::{Instant, timeout},
};

use crate::protocol::PacketKind;

pub const DEFAULT_ADDRESS: &str = "127.0.0.1:7447";
pub const DEFAULT_UDP_PAYLOAD: usize = 1200;
pub const MAX_RECORD_SIZE: usize = 8 * 1024;

pub struct Receiver {
    tcp: OwnedReadHalf,
    udp: UdpReceiver,
    tcp_key: SessionKey,
    next_tcp_packet: u64,
}

pub struct Sender {
    tcp: OwnedWriteHalf,
    udp: UdpSender,
    tcp_key: SessionKey,
    next_tcp_packet: u64,
    tcp_buffer: Vec<u8>,
}

struct UdpReceiver {
    socket: Arc<UdpSocket>,
    session_id: u64,
    key: SessionKey,
    buffer: Vec<u8>,
}

struct UdpSender {
    socket: Arc<UdpSocket>,
    session_id: u64,
    key: SessionKey,
    next_packet: u64,
    max_payload: usize,
    active: bool,
    buffer: Vec<u8>,
}

pub async fn connect_udp(
    address: impl ToSocketAddrs,
    router: &PeerBundle,
) -> io::Result<(Receiver, Sender)> {
    connect_udp_with_max_payload(address, router, DEFAULT_UDP_PAYLOAD).await
}

pub async fn connect_udp_with_max_payload(
    address: impl ToSocketAddrs,
    router: &PeerBundle,
    max_payload: usize,
) -> io::Result<(Receiver, Sender)> {
    if max_payload > protocol::MAX_UDP_PAYLOAD {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid UDP payload limit",
        ));
    }

    router.validate(&SoftwareCrypto).map_err(invalid_data)?;
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

    let identity = generate_identity(&SoftwareCrypto, "QL router transport");
    let route = RouteHeader {
        sender: identity.qid,
        recipient: router.qid,
    };
    let mut handshake = IkHandshake::new_ik_initiator(
        &SoftwareCrypto,
        identity,
        router.clone(),
        TransportParams::default(),
    );
    let mut random = [0; 4];
    ql_wire::QlRandom::fill_random_bytes(&SoftwareCrypto, &mut random);
    let handshake_id = HandshakeId::decode_bytes(random.as_slice()).unwrap();
    let request = ql_wire::encode_record_vec(
        RecordHeader::new(route, RecordType::Handshake),
        &QlHandshakeRecord::Ik1(
            handshake
                .write_1(&SoftwareCrypto, handshake_id)
                .map_err(invalid_data)?,
        ),
    );
    protocol::write_frame(&mut tcp, &request).await?;

    let response = protocol::read_frame(&mut tcp)
        .await?
        .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "router disconnected"))?;
    let protocol::TransportResponse {
        session_id,
        max_udp_payload,
        header,
        handshake: response,
    } = protocol::TransportResponse::decode_bytes(response.as_slice()).map_err(invalid_data)?;
    let router_max_payload = max_udp_payload as usize;
    if router_max_payload > protocol::MAX_UDP_PAYLOAD {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid router UDP payload limit",
        ));
    }
    let QlHandshakeRecord::Ik2(response) = response else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unexpected transport handshake record",
        ));
    };
    handshake
        .read_2(&SoftwareCrypto, header.route, &response)
        .map_err(invalid_data)?;
    let finalized = handshake.finalize(&SoftwareCrypto).map_err(invalid_data)?;
    if finalized.remote_bundle != *router {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "transport peer does not match router",
        ));
    }
    let udp_keys = protocol::derive_udp_keys(&finalized);
    let tcp_tx = finalized.tx_key;
    let tcp_rx = finalized.rx_key;

    let confirmation_payload = protocol::TransportConfirmation {
        max_udp_payload: max_payload as u32,
    }
    .encode_vec();
    let confirmation = protocol::seal_packet(
        &tcp_tx,
        PacketKind::Confirm,
        session_id,
        0,
        &confirmation_payload,
    );
    protocol::write_frame(&mut tcp, &confirmation).await?;

    let mut udp_packet: u64 = 0;
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let next_packet = udp_packet
            .checked_add(1)
            .ok_or_else(|| io::Error::other("UDP packet number exhausted during activation"))?;
        let bind =
            protocol::seal_packet(&udp_keys.tx, PacketKind::Bind, session_id, udp_packet, &[]);
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
            protocol::read_frame(&mut tcp),
        )
        .await
        {
            Ok(Ok(Some(packet))) => {
                let packet = protocol::open_packet(&tcp_rx, session_id, &packet)?;
                if packet.kind == PacketKind::UdpReady
                    && packet.payload.is_empty()
                    && packet.number == 0
                {
                    break;
                }
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unexpected packet during UDP activation",
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
                key: udp_keys.rx,
                buffer: vec![0; max_payload],
            },
            tcp_key: tcp_rx,
            next_tcp_packet: 1,
        },
        Sender {
            tcp: writer,
            udp: UdpSender {
                socket: udp,
                session_id,
                key: udp_keys.tx,
                next_packet: udp_packet,
                max_payload: router_max_payload,
                active: true,
                buffer: Vec::with_capacity(router_max_payload),
            },
            tcp_key: tcp_tx,
            next_tcp_packet: 1,
            tcp_buffer: Vec::new(),
        },
    ))
}

pub async fn receive(receiver: &mut Receiver) -> io::Result<Option<Vec<u8>>> {
    let Receiver {
        tcp,
        udp,
        tcp_key,
        next_tcp_packet,
    } = receiver;
    loop {
        tokio::select! {
            frame = protocol::read_frame(tcp) => {
                let Some(frame) = frame? else { return Ok(None) };
                let (kind, number, payload) =
                    protocol::open_packet_owned(tcp_key, udp.session_id, frame)?;
                let following_packet = next_tcp_packet.checked_add(1)
                    .ok_or_else(|| io::Error::other("TCP packet number exhausted"))?;
                if number != *next_tcp_packet {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "unexpected TCP packet number"));
                }
                *next_tcp_packet = following_packet;
                match kind {
                    PacketKind::Record => return Ok(Some(payload)),
                    PacketKind::UdpReady if payload.is_empty() => continue,
                    _ => return Err(io::Error::new(io::ErrorKind::InvalidData, "unexpected router packet")),
                }
            }
            received = udp.socket.recv(&mut udp.buffer) => {
                let len = received?;
                let Ok(packet) = protocol::open_packet(&udp.key, udp.session_id, &udp.buffer[..len]) else {
                    continue;
                };
                if packet.kind == PacketKind::Record {
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
    send_tcp_packet(sender, PacketKind::Record, record).await
}

pub async fn attach(sender: &mut Sender, bundle: &PeerBundle) -> io::Result<()> {
    let payload = bundle.encode_vec();
    send_tcp_packet(sender, PacketKind::Attach, &payload).await
}

async fn send_tcp_packet(sender: &mut Sender, kind: PacketKind, payload: &[u8]) -> io::Result<()> {
    let next_packet = sender
        .next_tcp_packet
        .checked_add(1)
        .ok_or_else(|| io::Error::other("TCP packet number exhausted"))?;
    protocol::seal_packet_into(
        &mut sender.tcp_buffer,
        &sender.tcp_key,
        kind,
        sender.udp.session_id,
        sender.next_tcp_packet,
        payload,
    );
    sender.next_tcp_packet = next_packet;
    protocol::write_frame(&mut sender.tcp, &sender.tcp_buffer).await
}

fn invalid_data(error: impl std::error::Error + Send + Sync + 'static) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

pub mod protocol {
    use std::{io, mem::size_of};

    use hkdf::Hkdf;
    use ql_codec::{Decode, Encode};
    use ql_wire::{
        ENCRYPTED_MESSAGE_AUTH_SIZE, FinalizedHandshake, Nonce, QlAead, QlHandshakeRecord,
        RecordHeader, SessionKey, SoftwareCrypto,
    };
    use sha2::Sha256;
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

    use crate::MAX_RECORD_SIZE;

    pub const PACKET_OVERHEAD: usize = PACKET_HEADER_SIZE + ENCRYPTED_MESSAGE_AUTH_SIZE;
    pub const MAX_UDP_PAYLOAD: usize = PACKET_OVERHEAD + MAX_RECORD_SIZE;

    const FRAME_HEADER_SIZE: usize = 4;
    const MAX_FRAME_SIZE: usize = MAX_RECORD_SIZE + PACKET_OVERHEAD;
    const PACKET_VERSION: u8 = 1;
    const PACKET_HEADER_SIZE: usize = size_of::<u8>() + size_of::<u64>() * 2;
    const UDP_KEY_INFO: &[u8] = b"ql-router:transport-keys:v1:udp";

    ql_codec::codec! {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum PacketKind {
            Confirm = 1,
            Attach = 2,
            UdpReady = 3,
            Bind = 4,
            Record = 5,
        }
    }

    ql_codec::codec! {
        pub struct PacketHeader {
            pub version_and_kind: u8,
            pub session_id: u64,
            pub number: u64,
        }
    }

    ql_codec::codec! {
        pub struct TransportResponse {
            pub session_id: u64,
            pub max_udp_payload: u32,
            pub header: RecordHeader,
            pub handshake: QlHandshakeRecord,
        }
    }

    ql_codec::codec! {
        pub struct TransportConfirmation {
            pub max_udp_payload: u32,
        }
    }

    pub struct Packet<'a> {
        pub kind: PacketKind,
        pub number: u64,
        pub payload: &'a [u8],
    }

    pub struct UdpKeys {
        pub tx: SessionKey,
        pub rx: SessionKey,
    }

    pub fn derive_udp_keys(handshake: &FinalizedHandshake) -> UdpKeys {
        let derive = |key: &SessionKey, info: &[u8]| {
            let hkdf = Hkdf::<Sha256>::new(Some(&handshake.handshake_hash), key.as_bytes());
            let mut out = [0; SessionKey::SIZE];
            hkdf.expand(info, &mut out)
                .expect("fixed transport key size");
            SessionKey(out)
        };
        UdpKeys {
            tx: derive(&handshake.tx_key, UDP_KEY_INFO),
            rx: derive(&handshake.rx_key, UDP_KEY_INFO),
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
        PacketHeader {
            version_and_kind: PACKET_VERSION << 4 | kind as u8,
            session_id,
            number,
        }
        .encode(packet);
        packet.extend_from_slice(payload);
        let authenticated_len = authenticated_prefix_len(kind, payload);
        let tag = SoftwareCrypto.aes256_gcm_encrypt(
            key,
            &Nonce::from_counter(number),
            &packet[..authenticated_len],
            &mut [],
        );
        packet.extend_from_slice(&tag);
    }

    pub fn open_packet<'a>(
        key: &SessionKey,
        expected_session_id: u64,
        packet: &'a [u8],
    ) -> io::Result<Packet<'a>> {
        if packet.len() < PACKET_OVERHEAD {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid authenticated packet header",
            ));
        }
        let header = PacketHeader::decode_bytes(packet).map_err(super::invalid_data)?;
        if header.version_and_kind >> 4 != PACKET_VERSION
            || header.session_id != expected_session_id
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid authenticated packet header",
            ));
        }
        let kind =
            PacketKind::try_from(header.version_and_kind & 0x0f).map_err(super::invalid_data)?;
        let tag_at = packet.len() - ENCRYPTED_MESSAGE_AUTH_SIZE;
        let tag = <[u8; ENCRYPTED_MESSAGE_AUTH_SIZE]>::decode_bytes(&packet[tag_at..])
            .map_err(super::invalid_data)?;
        let authenticated_len = authenticated_prefix_len(kind, &packet[PACKET_HEADER_SIZE..tag_at]);
        if !SoftwareCrypto.aes256_gcm_decrypt(
            key,
            &Nonce::from_counter(header.number),
            &packet[..authenticated_len],
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
            number: header.number,
            payload: &packet[PACKET_HEADER_SIZE..tag_at],
        })
    }

    pub fn open_packet_owned(
        key: &SessionKey,
        expected_session_id: u64,
        mut packet: Vec<u8>,
    ) -> io::Result<(PacketKind, u64, Vec<u8>)> {
        let opened = open_packet(key, expected_session_id, &packet)?;
        let kind = opened.kind;
        let number = opened.number;
        let payload_len = opened.payload.len();
        packet.copy_within(PACKET_HEADER_SIZE..PACKET_HEADER_SIZE + payload_len, 0);
        packet.truncate(payload_len);
        Ok((kind, number, packet))
    }

    fn authenticated_prefix_len(kind: PacketKind, payload: &[u8]) -> usize {
        PACKET_HEADER_SIZE
            + match kind {
                PacketKind::Record => payload.len().min(RecordHeader::WIRE_SIZE),
                _ => payload.len(),
            }
    }

    pub async fn read_frame(reader: &mut (impl AsyncRead + Unpin)) -> io::Result<Option<Vec<u8>>> {
        let mut header = [0; FRAME_HEADER_SIZE];
        if reader.read(&mut header[..1]).await? == 0 {
            return Ok(None);
        }
        reader.read_exact(&mut header[1..]).await?;
        let length = u32::decode_bytes(header.as_slice()).map_err(super::invalid_data)? as usize;
        if length > MAX_FRAME_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "frame exceeds the transport limit",
            ));
        }
        let mut payload = vec![0; length];
        reader.read_exact(&mut payload).await?;
        Ok(Some(payload))
    }

    pub async fn write_frame(
        writer: &mut (impl AsyncWrite + Unpin),
        payload: &[u8],
    ) -> io::Result<()> {
        if payload.len() > MAX_FRAME_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "frame exceeds the transport limit",
            ));
        }
        let mut header = [0; FRAME_HEADER_SIZE];
        (payload.len() as u32).encode(&mut header.as_mut_slice());
        writer.write_all(&header).await?;
        writer.write_all(payload).await
    }

    #[cfg(test)]
    mod tests {
        use ql_codec::Encode;
        use ql_wire::SessionKey;

        use super::{PACKET_HEADER_SIZE, PacketKind, open_packet_owned, seal_packet};

        #[test]
        fn record_authentication_covers_only_routing_metadata() {
            let key = SessionKey([7; SessionKey::SIZE]);
            let mut record = ql_wire::RecordHeader::new(
                ql_wire::RouteHeader {
                    sender: ql_common::QID([1; ql_common::QID::SIZE]),
                    recipient: ql_common::QID([2; ql_common::QID::SIZE]),
                },
                ql_wire::RecordType::Session,
            )
            .encode_vec();
            record.extend_from_slice(b"encrypted body");
            let packet = seal_packet(&key, PacketKind::Record, 9, 4, &record);
            let (kind, number, payload) = open_packet_owned(&key, 9, packet.clone()).unwrap();
            assert_eq!(kind, PacketKind::Record);
            assert_eq!(number, 4);
            assert_eq!(payload, record);

            let mut changed_header = packet.clone();
            changed_header[PACKET_HEADER_SIZE] ^= 1;
            assert!(open_packet_owned(&key, 9, changed_header).is_err());

            let mut changed_body = packet;
            changed_body[PACKET_HEADER_SIZE + ql_wire::RecordHeader::WIRE_SIZE] ^= 1;
            assert!(open_packet_owned(&key, 9, changed_body).is_ok());
        }

        #[test]
        fn control_authentication_covers_payload() {
            let key = SessionKey([7; SessionKey::SIZE]);
            let mut packet = seal_packet(&key, PacketKind::Attach, 9, 4, b"bundle");
            packet[PACKET_HEADER_SIZE] ^= 1;
            assert!(open_packet_owned(&key, 9, packet).is_err());
        }
    }
}
