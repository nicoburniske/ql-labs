use std::io;

use ql_codec::{Decode, Encode};
use ql_wire::{
    HandshakeId, IkHandshake, PeerBundle, QlHandshakeRecord, RecordHeader, RecordType, RouteHeader,
    SessionKey, SoftwareCrypto, TransportParams, generate_identity,
};
use tokio::net::{TcpStream, ToSocketAddrs, tcp::OwnedReadHalf, tcp::OwnedWriteHalf};

use crate::protocol::PacketKind;

pub const DEFAULT_ADDRESS: &str = "127.0.0.1:7447";
pub const MAX_RECORD_SIZE: usize = 8 * 1024;

pub struct Receiver {
    tcp: OwnedReadHalf,
    session_id: u64,
    key: SessionKey,
    next_packet: u64,
}

pub struct Sender {
    tcp: OwnedWriteHalf,
    session_id: u64,
    key: SessionKey,
    next_packet: u64,
    buffer: Vec<u8>,
}

pub async fn connect(
    address: impl ToSocketAddrs,
    router: &PeerBundle,
) -> io::Result<(Receiver, Sender)> {
    router.validate(&SoftwareCrypto).map_err(invalid_data)?;
    let mut tcp = TcpStream::connect(address).await?;
    tcp.set_nodelay(true)?;

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
        header,
        handshake: response,
    } = protocol::TransportResponse::decode_bytes(response.as_slice()).map_err(invalid_data)?;
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
    let tx_key = finalized.tx_key;
    let rx_key = finalized.rx_key;

    let confirmation = protocol::seal_packet(&tx_key, PacketKind::Confirm, session_id, 0, &[]);
    protocol::write_frame(&mut tcp, &confirmation).await?;

    let (tcp, writer) = tcp.into_split();
    Ok((
        Receiver {
            tcp,
            session_id,
            key: rx_key,
            next_packet: 0,
        },
        Sender {
            tcp: writer,
            session_id,
            key: tx_key,
            next_packet: 1,
            buffer: Vec::new(),
        },
    ))
}

pub async fn receive(receiver: &mut Receiver) -> io::Result<Option<Vec<u8>>> {
    let Receiver {
        tcp,
        session_id,
        key,
        next_packet,
    } = receiver;
    let Some(frame) = protocol::read_frame(tcp).await? else {
        return Ok(None);
    };
    let (kind, number, payload) = protocol::open_packet_owned(key, *session_id, frame)?;
    if protocol::next_packet_number(next_packet) != Some(number) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unexpected TCP packet number",
        ));
    }
    match kind {
        PacketKind::Record => Ok(Some(payload)),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unexpected router packet",
        )),
    }
}

pub async fn send(sender: &mut Sender, record: &[u8]) -> io::Result<()> {
    send_packet(sender, PacketKind::Record, record).await
}

pub async fn attach(sender: &mut Sender, bundle: &PeerBundle) -> io::Result<()> {
    let payload = bundle.encode_vec();
    send_packet(sender, PacketKind::Attach, &payload).await
}

async fn send_packet(sender: &mut Sender, kind: PacketKind, payload: &[u8]) -> io::Result<()> {
    let number = protocol::next_packet_number(&mut sender.next_packet)
        .ok_or_else(|| io::Error::other("packet number exhausted"))?;
    protocol::seal_packet_into(
        &mut sender.buffer,
        &sender.key,
        kind,
        sender.session_id,
        number,
        payload,
    );
    protocol::write_frame(&mut sender.tcp, &sender.buffer).await
}

fn invalid_data(error: impl std::error::Error + Send + Sync + 'static) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

pub mod protocol {
    use std::{io, mem::size_of};

    use ql_codec::{Decode, Encode, codec};
    use ql_wire::{
        ENCRYPTED_MESSAGE_AUTH_SIZE, Nonce, QlAead, QlHandshakeRecord, RecordHeader, SessionKey,
        SoftwareCrypto,
    };
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

    use crate::MAX_RECORD_SIZE;

    const FRAME_HEADER_SIZE: usize = 4;
    const PACKET_OVERHEAD: usize = PACKET_HEADER_SIZE + ENCRYPTED_MESSAGE_AUTH_SIZE;
    const MAX_FRAME_SIZE: usize = MAX_RECORD_SIZE + PACKET_OVERHEAD;
    const PACKET_VERSION: u8 = 1;
    const PACKET_HEADER_SIZE: usize = size_of::<u8>() * 2 + size_of::<u64>() * 2;

    codec! {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum PacketKind {
            Confirm = 1,
            Attach = 2,
            Record = 3,
        }
    }

    codec! {
        pub struct PacketHeader {
            pub version: u8,
            pub kind: PacketKind,
            pub session_id: u64,
            pub number: u64,
        }
    }

    codec! {
        pub struct TransportResponse {
            pub session_id: u64,
            pub header: RecordHeader,
            pub handshake: QlHandshakeRecord,
        }
    }

    pub struct Packet<'a> {
        pub kind: PacketKind,
        pub number: u64,
        pub payload: &'a [u8],
    }

    #[inline]
    pub fn next_packet_number(next: &mut u64) -> Option<u64> {
        let number = *next;
        *next = number.checked_add(1)?;
        Some(number)
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
            version: PACKET_VERSION,
            kind,
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
        if header.version != PACKET_VERSION || header.session_id != expected_session_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid authenticated packet header",
            ));
        }
        let kind = header.kind;
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
