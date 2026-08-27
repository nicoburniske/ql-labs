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
    key: SessionKey,
    counter: u64,
}

pub struct Sender {
    tcp: OwnedWriteHalf,
    key: SessionKey,
    counter: u64,
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
    let (header, response) =
        protocol::decode_handshake(response.payload()).map_err(invalid_data)?;
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

    let confirmation = protocol::seal_packet(&tx_key, PacketKind::Confirm, 0, &[])?;
    protocol::write_packet(&mut tcp, &confirmation).await?;

    let (tcp, writer) = tcp.into_split();
    Ok((
        Receiver {
            tcp,
            key: rx_key,
            counter: 0,
        },
        Sender {
            tcp: writer,
            key: tx_key,
            counter: 1,
            buffer: Vec::new(),
        },
    ))
}

pub async fn receive(receiver: &mut Receiver) -> io::Result<Option<Vec<u8>>> {
    let Receiver { tcp, key, counter } = receiver;
    let Some(frame) = protocol::read_frame(tcp).await? else {
        return Ok(None);
    };
    let nonce = protocol::take_counter(counter)
        .ok_or_else(|| io::Error::other("nonce counter exhausted"))?;
    let (kind, payload) = protocol::open_packet_owned(key, nonce, frame)?;
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
    if payload.len() > MAX_RECORD_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "frame exceeds the transport limit",
        ));
    }
    let nonce = protocol::take_counter(&mut sender.counter)
        .ok_or_else(|| io::Error::other("nonce counter exhausted"))?;
    protocol::seal_packet_into(&mut sender.buffer, &sender.key, kind, nonce, payload)?;
    protocol::write_packet(&mut sender.tcp, &sender.buffer).await
}

fn invalid_data(error: impl std::error::Error + Send + Sync + 'static) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

pub mod protocol {
    use std::io;

    use ql_codec::{Decode, Encode, Reader, codec};
    use ql_wire::{
        ENCRYPTED_MESSAGE_AUTH_SIZE, Nonce, QL_WIRE_VERSION, QlAead, QlHandshakeRecord,
        RecordHeader, RecordType, SessionKey, SoftwareCrypto,
    };
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

    use crate::MAX_RECORD_SIZE;

    const FRAME_HEADER_SIZE: usize = 4;
    const PACKET_HEADER_SIZE: usize = FRAME_HEADER_SIZE + 1;
    const PACKET_OVERHEAD: usize = 1 + ENCRYPTED_MESSAGE_AUTH_SIZE;
    const MAX_FRAME_SIZE: usize = MAX_RECORD_SIZE + PACKET_OVERHEAD;

    codec! {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum PacketKind {
            Confirm = 1,
            Attach = 2,
            Record = 3,
        }
    }

    pub struct Frame(Vec<u8>);

    impl Frame {
        pub fn as_bytes(&self) -> &[u8] {
            &self.0
        }

        pub fn payload(&self) -> &[u8] {
            &self.0[FRAME_HEADER_SIZE..]
        }
    }

    pub struct Packet<'a> {
        pub kind: PacketKind,
        pub payload: &'a [u8],
    }

    pub fn decode_handshake(
        bytes: &[u8],
    ) -> Result<(RecordHeader, QlHandshakeRecord), ql_codec::Error> {
        let mut reader = Reader::new(bytes);
        let header = reader.decode::<RecordHeader>()?;
        let handshake = reader.decode::<QlHandshakeRecord>()?;
        if header.version != QL_WIRE_VERSION
            || header.record_type != RecordType::Handshake
            || !reader.is_empty()
        {
            return Err(ql_codec::Error::InvalidData);
        }
        Ok((header, handshake))
    }

    #[inline]
    pub fn take_counter(next: &mut u64) -> Option<u64> {
        let number = *next;
        *next = number.checked_add(1)?;
        Some(number)
    }

    pub fn seal_packet(
        key: &SessionKey,
        kind: PacketKind,
        nonce: u64,
        payload: &[u8],
    ) -> io::Result<Vec<u8>> {
        let mut packet = Vec::with_capacity(FRAME_HEADER_SIZE + PACKET_OVERHEAD + payload.len());
        seal_packet_into(&mut packet, key, kind, nonce, payload)?;
        Ok(packet)
    }

    pub fn seal_packet_into(
        packet: &mut Vec<u8>,
        key: &SessionKey,
        kind: PacketKind,
        nonce: u64,
        payload: &[u8],
    ) -> io::Result<()> {
        let length = PACKET_OVERHEAD
            .checked_add(payload.len())
            .filter(|length| *length <= MAX_FRAME_SIZE)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "frame exceeds the transport limit",
                )
            })?;
        packet.clear();
        packet.reserve(FRAME_HEADER_SIZE + length);
        (length as u32).encode(packet);
        kind.encode(packet);
        packet.extend_from_slice(payload);
        let tag =
            SoftwareCrypto.aes256_gcm_encrypt(key, &Nonce::from_counter(nonce), packet, &mut []);
        packet.extend_from_slice(&tag);
        Ok(())
    }

    pub fn open_packet<'a>(
        key: &SessionKey,
        nonce: u64,
        packet: &'a [u8],
    ) -> io::Result<Packet<'a>> {
        if packet.len() < FRAME_HEADER_SIZE + PACKET_OVERHEAD {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid authenticated frame",
            ));
        }
        let length =
            u32::decode_bytes(&packet[..FRAME_HEADER_SIZE]).map_err(super::invalid_data)?;
        if length as usize != packet.len() - FRAME_HEADER_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid authenticated frame length",
            ));
        }
        let kind =
            PacketKind::decode_bytes(&packet[FRAME_HEADER_SIZE..]).map_err(super::invalid_data)?;
        let tag_at = packet.len() - ENCRYPTED_MESSAGE_AUTH_SIZE;
        let tag = <[u8; ENCRYPTED_MESSAGE_AUTH_SIZE]>::decode_bytes(&packet[tag_at..])
            .map_err(super::invalid_data)?;
        if !SoftwareCrypto.aes256_gcm_decrypt(
            key,
            &Nonce::from_counter(nonce),
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
            payload: &packet[PACKET_HEADER_SIZE..tag_at],
        })
    }

    pub fn open_packet_owned(
        key: &SessionKey,
        nonce: u64,
        frame: Frame,
    ) -> io::Result<(PacketKind, Vec<u8>)> {
        let mut packet = frame.0;
        let opened = open_packet(key, nonce, &packet)?;
        let kind = opened.kind;
        let payload_len = opened.payload.len();
        packet.copy_within(PACKET_HEADER_SIZE..PACKET_HEADER_SIZE + payload_len, 0);
        packet.truncate(payload_len);
        Ok((kind, packet))
    }

    pub async fn read_frame(reader: &mut (impl AsyncRead + Unpin)) -> io::Result<Option<Frame>> {
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
        let mut frame = Vec::with_capacity(FRAME_HEADER_SIZE + length);
        frame.extend_from_slice(&header);
        frame.resize(FRAME_HEADER_SIZE + length, 0);
        reader.read_exact(&mut frame[FRAME_HEADER_SIZE..]).await?;
        Ok(Some(Frame(frame)))
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

    pub async fn write_packet(
        writer: &mut (impl AsyncWrite + Unpin),
        packet: &[u8],
    ) -> io::Result<()> {
        writer.write_all(packet).await
    }

    #[cfg(test)]
    mod tests {
        use ql_wire::{
            HandshakeId, IkHandshake, QL_WIRE_VERSION, QlHandshakeRecord, RecordHeader, RecordType,
            RouteHeader, SessionKey, SoftwareCrypto, TransportParams, generate_identity,
        };

        use super::{
            FRAME_HEADER_SIZE, Frame, PACKET_HEADER_SIZE, PacketKind, decode_handshake,
            open_packet, open_packet_owned, seal_packet,
        };

        #[test]
        fn handshake_headers_are_validated() {
            let crypto = SoftwareCrypto;
            let local = generate_identity(&crypto, "local");
            let remote = generate_identity(&crypto, "remote");
            let route = RouteHeader {
                sender: local.qid,
                recipient: remote.qid,
            };
            let mut handshake = IkHandshake::new_ik_initiator(
                &crypto,
                local,
                remote.bundle(),
                TransportParams::default(),
            );
            let request = handshake.write_1(&crypto, HandshakeId(1)).unwrap();
            let mut record = ql_wire::encode_record_vec(
                RecordHeader::new(route, RecordType::Handshake),
                &QlHandshakeRecord::Ik1(request),
            );
            assert!(decode_handshake(&record).is_ok());

            record[0] = QL_WIRE_VERSION.wrapping_add(1);
            assert!(decode_handshake(&record).is_err());
            record[0] = QL_WIRE_VERSION;
            record[RecordHeader::WIRE_SIZE - 1] = RecordType::Session as u8;
            assert!(decode_handshake(&record).is_err());
        }

        #[test]
        fn packet_authentication_covers_the_entire_frame_and_counter() {
            let key = SessionKey([7; SessionKey::SIZE]);
            let packet = seal_packet(&key, PacketKind::Record, 4, b"record").unwrap();
            let (kind, payload) = open_packet_owned(&key, 4, Frame(packet.clone())).unwrap();
            assert_eq!(kind, PacketKind::Record);
            assert_eq!(payload, b"record");
            assert!(open_packet(&key, 5, &packet).is_err());

            for index in [0, FRAME_HEADER_SIZE, PACKET_HEADER_SIZE] {
                let mut changed = packet.clone();
                changed[index] ^= 1;
                assert!(open_packet(&key, 4, &changed).is_err());
            }
        }
    }
}
