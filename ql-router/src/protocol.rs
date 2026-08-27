use std::{collections::HashSet, io};

use ql_codec::{Decode, Encode, Reader, codec};
use ql_common::QID;
use ql_wire::{
    ENCRYPTED_MESSAGE_AUTH_SIZE, HandshakeId, IkHandshake, Nonce, PeerBundle, PeerChallenge,
    QL_WIRE_VERSION, QlAead, QlHandshakeRecord, QlIdentity, QlRandom, RecordHeader, RecordType,
    RouteHeader, SessionKey, SoftwareCrypto, TransportParams, generate_identity,
};

pub const MAX_RECORD_SIZE: usize = 8 * 1024;
pub const MAX_ROUTES_PER_CONNECTION: usize = 64;

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
    pub fn payload(&self) -> &[u8] {
        &self.0[FRAME_HEADER_SIZE..]
    }
}

pub struct FrameDecoder {
    frame: Vec<u8>,
    filled: usize,
}

impl FrameDecoder {
    pub fn new() -> Self {
        Self {
            frame: vec![0; FRAME_HEADER_SIZE],
            filled: 0,
        }
    }

    pub fn buffer(&mut self) -> &mut [u8] {
        &mut self.frame[self.filled..]
    }

    pub fn is_partial(&self) -> bool {
        self.filled != 0
    }

    pub fn advance(&mut self, amount: usize) -> io::Result<Option<Frame>> {
        if amount > self.buffer().len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "frame decoder advanced beyond its buffer",
            ));
        }
        self.filled += amount;

        if self.filled == FRAME_HEADER_SIZE && self.frame.len() == FRAME_HEADER_SIZE {
            let length = u32::decode_bytes(self.frame.as_slice()).map_err(invalid_data)? as usize;
            if length > MAX_FRAME_SIZE {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "frame exceeds the transport limit",
                ));
            }
            self.frame.resize(FRAME_HEADER_SIZE + length, 0);
        }

        if self.filled != self.frame.len() {
            return Ok(None);
        }

        self.filled = 0;
        let frame = std::mem::replace(&mut self.frame, vec![0; FRAME_HEADER_SIZE]);
        Ok(Some(Frame(frame)))
    }

    pub fn finish(&self) -> io::Result<()> {
        if self.filled == 0 {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection ended within a frame",
            ))
        }
    }
}

impl Default for FrameDecoder {
    fn default() -> Self {
        Self::new()
    }
}

pub struct SecureReceiver {
    key: SessionKey,
    counter: Option<u64>,
}

impl SecureReceiver {
    fn new(key: SessionKey, counter: u64) -> Self {
        Self {
            key,
            counter: Some(counter),
        }
    }

    pub fn open(&mut self, frame: Frame) -> io::Result<(PacketKind, Vec<u8>)> {
        // any invalid authenticated frame poisons the receiver because nonce recovery is unsafe
        let counter = self.counter.take().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "secure receiver has failed")
        })?;
        let next = counter
            .checked_add(1)
            .ok_or_else(|| io::Error::other("nonce counter exhausted"))?;
        let mut packet = frame.0;

        if packet.len() < FRAME_HEADER_SIZE + PACKET_OVERHEAD {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid authenticated frame",
            ));
        }
        let length = u32::decode_bytes(&packet[..FRAME_HEADER_SIZE]).map_err(invalid_data)?;
        if length as usize != packet.len() - FRAME_HEADER_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid authenticated frame length",
            ));
        }
        let kind = PacketKind::decode_bytes(&packet[FRAME_HEADER_SIZE..]).map_err(invalid_data)?;
        let tag_at = packet.len() - ENCRYPTED_MESSAGE_AUTH_SIZE;
        let tag = <[u8; ENCRYPTED_MESSAGE_AUTH_SIZE]>::decode_bytes(&packet[tag_at..])
            .map_err(invalid_data)?;
        if !SoftwareCrypto.aes256_gcm_decrypt(
            &self.key,
            &Nonce::from_counter(counter),
            &packet[..tag_at],
            &mut [],
            &tag,
        ) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "authenticated packet tag mismatch",
            ));
        }

        let payload_len = tag_at - PACKET_HEADER_SIZE;
        packet.copy_within(PACKET_HEADER_SIZE..tag_at, 0);
        packet.truncate(payload_len);
        self.counter = Some(next);
        Ok((kind, packet))
    }
}

pub struct SecureSender {
    key: SessionKey,
    counter: u64,
}

impl SecureSender {
    fn new(key: SessionKey, counter: u64) -> Self {
        Self { key, counter }
    }

    pub fn seal(
        &mut self,
        frame: &mut Vec<u8>,
        kind: PacketKind,
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
        let next = self
            .counter
            .checked_add(1)
            .ok_or_else(|| io::Error::other("nonce counter exhausted"))?;

        frame.clear();
        frame.reserve(FRAME_HEADER_SIZE + length);
        (length as u32).encode(frame);
        kind.encode(frame);
        frame.extend_from_slice(payload);
        let tag = SoftwareCrypto.aes256_gcm_encrypt(
            &self.key,
            &Nonce::from_counter(self.counter),
            frame,
            &mut [],
        );
        frame.extend_from_slice(&tag);
        self.counter = next;
        Ok(())
    }
}

pub struct ClientHandshake {
    handshake: IkHandshake,
}

impl ClientHandshake {
    pub fn start(router: &PeerBundle) -> io::Result<(Self, Vec<u8>)> {
        router.validate(&SoftwareCrypto).map_err(invalid_data)?;
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
        SoftwareCrypto.fill_random_bytes(&mut random);
        let handshake_id = HandshakeId::decode_bytes(random.as_slice()).map_err(invalid_data)?;
        let request = handshake
            .write_1(&SoftwareCrypto, handshake_id)
            .map_err(invalid_data)?;
        let request = encode_handshake(
            RecordHeader::new(route, RecordType::Handshake),
            &QlHandshakeRecord::Ik1(request),
        )?;
        Ok((Self { handshake }, request))
    }

    pub fn finish(mut self, response: &Frame) -> io::Result<(SecureReceiver, SecureSender)> {
        let (header, response) = decode_handshake(response.payload()).map_err(invalid_data)?;
        let QlHandshakeRecord::Ik2(response) = response else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unexpected transport handshake record",
            ));
        };
        self.handshake
            .read_2(&SoftwareCrypto, header.route, &response)
            .map_err(invalid_data)?;
        let transport = self
            .handshake
            .finalize(&SoftwareCrypto)
            .map_err(invalid_data)?;
        Ok((
            SecureReceiver::new(transport.rx_key, 0),
            SecureSender::new(transport.tx_key, 0),
        ))
    }
}

pub fn accept_handshake(
    identity: &QlIdentity,
    request: &Frame,
) -> io::Result<(Vec<u8>, SecureReceiver, SecureSender)> {
    let (header, request) = decode_handshake(request.payload()).map_err(invalid_data)?;
    if header.route.recipient != identity.qid {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "transport handshake has the wrong recipient",
        ));
    }
    let QlHandshakeRecord::Ik1(request) = request else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unexpected transport handshake record",
        ));
    };
    let mut handshake =
        IkHandshake::new_ik_responder(&SoftwareCrypto, identity, None, TransportParams::default());
    handshake
        .read_1(&SoftwareCrypto, header.route, &request)
        .map_err(invalid_data)?;
    let response = handshake
        .write_2(&SoftwareCrypto, request.handshake_id)
        .map_err(invalid_data)?;
    let response = encode_handshake(
        RecordHeader::new(
            RouteHeader {
                sender: header.route.recipient,
                recipient: header.route.sender,
            },
            RecordType::Handshake,
        ),
        &QlHandshakeRecord::Ik2(response),
    )?;
    let transport = handshake.finalize(&SoftwareCrypto).map_err(invalid_data)?;
    Ok((
        response,
        SecureReceiver::new(transport.rx_key, 0),
        SecureSender::new(transport.tx_key, 0),
    ))
}

pub struct RouterConnection<'a> {
    identity: &'a QlIdentity,
    secure: SecureReceiver,
    transition: RouteTransition<&'a QlIdentity>,
    next_handshake_id: u32,
    routes: HashSet<QID>,
}

enum RouteTransition<I> {
    Ready,
    Challenging(PeerChallenge<I>),
    Verified(QID),
}

pub enum RouterAction {
    Attach(PeerBundle),
    Authenticate(Vec<u8>),
    Forward {
        sender: QID,
        recipient: QID,
        record: Vec<u8>,
    },
    Unauthenticated {
        sender: QID,
        recipient: QID,
    },
}

impl<'a> RouterConnection<'a> {
    pub fn new(identity: &'a QlIdentity, secure: SecureReceiver) -> Self {
        Self {
            identity,
            secure,
            transition: RouteTransition::Ready,
            next_handshake_id: 1,
            routes: HashSet::new(),
        }
    }

    pub fn receive(&mut self, frame: Frame) -> io::Result<RouterAction> {
        if matches!(&self.transition, RouteTransition::Verified(_)) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "verified route has not been committed",
            ));
        }
        let (kind, payload) = self.secure.open(frame)?;
        match kind {
            PacketKind::Attach => {
                if !matches!(&self.transition, RouteTransition::Ready) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "route challenge already active",
                    ));
                }
                let mut payload = Reader::new(payload.as_slice());
                let bundle = payload.decode::<PeerBundle>().map_err(invalid_data)?;
                if !payload.is_empty() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "invalid route attachment",
                    ));
                }
                Ok(RouterAction::Attach(bundle))
            }
            PacketKind::Record => {
                if self.routes.is_empty() && matches!(&self.transition, RouteTransition::Ready) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "peer must attach before routing records",
                    ));
                }
                let header =
                    RecordHeader::decode_bytes(payload.as_slice()).map_err(invalid_data)?;
                if header.route.recipient == self.identity.qid {
                    if !matches!(&self.transition, RouteTransition::Challenging(_)) {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "no active route challenge",
                        ));
                    }
                    return Ok(RouterAction::Authenticate(payload));
                }
                if self.routes.contains(&header.route.sender) {
                    Ok(RouterAction::Forward {
                        sender: header.route.sender,
                        recipient: header.route.recipient,
                        record: payload,
                    })
                } else {
                    Ok(RouterAction::Unauthenticated {
                        sender: header.route.sender,
                        recipient: header.route.recipient,
                    })
                }
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unexpected transport packet",
            )),
        }
    }

    pub fn begin_challenge(&mut self, bundle: PeerBundle) -> io::Result<(QID, Vec<u8>)> {
        if !matches!(&self.transition, RouteTransition::Ready) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "route transition already active",
            ));
        }
        bundle.validate(&SoftwareCrypto).map_err(invalid_data)?;
        let qid = bundle.qid;
        if self.routes.len() == MAX_ROUTES_PER_CONNECTION && !self.routes.contains(&qid) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "connection route limit reached",
            ));
        }
        let handshake_id = HandshakeId(self.next_handshake_id);
        let next_handshake_id = self
            .next_handshake_id
            .checked_add(1)
            .ok_or_else(|| io::Error::other("route handshake ID exhausted"))?;
        let (challenge, request) =
            PeerChallenge::new(&SoftwareCrypto, self.identity, bundle, handshake_id)
                .map_err(invalid_data)?;
        self.next_handshake_id = next_handshake_id;
        self.transition = RouteTransition::Challenging(challenge);
        Ok((qid, request))
    }

    pub fn verify_route(&mut self, record: &[u8]) -> io::Result<Vec<u8>> {
        let challenge = match std::mem::replace(&mut self.transition, RouteTransition::Ready) {
            RouteTransition::Challenging(challenge) => challenge,
            transition => {
                self.transition = transition;
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "no active route challenge",
                ));
            }
        };
        let (qid, acceptance) = challenge
            .verify(&SoftwareCrypto, record)
            .map_err(invalid_data)?;
        self.transition = RouteTransition::Verified(qid);
        Ok(acceptance)
    }

    pub fn commit_route(&mut self) -> io::Result<QID> {
        // callers commit only after queueing the acceptance record
        let qid = match std::mem::replace(&mut self.transition, RouteTransition::Ready) {
            RouteTransition::Verified(qid) => qid,
            transition => {
                self.transition = transition;
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "no verified route to commit",
                ));
            }
        };
        self.routes.insert(qid);
        Ok(qid)
    }

    pub fn routes(&self) -> impl Iterator<Item = QID> + '_ {
        self.routes.iter().copied()
    }
}

fn decode_handshake(bytes: &[u8]) -> Result<(RecordHeader, QlHandshakeRecord), ql_codec::Error> {
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

fn encode_handshake(header: RecordHeader, handshake: &QlHandshakeRecord) -> io::Result<Vec<u8>> {
    let length = header
        .encoded_len()
        .checked_add(handshake.encoded_len())
        .filter(|length| *length <= MAX_FRAME_SIZE)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "frame exceeds the transport limit",
            )
        })?;
    let mut frame = Vec::with_capacity(FRAME_HEADER_SIZE + length);
    (length as u32).encode(&mut frame);
    ql_wire::encode_record(&mut frame, header, handshake);
    Ok(frame)
}

fn invalid_data(error: impl std::error::Error + Send + Sync + 'static) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

#[cfg(test)]
mod tests {
    use std::io;

    use ql_codec::Encode;
    use ql_wire::{
        HandshakeId, IkHandshake, QL_WIRE_VERSION, QlHandshakeRecord, RecordHeader, RecordType,
        RouteHeader, SessionKey, SoftwareCrypto, TransportParams, generate_identity,
    };

    use super::{
        FRAME_HEADER_SIZE, Frame, FrameDecoder, MAX_FRAME_SIZE, MAX_RECORD_SIZE, PacketKind,
        SecureReceiver, SecureSender, decode_handshake,
    };

    #[test]
    fn fragmented_frames_and_truncated_input_are_distinguished() {
        let payload = b"frame";
        let mut encoded = Vec::new();
        (payload.len() as u32).encode(&mut encoded);
        encoded.extend_from_slice(payload);

        let mut decoder = FrameDecoder::new();
        let mut decoded = None;
        for byte in encoded {
            decoder.buffer()[0] = byte;
            decoded = decoder.advance(1).unwrap().or(decoded);
        }
        assert_eq!(decoded.unwrap().payload(), payload);
        assert!(decoder.finish().is_ok());

        decoder.buffer()[0] = 1;
        decoder.advance(1).unwrap();
        assert_eq!(
            decoder.finish().unwrap_err().kind(),
            io::ErrorKind::UnexpectedEof
        );

        let mut decoder = FrameDecoder::new();
        ((MAX_FRAME_SIZE + 1) as u32).encode(&mut decoder.buffer());
        assert!(decoder.advance(FRAME_HEADER_SIZE).is_err());
    }

    #[test]
    fn secure_frames_authenticate_all_bytes_and_preserve_nonce_after_size_errors() {
        let key = SessionKey([7; SessionKey::SIZE]);
        let mut sender = SecureSender::new(key.clone(), 4);
        let mut frame = Vec::new();
        assert!(
            sender
                .seal(&mut frame, PacketKind::Record, &[0; MAX_RECORD_SIZE + 1])
                .is_err()
        );
        sender
            .seal(&mut frame, PacketKind::Record, b"record")
            .unwrap();

        let mut receiver = SecureReceiver::new(key.clone(), 4);
        let (kind, payload) = receiver.open(Frame(frame.clone())).unwrap();
        assert_eq!(kind, PacketKind::Record);
        assert_eq!(payload, b"record");

        for index in [0, FRAME_HEADER_SIZE, FRAME_HEADER_SIZE + 1] {
            let mut changed = frame.clone();
            changed[index] ^= 1;
            assert!(
                SecureReceiver::new(key.clone(), 4)
                    .open(Frame(changed))
                    .is_err()
            );
        }

        let mut next_frame = Vec::new();
        sender
            .seal(&mut next_frame, PacketKind::Record, b"next")
            .unwrap();
        frame[FRAME_HEADER_SIZE + 1] ^= 1;
        let mut failed = SecureReceiver::new(key, 4);
        assert!(failed.open(Frame(frame)).is_err());
        assert!(failed.open(Frame(next_frame)).is_err());
    }

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
}
