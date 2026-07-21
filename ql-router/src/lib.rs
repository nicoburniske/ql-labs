use std::io;

use ql_codec::Encode;
use ql_wire::PeerBundle;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpStream, ToSocketAddrs, tcp::OwnedReadHalf, tcp::OwnedWriteHalf},
};

pub const DEFAULT_ADDRESS: &str = "127.0.0.1:7447";
pub const MAX_RECORD_SIZE: usize = 8 * 1024;

const DATA_FRAME: u8 = 1;
const ATTACH_FRAME: u8 = 2;
const FRAME_HEADER_SIZE: usize = 5;

pub enum Frame {
    Record(Vec<u8>),
    Attach(Vec<u8>),
}

pub async fn connect(address: impl ToSocketAddrs) -> io::Result<(OwnedReadHalf, OwnedWriteHalf)> {
    Ok(TcpStream::connect(address).await?.into_split())
}

pub async fn receive(reader: &mut (impl AsyncRead + Unpin)) -> io::Result<Option<Vec<u8>>> {
    match receive_frame(reader).await? {
        Some(Frame::Record(record)) => Ok(Some(record)),
        Some(Frame::Attach(_)) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unexpected attach frame",
        )),
        None => Ok(None),
    }
}

pub async fn receive_frame(reader: &mut (impl AsyncRead + Unpin)) -> io::Result<Option<Frame>> {
    let mut header = [0; FRAME_HEADER_SIZE];
    if reader.read(&mut header[..1]).await? == 0 {
        return Ok(None);
    }
    reader.read_exact(&mut header[1..]).await?;
    let length = u32::from_be_bytes(header[1..].try_into().unwrap()) as usize;
    if length > MAX_RECORD_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame exceeds the record limit",
        ));
    }

    let mut payload = vec![0; length];
    reader.read_exact(&mut payload).await?;
    match header[0] {
        DATA_FRAME => Ok(Some(Frame::Record(payload))),
        ATTACH_FRAME => Ok(Some(Frame::Attach(payload))),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported frame type",
        )),
    }
}

pub async fn send(writer: &mut (impl AsyncWrite + Unpin), record: &[u8]) -> io::Result<()> {
    send_frame(writer, DATA_FRAME, record).await
}

pub async fn attach(writer: &mut (impl AsyncWrite + Unpin), bundle: &PeerBundle) -> io::Result<()> {
    send_frame(writer, ATTACH_FRAME, &bundle.encode_vec()).await
}

async fn send_frame(
    writer: &mut (impl AsyncWrite + Unpin),
    kind: u8,
    payload: &[u8],
) -> io::Result<()> {
    if payload.len() > MAX_RECORD_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "frame exceeds the record limit",
        ));
    }

    let mut header = [0; FRAME_HEADER_SIZE];
    header[0] = kind;
    header[1..].copy_from_slice(&(payload.len() as u32).to_be_bytes());
    writer.write_all(&header).await?;
    writer.write_all(payload).await
}
