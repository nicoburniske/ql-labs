use std::{io, time::Duration};

use ql_codec::Encode;
use ql_wire::PeerBundle;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::{TcpStream, ToSocketAddrs, tcp::OwnedReadHalf, tcp::OwnedWriteHalf},
    time::{Instant, timeout, timeout_at},
};

use crate::protocol::{
    ClientHandshake, Frame, FrameDecoder, PacketKind, SecureReceiver, SecureSender,
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

pub struct Receiver {
    tcp: OwnedReadHalf,
    frames: FrameDecoder,
    secure: SecureReceiver,
}

pub struct Sender {
    tcp: SocketWriter<OwnedWriteHalf>,
    secure: SecureSender,
    frame: Vec<u8>,
}

pub async fn connect(
    address: impl ToSocketAddrs,
    router: &PeerBundle,
) -> io::Result<(Receiver, Sender)> {
    timeout(CONNECT_TIMEOUT, async {
        let mut tcp = TcpStream::connect(address).await?;
        tcp.set_nodelay(true)?;
        let (handshake, request) = ClientHandshake::start(router)?;
        tcp.write_all(&request).await?;

        let mut frames = FrameDecoder::new();
        let response = read_frame(&mut frames, &mut tcp)
            .await?
            .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "router disconnected"))?;
        let (secure_rx, mut secure_tx) = handshake.finish(&response)?;
        let mut frame = Vec::new();
        secure_tx.seal(&mut frame, PacketKind::Confirm, &[])?;
        tcp.write_all(&frame).await?;

        let (tcp, writer) = tcp.into_split();
        Ok((
            Receiver {
                tcp,
                frames,
                secure: secure_rx,
            },
            Sender {
                tcp: SocketWriter::new(writer),
                secure: secure_tx,
                frame,
            },
        ))
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "router connection timed out"))?
}

pub async fn receive(receiver: &mut Receiver) -> io::Result<Option<Vec<u8>>> {
    let Some(frame) = read_frame(&mut receiver.frames, &mut receiver.tcp).await? else {
        return Ok(None);
    };
    let (kind, payload) = receiver.secure.open(frame)?;
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
    send_packet(sender, PacketKind::Attach, &bundle.encode_vec()).await
}

pub async fn read_frame(
    decoder: &mut FrameDecoder,
    reader: &mut (impl AsyncRead + Unpin),
) -> io::Result<Option<Frame>> {
    read_frame_inner(decoder, reader, None).await
}

pub async fn read_frame_with_timeout(
    decoder: &mut FrameDecoder,
    reader: &mut (impl AsyncRead + Unpin),
    assembly_timeout: Duration,
) -> io::Result<Option<Frame>> {
    read_frame_inner(decoder, reader, Some(assembly_timeout)).await
}

async fn send_packet(sender: &mut Sender, kind: PacketKind, payload: &[u8]) -> io::Result<()> {
    sender.secure.seal(&mut sender.frame, kind, payload)?;
    sender.tcp.write_all(&sender.frame).await
}

async fn read_frame_inner(
    decoder: &mut FrameDecoder,
    reader: &mut (impl AsyncRead + Unpin),
    assembly_timeout: Option<Duration>,
) -> io::Result<Option<Frame>> {
    let mut deadline = assembly_timeout
        .filter(|_| decoder.is_partial())
        .map(|duration| Instant::now() + duration);
    loop {
        let read = if let Some(deadline) = deadline {
            timeout_at(deadline, reader.read(decoder.buffer()))
                .await
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::TimedOut, "frame assembly timed out")
                })??
        } else {
            reader.read(decoder.buffer()).await?
        };
        if read == 0 {
            decoder.finish()?;
            return Ok(None);
        }
        if let Some(frame) = decoder.advance(read)? {
            return Ok(Some(frame));
        }
        if deadline.is_none() {
            deadline = assembly_timeout.map(|duration| Instant::now() + duration);
        }
    }
}

struct SocketWriter<W> {
    inner: W,
    writable: bool,
}

impl<W: tokio::io::AsyncWrite + Unpin> SocketWriter<W> {
    fn new(inner: W) -> Self {
        Self {
            inner,
            writable: true,
        }
    }

    async fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        if !self.writable {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "sender write was cancelled or failed",
            ));
        }
        // cancellation leaves this false so no later nonce can follow a partial frame
        self.writable = false;
        self.inner.write_all(bytes).await?;
        self.writable = true;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        task::{Context, Poll, Waker},
    };

    use super::*;

    #[tokio::test]
    async fn cancelled_write_poisons_writer() {
        let (writer, _reader) = tokio::io::duplex(1);
        let mut writer = SocketWriter::new(writer);
        let mut write = Box::pin(writer.write_all(&[0; 64]));
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(write.as_mut().poll(&mut context), Poll::Pending));
        drop(write);

        let error = writer.write_all(&[]).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
    }

    #[tokio::test]
    async fn partial_frame_deadline_starts_on_first_byte_and_does_not_reset() {
        let (mut writer, mut reader) = tokio::io::duplex(64);
        let writes = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(150)).await;
            for _ in 0..3 {
                if writer.write_all(&[0]).await.is_err() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(60)).await;
            }
        });
        let started = Instant::now();
        let mut decoder = FrameDecoder::new();
        let error = match tokio::time::timeout(
            Duration::from_secs(1),
            read_frame_with_timeout(&mut decoder, &mut reader, Duration::from_millis(100)),
        )
        .await
        {
            Ok(Err(error)) => error,
            Ok(Ok(_)) => panic!("partial frame unexpectedly completed"),
            Err(_) => panic!("partial frame timeout was not enforced"),
        };
        writes.abort();

        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(started.elapsed() >= Duration::from_millis(150));
    }
}
