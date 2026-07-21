use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use ql_common::{QID, StreamInfo};
use ql_fsm::{PeerStatus, ReceiveError};
use ql_runtime::{
    QlStream,
    platform::{QlInbound, QlPlatform, QlTimer},
};
use ql_wire::{PeerBundle, SoftwareCrypto};
use tokio::{sync::mpsc, time::Sleep};

use crate::{Peers, rpc::Rpc};

pub struct Platform {
    peer: QID,
    outbound: mpsc::Sender<Vec<u8>>,
    inbound: Option<mpsc::Receiver<Vec<u8>>>,
    peers: Peers,
    rpc: Rpc,
}

impl Platform {
    pub fn new(
        peer: QID,
        outbound: mpsc::Sender<Vec<u8>>,
        inbound: mpsc::Receiver<Vec<u8>>,
        peers: Peers,
    ) -> Self {
        Self {
            peer,
            outbound,
            inbound: Some(inbound),
            peers,
            rpc: Rpc::new(),
        }
    }
}

impl QlPlatform for Platform {
    type Crypto = SoftwareCrypto;
    type Timer = Timer;
    type WriteMessageFut<'a> = Pin<Box<dyn Future<Output = bool> + Send + 'a>>;
    type Inbound = Inbound;

    fn crypto(&self) -> &Self::Crypto {
        &SoftwareCrypto
    }

    fn write_message(&self, message: Vec<u8>) -> Self::WriteMessageFut<'_> {
        Box::pin(async move { self.outbound.send(message).await.is_ok() })
    }

    fn inbound(&mut self) -> Self::Inbound {
        Inbound {
            receiver: self.inbound.take().expect("inbound already taken"),
        }
    }

    fn timer(&self) -> Self::Timer {
        Timer {
            sleep: Box::pin(tokio::time::sleep(Duration::from_secs(365 * 24 * 60 * 60))),
        }
    }

    fn persist_peer(&self, peer: PeerBundle) {
        eprintln!("authenticated {} ({})", hex::encode(peer.qid.0), peer.name);
    }

    fn handle_peer_status(&self, peer: Option<QID>, status: PeerStatus) {
        eprintln!(
            "peer {}: {status:?}",
            peer.map_or_else(|| "unknown".into(), |qid| hex::encode(qid.0))
        );
        if matches!(status, PeerStatus::Disconnected | PeerStatus::Unpaired) {
            self.peers.remove(&self.peer);
        }
    }

    fn handle_inbound(&self, info: StreamInfo, stream: QlStream) {
        self.rpc.handle(info, stream);
    }

    fn handle_recv_error(&self, error: ReceiveError) {
        eprintln!("rejected QL record: {error:?}");
        self.peers.remove(&self.peer);
    }
}

pub struct Inbound {
    receiver: mpsc::Receiver<Vec<u8>>,
}

impl QlInbound for Inbound {
    fn poll_recv(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Vec<u8>> {
        match self.receiver.poll_recv(cx) {
            Poll::Ready(Some(message)) => Poll::Ready(message),
            Poll::Ready(None) | Poll::Pending => Poll::Pending,
        }
    }
}

pub struct Timer {
    sleep: Pin<Box<Sleep>>,
}

impl QlTimer for Timer {
    fn set_deadline(mut self: Pin<&mut Self>, deadline: Option<Instant>) {
        self.sleep.as_mut().reset(deadline.map_or_else(
            || tokio::time::Instant::now() + Duration::from_secs(365 * 24 * 60 * 60),
            tokio::time::Instant::from_std,
        ));
    }

    fn poll_wait(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        self.sleep.as_mut().poll(cx)
    }
}
