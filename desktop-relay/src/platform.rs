use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use blit_desktop::EventLoopProxy;
use ql_common::{QID, StreamInfo};
use ql_fsm::{PeerStatus, ReceiveError};
use ql_runtime::{
    QlStream,
    platform::{QlInbound, QlPlatform, QlTimer},
};
use ql_wire::{PeerBundle, SoftwareCrypto};
use tokio::{sync::mpsc, time::Sleep};

use crate::connection::{Connection, Event};

pub struct Platform {
    pub connection: Connection,
    pub inbound: Option<mpsc::Receiver<Vec<u8>>>,
    pub events: EventLoopProxy<crate::Event>,
}

impl QlPlatform for Platform {
    type Crypto = SoftwareCrypto;
    type Timer = Timer;
    type WriteMessageFut<'a> = Pin<Box<dyn Future<Output = bool> + 'a>>;
    type Inbound = Inbound;

    fn crypto(&self) -> &Self::Crypto {
        &SoftwareCrypto
    }

    fn write_message(&self, message: Vec<u8>) -> Self::WriteMessageFut<'_> {
        Box::pin(self.connection.write(message))
    }

    fn inbound(&mut self) -> Self::Inbound {
        Inbound {
            receiver: self
                .inbound
                .take()
                .expect("inbound transport already taken"),
        }
    }

    fn timer(&self) -> Self::Timer {
        Timer {
            sleep: Box::pin(tokio::time::sleep(Duration::from_secs(365 * 24 * 60 * 60))),
        }
    }

    fn persist_peer(&self, peer: PeerBundle) {
        eprintln!("paired QID {} ({})", hex::encode(peer.qid.0), peer.name);
        self.connection.peer(peer);
    }

    fn handle_peer_status(&self, peer: Option<QID>, status: PeerStatus) {
        eprintln!(
            "QLv2 peer status: peer={:?} status={status:?}",
            peer.map(|qid| hex::encode(qid.0))
        );
        self.connection.status(peer, status);
        self.events
            .send_event(crate::Event::Connection(Event::Peer(status)))
            .ok();
    }

    fn handle_inbound(&self, info: StreamInfo, _: QlStream) {
        eprintln!(
            "ignoring inbound QL stream from {} with {} header bytes",
            hex::encode(info.qid.0),
            info.header.len()
        );
    }

    fn handle_recv_error(&self, error: ReceiveError) {
        eprintln!("rejected QL record: {error:?}");
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
        let deadline = deadline.map_or_else(
            || tokio::time::Instant::now() + Duration::from_secs(365 * 24 * 60 * 60),
            tokio::time::Instant::from_std,
        );
        self.sleep.as_mut().reset(deadline);
    }

    fn poll_wait(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        self.sleep.as_mut().poll(cx)
    }
}
