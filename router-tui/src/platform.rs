use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use ql_common::{QID, StreamInfo};
use ql_fsm::PeerStatus;
use ql_runtime::{
    QlStream,
    platform::{QlInbound, QlPlatform, QlTimer},
};
use ql_wire::{PeerBundle, SoftwareCrypto};
use tokio::{
    sync::{mpsc, watch},
    time::Sleep,
};

use crate::rpc::{Service, TokioSpawner};
use ql_keyos::ServiceRouteKey;

pub struct Platform {
    pub outbound: mpsc::Sender<Vec<u8>>,
    pub inbound: Option<mpsc::Receiver<Vec<u8>>>,
    pub status: watch::Sender<PeerStatus>,
    pub peer: watch::Sender<Option<PeerBundle>>,
    pub rpc: ql_rpc::Router<ServiceRouteKey, Service, QlStream, TokioSpawner>,
}

impl QlPlatform for Platform {
    type Crypto = SoftwareCrypto;
    type Timer = Timer;
    type WriteMessageFut<'a> = Pin<Box<dyn Future<Output = bool> + Send + 'a>>;
    type Inbound = Inbound;

    fn crypto(&self) -> &SoftwareCrypto {
        &SoftwareCrypto
    }

    fn write_message(&self, message: Vec<u8>) -> Self::WriteMessageFut<'_> {
        Box::pin(async move { self.outbound.send(message).await.is_ok() })
    }

    fn inbound(&mut self) -> Inbound {
        Inbound(self.inbound.take().expect("inbound already taken"))
    }

    fn timer(&self) -> Timer {
        Timer {
            sleep: Box::pin(tokio::time::sleep(Duration::ZERO)),
            armed: false,
        }
    }

    fn persist_peer(&self, peer: PeerBundle) {
        self.peer.send_replace(Some(peer));
    }

    fn handle_peer_status(&self, _: Option<QID>, status: PeerStatus) {
        self.status.send_replace(status);
    }

    fn handle_inbound(&self, info: StreamInfo, stream: QlStream) {
        self.rpc.handle(info, stream);
    }
}

pub struct Inbound(mpsc::Receiver<Vec<u8>>);

impl QlInbound for Inbound {
    fn poll_recv(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Vec<u8>> {
        match self.0.poll_recv(cx) {
            Poll::Ready(Some(record)) => Poll::Ready(record),
            Poll::Ready(None) | Poll::Pending => Poll::Pending,
        }
    }
}

pub struct Timer {
    sleep: Pin<Box<Sleep>>,
    armed: bool,
}

impl QlTimer for Timer {
    fn set_deadline(mut self: Pin<&mut Self>, deadline: Option<Instant>) {
        self.armed = deadline.is_some();
        if let Some(deadline) = deadline {
            self.sleep
                .as_mut()
                .reset(tokio::time::Instant::from_std(deadline));
        }
    }

    fn poll_wait(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.armed {
            self.sleep.as_mut().poll(cx)
        } else {
            Poll::Pending
        }
    }
}
