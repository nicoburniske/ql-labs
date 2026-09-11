use std::{future::Future, str::Utf8Error};

use ql_rpc::{
    Route, RpcRouteKey, SendSpawner, Spawner,
    download::{Download, DownloadHandler, DownloadStart},
    request::{Request, RequestHandler, Response},
};
use ql_runtime::{QlStream, QlStreamError, StreamWriter};
use tokio::sync::mpsc;

pub struct Ping;

impl Route for Ping {
    type Key = Key;

    fn key() -> Key {
        Key(1)
    }
}

impl Request for Ping {
    type Error = Utf8Error;
    type Request = String;
    type Response = String;
}

#[derive(Clone)]
pub struct Service {
    pub completed: mpsc::Sender<Result<(), QlStreamError>>,
}

pub struct Transfer;

impl Route for Transfer {
    type Key = Key;

    fn key() -> Key {
        Key(2)
    }
}

impl Download for Transfer {
    type Error = Utf8Error;
    type Request = String;
    type ResponseHeader = String;
    type PartHeader = String;
}

impl DownloadHandler<Transfer, QlStream> for Service {
    async fn handle(
        self,
        _: ql_rpc::Context,
        request: String,
        download: DownloadStart<Transfer, StreamWriter>,
    ) {
        let Ok(length @ 1..=16777216) = request.parse::<usize>() else {
            download.reset(ql_common::ResetCode::PROTOCOL);
            return;
        };
        let result = async {
            let pattern =
                bytes::Bytes::from((0..4096).map(|i| (i % 251) as u8).collect::<Vec<_>>());
            let mut writer = download.start(request).await?;
            let mut part = writer.start_part("pattern.bin".into()).await?;
            let mut remaining = length;
            while remaining > 0 {
                let size = remaining.min(pattern.len());
                part.send(pattern.slice(..size)).await?;
                remaining -= size;
            }
            part.finish().await?;
            writer.finish().await
        }
        .await;
        let _ = self.completed.send(result).await;
    }
}

impl RequestHandler<Ping, QlStream> for Service {
    async fn handle(
        self,
        _: ql_rpc::Context,
        request: String,
        response: Response<String, StreamWriter>,
    ) {
        let Some(sequence) = request.strip_prefix("ping ") else {
            response.reset(ql_common::ResetCode::PROTOCOL);
            return;
        };
        let result = response.respond(format!("pong {sequence}")).await;
        let _ = self.completed.send(result).await;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Key(u8);

impl RpcRouteKey for Key {
    fn encoded_len(&self) -> usize {
        1
    }

    fn encode<W: bytes::BufMut + ?Sized>(&self, out: &mut W) {
        out.put_u8(self.0);
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        match bytes {
            [id @ (1 | 2)] => Some(Self(*id)),
            _ => None,
        }
    }
}

#[derive(Clone, Copy)]
pub struct TokioSpawner;

impl Spawner for TokioSpawner {
    type Handle = tokio::task::JoinHandle<()>;
}

impl SendSpawner for TokioSpawner {
    fn spawn<F: Future<Output = ()> + Send + 'static>(&self, future: F) -> Self::Handle {
        tokio::spawn(future)
    }
}
