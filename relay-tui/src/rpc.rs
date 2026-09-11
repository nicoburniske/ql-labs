use std::future::Future;

use ql_api::{
    DownloadBenchmark, DownloadBenchmarkHeader, DownloadBenchmarkParams,
    DownloadBenchmarkPartHeader, EchoParams, EchoResponse, RequestEcho,
};
use ql_rpc::{
    download::{DownloadHandler, DownloadStart},
    request::{RequestHandler, Response},
};
use ql_runtime::{QlStream, StreamWriter};
use sha2::{Digest, Sha256};
use tokio::sync::watch;

#[derive(Clone)]
pub struct Service {
    pub activity: watch::Sender<Activity>,
}

#[derive(Clone, Default)]
pub struct Activity {
    pub echo: String,
    pub download: String,
}

impl RequestHandler<RequestEcho, QlStream> for Service {
    async fn handle(
        self,
        _: ql_rpc::Context,
        request: EchoParams,
        response: Response<EchoResponse, StreamWriter>,
    ) {
        self.activity
            .send_modify(|activity| activity.echo = format!("In: {}", request.message));
        if let Err(error) = response
            .respond(EchoResponse {
                message: request.message,
            })
            .await
        {
            self.activity
                .send_modify(|activity| activity.echo = format!("In: {error}"));
        }
    }
}

impl DownloadHandler<DownloadBenchmark, QlStream> for Service {
    async fn handle(
        self,
        _: ql_rpc::Context,
        request: DownloadBenchmarkParams,
        download: DownloadStart<DownloadBenchmark, StreamWriter>,
    ) {
        let length = request.length;
        if length == 0 || length > 16 * 1024 * 1024 {
            download.reset(ql_common::ResetCode::PROTOCOL);
            return;
        }
        let result = async {
            let pattern =
                bytes::Bytes::from((0..32768).map(|i| (i % 251) as u8).collect::<Vec<_>>());
            let mut hash = Sha256::new();
            let mut remaining = length as usize;
            while remaining > 0 {
                let size = remaining.min(pattern.len());
                hash.update(&pattern[..size]);
                remaining -= size;
            }
            let mut writer = download
                .start(DownloadBenchmarkHeader {
                    hash: hash.finalize().to_vec(),
                })
                .await?;
            let mut part = writer.start_part(DownloadBenchmarkPartHeader {}).await?;
            remaining = length as usize;
            while remaining > 0 {
                let size = remaining.min(pattern.len());
                part.send(pattern.slice(..size)).await?;
                remaining -= size;
            }
            part.finish().await?;
            writer.finish().await
        }
        .await;
        self.activity.send_modify(|activity| {
            activity.download = match result {
                Ok(()) => format!("served {length} bytes to Prime"),
                Err(error) => format!("download failed: {error}"),
            }
        });
    }
}

#[derive(Clone, Copy)]
pub struct TokioSpawner;

impl ql_rpc::Spawner for TokioSpawner {
    type Handle = tokio::task::JoinHandle<()>;
}

impl ql_rpc::SendSpawner for TokioSpawner {
    fn spawn<F: Future<Output = ()> + Send + 'static>(&self, future: F) -> Self::Handle {
        tokio::spawn(future)
    }
}
