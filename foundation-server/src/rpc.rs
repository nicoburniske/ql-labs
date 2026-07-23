use std::future::Future;

use ql_api::{
    DownloadBenchmark, DownloadBenchmarkHeader, DownloadBenchmarkParams,
    DownloadBenchmarkPartHeader, EchoParams, EchoResponse, RequestEcho,
};
use ql_common::StreamInfo;
use ql_fsm::Bytes;
use ql_keyos::ServiceRouteKey;
use ql_rpc::{
    SendSpawner, Spawner,
    download::{DownloadHandler, DownloadStart},
    request::{RequestHandler, Response},
};
use ql_runtime::{QlStream, StreamWriter};
use sha2::{Digest, Sha256};

pub struct Rpc {
    router: ql_rpc::Router<ServiceRouteKey, Service, QlStream, TokioSpawner>,
}

impl Rpc {
    pub fn new() -> Self {
        Self {
            router: ql_rpc::Router::builder_send(TokioSpawner)
                .request::<RequestEcho>()
                .download::<DownloadBenchmark>()
                .build(Service),
        }
    }

    pub fn handle(&self, info: StreamInfo, stream: QlStream) {
        self.router.handle(info, stream);
    }
}

#[derive(Clone, Copy)]
struct Service;

impl RequestHandler<RequestEcho, QlStream> for Service {
    async fn handle(
        self,
        context: ql_rpc::Context,
        request: EchoParams,
        response: Response<EchoResponse, StreamWriter>,
    ) {
        eprintln!(
            "echo from {}: {}",
            hex::encode(context.qid.0),
            request.message
        );
        if let Err(error) = response
            .respond(EchoResponse {
                message: request.message,
            })
            .await
        {
            eprintln!("echo response failed: {error}");
        }
    }
}

impl DownloadHandler<DownloadBenchmark, QlStream> for Service {
    async fn handle(
        self,
        context: ql_rpc::Context,
        request: DownloadBenchmarkParams,
        download: DownloadStart<DownloadBenchmark, StreamWriter>,
    ) {
        const CHUNK_LEN: usize = 4096;

        eprintln!(
            "download request from {}: {} bytes",
            hex::encode(context.qid.0),
            request.length
        );
        let result: anyhow::Result<()> = async {
            let mut pattern = vec![0; CHUNK_LEN];
            for (index, byte) in pattern.iter_mut().enumerate() {
                *byte = (index % 251) as u8;
            }
            let pattern = Bytes::from(pattern);

            let mut hasher = Sha256::new();
            let mut remaining = request.length;
            while remaining > 0 {
                let length = remaining.min(CHUNK_LEN as u64) as usize;
                hasher.update(&pattern[..length]);
                remaining -= length as u64;
            }

            let mut writer = download
                .start(DownloadBenchmarkHeader {
                    hash: hasher.finalize().to_vec(),
                })
                .await?;
            let mut part = writer.start_part(DownloadBenchmarkPartHeader {}).await?;

            remaining = request.length;
            while remaining > 0 {
                let length = remaining.min(CHUNK_LEN as u64) as usize;
                part.send(pattern.slice(..length)).await?;
                remaining -= length as u64;
            }
            part.finish().await?;
            writer.finish().await?;
            Ok(())
        }
        .await;

        match result {
            Ok(()) => eprintln!("download complete: {} bytes", request.length),
            Err(error) => eprintln!("download failed: {error:#}"),
        }
    }
}

#[derive(Clone, Copy)]
struct TokioSpawner;

impl Spawner for TokioSpawner {
    type Handle = tokio::task::JoinHandle<()>;
}

impl SendSpawner for TokioSpawner {
    fn spawn<F>(&self, future: F) -> Self::Handle
    where
        F: Future<Output = ()> + Send + 'static,
    {
        tokio::spawn(future)
    }
}
