mod platform;
mod rpc;

use std::time::{Duration, Instant};

use anyhow::{Context, bail, ensure};
use ql_codec::Decode;
use ql_fsm::PeerStatus;
use ql_runtime::{QlStreamError, RuntimeConfig, RuntimeHandle, StreamOptions, new_runtime};
use ql_wire::{PeerBundle, QlIdentity, SoftwareCrypto, generate_identity};
use tokio::{
    sync::{mpsc, watch},
    task::JoinSet,
    time::timeout,
};

use platform::Platform;
use rpc::{Key, Ping, Service, TokioSpawner, Transfer};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let address = args
        .next()
        .unwrap_or_else(|| "router.foundation.xyz:7447".into());
    let bundle_path = args.next().unwrap_or_else(|| "bundle.bin".into());
    ensure!(
        args.next().is_none(),
        "usage: router-ping-pong [router:port] [bundle.bin]"
    );
    let bytes = std::fs::read(&bundle_path).context("read router public bundle")?;
    let router = PeerBundle::decode_bytes(bytes.as_slice())?;
    router.validate(&SoftwareCrypto)?;
    println!("router: {address} ({})", hex::encode(router.qid.0));

    let alice = generate_identity(&SoftwareCrypto, "alice");
    let bob = generate_identity(&SoftwareCrypto, "bob");
    let alice_bundle = alice.bundle();
    let bob_bundle = bob.bundle();
    let mut tasks = JoinSet::new();
    let (completed_tx, mut completed) = mpsc::channel(1);

    let exchange = async {
        let (alice, mut alice_status) =
            start_peer(alice, &router, &address, completed_tx.clone(), &mut tasks).await?;
        let (bob, mut bob_status) =
            start_peer(bob, &router, &address, completed_tx, &mut tasks).await?;

        // simulate bundles saved during an earlier pairing
        alice.bind_peer(bob_bundle);
        bob.bind_peer(alice_bundle);
        alice.connect();
        alice_status
            .wait_for(|status| *status == PeerStatus::Connected)
            .await?;
        bob_status
            .wait_for(|status| *status == PeerStatus::Connected)
            .await?;
        println!("post-pair QLv2 session established");

        for round in 1..=3 {
            for (name, peer) in [("alice -> bob", &alice), ("bob -> alice", &bob)] {
                let request = format!("ping {round}");
                let started = Instant::now();
                let response = peer
                    .rpc()
                    .request::<Ping>(&request, StreamOptions::default())
                    .await?;
                ensure!(
                    response == format!("pong {round}"),
                    "unexpected response: {response:?}"
                );
                println!("{name}: {request} / {response} ({:.1?})", started.elapsed());
                // keep both runtimes alive until the responder's stream is acknowledged
                completed.recv().await.context("responder stopped")??;
            }
        }
        for length in [1024 * 1024, 8 * 1024 * 1024] {
            for (name, peer) in [("bob -> alice", &alice), ("alice -> bob", &bob)] {
                let started = Instant::now();
                let call = peer
                    .rpc()
                    .download::<Transfer>(&length.to_string(), StreamOptions::default())
                    .await?;
                let (header, mut parts) = call.start().await?;
                ensure!(
                    header == length.to_string(),
                    "incorrect transfer length header"
                );
                let mut received = 0;
                {
                    let (filename, mut part) =
                        parts.next_part().await?.context("missing byte part")?;
                    ensure!(filename == "pattern.bin", "unexpected filename");
                    loop {
                        let chunk = part.read_chunk().await?;
                        if chunk.is_empty() {
                            break;
                        }
                        ensure!(received + chunk.len() <= length, "too many bytes");
                        for (offset, byte) in chunk.iter().enumerate() {
                            ensure!(
                                *byte == (((received + offset) % 4096) % 251) as u8,
                                "byte mismatch at {}",
                                received + offset
                            );
                        }
                        received += chunk.len();
                    }
                }
                ensure!(parts.next_part().await?.is_none(), "unexpected extra part");
                parts.complete().await?;
                ensure!(
                    received == length,
                    "received {received} bytes, expected {length}"
                );
                let elapsed = started.elapsed();
                completed.recv().await.context("responder stopped")??;
                println!(
                    "{name}: verified {} MiB in {:.3}s = {:.2} MiB/s ({:.2} Mbit/s)",
                    length / (1024 * 1024),
                    elapsed.as_secs_f64(),
                    length as f64 / elapsed.as_secs_f64() / 1048576.0,
                    length as f64 * 8.0 / elapsed.as_secs_f64() / 1e6
                );
            }
        }
        Ok::<_, anyhow::Error>(())
    };
    // setup and exchange share a deadline so a broken connection cannot hang the example
    timeout(Duration::from_secs(180), exchange)
        .await
        .context("router ping/pong timed out")??;
    tasks.shutdown().await;
    println!("verified all 6 ping responses and 4 byte transfers");
    Ok(())
}

async fn start_peer(
    identity: QlIdentity,
    router: &PeerBundle,
    address: &str,
    completed: mpsc::Sender<Result<(), QlStreamError>>,
    tasks: &mut JoinSet<anyhow::Result<()>>,
) -> anyhow::Result<(RuntimeHandle, watch::Receiver<PeerStatus>)> {
    let name = identity.bundle().name;
    let (mut reader, mut writer) = ql_router::connect(address, router).await?;
    ql_router::attach(&mut reader, &mut writer, &identity, router).await?;
    println!("{name}: attached {}", hex::encode(identity.qid.0));

    let (outbound, mut outbound_rx) = mpsc::channel(64);
    let (inbound_tx, inbound) = mpsc::channel(64);
    let (status, status_rx) = watch::channel(PeerStatus::Disconnected);
    let platform = Platform {
        outbound,
        inbound: Some(inbound),
        status,
        rpc: ql_rpc::Router::<Key, _, _, TokioSpawner>::builder_send(TokioSpawner)
            .request::<Ping>()
            .download::<Transfer>()
            .build(Service { completed }),
    };
    let mut config = RuntimeConfig::default();
    config.fsm.session.stream_send_buffer_size = 256 * 1024;
    config.fsm.session.initial_stream_receive_window = 256 * 1024;
    config.fsm.session.max_stream_receive_window = 256 * 1024;
    let (runtime, handle) = new_runtime(identity, platform, config);
    tasks.spawn(async move {
        let read = async {
            while let Some(record) = ql_router::receive(&mut reader).await? {
                inbound_tx.send(record).await?;
            }
            bail!("router disconnected")
        };
        let write = async {
            while let Some(record) = outbound_rx.recv().await {
                ql_router::send(&mut writer, &record).await?;
            }
            bail!("runtime outbound closed")
        };
        let result: anyhow::Result<()> = tokio::select! {
            result = read => result,
            result = write => result,
            _ = runtime.run() => anyhow::bail!("runtime stopped"),
        };
        if let Err(error) = &result {
            eprintln!("{name}: {error:#}");
        }
        result
    });
    Ok((handle, status_rx))
}
