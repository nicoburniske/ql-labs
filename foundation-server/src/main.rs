mod platform;
mod rpc;

use std::{fs, io::ErrorKind, os::unix::fs::PermissionsExt, sync::Arc};

use anyhow::{Context, Result};
use dashmap::DashMap;
use ql_codec::{Decode, Encode};
use ql_common::QID;
use ql_router::{DEFAULT_ADDRESS, attach, connect, receive, send};
use ql_runtime::{RuntimeConfig, RuntimeHandle, new_runtime};
use ql_wire::{
    PeerBundle, QlHandshakeRecord, QlIdentity, RecordHeader, RecordType, SoftwareCrypto,
    answer_peer_challenge, generate_identity,
};
use tokio::sync::mpsc;

use crate::platform::Platform;

type Peers = Arc<DashMap<QID, Peer>>;

struct Peer {
    inbound: mpsc::Sender<Vec<u8>>,
    _runtime: RuntimeHandle,
}

#[tokio::main]
async fn main() -> Result<()> {
    let crypto = SoftwareCrypto;
    let identity_path = std::env::var("QL_FOUNDATION_IDENTITY_PATH")
        .unwrap_or_else(|_| "foundation-server/identity.bin".into());
    let identity = match fs::read(&identity_path) {
        Ok(bytes) => {
            QlIdentity::decode_bytes(bytes.as_slice()).context("decoding foundation identity")?
        }
        Err(error) if error.kind() == ErrorKind::NotFound => {
            let identity = generate_identity(&crypto, "Foundation Server");
            fs::write(&identity_path, identity.encode_vec())
                .context("persisting foundation identity")?;
            identity
        }
        Err(error) => return Err(error).context("reading foundation identity"),
    };
    fs::set_permissions(&identity_path, fs::Permissions::from_mode(0o600))
        .context("securing foundation identity")?;
    let bundle_path = std::env::var("QL_FOUNDATION_BUNDLE_PATH")
        .unwrap_or_else(|_| "foundation-server/bundle.bin".into());
    fs::write(&bundle_path, identity.bundle().encode_vec())
        .context("writing foundation peer bundle")?;
    eprintln!("Foundation server QID: {}", hex::encode(identity.qid.0));
    eprintln!("Foundation server peer bundle: {bundle_path}");

    let router_bundle_path =
        std::env::var("QL_ROUTER_BUNDLE_PATH").unwrap_or_else(|_| "ql-router/bundle.bin".into());
    let router_bundle = fs::read(&router_bundle_path).context("reading router peer bundle")?;
    let router = PeerBundle::decode_bytes(router_bundle.as_slice())
        .context("decoding router peer bundle")?;
    let address = std::env::var("QL_ROUTER_ADDRESS").unwrap_or_else(|_| DEFAULT_ADDRESS.into());
    let (mut reader, mut writer) = connect(&address).await?;

    attach(&mut writer, &identity.bundle()).await?;
    let request = receive(&mut reader)
        .await?
        .context("router disconnected during registration")?;
    let (response, pending) = answer_peer_challenge(&crypto, &identity, router, &request)?;
    send(&mut writer, &response).await?;
    let mut accepted = receive(&mut reader)
        .await?
        .context("router disconnected before accepting the route")?;
    pending.verify(&crypto, &mut accepted)?;

    eprintln!("Foundation server registered with QL router");

    let (outbound, mut outbound_rx) = mpsc::channel::<Vec<u8>>(64);
    let peers = Peers::default();
    let writer = tokio::spawn(async move {
        while let Some(record) = outbound_rx.recv().await {
            if let Err(error) = send(&mut writer, &record).await {
                eprintln!("router write failed: {error}");
                break;
            }
        }
    });

    while let Some(record) = receive(&mut reader).await? {
        handle_inbound(record, &identity, &outbound, &peers).await;
    }
    writer.abort();
    anyhow::bail!("router disconnected")
}

async fn handle_inbound(
    record: Vec<u8>,
    identity: &QlIdentity,
    outbound: &mpsc::Sender<Vec<u8>>,
    peers: &Peers,
) {
    let header = match RecordHeader::decode_bytes(record.as_slice()) {
        Ok(header) if header.route.recipient == identity.qid => header,
        Ok(_) => return,
        Err(error) => {
            eprintln!("invalid routed record: {error}");
            return;
        }
    };
    let sender = header.route.sender;

    if let Some(inbound) = peers.get(&sender).map(|peer| peer.inbound.clone()) {
        if inbound.send(record).await.is_err() {
            peers.remove(&sender);
        }
        return;
    }

    if header.record_type != RecordType::Handshake
        || !matches!(
            QlHandshakeRecord::decode_bytes(&record[RecordHeader::WIRE_SIZE..]),
            Ok(QlHandshakeRecord::Ik1(_))
        )
    {
        return;
    }

    let (inbound, inbound_rx) = mpsc::channel(64);
    let platform = Platform::new(sender, outbound.clone(), inbound_rx, peers.clone());
    let (runtime, handle) = new_runtime(identity.clone(), platform, RuntimeConfig::default());
    inbound.send(record).await.expect("new runtime is alive");
    peers.insert(
        sender,
        Peer {
            inbound,
            _runtime: handle,
        },
    );
    tokio::spawn(runtime.run());
}
