mod platform;
mod rpc;

use std::{fs, io::ErrorKind, os::unix::fs::PermissionsExt, path::PathBuf, sync::Arc};

use dashmap::DashMap;
use figment::{
    Figment,
    providers::{Env, Serialized},
};
use ql_codec::{Decode, Encode};
use ql_common::QID;
use ql_router::{
    DEFAULT_ADDRESS,
    tokio::{attach, connect, receive, send},
};
use ql_runtime::{RuntimeConfig, RuntimeHandle, new_runtime};
use ql_wire::{
    PeerBundle, QlHandshakeRecord, QlIdentity, RecordHeader, RecordType, SoftwareCrypto,
    answer_peer_challenge, generate_identity,
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::platform::Platform;

type Peers = Arc<DashMap<QID, Peer>>;

#[derive(Deserialize, Serialize)]
struct Config {
    identity_path: PathBuf,
    bundle_path: PathBuf,
    router: RouterConfig,
}

#[derive(Deserialize, Serialize)]
struct RouterConfig {
    bundle_path: PathBuf,
    address: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            identity_path: "foundation-server/identity.bin".into(),
            bundle_path: "foundation-server/bundle.bin".into(),
            router: RouterConfig {
                bundle_path: "ql-router/bundle.bin".into(),
                address: DEFAULT_ADDRESS.into(),
            },
        }
    }
}

struct Peer {
    inbound: mpsc::Sender<Vec<u8>>,
    _runtime: RuntimeHandle,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config: Config = Figment::from(Serialized::defaults(Config::default()))
        .merge(Env::prefixed("QL_FOUNDATION_"))
        .merge(Env::prefixed("QL_ROUTER_").map(|key| format!("router.{key}").into()))
        .extract()?;
    let crypto = SoftwareCrypto;
    let identity = match fs::read(&config.identity_path) {
        Ok(bytes) => QlIdentity::decode_bytes(bytes.as_slice())?,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            let identity = generate_identity(&crypto, "Foundation Server");
            fs::write(&config.identity_path, identity.encode_vec())?;
            identity
        }
        Err(error) => return Err(error.into()),
    };
    fs::set_permissions(&config.identity_path, fs::Permissions::from_mode(0o600))?;
    fs::write(&config.bundle_path, identity.bundle().encode_vec())?;
    eprintln!("Foundation server QID: {}", hex::encode(identity.qid.0));
    eprintln!(
        "Foundation server peer bundle: {}",
        config.bundle_path.display()
    );

    let router_bundle = fs::read(&config.router.bundle_path)?;
    let router = PeerBundle::decode_bytes(router_bundle.as_slice())?;
    let (mut reader, mut writer) = connect(&config.router.address, &router).await?;

    attach(&mut writer, &identity.bundle()).await?;
    let request = receive(&mut reader).await?.unwrap();
    let (response, pending) = answer_peer_challenge(&crypto, &identity, router, &request)?;
    send(&mut writer, &response).await?;
    let mut accepted = receive(&mut reader).await?.unwrap();
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
