use std::{pin::Pin, time::Duration};

use anyhow::{Context as _, Result, anyhow};
use btleplug::{
    api::{
        Central, Characteristic, Manager as _, Peripheral as _, ScanFilter, ValueNotification,
        WriteType,
    },
    platform::{Manager, Peripheral},
};
use futures_lite::{Stream, StreamExt, future};
use ql_api::{InstallPeerBundlesParams, InstallPeerBundlesResponse, RequestInstallPeerBundles};
use ql_codec::Decode;
use ql_common::QID;
use ql_fsm::{PairingInvite, PeerStatus};
use ql_runtime::{RuntimeConfig, RuntimeHandle, new_runtime};
use ql_wire::{
    PeerBundle, QL_WIRE_VERSION, RecordHeader, RecordType, SessionCloseCode, SoftwareCrypto,
    generate_identity,
};
use tokio::{
    sync::{mpsc, oneshot, watch},
    time::Instant,
};
use url::Url;
use uuid::Uuid;

use crate::platform::Platform;

const NUS_UUID: Uuid = Uuid::from_u128(0x6E400001_B5A3_F393_E0A9_E50E24DCCA9E);
const WRITE_UUID: Uuid = Uuid::from_u128(0x6E400002_B5A3_F393_E0A9_E50E24DCCA9E);
const NOTIFY_UUID: Uuid = Uuid::from_u128(0x6E400003_B5A3_F393_E0A9_E50E24DCCA9E);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    Unpaired,
    Searching,
    Connecting,
    BluetoothConnected,
    SecureSession,
    SecureConnected,
    Provisioning,
    Ready,
    Failed,
}

#[derive(Clone)]
pub struct Peer {
    pub name: String,
    pub passport_qid: String,
    pub desktop_qid: String,
}

#[derive(Clone)]
pub struct State {
    pub phase: Phase,
    pub peer: Option<Peer>,
    pub rx_bytes_per_second: u64,
    pub tx_bytes_per_second: u64,
}

#[derive(Clone)]
pub struct Connection(mpsc::Sender<Command>);

impl Connection {
    pub fn new() -> (Self, watch::Receiver<State>) {
        let (commands, command_rx) = mpsc::channel(64);
        let connection = Self(commands);
        let (states, state) = watch::channel(State {
            phase: Phase::Unpaired,
            peer: None,
            rx_bytes_per_second: 0,
            tx_bytes_per_second: 0,
        });
        std::thread::spawn({
            let connection = connection.clone();
            move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                let (inbound, inbound_rx) = mpsc::channel(16);
                let (router, router_rx) = mpsc::channel(64);
                let platform = Platform {
                    connection: connection.clone(),
                    inbound: Some(inbound_rx),
                };
                let identity = generate_identity(&SoftwareCrypto, "QL Lab Desktop");
                let relay = Relay {
                    qid: identity.qid,
                    runtime: inbound,
                    router,
                };
                let (ql, handle) = new_runtime(identity, platform, RuntimeConfig::default());

                let local = tokio::task::LocalSet::new();
                local.spawn_local(ql.run());
                runtime.block_on(local.run_until(run_connection(
                    connection, command_rx, router_rx, relay, handle, states,
                )));
            }
        });
        (connection, state)
    }

    pub fn pair(&self, target: Target) {
        self.0.try_send(Command::Pair(target)).unwrap();
    }

    pub fn unpair(&self) {
        self.0.try_send(Command::Unpair).ok();
    }

    pub async fn write(&self, record: Vec<u8>) -> bool {
        let (response, result) = oneshot::channel();
        self.0.send(Command::Write(record, response)).await.is_ok() && result.await.unwrap_or(false)
    }

    pub fn peer(&self, peer: PeerBundle) {
        self.0.try_send(Command::Peer(peer)).ok();
    }

    pub fn status(&self, peer: Option<QID>, status: PeerStatus) {
        self.0.try_send(Command::Status(peer, status)).ok();
    }
}

pub struct Target {
    address: String,
    invite: PairingInvite,
}

impl Target {
    pub fn parse(payload: &str) -> Option<Self> {
        let url = Url::parse(payload).ok()?;
        (url.host_str() == Some("qr.foundation.xyz")).then_some(())?;
        let address = url
            .query_pairs()
            .find_map(|(key, value)| (key == "p").then(|| value.into_owned()))?;
        let invite = url
            .query_pairs()
            .find_map(|(key, value)| (key == "k").then(|| value.into_owned()))?;
        let invite = hex::decode(invite).ok()?;
        let invite = PairingInvite::decode_bytes(invite.as_slice()).ok()?;
        Some(Self { address, invite })
    }
}

enum Command {
    Pair(Target),
    Unpair,
    Write(Vec<u8>, oneshot::Sender<bool>),
    Peer(PeerBundle),
    Status(Option<QID>, PeerStatus),
}

struct Bluetooth {
    address: String,
    peripheral: Peripheral,
    write: Characteristic,
    notifications: Pin<Box<dyn Stream<Item = ValueNotification> + Send>>,
    dechunker: btp::MasterDechunker<10>,
}

#[derive(Clone)]
struct Relay {
    qid: QID,
    runtime: mpsc::Sender<Vec<u8>>,
    router: mpsc::Sender<RouterMessage>,
}

enum RouterMessage {
    Record(Vec<u8>),
    Attach(PeerBundle),
}

async fn run_connection(
    connection: Connection,
    mut command_rx: mpsc::Receiver<Command>,
    router_rx: mpsc::Receiver<RouterMessage>,
    relay: Relay,
    handle: RuntimeHandle,
    states: watch::Sender<State>,
) {
    enum Step {
        Command(Option<Command>),
        Notification(Option<ValueNotification>),
        Sample,
    }

    let manager = Manager::new().await.unwrap();
    let adapter = manager
        .adapters()
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    tokio::task::spawn_local(run_router(connection.clone(), router_rx));

    let mut bluetooth: Option<Bluetooth> = None;
    let mut peer = None;
    let mut pairing = None;
    let mut rx_since_sample = 0_u64;
    let mut tx_since_sample = 0_u64;
    let mut sample_started = Instant::now();
    let mut sample_at = sample_started + Duration::from_secs(1);
    loop {
        let step = if let Some(bluetooth) = bluetooth.as_mut() {
            future::race(
                async { Step::Command(command_rx.recv().await) },
                future::race(
                    async { Step::Notification(bluetooth.notifications.next().await) },
                    async {
                        tokio::time::sleep_until(sample_at).await;
                        Step::Sample
                    },
                ),
            )
            .await
        } else {
            Step::Command(command_rx.recv().await)
        };

        match step {
            Step::Command(None) => break,
            Step::Command(Some(Command::Unpair)) => {
                pairing = None;
                handle.unpair();
            }
            Step::Command(Some(Command::Peer(value))) => {
                let display = Peer {
                    name: value.name.clone(),
                    passport_qid: hex::encode(value.qid.0),
                    desktop_qid: hex::encode(relay.qid.0),
                };
                peer = Some(value);
                states.send_modify(|state| state.peer = Some(display));
            }
            Step::Command(Some(Command::Status(qid, status))) => {
                if status == PeerStatus::Unpaired {
                    pairing = None;
                    peer = None;
                    rx_since_sample = 0;
                    tx_since_sample = 0;
                    states.send_modify(|state| {
                        state.phase = Phase::Unpaired;
                        state.peer = None;
                        state.rx_bytes_per_second = 0;
                        state.tx_bytes_per_second = 0;
                    });
                    continue;
                }
                if status == PeerStatus::Disconnected {
                    states.send_modify(|state| {
                        state.phase = Phase::Failed;
                        state.rx_bytes_per_second = 0;
                        state.tx_bytes_per_second = 0;
                    });
                    continue;
                }
                if status == PeerStatus::Initiator {
                    states.send_modify(|state| state.phase = Phase::SecureSession);
                    continue;
                }
                if status != PeerStatus::Connected || qid != pairing {
                    continue;
                }
                states.send_modify(|state| state.phase = Phase::SecureConnected);
                pairing = None;
                let Some(peer) = peer.clone() else {
                    tracing::error!("paired peer bundle unavailable");
                    states.send_modify(|state| state.phase = Phase::Failed);
                    continue;
                };
                let router = relay.router.clone();
                let handle = handle.clone();
                let states = states.clone();
                states.send_modify(|state| state.phase = Phase::Provisioning);
                tokio::task::spawn_local(async move {
                    let bundles = match (
                        std::fs::read("ql-router/bundle.bin"),
                        std::fs::read("foundation-server/bundle.bin"),
                    ) {
                        (Ok(router), Ok(foundation)) => InstallPeerBundlesParams {
                            router,
                            peers: vec![foundation],
                        },
                        (Err(error), _) | (_, Err(error)) => {
                            tracing::error!(%error, "reading peer bundles failed");
                            states.send_modify(|state| state.phase = Phase::Failed);
                            return;
                        }
                    };
                    match handle
                        .rpc()
                        .request::<RequestInstallPeerBundles>(&bundles)
                        .await
                    {
                        Ok(InstallPeerBundlesResponse::Installed) => {
                            tracing::info!("installed router and Foundation peer bundles");
                            if router.send(RouterMessage::Attach(peer)).await.is_err() {
                                tracing::error!("QL router stopped before peer attachment");
                                states.send_modify(|state| state.phase = Phase::Failed);
                            } else {
                                states.send_modify(|state| state.phase = Phase::Ready);
                            }
                        }
                        Ok(InstallPeerBundlesResponse::Rejected) => {
                            tracing::error!("Passport rejected peer bundles");
                            states.send_modify(|state| state.phase = Phase::Failed);
                        }
                        Err(error) => {
                            tracing::error!(%error, "installing peer bundles failed");
                            states.send_modify(|state| state.phase = Phase::Failed);
                        }
                    }
                });
            }
            Step::Command(Some(Command::Write(record, response))) => {
                let connected = bluetooth.is_some();
                let mut success = connected;
                if let Some(bluetooth) = bluetooth.as_ref() {
                    for chunk in btp::chunk(&record) {
                        if let Err(error) = bluetooth
                            .peripheral
                            .write(&bluetooth.write, &chunk, WriteType::WithoutResponse)
                            .await
                        {
                            tracing::error!(%error, "writing Passport BTP chunk failed");
                            success = false;
                            break;
                        }
                        tx_since_sample += chunk.len() as u64;
                    }
                }
                response.send(success).ok();
                if connected && !success {
                    bluetooth = None;
                    handle.close_session(SessionCloseCode::CANCELLED);
                    rx_since_sample = 0;
                    tx_since_sample = 0;
                    states.send_modify(|state| {
                        state.phase = Phase::Failed;
                        state.rx_bytes_per_second = 0;
                        state.tx_bytes_per_second = 0;
                    });
                }
            }
            Step::Command(Some(Command::Pair(target))) => {
                states.send_modify(|state| {
                    state.phase = Phase::Searching;
                    state.peer = None;
                    state.rx_bytes_per_second = 0;
                    state.tx_bytes_per_second = 0;
                });
                let reusable = if let Some(bluetooth) = bluetooth.as_ref() {
                    bluetooth.address.eq_ignore_ascii_case(&target.address)
                        && bluetooth.peripheral.is_connected().await.unwrap_or(false)
                } else {
                    false
                };
                handle.close_session(SessionCloseCode::CANCELLED);

                if reusable {
                    tracing::info!("reusing active Bluetooth connection");
                } else {
                    if let Some(bluetooth) = bluetooth.take() {
                        bluetooth.peripheral.disconnect().await.ok();
                    }
                    let result: Result<Bluetooth> = async {
                        tracing::info!(address = %target.address, "searching for Passport");
                        adapter
                            .start_scan(ScanFilter {
                                services: vec![NUS_UUID],
                            })
                            .await
                            .context("starting Bluetooth scan")?;
                        let peripheral = tokio::time::timeout(Duration::from_secs(30), async {
                            loop {
                                for peripheral in adapter.peripherals().await? {
                                    let Some(properties) = peripheral.properties().await? else {
                                        continue;
                                    };
                                    if properties
                                        .address
                                        .to_string()
                                        .eq_ignore_ascii_case(&target.address)
                                    {
                                        return Ok::<Peripheral, btleplug::Error>(peripheral);
                                    }
                                }
                                tokio::time::sleep(Duration::from_millis(200)).await;
                            }
                        })
                        .await
                        .map_err(|_| anyhow!("Passport was not found within 30 seconds"));
                        adapter
                            .stop_scan()
                            .await
                            .context("stopping Bluetooth scan")?;
                        let peripheral = peripheral??;

                        states.send_modify(|state| state.phase = Phase::Connecting);
                        if !peripheral
                            .is_connected()
                            .await
                            .context("checking Bluetooth connection")?
                        {
                            peripheral
                                .connect()
                                .await
                                .context("connecting to Passport")?;
                        }
                        peripheral
                            .discover_services()
                            .await
                            .context("discovering Passport services")?;
                        let characteristics = peripheral.characteristics();
                        let write = characteristics
                            .iter()
                            .find(|characteristic| characteristic.uuid == WRITE_UUID)
                            .cloned()
                            .context("Passport write characteristic unavailable")?;
                        let notify = characteristics
                            .iter()
                            .find(|characteristic| characteristic.uuid == NOTIFY_UUID)
                            .cloned()
                            .context("Passport notification characteristic unavailable")?;
                        peripheral
                            .subscribe(&notify)
                            .await
                            .context("subscribing to Passport notifications")?;
                        let notifications = peripheral
                            .notifications()
                            .await
                            .context("opening Passport notifications")?;
                        Ok(Bluetooth {
                            address: target.address.clone(),
                            peripheral,
                            write,
                            notifications,
                            dechunker: btp::MasterDechunker::default(),
                        })
                    }
                    .await;
                    match result {
                        Ok(connection) => bluetooth = Some(connection),
                        Err(error) => {
                            tracing::error!(error = ?error, "Bluetooth session failed");
                            states.send_modify(|state| state.phase = Phase::Failed);
                            continue;
                        }
                    }
                }

                rx_since_sample = 0;
                tx_since_sample = 0;
                sample_started = Instant::now();
                sample_at = sample_started + Duration::from_secs(1);
                states.send_modify(|state| state.phase = Phase::BluetoothConnected);
                let qid = target.invite.qid;
                pairing = Some(qid);
                handle.start_pairing(target.invite);
            }
            Step::Notification(None) => {
                bluetooth = None;
                pairing = None;
                handle.close_session(SessionCloseCode::CANCELLED);
                tracing::warn!("Passport disconnected");
                rx_since_sample = 0;
                tx_since_sample = 0;
                states.send_modify(|state| {
                    state.phase = Phase::Failed;
                    state.rx_bytes_per_second = 0;
                    state.tx_bytes_per_second = 0;
                });
            }
            Step::Notification(Some(notification)) => {
                rx_since_sample += notification.value.len() as u64;
                let chunk = match btp::Chunk::decode(&notification.value) {
                    Ok(chunk) => chunk,
                    Err(error) => {
                        tracing::warn!(%error, "invalid Passport BTP chunk");
                        continue;
                    }
                };
                let Some(record) = bluetooth
                    .as_mut()
                    .and_then(|bluetooth| bluetooth.dechunker.insert_chunk(chunk))
                else {
                    continue;
                };
                if record.len() > ql_router::MAX_RECORD_SIZE {
                    tracing::warn!(
                        bytes = record.len(),
                        "Passport QL record exceeds router limit"
                    );
                    continue;
                }
                let Ok(header) = RecordHeader::decode_bytes(record.as_slice()) else {
                    tracing::warn!("invalid Passport QL record header");
                    continue;
                };
                if header.version != QL_WIRE_VERSION {
                    tracing::warn!(
                        version = header.version,
                        "unsupported Passport QL record version"
                    );
                    continue;
                }
                if header.route.recipient == relay.qid {
                    if header.record_type == RecordType::Handshake {
                        tracing::debug!(
                            sender = %hex::encode(header.route.sender.0),
                            "delivering handshake to desktop runtime"
                        );
                    }
                    if relay.runtime.send(record).await.is_err() {
                        break;
                    }
                } else {
                    if header.record_type == RecordType::Handshake {
                        tracing::debug!(
                            sender = %hex::encode(header.route.sender.0),
                            recipient = %hex::encode(header.route.recipient.0),
                            "forwarding handshake to router"
                        );
                    }
                    if relay
                        .router
                        .try_send(RouterMessage::Record(record))
                        .is_err()
                    {
                        tracing::warn!("QL router queue unavailable; dropping record");
                    }
                }
            }
            Step::Sample => {
                let elapsed = sample_started.elapsed().as_secs_f64();
                let rx_bytes_per_second = (rx_since_sample as f64 / elapsed).round() as u64;
                let tx_bytes_per_second = (tx_since_sample as f64 / elapsed).round() as u64;
                rx_since_sample = 0;
                tx_since_sample = 0;
                sample_started = Instant::now();
                sample_at = sample_started + Duration::from_secs(1);
                states.send_if_modified(|state| {
                    if state.rx_bytes_per_second == rx_bytes_per_second
                        && state.tx_bytes_per_second == tx_bytes_per_second
                    {
                        false
                    } else {
                        state.rx_bytes_per_second = rx_bytes_per_second;
                        state.tx_bytes_per_second = tx_bytes_per_second;
                        true
                    }
                });
            }
        }
    }
}

async fn run_router(connection: Connection, mut outbound: mpsc::Receiver<RouterMessage>) {
    enum Step {
        Record(std::io::Result<Option<Vec<u8>>>),
        Outbound(Option<RouterMessage>),
    }
    loop {
        let (mut reader, mut writer) = match ql_router::connect(ql_router::DEFAULT_ADDRESS).await {
            Ok(connection) => connection,
            Err(error) => {
                tracing::warn!(%error, "QL router unavailable");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        tracing::info!("connected to QL router");

        'connected: loop {
            // keep the frame future alive while outbound messages are handled
            let mut record = std::pin::pin!(ql_router::receive(&mut reader));
            loop {
                let step = future::race(async { Step::Record(record.as_mut().await) }, async {
                    Step::Outbound(outbound.recv().await)
                })
                .await;

                match step {
                    Step::Record(Ok(Some(record))) => {
                        connection.write(record).await;
                        break;
                    }
                    Step::Record(Ok(None)) => break 'connected,
                    Step::Record(Err(error)) => {
                        tracing::warn!(%error, "QL router read failed");
                        break 'connected;
                    }
                    Step::Outbound(None) => return,
                    Step::Outbound(Some(message)) => {
                        let result = match message {
                            RouterMessage::Record(record) => {
                                ql_router::send(&mut writer, &record).await
                            }
                            RouterMessage::Attach(peer) => {
                                ql_router::attach(&mut writer, &peer).await
                            }
                        };
                        if let Err(error) = result {
                            tracing::warn!(%error, "QL router write failed");
                            break 'connected;
                        }
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
