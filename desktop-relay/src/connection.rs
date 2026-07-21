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
use tokio::sync::{mpsc, oneshot};
use url::Url;
use uuid::Uuid;

use blit_desktop::EventLoopProxy;

use crate::platform::Platform;

const NUS_UUID: Uuid = Uuid::from_u128(0x6E400001_B5A3_F393_E0A9_E50E24DCCA9E);
const WRITE_UUID: Uuid = Uuid::from_u128(0x6E400002_B5A3_F393_E0A9_E50E24DCCA9E);
const NOTIFY_UUID: Uuid = Uuid::from_u128(0x6E400003_B5A3_F393_E0A9_E50E24DCCA9E);

pub enum Event {
    Searching,
    Connecting,
    Peer(PeerStatus),
    Failed,
}

#[derive(Clone)]
pub struct Connection(mpsc::Sender<Command>);

impl Connection {
    pub fn new(events: EventLoopProxy<crate::Event>) -> Self {
        let (commands, command_rx) = mpsc::channel(64);
        let connection = Self(commands);
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
                    events: events.clone(),
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
                    connection, command_rx, router_rx, relay, handle, events,
                )));
            }
        });
        connection
    }

    pub fn pair(&self, target: Target) {
        self.0.try_send(Command::Pair(target)).unwrap();
    }

    pub fn reset(&self) {
        self.0.try_send(Command::Reset).ok();
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
    Reset,
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
    events: EventLoopProxy<crate::Event>,
) {
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
    loop {
        let step = if let Some(bluetooth) = bluetooth.as_mut() {
            future::race(
                async { ConnectionStep::Command(command_rx.recv().await) },
                async { ConnectionStep::Notification(bluetooth.notifications.next().await) },
            )
            .await
        } else {
            ConnectionStep::Command(command_rx.recv().await)
        };

        match step {
            ConnectionStep::Command(None) => break,
            ConnectionStep::Command(Some(Command::Reset)) => {
                pairing = None;
                handle.close_session(SessionCloseCode::CANCELLED);
            }
            ConnectionStep::Command(Some(Command::Peer(value))) => peer = Some(value),
            ConnectionStep::Command(Some(Command::Status(qid, status))) => {
                if status != PeerStatus::Connected || qid != pairing {
                    continue;
                }
                pairing = None;
                let Some(peer) = peer.clone() else {
                    eprintln!("paired peer bundle unavailable");
                    continue;
                };
                let router = relay.router.clone();
                let handle = handle.clone();
                tokio::task::spawn_local(async move {
                    let bundles = match (
                        std::fs::read("ql-router-bundle.bin"),
                        std::fs::read("foundation-server-bundle.bin"),
                    ) {
                        (Ok(router), Ok(foundation)) => InstallPeerBundlesParams {
                            router,
                            peers: vec![foundation],
                        },
                        (Err(error), _) | (_, Err(error)) => {
                            eprintln!("reading peer bundles failed: {error}");
                            return;
                        }
                    };
                    match handle
                        .rpc()
                        .request::<RequestInstallPeerBundles>(&bundles)
                        .await
                    {
                        Ok(InstallPeerBundlesResponse::Installed) => {
                            eprintln!("installed router and Foundation peer bundles");
                            if router.send(RouterMessage::Attach(peer)).await.is_err() {
                                eprintln!("QL router stopped before peer attachment");
                            }
                        }
                        Ok(InstallPeerBundlesResponse::Rejected) => {
                            eprintln!("Passport rejected peer bundles");
                        }
                        Err(error) => eprintln!("installing peer bundles failed: {error}"),
                    }
                });
            }
            ConnectionStep::Command(Some(Command::Write(record, response))) => {
                let connected = bluetooth.is_some();
                let mut success = connected;
                if let Some(bluetooth) = bluetooth.as_ref() {
                    for chunk in btp::chunk(&record) {
                        if bluetooth
                            .peripheral
                            .write(&bluetooth.write, &chunk, WriteType::WithoutResponse)
                            .await
                            .is_err()
                        {
                            success = false;
                            break;
                        }
                    }
                }
                response.send(success).ok();
                if connected && !success {
                    bluetooth = None;
                    handle.close_session(SessionCloseCode::CANCELLED);
                }
            }
            ConnectionStep::Command(Some(Command::Pair(target))) => {
                let reusable = if let Some(bluetooth) = bluetooth.as_ref() {
                    bluetooth.address.eq_ignore_ascii_case(&target.address)
                        && bluetooth.peripheral.is_connected().await.unwrap_or(false)
                } else {
                    false
                };
                handle.close_session(SessionCloseCode::CANCELLED);

                if reusable {
                    eprintln!("reusing active Bluetooth connection");
                } else {
                    if let Some(bluetooth) = bluetooth.take() {
                        bluetooth.peripheral.disconnect().await.ok();
                    }
                    let result: Result<Bluetooth> = async {
                        eprintln!("searching for Prime at {}", target.address);
                        events
                            .send_event(crate::Event::Connection(Event::Searching))
                            .ok();
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
                        .map_err(|_| anyhow!("Prime was not found within 30 seconds"));
                        adapter
                            .stop_scan()
                            .await
                            .context("stopping Bluetooth scan")?;
                        let peripheral = peripheral??;

                        events
                            .send_event(crate::Event::Connection(Event::Connecting))
                            .ok();
                        if !peripheral
                            .is_connected()
                            .await
                            .context("checking Bluetooth connection")?
                        {
                            peripheral.connect().await.context("connecting to Prime")?;
                        }
                        peripheral
                            .discover_services()
                            .await
                            .context("discovering Prime services")?;
                        let characteristics = peripheral.characteristics();
                        let write = characteristics
                            .iter()
                            .find(|characteristic| characteristic.uuid == WRITE_UUID)
                            .cloned()
                            .context("Prime write characteristic unavailable")?;
                        let notify = characteristics
                            .iter()
                            .find(|characteristic| characteristic.uuid == NOTIFY_UUID)
                            .cloned()
                            .context("Prime notification characteristic unavailable")?;
                        peripheral
                            .subscribe(&notify)
                            .await
                            .context("subscribing to Prime notifications")?;
                        let notifications = peripheral
                            .notifications()
                            .await
                            .context("opening Prime notifications")?;
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
                            eprintln!("Bluetooth session failed: {error:#}");
                            events
                                .send_event(crate::Event::Connection(Event::Failed))
                                .ok();
                            continue;
                        }
                    }
                }

                let qid = target.invite.qid;
                pairing = Some(qid);
                handle.start_pairing(target.invite);
            }
            ConnectionStep::Notification(None) => {
                bluetooth = None;
                pairing = None;
                handle.close_session(SessionCloseCode::CANCELLED);
                eprintln!("Prime disconnected");
                events
                    .send_event(crate::Event::Connection(Event::Failed))
                    .ok();
            }
            ConnectionStep::Notification(Some(notification)) => {
                let chunk = match btp::Chunk::decode(&notification.value) {
                    Ok(chunk) => chunk,
                    Err(error) => {
                        eprintln!("invalid Prime BTP chunk: {error}");
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
                    eprintln!("Prime QL record exceeds the router limit");
                    continue;
                }
                let Ok(header) = RecordHeader::decode_bytes(record.as_slice()) else {
                    eprintln!("invalid Prime QL record header");
                    continue;
                };
                if header.version != QL_WIRE_VERSION {
                    eprintln!("unsupported Prime QL record version");
                    continue;
                }
                if header.route.recipient == relay.qid {
                    if header.record_type == RecordType::Handshake {
                        eprintln!(
                            "delivering handshake sender={} to desktop runtime",
                            hex::encode(header.route.sender.0)
                        );
                    }
                    if relay.runtime.send(record).await.is_err() {
                        break;
                    }
                } else {
                    if header.record_type == RecordType::Handshake {
                        eprintln!(
                            "forwarding handshake sender={} recipient={} to router",
                            hex::encode(header.route.sender.0),
                            hex::encode(header.route.recipient.0)
                        );
                    }
                    if relay
                        .router
                        .try_send(RouterMessage::Record(record))
                        .is_err()
                    {
                        eprintln!("QL router queue unavailable; dropping record");
                    }
                }
            }
        }
    }
}

async fn run_router(connection: Connection, mut outbound: mpsc::Receiver<RouterMessage>) {
    loop {
        let (mut reader, mut writer) = match ql_router::connect(ql_router::DEFAULT_ADDRESS).await {
            Ok(connection) => connection,
            Err(error) => {
                eprintln!("QL router unavailable: {error}");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        eprintln!("connected to QL router");

        'connected: loop {
            // keep the frame future alive while outbound messages are handled
            let mut record = std::pin::pin!(ql_router::receive(&mut reader));
            loop {
                let step =
                    future::race(async { RouterStep::Record(record.as_mut().await) }, async {
                        RouterStep::Outbound(outbound.recv().await)
                    })
                    .await;

                match step {
                    RouterStep::Record(Ok(Some(record))) => {
                        connection.write(record).await;
                        break;
                    }
                    RouterStep::Record(Ok(None)) => break 'connected,
                    RouterStep::Record(Err(error)) => {
                        eprintln!("QL router read failed: {error}");
                        break 'connected;
                    }
                    RouterStep::Outbound(None) => return,
                    RouterStep::Outbound(Some(message)) => {
                        let result = match message {
                            RouterMessage::Record(record) => {
                                ql_router::send(&mut writer, &record).await
                            }
                            RouterMessage::Attach(peer) => {
                                ql_router::attach(&mut writer, &peer).await
                            }
                        };
                        if let Err(error) = result {
                            eprintln!("QL router write failed: {error}");
                            break 'connected;
                        }
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

enum RouterStep {
    Record(std::io::Result<Option<Vec<u8>>>),
    Outbound(Option<RouterMessage>),
}

enum ConnectionStep {
    Command(Option<Command>),
    Notification(Option<ValueNotification>),
}
