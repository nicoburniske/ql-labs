use std::{
    net::TcpListener,
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::{Duration, SystemTime},
};

use ql_codec::{Decode, Encode};
use ql_common::QID;
use ql_router::{
    protocol::MAX_RECORD_SIZE,
    tokio::{Receiver, Sender, attach, connect, receive, send},
};
use ql_wire::{
    PeerBundle, QL_WIRE_VERSION, QlIdentity, RecordHeader, RecordType, RouteHeader, SoftwareCrypto,
    answer_peer_challenge, generate_identity,
};
use tokio::time::{sleep, timeout};

struct RouterProcess {
    child: Child,
    directory: PathBuf,
}

impl Drop for RouterProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn authenticated_tcp_routes_and_takeover() {
    let port = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let address = format!("127.0.0.1:{port}");
    let directory = std::env::temp_dir().join(format!(
        "ql-router-test-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&directory).unwrap();
    let identity_path = directory.join("identity.bin");
    let bundle_path = directory.join("bundle.bin");
    let child = Command::new(env!("CARGO_BIN_EXE_ql-router"))
        .env("QL_ROUTER_ADDRESS", &address)
        .env("QL_ROUTER_IDENTITY_PATH", &identity_path)
        .env("QL_ROUTER_BUNDLE_PATH", &bundle_path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let _router_process = RouterProcess { child, directory };

    let router = timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(bytes) = std::fs::read(&bundle_path) {
                break PeerBundle::decode_bytes(bytes.as_slice()).unwrap();
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    let crypto = SoftwareCrypto;
    let alice = generate_identity(&crypto, "alice");
    let bob = generate_identity(&crypto, "bob");
    let (mut alice_rx, mut alice_tx) = connect(&address, &router).await.unwrap();
    authenticate(&mut alice_rx, &mut alice_tx, &alice, &router).await;
    let (mut bob_rx, mut bob_tx) = connect(&address, &router).await.unwrap();
    authenticate(&mut bob_rx, &mut bob_tx, &bob, &router).await;

    let forged = record(QID([9; QID::SIZE]), bob.qid, 32);
    send(&mut alice_tx, &forged).await.unwrap();
    assert!(
        timeout(Duration::from_millis(100), receive(&mut bob_rx))
            .await
            .is_err()
    );

    let small = record(alice.qid, bob.qid, 32);
    send(&mut alice_tx, &small).await.unwrap();
    assert_eq!(
        timeout(Duration::from_secs(2), receive(&mut bob_rx))
            .await
            .unwrap()
            .unwrap(),
        Some(small)
    );

    let small = record(bob.qid, alice.qid, 32);
    send(&mut bob_tx, &small).await.unwrap();
    assert_eq!(
        timeout(Duration::from_secs(2), receive(&mut alice_rx))
            .await
            .unwrap()
            .unwrap(),
        Some(small)
    );

    let (mut replacement_rx, mut replacement_tx) = connect(&address, &router).await.unwrap();
    authenticate(&mut replacement_rx, &mut replacement_tx, &alice, &router).await;
    let takeover = record(bob.qid, alice.qid, 32);
    send(&mut bob_tx, &takeover).await.unwrap();
    assert_eq!(
        timeout(Duration::from_secs(2), receive(&mut replacement_rx))
            .await
            .unwrap()
            .unwrap(),
        Some(takeover)
    );
    assert!(
        timeout(Duration::from_millis(100), receive(&mut alice_rx))
            .await
            .is_err()
    );

    authenticate(&mut alice_rx, &mut alice_tx, &alice, &router).await;
    drop((replacement_rx, replacement_tx));
    sleep(Duration::from_millis(100)).await;
    let reclaimed = record(bob.qid, alice.qid, 32);
    send(&mut bob_tx, &reclaimed).await.unwrap();
    assert_eq!(
        timeout(Duration::from_secs(2), receive(&mut alice_rx))
            .await
            .unwrap()
            .unwrap(),
        Some(reclaimed)
    );

    let largest = record(
        bob.qid,
        alice.qid,
        MAX_RECORD_SIZE - RecordHeader::WIRE_SIZE,
    );
    send(&mut bob_tx, &largest).await.unwrap();
    assert_eq!(
        timeout(Duration::from_secs(2), receive(&mut alice_rx))
            .await
            .unwrap()
            .unwrap(),
        Some(largest)
    );

    let oversized = record(
        bob.qid,
        alice.qid,
        MAX_RECORD_SIZE + 1 - RecordHeader::WIRE_SIZE,
    );
    assert!(send(&mut bob_tx, &oversized).await.is_err());

    let after_error = record(bob.qid, alice.qid, 32);
    send(&mut bob_tx, &after_error).await.unwrap();
    assert_eq!(
        timeout(Duration::from_secs(2), receive(&mut alice_rx))
            .await
            .unwrap()
            .unwrap(),
        Some(after_error)
    );
}

async fn authenticate(
    receiver: &mut Receiver,
    sender: &mut Sender,
    identity: &QlIdentity,
    router: &PeerBundle,
) {
    attach(sender, &identity.bundle()).await.unwrap();
    let request = receive(receiver).await.unwrap().unwrap();
    let (response, pending) =
        answer_peer_challenge(&SoftwareCrypto, identity, router.clone(), &request).unwrap();
    send(sender, &response).await.unwrap();
    let mut confirmation = receive(receiver).await.unwrap().unwrap();
    pending.verify(&SoftwareCrypto, &mut confirmation).unwrap();
}

fn record(sender: QID, recipient: QID, payload_size: usize) -> Vec<u8> {
    let mut record = RecordHeader {
        version: QL_WIRE_VERSION,
        route: RouteHeader { sender, recipient },
        record_type: RecordType::Session,
    }
    .encode_vec();
    record.resize(record.len() + payload_size, 0);
    record
}
