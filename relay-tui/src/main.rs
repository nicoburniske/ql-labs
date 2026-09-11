#![feature(must_not_suspend)]
#![deny(must_not_suspend)]

mod platform;
mod rpc;

use std::{
    fs::OpenOptions,
    io::{Read, Write},
    os::unix::net::UnixStream,
    sync::mpsc as ready_queue,
    time::{Duration, Instant},
};

use anyhow::{Context, bail, ensure};
use blit::{Frame, Input, Key, Sense, Sides, Size, Sizing, WidgetId};
use blit_executor::LocalExecutor;
use blit_tui::{
    BoundsClip, Session, Ui,
    atom::{Border, BorderStyle, Image},
    color::Color,
    image::{ImageData, ImageFormat, ImagePixels},
    layout::{Align, Justify, flex, single},
    text::{TextAttributes, TextOptions, TextWrap},
    widget::{Block, Text, TextInput, Title, text_input},
};
use ql_api::{
    DownloadBenchmark, DownloadBenchmarkParams, DownloadPassportBenchmark, EchoParams, RequestEcho,
    RequestPassportEcho,
};
use ql_codec::{Decode, Encode};
use ql_fsm::PeerStatus;
use ql_runtime::{PairingInvite, RuntimeConfig, RuntimeHandle, StreamOptions, new_runtime};
use ql_wire::{
    PairingToken, PeerBundle, QlRandom, SoftwareCrypto, answer_peer_challenge, generate_identity,
};
use rustix::event::{PollFd, PollFlags, Timespec, poll};
use sha2::{Digest, Sha256};
use tokio::sync::{mpsc, watch};

struct App {
    relay: String,
    peer: PeerStatus,
    peer_details: Option<PeerBundle>,
    handle: RuntimeHandle,
    relay_connected: bool,
    echo: String,
    echo_input: text_input::State,
    download_size: String,
    download_input: text_input::State,
    result: String,
    activity: String,
    busy: bool,
    permitted: bool,
}

fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let address = args
        .next()
        .unwrap_or_else(|| "relay.foundation.xyz:7447".into());
    let bundle_path = args.next().unwrap_or_else(|| "bundle.bin".into());
    ensure!(
        args.next().is_none(),
        "usage: relay-tui [relay:port] [bundle.bin]"
    );
    let router = PeerBundle::decode_bytes(std::fs::read(bundle_path)?.as_slice())?;
    router.validate(&SoftwareCrypto)?;
    let identity = generate_identity(&SoftwareCrypto, "relay-tui");
    let mut token = [0; PairingToken::SIZE];
    SoftwareCrypto.fill_random_bytes(&mut token);
    let token = PairingToken(token);
    let invite = PairingInvite {
        version: PairingInvite::VERSION,
        qid: identity.qid,
        token,
    };
    let qr = qrcode::QrCode::new(hex::encode(invite.encode_vec()))?;
    let side = (qr.width() + 8) * 8;
    let mut pixels = vec![255; side * side * 3];
    for y in 0..qr.width() {
        for x in 0..qr.width() {
            if qr[(x, y)] == qrcode::Color::Dark {
                for dy in 0..8 {
                    for dx in 0..8 {
                        let at = (((y + 4) * 8 + dy) * side + (x + 4) * 8 + dx) * 3;
                        pixels[at..at + 3].fill(0);
                    }
                }
            }
        }
    }

    // Tokio drives networking and timers while Blit polls local ui tasks
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let _entered = runtime.enter();
    let mut session = Session::new()?;
    let qr_image = session
        .platform_mut()
        .renderer_mut()
        .create_image(ImageData::new(
            ImagePixels::Owned(pixels.into_boxed_slice()),
            ImageFormat::Rgb8,
            side,
            side,
        ));
    let terminal_input = OpenOptions::new().read(true).open("/dev/tty")?;
    let (mut wake_read, wake_write) = UnixStream::pair()?;
    wake_read.set_nonblocking(true)?;
    wake_write.set_nonblocking(true)?;
    let resize = signal_hook::low_level::pipe::register(
        signal_hook::consts::SIGWINCH,
        wake_write.try_clone()?,
    )?;
    struct Signal(signal_hook::SigId);
    impl Drop for Signal {
        fn drop(&mut self) {
            signal_hook::low_level::unregister(self.0);
        }
    }
    let _resize = Signal(resize);
    let (ready, tasks) = ready_queue::channel();
    let executor = Box::pin(LocalExecutor::<App>::new(move |task| {
        if ready.send(task).is_ok() {
            // a full socket already has a wake pending
            loop {
                match (&wake_write).write(&[1]) {
                    Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                    _ => break,
                }
            }
        }
    }));
    // safety: app and root are dropped before the pinned executor
    let mut root = unsafe { executor.as_ref().root() };
    let (status, mut statuses) = watch::channel(PeerStatus::Disconnected);
    let (peer, mut peers) = watch::channel(None);
    let (activity, mut activities) = watch::channel(String::new());
    let (outbound, mut outgoing) = mpsc::channel(16);
    let (incoming, inbound) = mpsc::channel(16);
    let platform = platform::Platform {
        outbound,
        inbound: Some(inbound),
        status,
        peer,
        rpc: ql_rpc::Router::builder_send(rpc::TokioSpawner)
            .request::<RequestEcho>()
            .download::<DownloadBenchmark>()
            .build(rpc::Service { activity }),
    };
    let (ql, handle) = new_runtime(identity.clone(), platform, RuntimeConfig::default());
    handle.arm_pairing(token);
    let mut app = App {
        relay: format!("connecting to {address}"),
        peer: PeerStatus::Disconnected,
        peer_details: None,
        handle,
        relay_connected: false,
        echo: "hello Prime".into(),
        echo_input: Default::default(),
        download_size: "256".into(),
        download_input: Default::default(),
        result: String::new(),
        activity: String::new(),
        busy: false,
        permitted: false,
    };
    root.spawn(async move |cx| {
        while peers.changed().await.is_ok() {
            cx.app().peer_details = peers.borrow_and_update().clone();
        }
    });
    root.spawn(async move |cx| {
        while statuses.changed().await.is_ok() {
            let status = *statuses.borrow_and_update();
            cx.app().peer = status;
            cx.app().permitted = false;
            if status == PeerStatus::Connected {
                let handle = cx.app().handle.clone();
                let result = handle
                    .rpc()
                    .request::<ql_api::RequestPeerPermissions>(
                        &ql_api::PeerPermissionsParams(ql_keyos::PeerPermissions {
                            app_ids: vec![ql_api::app_id::DEBUG],
                        }),
                        StreamOptions::default(),
                    )
                    .await;
                match result {
                    Ok(ql_api::PeerPermissionsResponse::Updated) => cx.app().permitted = true,
                    _ => cx.app().result = "Prime did not grant debug RPC permission".into(),
                }
            }
        }
    });
    root.spawn(async move |cx| {
        while activities.changed().await.is_ok() {
            cx.app().activity = activities.borrow_and_update().clone();
        }
    });
    root.spawn(async move |cx| {
        let result: anyhow::Result<()> = async {
            let (mut reader, mut writer) = ql_relay::connect(&address, &router).await?;
            ql_relay::attach(&mut writer, &identity.bundle()).await?;
            let challenge = ql_relay::receive(&mut reader)
                .await?
                .context("relay closed before challenge")?;
            let (answer, pending) =
                answer_peer_challenge(&SoftwareCrypto, &identity, router, &challenge)?;
            ql_relay::send(&mut writer, &answer).await?;
            let mut confirmation = ql_relay::receive(&mut reader)
                .await?
                .context("relay closed before confirmation")?;
            pending.verify(&SoftwareCrypto, &mut confirmation)?;

            {
                let mut app = cx.app();
                app.relay = format!("Relay online · {address}");
                app.relay_connected = true;
            }
            // receive is polled continuously until this entire connection ends
            let read = async {
                while let Some(record) = ql_relay::receive(&mut reader).await? {
                    incoming.send(record).await?;
                }
                bail!("relay disconnected")
            };
            let write = async {
                while let Some(record) = outgoing.recv().await {
                    ql_relay::send(&mut writer, &record).await?;
                }
                bail!("QL runtime stopped")
            };
            tokio::select! {
                result = read => result,
                result = write => result,
                _ = ql.run() => bail!("QL runtime stopped"),
            }
        }
        .await;
        if let Err(error) = result {
            let mut app = cx.app();
            app.relay = format!("disconnected: {error:#}");
            app.relay_connected = false;
            app.permitted = false;
            app.peer = PeerStatus::Disconnected;
        }
    });

    let started = Instant::now();
    let mut frame = Frame::default();
    let mut inputs = [Input::None; 32];
    loop {
        // drain wake bytes before polling tasks so concurrent wakes are not lost
        let mut bytes = [0; 1024];
        loop {
            match wake_read.read(&mut bytes) {
                Ok(0) => break,
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error.into()),
            }
        }
        let mut polled = 0;
        for task in tasks.try_iter().take(64) {
            executor.as_ref().run(&mut app, task);
            polled += 1;
        }
        let events = session.poll(Some(Duration::ZERO), &mut inputs)?;
        let info = session.frame_info();
        let mut send = false;
        let mut download = false;
        let enabled =
            app.relay_connected && app.peer == PeerStatus::Connected && app.permitted && !app.busy;
        frame.render_inputs(
            session.platform_mut(),
            info,
            started.elapsed(),
            inputs[..events.input_count].iter().copied(),
            |mut ui| {
                let echo_id = WidgetId::new("echo-input");
                let download_id = WidgetId::new("download-input");
                if let Input::Key(key) = *ui.input() {
                    if key.pressed && key.key == Key::Tab {
                        ui.focus(if ui.is_focused(echo_id) {
                            download_id
                        } else {
                            echo_id
                        });
                    }
                    if key.pressed && key.key == Key::Character('c') && key.modifiers.control() {
                        ui.platform().quit();
                    }
                }
                let mut page = ui.layout(flex::column().padding(Sides::all(1.0)).gap(1.0));
                page.insert(Block::new().background(Color::Reset));
                {
                    let mut header = page
                        .child(flex::item().height(Sizing::fixed(1.0)))
                        .layout(flex::row().gap(2.0).align(Align::Center));
                    header.insert(Block::new().background(Color::BLACK));
                    header.child(flex::item()).insert(
                        Text::new(" QL LAB ")
                            .color(Color::LIGHT_CYAN)
                            .attributes(TextAttributes::BOLD),
                    );
                    header
                        .child(flex::item().grow())
                        .insert(Text::new(&app.relay).color(if app.relay_connected {
                            Color::LIGHT_GREEN
                        } else {
                            Color::YELLOW
                        }));
                    if header
                        .child(flex::item())
                        .build(|ui: Ui<'_>| button(ui, "quit", " quit ", true))
                    {
                        header.platform().quit();
                    }
                }
                {
                    let mut body = page.child(flex::item().grow()).layout(flex::row().gap(2.0));
                    {
                        let mut pairing = body
                            .child(
                                flex::item()
                                    .width(Sizing::fixed(32.0))
                                    .height(Sizing::fixed(16.0)),
                            )
                            .layout(
                                flex::column()
                                    .padding(Sides::all(1.0))
                                    .align(Align::Center)
                                    .justify(Justify::Center),
                            );
                        pairing.insert(panel(" PAIR "));
                        // keep exactly the same image slot before and after pairing
                        pairing
                            .child(flex::item().fixed(28.0, 14.0))
                            .build(|ui: Ui<'_>| {
                                let mut slot = ui
                                    .layout(
                                        flex::column()
                                            .align(Align::Center)
                                            .justify(Justify::Center),
                                    )
                                    .clip(BoundsClip);
                                if app.relay_connected && app.peer_details.is_none() {
                                    slot.child(flex::item().fixed(28.0, 14.0))
                                        .insert(Image::new(qr_image.id(), Size::new(28.0, 14.0)));
                                } else {
                                    slot.insert(Block::new().background(Color::BLACK));
                                    slot.child(flex::item()).insert(
                                        Text::new(if app.peer_details.is_some() {
                                            "Paired"
                                        } else {
                                            "—"
                                        })
                                        .color(Color::GRAY),
                                    );
                                }
                            });
                    }
                    let mut controls = body
                        .child(flex::item().grow())
                        .layout(flex::column().gap(1.0));
                    {
                        let mut peer = controls
                            .child(flex::item().height(Sizing::fixed(6.0)))
                            .layout(flex::column().padding(Sides::all(1.0)));
                        peer.insert(panel(" PEER "));
                        let (status, color) = match app.peer {
                            PeerStatus::Connected if app.permitted => {
                                ("● Connected", Color::LIGHT_GREEN)
                            }
                            PeerStatus::Connected => ("● Authorizing", Color::YELLOW),
                            PeerStatus::Initiator => ("● Pairing", Color::YELLOW),
                            _ => ("○ Disconnected", Color::GRAY),
                        };
                        peer.child(flex::item().height(Sizing::fixed(1.0)))
                            .insert(Text::new(status).color(color));
                        peer.child(flex::item().height(Sizing::fixed(1.0))).insert(
                            Text::new(
                                app.peer_details
                                    .as_ref()
                                    .map_or("", |peer| peer.name.as_str()),
                            )
                            .color(Color::WHITE),
                        );
                        let qid = app
                            .peer_details
                            .as_ref()
                            .map(|peer| hex::encode(peer.qid.0))
                            .unwrap_or_default();
                        peer.child(flex::item().height(Sizing::fixed(2.0))).insert(
                            Text::new(&qid)
                                .color(Color::GRAY)
                                .options(TextOptions::new().wrap(TextWrap::Character)),
                        );
                    }
                    {
                        let mut echo = controls
                            .child(flex::item().height(Sizing::fixed(5.0)))
                            .layout(flex::column().padding(Sides::all(1.0)));
                        echo.insert(panel(" ECHO "));
                        let mut row = echo
                            .child(flex::item().height(Sizing::fixed(3.0)))
                            .layout(flex::row().align(Align::Center).gap(2.0));
                        send |= row.child(flex::item().grow()).build(|ui: Ui<'_>| {
                            input(ui, &mut app.echo_input, echo_id, &mut app.echo)
                        });
                        send |= row
                            .child(flex::item().fixed(12.0, 1.0))
                            .build(|ui: Ui<'_>| button(ui, "echo", "Send", enabled));
                    }
                    {
                        let mut transfer = controls
                            .child(flex::item().height(Sizing::fixed(5.0)))
                            .layout(flex::column().padding(Sides::all(1.0)));
                        transfer.insert(panel(" DOWNLOAD "));
                        let mut row = transfer
                            .child(flex::item().height(Sizing::fixed(3.0)))
                            .layout(flex::row().align(Align::Center).gap(2.0));
                        download |= row.child(flex::item().grow()).build(|ui: Ui<'_>| {
                            input(
                                ui,
                                &mut app.download_input,
                                download_id,
                                &mut app.download_size,
                            )
                        });
                        row.child(flex::item())
                            .insert(Text::new("KiB").color(Color::GRAY));
                        download |= row
                            .child(flex::item().fixed(12.0, 1.0))
                            .build(|ui: Ui<'_>| button(ui, "download", "Download", enabled));
                    }
                    controls
                        .child(flex::item().height(Sizing::fixed(2.0)))
                        .insert(
                            Text::new(&app.result)
                                .color(if app.busy {
                                    Color::YELLOW
                                } else {
                                    Color::WHITE
                                })
                                .options(TextOptions::new().wrap(TextWrap::Word)),
                        );
                    controls.child(flex::item().grow()).insert(
                        Text::new(&app.activity)
                            .color(Color::GRAY)
                            .options(TextOptions::new().wrap(TextWrap::Word)),
                    );
                }
            },
        );
        if (send || download) && enabled {
            let handle = app.handle.clone();
            let message = app.echo.clone();
            let download_size = app.download_size.clone();
            app.busy = true;
            app.result = if download {
                "Downloading..."
            } else {
                "Sending echo..."
            }
            .into();
            root.spawn(async move |cx| {
                let result: anyhow::Result<String> = async {
                    if !download {
                        let reply = handle
                            .rpc()
                            .request::<RequestPassportEcho>(
                                &EchoParams { message },
                                StreamOptions::default(),
                            )
                            .await?;
                        return Ok(format!("echo: {}", reply.message));
                    }
                    let length = download_size
                        .trim()
                        .parse::<u64>()?
                        .checked_mul(1024)
                        .context("size too large")?;
                    ensure!(
                        (1..=16 * 1024 * 1024).contains(&length),
                        "choose 1 to 16384 KiB"
                    );
                    let download = handle
                        .rpc()
                        .download::<DownloadPassportBenchmark>(
                            &DownloadBenchmarkParams { length },
                            StreamOptions::default(),
                        )
                        .await?;
                    let (header, mut parts) = download.start().await?;
                    let mut hash = Sha256::new();
                    let mut total = 0;
                    while let Some((_, mut part)) = parts.next_part().await? {
                        loop {
                            let chunk = part.read_chunk().await?;
                            if chunk.is_empty() {
                                break;
                            }
                            total += chunk.len() as u64;
                            ensure!(total <= length, "download exceeds requested length");
                            hash.update(&chunk);
                        }
                    }
                    parts.complete().await?;
                    ensure!(total == length, "download length mismatch");
                    ensure!(
                        header.hash == hash.finalize().as_slice(),
                        "download hash mismatch"
                    );
                    Ok(format!("download: verified {total} bytes (SHA-256)"))
                }
                .await;
                let mut app = cx.app();
                app.result = match result {
                    Ok(message) => message,
                    Err(error) => format!("failed: {error:#}"),
                };
                app.busy = false;
            });
        }
        session.present()?;
        if session.platform().should_quit() {
            break;
        }
        let delay = if polled == 64 || events.input_count == inputs.len() {
            Some(Duration::ZERO)
        } else if frame.has_pending_redraw() {
            Some(Duration::from_millis(16))
        } else {
            frame
                .next_timer_deadline()
                .map(|deadline| deadline.saturating_sub(started.elapsed()))
        };
        let timeout = delay.map(Timespec::try_from).transpose()?;
        let mut fds = [
            PollFd::new(&terminal_input, PollFlags::IN),
            PollFd::new(&wake_read, PollFlags::IN),
        ];
        match poll(&mut fds, timeout.as_ref()) {
            Ok(_) => {}
            Err(rustix::io::Errno::INTR) => {}
            Err(error) => return Err(error.into()),
        }
    }
    session.finish()?;
    Ok(())
}

fn button(mut ui: Ui<'_>, id: &'static str, label: &'static str, enabled: bool) -> bool {
    let id = WidgetId::new(id);
    let interaction = if enabled {
        ui.interact(id, Sense::CLICK)
    } else {
        Default::default()
    };
    let mut button = ui
        .widget_id(id)
        .layout(flex::row().padding(Sides::x(1.0)).justify(Justify::Center));
    button.insert(Block::new().background(if !enabled {
        Color::DARK_GRAY
    } else if interaction.active {
        Color::CYAN
    } else if interaction.hovered {
        Color::DARK_GRAY
    } else {
        Color::BLUE
    }));
    button
        .child(flex::item())
        .insert(Text::new(label).color(if enabled { Color::WHITE } else { Color::GRAY }));
    enabled && interaction.clicked
}

fn panel(title: &str) -> Block<'_> {
    Block::new()
        .background(Color::BLACK)
        .border(Border::new(Color::DARK_GRAY).style(BorderStyle::Rounded))
        .title(
            Title::new(title)
                .color(Color::LIGHT_CYAN)
                .attributes(TextAttributes::BOLD),
        )
}

fn input(ui: Ui<'_>, state: &mut text_input::State, id: WidgetId, value: &mut String) -> bool {
    let color = if ui.is_focused(id) {
        Color::CYAN
    } else {
        Color::GRAY
    };
    let mut field = ui.layout(single::layout().padding(Sides::all(1.0)));
    field.insert(
        Block::new()
            .background(Color::BLACK)
            .border(Border::new(color).style(BorderStyle::Rounded)),
    );
    field
        .child(single::item().width(Sizing::grow()))
        .build(
            TextInput::new(state, id, value)
                .color(Color::WHITE)
                .background(Color::BLACK)
                .cursor_background(Color::CYAN)
                .selection_background(Color::BLUE),
        )
        .submitted
}
