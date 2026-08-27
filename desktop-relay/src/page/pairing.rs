use std::{collections::HashSet, thread::JoinHandle};

use async_channel::Receiver;
use blit::{
    Ui,
    container::Sizing,
    geometry::LogicalInsets,
    image::{ImageData, ImageFit, ImageFormat, ImageHandle, ImagePixels, ImageSampling},
    layout::{Align, Flex, Justify},
    style::{Clip, Style},
    text::TextWrap,
    widget::{Image, Text},
};
use blit_desktop::Scope;
use rxing::{
    BarcodeFormat, BinaryBitmap, DecodeHints, Luma8LuminanceSource, MultiFormatReader,
    common::HybridBinarizer,
};

use crate::{connection, theme};

pub struct Page {
    _scope: Scope<Self>,
    camera: Camera,
    preview: Option<ImageHandle>,
    frame: Option<Box<[u8]>>,
    target: Option<connection::Target>,
}

impl Page {
    pub fn new(mut scope: Scope<Self>) -> Self {
        let (camera, frames) = start_camera();
        scope.spawn(async move |cx| {
            while let Ok(frame) = frames.recv().await {
                cx.app().set_camera_frame(frame);
                futures_lite::future::yield_now().await;
            }
        });
        Self {
            _scope: scope,
            camera,
            preview: None,
            frame: None,
            target: None,
        }
    }

    fn set_camera_frame(&mut self, frame: CameraFrame) {
        self.frame = Some(frame.luma);
        if frame.target.is_some() {
            self.target = frame.target;
        }
    }

    pub fn render(&mut self, ui: &mut Ui) -> Option<connection::Target> {
        if let Some(luma) = self.frame.take() {
            self.preview = Some(ui.create_image(ImageData::new(
                ImagePixels::Owned(luma),
                ImageFormat::Luma8,
                self.camera.width,
                self.camera.height,
            )));
        }
        let target = self.target.take();

        ui.clear();
        let mut root = ui
            .layout(
                Flex::column()
                    .align(Align::Center)
                    .justify(Justify::Center)
                    .padding(LogicalInsets::uniform(theme::SPACE_6))
                    .gap(theme::SPACE_4),
            )
            .grow()
            .style(Style::new().background(theme::BACKGROUND))
            .open();
        root.add(
            Text::new("Pair Passport")
                .color(theme::TEXT)
                .text_size(theme::TEXT_TITLE)
                .text_weight(600),
        );
        root.add(
            Text::new("Secure Bluetooth pairing")
                .color(theme::TEXT_MUTED)
                .text_size(theme::TEXT_STATUS),
        );
        root.add(|ui: &mut Ui| {
            let mut cards = ui
                .layout(Flex::row().gap(theme::SPACE_4))
                .width(Sizing::grow().max(1_120.0))
                .height(Sizing::grow().max(520.0))
                .open();
            cards.add(|ui: &mut Ui| {
                let mut details = ui
                    .layout(
                        Flex::column()
                            .padding(LogicalInsets::uniform(theme::SPACE_5))
                            .gap(theme::SPACE_4),
                    )
                    .width(Sizing::percent(0.34))
                    .height(Sizing::grow())
                    .style(
                        Style::new()
                            .background(theme::SURFACE)
                            .solid_border(theme::BORDER_WIDTH, theme::BORDER)
                            .uniform_radius(theme::RADIUS_LARGE),
                    )
                    .open();
                details.add(
                    Text::new("PAIRING")
                        .color(theme::ACCENT)
                        .text_size(theme::TEXT_LABEL)
                        .text_weight(600),
                );
                details.add(
                    Text::new(
                        "Show Passport’s pairing QR code to the camera. Keep the code inside the frame.",
                    )
                    .color(theme::TEXT_SECONDARY)
                    .text_size(theme::TEXT_BODY)
                    .wrap(TextWrap::Word)
                    .width(Sizing::grow()),
                );
                details.add(|ui: &mut Ui| {
                    let mut status = ui
                        .layout(
                            Flex::column()
                                .padding(LogicalInsets::uniform(theme::SPACE_4))
                                .gap(theme::SPACE_2),
                        )
                        .width(Sizing::grow())
                        .style(
                            Style::new()
                                .background(theme::ACCENT_SUBTLE)
                                .solid_border(theme::BORDER_WIDTH, theme::ACCENT)
                                .uniform_radius(theme::RADIUS_MEDIUM),
                        )
                        .open();
                    status.add(
                        Text::new("CONNECTION")
                            .color(theme::ACCENT)
                            .text_size(theme::TEXT_LABEL)
                            .text_weight(600),
                    );
                    status.add(
                        Text::new("Looking for a pairing QR code…")
                            .color(theme::TEXT)
                            .text_size(theme::TEXT_STATUS)
                            .wrap(TextWrap::Word)
                            .width(Sizing::grow()),
                    );
                });
            });
            cards.add(|ui: &mut Ui| {
                let mut camera = ui
                    .layout(
                        Flex::column()
                            .padding(LogicalInsets::uniform(theme::SPACE_4))
                            .gap(theme::SPACE_3),
                    )
                    .width(Sizing::grow())
                    .height(Sizing::grow())
                    .style(
                        Style::new()
                            .background(theme::SURFACE)
                            .solid_border(theme::BORDER_WIDTH, theme::BORDER)
                            .uniform_radius(theme::RADIUS_LARGE),
                    )
                    .open();
                camera.add(
                    Text::new("CAMERA")
                        .color(theme::ACCENT)
                        .text_size(theme::TEXT_LABEL)
                        .text_weight(600),
                );
                camera.add(|ui: &mut Ui| {
                    let mut preview = ui
                        .layout(Flex::row().align(Align::Center).justify(Justify::Center))
                        .grow()
                        .style(
                            Style::new()
                                .background(theme::PREVIEW)
                                .solid_border(theme::GUIDE_WIDTH, theme::PREVIEW_GUIDE)
                                .uniform_radius(theme::RADIUS_MEDIUM),
                        )
                        .clip(Clip::Rounded(
                            Style::new()
                                .uniform_radius(theme::RADIUS_MEDIUM)
                                .radius,
                        ))
                        .open();
                    if let Some(image) = &self.preview {
                        preview.add(
                            Image::new(image)
                                .fit(ImageFit::Contain)
                                .sampling(ImageSampling::Nearest)
                                .width(Sizing::grow())
                                .height(Sizing::grow()),
                        );
                    }
                });
            });
        });

        target
    }
}

struct Camera {
    thread: Option<JoinHandle<()>>,
    width: usize,
    height: usize,
}

struct CameraFrame {
    luma: Box<[u8]>,
    target: Option<connection::Target>,
}

impl Drop for Camera {
    fn drop(&mut self) {
        self.thread.take().unwrap().join().unwrap();
    }
}

fn start_camera() -> (Camera, Receiver<CameraFrame>) {
    use v4l::{
        FourCC,
        buffer::Type,
        io::traits::CaptureStream,
        prelude::{Device, MmapStream},
        video::Capture,
    };

    let (device, format) = v4l::context::enum_devices()
        .into_iter()
        .map(|node| Device::with_path(node.path()))
        .filter_map(Result::ok)
        .filter_map(|device| match device.format() {
            Ok(format) => Some((device, format)),
            Err(_) => None,
        })
        .find(|(_, format)| {
            (format.fourcc == FourCC::new(b"YU12") || format.fourcc == FourCC::new(b"NV12"))
                && (format.stride == 0 || format.stride == format.width)
        })
        .unwrap();
    assert!(format.stride == 0 || format.stride == format.width);

    let mut stream = MmapStream::with_buffers(&device, Type::VideoCapture, 4).unwrap();
    let width = format.width as usize;
    let height = format.height as usize;
    let (frames, receiver) = async_channel::bounded(2);
    let thread = std::thread::spawn(move || {
        const QR_SCAN_EVERY: u64 = 3;

        let mut scanner = MultiFormatReader::default();
        scanner.set_hints(&DecodeHints {
            PossibleFormats: Some(HashSet::from([BarcodeFormat::QR_CODE])),
            TryHarder: Some(true),
            AlsoInverted: Some(true),
            ..Default::default()
        });
        let mut produced = 0_u64;
        loop {
            let (camera_frame, _) = stream.next().unwrap();
            produced += 1;
            let luma: Box<[u8]> = camera_frame[..width * height].into();
            let target = if produced.is_multiple_of(QR_SCAN_EVERY) {
                let mut scan_frame = luma.to_vec();
                let minimum = scan_frame.iter().copied().min().unwrap();
                let maximum = scan_frame.iter().copied().max().unwrap();
                let threshold = ((minimum as u16 + maximum as u16) / 2) as u8;
                for pixel in &mut scan_frame {
                    *pixel = if *pixel >= threshold { 255 } else { 0 };
                }
                let mut bitmap = BinaryBitmap::new(HybridBinarizer::new(
                    Luma8LuminanceSource::new(scan_frame, width as u32, height as u32),
                ));
                scanner
                    .decode_with_state(&mut bitmap)
                    .ok()
                    .and_then(|decoded| connection::Target::parse(decoded.getText()))
            } else {
                None
            };
            let frame = CameraFrame { luma, target };
            if frame.target.is_some() {
                let _ = frames.send_blocking(frame);
                break;
            }
            if frames.force_send(frame).is_err() {
                break;
            }
        }
    });
    (
        Camera {
            thread: Some(thread),
            width,
            height,
        },
        receiver,
    )
}
