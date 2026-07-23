use std::{collections::HashSet, thread::JoinHandle};

use super::{render_card, render_page};
use crate::{connection, theme};
use async_channel::Receiver;
use blit::{
    Ui,
    layout::{Constraint, Direction, Layout, LayoutAlign},
    paint::{BorderRadius, ImageFit, ImageSampling, Rectangle, TextWrap, VerticalAlign},
    platform::Platform,
    resource::{ImageData, ImageFormat, ImageHandle, ImagePixels},
    widget::{Image, Text},
};
use blit_desktop::Scope;
use rxing::{
    BarcodeFormat, BinaryBitmap, DecodeHints, Luma8LuminanceSource, MultiFormatReader,
    common::HybridBinarizer,
};

pub struct Page {
    _scope: Scope<Self>,
    camera: Camera,
    platform: Platform,
    preview: ImageHandle,
    frame: Option<Box<[u8]>>,
    target: Option<connection::Target>,
}

impl Page {
    pub fn new(platform: Platform, mut scope: Scope<Self>) -> Self {
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
            platform,
            preview: ImageHandle::default(),
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
        const QR_GUIDE_SCALE: f32 = 0.75;
        const STATUS_INDICATOR_SIZE: f32 = 8.0;

        if let Some(luma) = self.frame.take() {
            self.preview = self.platform.create_image(ImageData::new(
                ImagePixels::Owned(luma),
                ImageFormat::Luma8,
                self.camera.width,
                self.camera.height,
            ));
        }
        let target = self.target.take();

        let content = render_page(ui, "Pair Passport", "Secure Bluetooth pairing");
        let [details, camera] = Layout::default()
            .spacing(theme::SPACE_4)
            .constraints([Constraint::Fill(34), Constraint::Fill(66)])
            .areas(content);
        render_card(ui, details);
        render_card(ui, camera);

        let details_content = theme::layout::padded(details, theme::SPACE_5, theme::SPACE_4);
        let status_height = (details_content.height * 0.24).clamp(76.0, 128.0);
        let [details_label, instruction, status] = Layout::default()
            .direction(Direction::Vertical)
            .spacing(theme::SPACE_3)
            .constraints([
                Constraint::Length(theme::SPACE_4),
                Constraint::Fill(1),
                Constraint::Length(status_height),
            ])
            .areas(details_content);
        Text::new("PAIRING")
            .color(theme::ACCENT)
            .text_size(theme::TEXT_LABEL)
            .text_weight(600)
            .render(ui, details_label);
        Text::new("Show Passport’s pairing QR code to the camera. Keep the code inside the frame.")
            .color(theme::TEXT_SECONDARY)
            .text_size(theme::TEXT_BODY)
            .wrap(TextWrap::Word)
            .render(ui, instruction);

        Rectangle::new(status)
            .background(theme::ACCENT_SUBTLE)
            .border(theme::BORDER_WIDTH, theme::ACCENT)
            .uniform_radius(theme::RADIUS_MEDIUM)
            .render(ui);
        let status_content = theme::layout::padded(status, theme::SPACE_4, theme::SPACE_3);
        let [status_label, status_line] = Layout::default()
            .direction(Direction::Vertical)
            .spacing(theme::SPACE_2)
            .constraints([Constraint::Length(theme::SPACE_5), Constraint::Fill(1)])
            .areas(status_content);
        Text::new("CONNECTION")
            .color(theme::ACCENT)
            .text_size(theme::TEXT_LABEL)
            .text_weight(600)
            .render(ui, status_label);
        let [indicator, status_text] = Layout::default()
            .spacing(theme::SPACE_2)
            .constraints([
                Constraint::Length(STATUS_INDICATOR_SIZE),
                Constraint::Fill(1),
            ])
            .areas(status_line);
        let [indicator] = Layout::default()
            .direction(Direction::Vertical)
            .align(LayoutAlign::Center)
            .constraints([Constraint::Length(STATUS_INDICATOR_SIZE)])
            .areas(indicator);
        Rectangle::new(indicator)
            .background(theme::ACCENT)
            .uniform_radius(STATUS_INDICATOR_SIZE / 2.0)
            .render(ui);
        Text::new("Looking for a pairing QR code…")
            .color(theme::TEXT)
            .text_size(theme::TEXT_STATUS)
            .wrap(TextWrap::Word)
            .vertical_align(VerticalAlign::Center)
            .render(ui, status_text);

        let camera_content = theme::layout::padded(camera, theme::SPACE_4, theme::SPACE_3);
        let [camera_label, preview] = Layout::default()
            .direction(Direction::Vertical)
            .spacing(theme::SPACE_3)
            .constraints([Constraint::Length(theme::SPACE_5), Constraint::Fill(1)])
            .areas(camera_content);
        Text::new("CAMERA")
            .color(theme::ACCENT)
            .text_size(theme::TEXT_LABEL)
            .text_weight(600)
            .render(ui, camera_label);

        let radius = BorderRadius {
            top_left: theme::RADIUS_MEDIUM,
            top_right: theme::RADIUS_MEDIUM,
            bottom_right: theme::RADIUS_MEDIUM,
            bottom_left: theme::RADIUS_MEDIUM,
        };
        let mut clipped = ui.begin_rounded_clip(preview, radius);
        Rectangle::new(preview)
            .background(theme::PREVIEW)
            .render(&mut clipped);
        Image::new(&self.preview)
            .fit(ImageFit::Contain)
            .sampling(ImageSampling::Nearest)
            .render(&mut clipped, preview);

        let guide_size = preview.width.min(preview.height) * QR_GUIDE_SCALE;
        let [guide] = Layout::default()
            .align(LayoutAlign::Center)
            .constraints([Constraint::Length(guide_size)])
            .areas(preview);
        let [guide] = Layout::default()
            .direction(Direction::Vertical)
            .align(LayoutAlign::Center)
            .constraints([Constraint::Length(guide_size)])
            .areas(guide);
        Rectangle::new(guide)
            .border(theme::GUIDE_WIDTH, theme::PREVIEW_GUIDE)
            .uniform_radius(theme::RADIUS_MEDIUM)
            .render(&mut clipped);
        Rectangle::new(preview)
            .border(theme::BORDER_WIDTH, theme::BORDER)
            .radius(radius)
            .render(&mut clipped);

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
