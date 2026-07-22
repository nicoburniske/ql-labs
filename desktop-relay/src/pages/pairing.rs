use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
    thread::JoinHandle,
    time::Duration,
};

use blit::{
    Ui,
    layout::{Constraint, Direction, Layout, LayoutAlign},
    paint::{BorderRadius, ImageFit, ImageSampling, Rectangle, TextWrap, VerticalAlign},
    platform::Platform,
    resource::{ImageData, ImageFormat, ImageHandle, ImagePixels, StringHandle},
    widget::{Image, Text},
};
use blit_desktop::EventLoopProxy;
use rxing::{
    BarcodeFormat, BinaryBitmap, DecodeHints, Luma8LuminanceSource, MultiFormatReader,
    common::HybridBinarizer,
};
use v4l::{
    Format, FourCC,
    buffer::Type,
    io::traits::CaptureStream,
    prelude::{Device, MmapStream},
    video::{Capture, capture::Parameters},
};

use super::{render_card, render_page};
use crate::{Event, connection, theme};

pub struct Page {
    camera: Camera,
    scanner: MultiFormatReader,
    platform: Platform,
    preview: ImageHandle,
    title: StringHandle,
    subtitle: StringHandle,
    details_label: StringHandle,
    instruction: StringHandle,
    camera_label: StringHandle,
    status_label: StringHandle,
    status: StringHandle,
}

impl Page {
    pub fn new(mut platform: Platform, events: EventLoopProxy<Event>) -> Self {
        let mut scanner = MultiFormatReader::default();
        scanner.set_hints(&DecodeHints {
            PossibleFormats: Some(HashSet::from([BarcodeFormat::QR_CODE])),
            TryHarder: Some(true),
            AlsoInverted: Some(true),
            ..Default::default()
        });
        Self {
            camera: start_camera(events),
            scanner,
            platform,
            preview: ImageHandle::default(),
            title: platform.create_string("Pair Passport Prime"),
            subtitle: platform.create_string("Secure Bluetooth pairing"),
            details_label: platform.create_string("PAIRING"),
            instruction: platform.create_string(
                "Show Prime’s pairing QR code to the camera. Keep the code inside the frame.",
            ),
            camera_label: platform.create_string("CAMERA"),
            status_label: platform.create_string("CONNECTION"),
            status: platform.create_string("Looking for a pairing QR code…"),
        }
    }

    pub fn render(&mut self, ui: &mut Ui) -> Option<connection::Target> {
        const QR_SCAN_INTERVAL: Duration = Duration::from_millis(50);
        const STATUS_INDICATOR_SIZE: f32 = 8.0;

        let scan = ui.timer_loop(ui.id("qr scan"), QR_SCAN_INTERVAL);
        let luma = self.camera.state.lock().unwrap().frame.take();
        let target = if let Some(luma) = luma {
            let target = if scan {
                let scan_size = self.camera.width.min(self.camera.height);
                let scan_left = (self.camera.width - scan_size) / 2;
                let scan_top = (self.camera.height - scan_size) / 2;
                let mut scan_frame = Vec::with_capacity(scan_size * scan_size);
                for row in luma
                    .chunks_exact(self.camera.width)
                    .skip(scan_top)
                    .take(scan_size)
                {
                    scan_frame.extend_from_slice(&row[scan_left..scan_left + scan_size]);
                }
                let mut bitmap = BinaryBitmap::new(HybridBinarizer::new(
                    Luma8LuminanceSource::new(scan_frame, scan_size as u32, scan_size as u32),
                ));
                self.scanner
                    .decode_with_state(&mut bitmap)
                    .ok()
                    .and_then(|decoded| {
                        let bytes = decoded.getRawBytes();
                        let bytes = if bytes.is_empty() {
                            decoded.getText().as_bytes()
                        } else {
                            bytes
                        };
                        connection::Target::parse(std::str::from_utf8(bytes).ok()?)
                    })
            } else {
                None
            };
            self.preview = self.platform.create_image(ImageData::new(
                ImagePixels::Owned(luma),
                ImageFormat::Luma8,
                self.camera.width,
                self.camera.height,
            ));
            target
        } else {
            None
        };

        let content = render_page(ui, &self.title, &self.subtitle);
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
        Text::new(&self.details_label)
            .color(theme::ACCENT)
            .text_size(theme::TEXT_LABEL)
            .text_weight(600)
            .render(ui, details_label);
        Text::new(&self.instruction)
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
        Text::new(&self.status_label)
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
        Text::new(&self.status)
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
        Text::new(&self.camera_label)
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

        let guide_size = preview.width.min(preview.height) * 0.75;
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
    state: Arc<Mutex<CameraState>>,
    thread: Option<JoinHandle<()>>,
    width: usize,
    height: usize,
}

struct CameraState {
    frame: Option<Box<[u8]>>,
    running: bool,
}

impl Drop for Camera {
    fn drop(&mut self) {
        self.state.lock().unwrap().running = false;
        self.thread.take().unwrap().join().unwrap();
    }
}

fn start_camera(events: EventLoopProxy<Event>) -> Camera {
    const CAMERA_WIDTH: u32 = 1920;
    const CAMERA_HEIGHT: u32 = 1080;
    const CAMERA_FPS: u32 = 60;

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
    let format = device
        .set_format(&Format::new(CAMERA_WIDTH, CAMERA_HEIGHT, format.fourcc))
        .unwrap_or(format);
    assert!(format.stride == 0 || format.stride == format.width);
    let parameters = device
        .set_params(&Parameters::with_fps(CAMERA_FPS))
        .unwrap();
    assert_eq!(parameters.interval.numerator, 1);
    assert_eq!(parameters.interval.denominator, CAMERA_FPS);

    let mut stream = MmapStream::with_buffers(&device, Type::VideoCapture, 4).unwrap();
    let width = format.width as usize;
    let height = format.height as usize;
    let state = Arc::new(Mutex::new(CameraState {
        frame: None,
        running: true,
    }));
    let thread = std::thread::spawn({
        let state = state.clone();
        move || {
            loop {
                let (camera_frame, _) = stream.next().unwrap();
                let mut state = state.lock().unwrap();
                if !state.running {
                    break;
                }
                state.frame = Some(camera_frame[..width * height].into());
                if events.send_event(Event::CameraFrame).is_err() {
                    break;
                }
            }
        }
    });
    Camera {
        state,
        thread: Some(thread),
        width,
        height,
    }
}
