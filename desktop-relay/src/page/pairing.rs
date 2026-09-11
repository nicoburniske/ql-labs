use std::collections::HashSet;

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

use super::render_button;
use crate::{connection, theme};

pub struct Page {
    _scope: Scope<Self>,
    _camera: Option<Camera>,
    camera_error: Option<String>,
    preview: Option<ImageHandle>,
    frame: Option<CameraFrame>,
    target: Option<connection::Target>,
}

impl Page {
    pub fn new(mut scope: Scope<Self>) -> Self {
        let (camera, camera_error) = match start_camera() {
            Ok((camera, frames)) => {
                scope.spawn(async move |cx| {
                    while let Ok(frame) = frames.recv().await {
                        cx.app().set_camera_frame(frame);
                        futures_lite::future::yield_now().await;
                    }
                });
                (Some(camera), None)
            }
            Err(error) => {
                tracing::error!(%error, "camera unavailable");
                (None, Some(error.to_string()))
            }
        };
        Self {
            _scope: scope,
            _camera: camera,
            camera_error,
            preview: None,
            frame: None,
            target: None,
        }
    }

    fn set_camera_frame(&mut self, mut frame: CameraFrame) {
        if frame.target.is_some() {
            self.target = frame.target.take();
        }
        self.frame = Some(frame);
    }

    pub fn render(&mut self, ui: &mut Ui) -> Option<connection::Target> {
        if let Some(frame) = self.frame.take() {
            self.preview = Some(ui.create_image(ImageData::new(
                ImagePixels::Owned(frame.luma),
                ImageFormat::Luma8,
                frame.width,
                frame.height,
            )));
        }
        let found = self.target.is_some();
        let mut start = false;

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
                        Text::new(if found {
                            "Pairing QR code found. Start pairing when ready."
                        } else {
                            "Looking for a pairing QR code…"
                        })
                            .color(theme::TEXT)
                            .text_size(theme::TEXT_STATUS)
                            .wrap(TextWrap::Word)
                            .width(Sizing::grow()),
                    );
                });
                if found {
                    start = details.add(|ui: &mut Ui| {
                        render_button(
                            ui,
                            "Start pairing",
                            "start pairing",
                            theme::ACCENT_SUBTLE,
                            theme::ACCENT,
                        )
                    });
                }
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
                    } else if let Some(error) = &self.camera_error {
                        preview.add(
                            Text::new(error)
                                .color(theme::TEXT_MUTED)
                                .text_size(theme::TEXT_STATUS)
                                .wrap(TextWrap::Word),
                        );
                    }
                });
            });
        });

        if start { self.target.take() } else { None }
    }
}

struct Camera {
    pipeline: gstreamer::Pipeline,
}

struct CameraFrame {
    luma: Box<[u8]>,
    width: usize,
    height: usize,
    target: Option<connection::Target>,
}

impl Drop for Camera {
    fn drop(&mut self) {
        use gstreamer::prelude::ElementExt;

        let _ = self.pipeline.set_state(gstreamer::State::Null);
    }
}

fn start_camera() -> anyhow::Result<(Camera, Receiver<CameraFrame>)> {
    use gstreamer::prelude::{Cast, ElementExt, GstBinExt};
    gstreamer::init()?;
    let pipeline = gstreamer::parse::launch(
        "pipewiresrc ! image/jpeg,width=1280,height=720,framerate=30/1 ! jpegdec ! videoconvert ! video/x-raw,format=GRAY8 ! appsink name=camera max-buffers=1 drop=true sync=false",
    )?
    .downcast::<gstreamer::Pipeline>()
    .map_err(|_| anyhow::anyhow!("camera pipeline is not a pipeline"))?;
    let sink = pipeline
        .by_name("camera")
        .ok_or_else(|| anyhow::anyhow!("camera pipeline has no sink"))?
        .downcast::<gstreamer_app::AppSink>()
        .map_err(|_| anyhow::anyhow!("camera sink has the wrong type"))?;
    let (frames, receiver) = async_channel::bounded(2);
    let mut scanner = MultiFormatReader::default();
    scanner.set_hints(&DecodeHints {
        PossibleFormats: Some(HashSet::from([BarcodeFormat::QR_CODE])),
        TryHarder: Some(true),
        AlsoInverted: Some(true),
        ..Default::default()
    });
    let mut produced = 0_u64;
    sink.set_callbacks(
        gstreamer_app::AppSinkCallbacks::builder()
            .new_sample(move |sink| {
                let sample = sink.pull_sample().map_err(|error| {
                    tracing::error!(%error, "reading camera sample failed");
                    gstreamer::FlowError::Error
                })?;
                let info =
                    gstreamer_video::VideoInfo::from_caps(sample.caps().ok_or_else(|| {
                        tracing::error!("camera sample has no format");
                        gstreamer::FlowError::Error
                    })?)
                    .map_err(|error| {
                        tracing::error!(%error, "reading camera format failed");
                        gstreamer::FlowError::Error
                    })?;
                let frame = gstreamer_video::VideoFrame::from_buffer_readable(
                    sample.buffer_owned().ok_or_else(|| {
                        tracing::error!("camera sample has no frame");
                        gstreamer::FlowError::Error
                    })?,
                    &info,
                )
                .map_err(|_| {
                    tracing::error!("mapping camera frame failed");
                    gstreamer::FlowError::Error
                })?;
                let width = info.width() as usize;
                let height = info.height() as usize;
                let stride = info.stride()[0] as usize;
                let source = frame.plane_data(0).map_err(|error| {
                    tracing::error!(%error, "reading camera frame failed");
                    gstreamer::FlowError::Error
                })?;
                let mut luma = vec![0; width * height].into_boxed_slice();
                for row in 0..height {
                    luma[row * width..(row + 1) * width]
                        .copy_from_slice(&source[row * stride..row * stride + width]);
                }

                produced += 1;
                if produced == 1 {
                    tracing::info!(width, height, "camera connected");
                }
                let target = if produced.is_multiple_of(3) {
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
                let found = target.is_some();
                let frame = CameraFrame {
                    luma,
                    width,
                    height,
                    target,
                };
                if found {
                    frames
                        .send_blocking(frame)
                        .map_err(|_| gstreamer::FlowError::Flushing)?;
                    return Err(gstreamer::FlowError::Eos);
                }
                frames
                    .force_send(frame)
                    .map_err(|_| gstreamer::FlowError::Flushing)?;
                Ok(gstreamer::FlowSuccess::Ok)
            })
            .build(),
    );
    pipeline
        .set_state(gstreamer::State::Playing)
        .map_err(|_| anyhow::anyhow!("starting camera pipeline failed"))?;
    Ok((Camera { pipeline }, receiver))
}
