mod connecting;
mod paired;
mod pairing;

use crate::{App, connection, theme};
use blit::{
    Ui,
    color::Color,
    geometry::LogicalRect,
    layout::{Constraint, Direction, Layout, LayoutAlign},
    paint::{BoxShadow, HorizontalAlign, Rectangle, TextOptions, VerticalAlign},
    platform::Platform,
    resource::TextSource,
    widget::{Button, Text},
};
use blit_desktop::{Project, Root};

#[allow(clippy::large_enum_variant)]
pub enum Page {
    Pairing(pairing::Page),
    Connecting(connecting::Page),
    Paired(paired::Page),
}

impl Page {
    pub fn new(platform: Platform, root: &Root<App>) -> Self {
        Self::Pairing(pairing::Page::new(platform, root.project()))
    }
}

impl Project<pairing::Page> for App {
    fn project(&mut self) -> Option<&mut pairing::Page> {
        match &mut self.page {
            Page::Pairing(page) => Some(page),
            _ => None,
        }
    }
}

impl App {
    pub fn update(&mut self, state: &connection::State) {
        if self.phase != state.phase {
            self.phase = state.phase;
            match state.phase {
                connection::Phase::Unpaired => {
                    if !matches!(self.page, Page::Pairing(_)) {
                        self.page =
                            Page::Pairing(pairing::Page::new(self.platform, self.root.project()));
                    }
                }
                connection::Phase::Searching => {
                    if let Page::Connecting(page) = &mut self.page {
                        page.set_status(connecting::Status::Searching);
                    }
                }
                connection::Phase::Connecting => {
                    if let Page::Connecting(page) = &mut self.page {
                        page.set_status(connecting::Status::Connecting);
                    }
                }
                connection::Phase::BluetoothConnected => {
                    if let Page::Connecting(page) = &mut self.page {
                        page.set_status(connecting::Status::BluetoothConnected);
                    }
                }
                connection::Phase::SecureSession => {
                    if let Page::Connecting(page) = &mut self.page {
                        page.set_status(connecting::Status::SecureSession);
                    }
                }
                connection::Phase::SecureConnected => {
                    if let Page::Connecting(page) = &mut self.page {
                        page.set_status(connecting::Status::SecureConnected);
                    }
                }
                connection::Phase::Provisioning => {
                    if let Page::Connecting(page) = &mut self.page {
                        page.set_status(connecting::Status::Provisioning);
                    }
                }
                connection::Phase::Ready => {
                    let ready = matches!(&self.page, Page::Connecting(page) if page.can_finish());
                    if ready && let Some(peer) = state.peer.clone() {
                        self.page = Page::Paired(paired::Page::new(self.platform, peer));
                    } else if ready {
                        tracing::error!("Passport connected without identity details");
                        self.failed();
                    }
                }
                connection::Phase::Failed => {
                    self.failed();
                }
            }
        }

        if let Page::Paired(page) = &mut self.page {
            page.rates(state.rx_bytes_per_second, state.tx_bytes_per_second);
        }
    }

    pub fn render_page(&mut self, ui: &mut Ui) {
        match &mut self.page {
            Page::Pairing(page) => {
                let Some(target) = page.render(ui) else {
                    return;
                };
                self.connection.pair(target);
                self.page = Page::Connecting(connecting::Page::new());
                ui.request_frame();
            }
            Page::Connecting(page) => {
                if !page.render(ui) {
                    return;
                }
                self.connection.unpair();
                self.page = Page::Pairing(pairing::Page::new(self.platform, self.root.project()));
                ui.request_frame();
            }
            Page::Paired(page) => {
                if !page.render(ui) {
                    return;
                }
                page.unpairing();
                self.connection.unpair();
                ui.request_frame();
            }
        }
    }

    fn failed(&mut self) {
        match &mut self.page {
            Page::Connecting(page) => page.set_status(connecting::Status::Failed),
            Page::Paired(page) => page.disconnected(),
            Page::Pairing(_) => {}
        }
    }
}

fn render_page(ui: &mut Ui, title: &'static str, subtitle: &'static str) -> LogicalRect {
    let screen = ui.screen();
    Rectangle::new(screen)
        .background(theme::BACKGROUND)
        .render(ui);
    let page = theme::layout::page(screen);
    let page_width = if page.width > 1_000.0 {
        page.width * 0.8
    } else {
        page.width
    };
    let page_height = if page.height > 640.0 {
        page.height * 0.8
    } else {
        page.height
    };
    let [page] = Layout::default()
        .align(LayoutAlign::Center)
        .constraints([Constraint::Length(page_width)])
        .areas(page);
    let [page] = Layout::default()
        .direction(Direction::Vertical)
        .align(LayoutAlign::Center)
        .constraints([Constraint::Length(page_height)])
        .areas(page);
    let [header, content] = Layout::default()
        .direction(Direction::Vertical)
        .spacing(theme::SPACE_4)
        .constraints([Constraint::Length(theme::TITLE_HEIGHT), Constraint::Fill(1)])
        .areas(page);
    let [title_area, subtitle_area] = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Fill(1), Constraint::Length(theme::SPACE_5)])
        .areas(header);
    Text::new(title)
        .color(theme::TEXT)
        .text_size(theme::TEXT_TITLE)
        .text_weight(600)
        .render(ui, title_area);
    Text::new(subtitle)
        .color(theme::TEXT_MUTED)
        .text_size(theme::TEXT_STATUS)
        .vertical_align(VerticalAlign::Bottom)
        .render(ui, subtitle_area);
    content
}

fn render_card(ui: &mut Ui, area: LogicalRect) {
    BoxShadow::new(area, theme::SHADOW)
        .uniform_radius(theme::RADIUS_LARGE)
        .offset(0.0, theme::SHADOW_OFFSET)
        .blur(theme::SHADOW_BLUR)
        .render(ui);
    Rectangle::new(area)
        .background(theme::SURFACE)
        .border(theme::BORDER_WIDTH, theme::BORDER)
        .uniform_radius(theme::RADIUS_LARGE)
        .render(ui);
}

fn render_button(
    ui: &mut Ui,
    label: impl Into<TextSource>,
    id: &str,
    area: LogicalRect,
    background: Color,
    clicked_background: Color,
) -> bool {
    Button::new(label)
        .id(id)
        .background(background)
        .clicked_background(clicked_background)
        .border_width(0.0)
        .uniform_radius(theme::RADIUS_LARGE)
        .text_color(theme::TEXT)
        .text_size(theme::TEXT_STATUS)
        .text_weight(600)
        .text_options(TextOptions {
            horizontal_align: HorizontalAlign::Center,
            vertical_align: VerticalAlign::Center,
            ..TextOptions::default()
        })
        .padding_x(theme::SPACE_6)
        .render(ui, area)
        .clicked()
}
