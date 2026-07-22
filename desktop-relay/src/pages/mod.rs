mod connecting;
mod paired;
mod pairing;

use blit::{
    Ui,
    color::Color,
    geometry::LogicalRect,
    layout::{Constraint, Direction, Layout, LayoutAlign},
    paint::{BoxShadow, HorizontalAlign, Rectangle, TextOptions, VerticalAlign},
    platform::Platform,
    resource::StringHandle,
    widget::{Button, Text},
};
use blit_desktop::EventLoopProxy;
use ql_fsm::PeerStatus;

use crate::{Event, connection, theme};

pub struct Pages {
    platform: Platform,
    events: EventLoopProxy<Event>,
    page: Page,
    peer: Option<connection::Peer>,
}

pub enum Action {
    Pair(connection::Target),
    StartOver,
    Unpair,
}

#[allow(clippy::large_enum_variant)]
enum Page {
    Pairing(pairing::Page),
    Connecting(connecting::Page),
    Paired(paired::Page),
}

impl Pages {
    pub fn new(platform: Platform, events: EventLoopProxy<Event>) -> Self {
        Self {
            platform,
            events: events.clone(),
            page: Page::Pairing(pairing::Page::new(platform, events)),
            peer: None,
        }
    }

    pub fn input(&mut self, event: connection::Event) {
        match event {
            connection::Event::Searching => {
                if let Page::Connecting(page) = &mut self.page {
                    page.searching();
                }
            }
            connection::Event::Connecting => {
                if let Page::Connecting(page) = &mut self.page {
                    page.connecting();
                }
            }
            connection::Event::BluetoothConnected => {
                if let Page::Connecting(page) = &mut self.page {
                    page.bluetooth_connected();
                }
            }
            connection::Event::Peer(peer) => self.peer = Some(peer),
            connection::Event::Status(PeerStatus::Initiator) => {
                if let Page::Connecting(page) = &mut self.page {
                    page.secure_session();
                }
            }
            connection::Event::Status(PeerStatus::Connected) => {
                if let Page::Connecting(page) = &mut self.page {
                    page.secure_connected();
                }
            }
            connection::Event::Status(PeerStatus::Disconnected) => {
                self.failed("The secure connection was interrupted.");
            }
            connection::Event::Status(PeerStatus::Unpaired) => {
                self.peer = None;
                self.page = Page::Pairing(pairing::Page::new(self.platform, self.events.clone()));
            }
            connection::Event::Provisioning => {
                if let Page::Connecting(page) = &mut self.page {
                    page.provisioning();
                }
            }
            connection::Event::Ready => {
                let ready = matches!(&self.page, Page::Connecting(page) if page.can_finish());
                if ready && let Some(peer) = self.peer.take() {
                    self.page = Page::Paired(paired::Page::new(self.platform, peer));
                } else if ready {
                    self.failed("Prime connected without identity details.");
                }
            }
            connection::Event::Failed => self.failed("Could not connect to Prime."),
            connection::Event::ProvisioningFailed => {
                self.failed("Prime connected, but desktop services could not be prepared.");
            }
        }
    }

    pub fn render(&mut self, ui: &mut Ui) -> Option<Action> {
        let action = match &mut self.page {
            Page::Pairing(page) => page.render(ui).map(Action::Pair),
            Page::Connecting(page) => page.render(ui).then_some(Action::StartOver),
            Page::Paired(page) => page.render(ui).then_some(Action::Unpair),
        };
        match &action {
            Some(Action::Pair(_)) => {
                self.page = Page::Connecting(connecting::Page::new(self.platform));
                ui.request_frame();
            }
            Some(Action::StartOver) => {
                self.peer = None;
                self.page = Page::Pairing(pairing::Page::new(self.platform, self.events.clone()));
                ui.request_frame();
            }
            Some(Action::Unpair) => {
                if let Page::Paired(page) = &mut self.page {
                    page.unpairing();
                } else {
                    unreachable!();
                }
                ui.request_frame();
            }
            None => {}
        }
        action
    }

    fn failed(&mut self, message: &str) {
        match &mut self.page {
            Page::Connecting(page) => page.failed(message),
            Page::Paired(page) => page.disconnected(),
            Page::Pairing(_) => {}
        }
    }
}

fn render_page(ui: &mut Ui, title: &StringHandle, subtitle: &StringHandle) -> LogicalRect {
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
    label: &StringHandle,
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
