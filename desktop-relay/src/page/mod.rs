mod connecting;
mod paired;
mod pairing;

use crate::{App, connection, theme};
use blit::{
    Ui,
    color::Color,
    container::Sizing,
    geometry::LogicalInsets,
    interact::{Sense, WidgetId},
    layout::{Align, Flex, Justify},
    style::Style,
    widget::Text,
};
use blit_desktop::{Project, Root};

#[allow(clippy::large_enum_variant)]
pub enum Page {
    Pairing(pairing::Page),
    Connecting(connecting::Page),
    Paired(paired::Page),
}

impl Page {
    pub fn new(root: &Root<App>) -> Self {
        Self::Pairing(pairing::Page::new(root.project()))
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
                        self.page = Page::Pairing(pairing::Page::new(self.root.project()));
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
                        self.page = Page::Paired(paired::Page::new(peer));
                    } else if ready {
                        tracing::error!("Passport connected without identity details");
                        self.failed();
                    }
                }
                connection::Phase::Failed => self.failed(),
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
                self.page = Page::Pairing(pairing::Page::new(self.root.project()));
                ui.request_frame();
            }
            Page::Paired(page) => {
                if !page.render(ui) {
                    return;
                }
                self.connection.unpair();
                self.page = Page::Pairing(pairing::Page::new(self.root.project()));
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

fn render_button(
    ui: &mut Ui,
    label: &str,
    id: &'static str,
    background: Color,
    clicked_background: Color,
) -> bool {
    let id = WidgetId::new(id);
    let interaction = ui.interact(id, Sense::CLICK);
    let mut button = ui
        .layout(
            Flex::row()
                .align(Align::Center)
                .justify(Justify::Center)
                .padding(LogicalInsets::uniform(theme::SPACE_3)),
        )
        .id(id)
        .width(Sizing::grow())
        .height(Sizing::fixed(theme::BUTTON_HEIGHT))
        .style(
            Style::new()
                .background(if interaction.active || interaction.clicked {
                    clicked_background
                } else {
                    background
                })
                .uniform_radius(theme::RADIUS_LARGE),
        )
        .open();
    button.add(
        Text::new(label)
            .color(theme::TEXT)
            .text_size(theme::TEXT_STATUS)
            .text_weight(600),
    );
    interaction.clicked
}
