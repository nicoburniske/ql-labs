use blit::{
    Ui,
    container::Sizing,
    geometry::LogicalInsets,
    layout::{Align, Flex, Justify},
    style::Style,
    text::TextWrap,
    widget::Text,
};

use super::render_button;
use crate::theme;

pub struct Page {
    status: Status,
    completed: u8,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Preparing,
    Searching,
    Connecting,
    BluetoothConnected,
    SecureSession,
    SecureConnected,
    Provisioning,
    Failed,
}

impl Page {
    pub fn new() -> Self {
        Self {
            status: Status::Preparing,
            completed: 1,
        }
    }

    pub fn set_status(&mut self, status: Status) {
        match status {
            Status::Preparing | Status::Searching | Status::Connecting => self.completed = 1,
            Status::BluetoothConnected | Status::SecureSession => self.completed = 2,
            Status::SecureConnected | Status::Provisioning => self.completed = 3,
            Status::Failed => {}
        }
        self.status = status;
    }

    pub fn can_finish(&self) -> bool {
        self.status != Status::Failed
    }

    pub fn render(&mut self, ui: &mut Ui) -> bool {
        let failed = self.status == Status::Failed;
        let status_message = match self.status {
            Status::Preparing => "QR code verified. Preparing connection…",
            Status::Searching => "Looking for Passport over Bluetooth…",
            Status::Connecting => "Passport found. Opening Bluetooth connection…",
            Status::BluetoothConnected => "Bluetooth connected. Starting secure session…",
            Status::SecureSession => "Establishing the encrypted QLv2 session…",
            Status::SecureConnected => "Secure session established.",
            Status::Provisioning => "Installing router and Foundation services…",
            Status::Failed => "Connection failed. Check the logs and try again.",
        };

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
            Text::new("Connecting to Passport")
                .color(theme::TEXT)
                .text_size(theme::TEXT_TITLE)
                .text_weight(600),
        );
        root.add(
            Text::new("Preparing the secure desktop relay")
                .color(theme::TEXT_MUTED)
                .text_size(theme::TEXT_STATUS),
        );
        root.add(|ui: &mut Ui| {
            let mut card = ui
                .layout(
                    Flex::column()
                        .padding(LogicalInsets::uniform(theme::SPACE_5))
                        .gap(theme::SPACE_3),
                )
                .width(Sizing::grow().max(720.0))
                .style(
                    Style::new()
                        .background(theme::SURFACE)
                        .solid_border(theme::BORDER_WIDTH, theme::BORDER)
                        .uniform_radius(theme::RADIUS_LARGE),
                )
                .open();
            card.add(
                Text::new("CONNECTION PROGRESS")
                    .color(theme::ACCENT)
                    .text_size(theme::TEXT_LABEL)
                    .text_weight(600),
            );
            card.add(
                Text::new(status_message)
                    .color(if failed { theme::NEGATIVE } else { theme::TEXT })
                    .text_size(theme::TEXT_BODY)
                    .wrap(TextWrap::Word)
                    .width(Sizing::grow()),
            );
            for (index, label) in [
                "Pairing QR code verified",
                "Bluetooth connection",
                "Secure QLv2 session",
                "Desktop services",
            ]
            .into_iter()
            .enumerate()
            {
                let step = index as u8 + 1;
                let active = step == self.completed + 1;
                let color = if step <= self.completed {
                    theme::POSITIVE
                } else if active && failed {
                    theme::NEGATIVE
                } else if active {
                    theme::ACCENT
                } else {
                    theme::TEXT_MUTED
                };
                card.add(|ui: &mut Ui| {
                    let mut row = ui
                        .layout(
                            Flex::row()
                                .align(Align::Center)
                                .padding(LogicalInsets::uniform(theme::SPACE_3))
                                .gap(theme::SPACE_3),
                        )
                        .width(Sizing::grow())
                        .style(
                            Style::new()
                                .background(theme::SURFACE_SUBTLE)
                                .solid_border(theme::BORDER_WIDTH, color)
                                .uniform_radius(theme::RADIUS_MEDIUM),
                        )
                        .open();
                    row.add(
                        Text::new(if step <= self.completed { "●" } else { "○" })
                            .color(color)
                            .text_size(theme::TEXT_STATUS),
                    );
                    row.add(
                        Text::new(label)
                            .color(theme::TEXT)
                            .text_size(theme::TEXT_STATUS),
                    );
                });
            }
            failed
                && card.add(|ui: &mut Ui| {
                    render_button(
                        ui,
                        "Start over",
                        "start over",
                        theme::NEGATIVE_SUBTLE,
                        theme::NEGATIVE,
                    )
                })
        })
    }
}
