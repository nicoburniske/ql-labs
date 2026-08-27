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
use crate::{connection, theme};

pub struct Page {
    status: Status,
    passport_name: String,
    passport_qid: String,
    desktop_qid: String,
    receive_rate: String,
    send_rate: String,
    confirming: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Status {
    Connected,
    Disconnected,
}

impl Page {
    pub fn new(peer: connection::Peer) -> Self {
        Self {
            status: Status::Connected,
            passport_name: peer.name,
            passport_qid: peer.passport_qid,
            desktop_qid: peer.desktop_qid,
            receive_rate: "0 B/s".into(),
            send_rate: "0 B/s".into(),
            confirming: false,
        }
    }

    pub fn disconnected(&mut self) {
        self.status = Status::Disconnected;
    }

    pub fn rates(&mut self, receive: u64, send: u64) {
        let format = |bytes_per_second| {
            if bytes_per_second < 1_000 {
                format!("{bytes_per_second} B/s")
            } else if bytes_per_second < 1_000_000 {
                format!("{:.1} KB/s", bytes_per_second as f64 / 1_000.0)
            } else {
                format!("{:.1} MB/s", bytes_per_second as f64 / 1_000_000.0)
            }
        };
        self.receive_rate = format(receive);
        self.send_rate = format(send);
    }

    pub fn render(&mut self, ui: &mut Ui) -> bool {
        let (status_message, description_message, status_color, status_background) =
            match self.status {
                Status::Connected => (
                    "Passport is connected",
                    "The encrypted relay is ready for desktop services.",
                    theme::POSITIVE,
                    theme::POSITIVE_SUBTLE,
                ),
                Status::Disconnected => (
                    "Passport is disconnected",
                    "The secure session ended. Pair again to reconnect.",
                    theme::NEGATIVE,
                    theme::NEGATIVE_SUBTLE,
                ),
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
            Text::new("Passport")
                .color(theme::TEXT)
                .text_size(theme::TEXT_TITLE)
                .text_weight(600),
        );
        root.add(
            Text::new("Secure desktop relay")
                .color(theme::TEXT_MUTED)
                .text_size(theme::TEXT_STATUS),
        );
        root.add(|ui: &mut Ui| {
            let mut cards = ui
                .layout(Flex::row().gap(theme::SPACE_4))
                .width(Sizing::grow().max(1_120.0))
                .open();
            let unpair = cards.add(|ui: &mut Ui| {
                let mut summary = ui
                    .layout(
                        Flex::column()
                            .padding(LogicalInsets::uniform(theme::SPACE_5))
                            .gap(theme::SPACE_3),
                    )
                    .width(Sizing::percent(0.38))
                    .style(
                        Style::new()
                            .background(theme::SURFACE)
                            .solid_border(theme::BORDER_WIDTH, theme::BORDER)
                            .uniform_radius(theme::RADIUS_LARGE),
                    )
                    .open();
                summary.add(
                    Text::new("STATUS")
                        .color(status_color)
                        .text_size(theme::TEXT_LABEL)
                        .text_weight(600),
                );
                summary.add(|ui: &mut Ui| {
                    let mut status = ui
                        .layout(Flex::column().padding(LogicalInsets::uniform(theme::SPACE_4)))
                        .width(Sizing::grow())
                        .style(
                            Style::new()
                                .background(status_background)
                                .solid_border(theme::BORDER_WIDTH, status_color)
                                .uniform_radius(theme::RADIUS_MEDIUM),
                        )
                        .open();
                    status.add(
                        Text::new(status_message)
                            .color(theme::TEXT)
                            .text_size(theme::TEXT_BODY)
                            .text_weight(600)
                            .wrap(TextWrap::Word)
                            .width(Sizing::grow()),
                    );
                });
                summary.add(
                    Text::new(description_message)
                        .color(theme::TEXT_SECONDARY)
                        .text_size(theme::TEXT_STATUS)
                        .wrap(TextWrap::Word)
                        .width(Sizing::grow()),
                );
                if self.confirming {
                    let cancel = summary.add(|ui: &mut Ui| {
                        let mut actions = ui.layout(Flex::row().gap(theme::SPACE_3)).open();
                        let cancel = actions.add(|ui: &mut Ui| {
                            render_button(
                                ui,
                                "Cancel",
                                "cancel unpair",
                                theme::SURFACE_SUBTLE,
                                theme::SURFACE_PRESSED,
                            )
                        });
                        let unpair = actions.add(|ui: &mut Ui| {
                            render_button(
                                ui,
                                "Confirm unpair",
                                "confirm unpair",
                                theme::NEGATIVE_SUBTLE,
                                theme::NEGATIVE,
                            )
                        });
                        (cancel, unpair)
                    });
                    if cancel.0 {
                        self.confirming = false;
                    }
                    cancel.1
                } else {
                    let clicked = summary.add(|ui: &mut Ui| {
                        render_button(
                            ui,
                            if self.status == Status::Disconnected {
                                "Unpair and pair again"
                            } else {
                                "Unpair Passport"
                            },
                            "unpair",
                            theme::SURFACE_SUBTLE,
                            theme::SURFACE_PRESSED,
                        )
                    });
                    if clicked {
                        self.confirming = true;
                    }
                    false
                }
            });
            cards.add(|ui: &mut Ui| {
                let mut details = ui
                    .layout(
                        Flex::column()
                            .padding(LogicalInsets::uniform(theme::SPACE_5))
                            .gap(theme::SPACE_3),
                    )
                    .width(Sizing::grow())
                    .style(
                        Style::new()
                            .background(theme::SURFACE)
                            .solid_border(theme::BORDER_WIDTH, theme::BORDER)
                            .uniform_radius(theme::RADIUS_LARGE),
                    )
                    .open();
                details.add(
                    Text::new("CONNECTION DETAILS")
                        .color(theme::ACCENT)
                        .text_size(theme::TEXT_LABEL)
                        .text_weight(600),
                );
                for (label, value) in [
                    ("PASSPORT NAME", self.passport_name.as_str()),
                    ("PASSPORT QID", self.passport_qid.as_str()),
                    ("DESKTOP QID", self.desktop_qid.as_str()),
                    ("RECEIVE", self.receive_rate.as_str()),
                    ("SEND", self.send_rate.as_str()),
                ] {
                    details.add(|ui: &mut Ui| {
                        let mut row = ui
                            .layout(Flex::row().align(Align::Center).gap(theme::SPACE_3))
                            .width(Sizing::grow())
                            .open();
                        row.add(
                            Text::new(label)
                                .color(theme::TEXT_MUTED)
                                .text_size(theme::TEXT_LABEL)
                                .text_weight(600)
                                .width(Sizing::percent(0.32)),
                        );
                        row.add(
                            Text::new(value)
                                .color(theme::TEXT)
                                .text_size(theme::TEXT_STATUS)
                                .width(Sizing::grow()),
                        );
                    });
                }
            });
            unpair
        })
    }
}
