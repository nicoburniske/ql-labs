use blit::{
    Ui,
    layout::{Constraint, Direction, Layout},
    paint::{Rectangle, TextWrap, VerticalAlign},
    platform::Platform,
    resource::{StringHandle, TextSource},
    widget::Text,
};

use super::{render_button, render_card, render_page};
use crate::{connection, theme};

pub struct Page {
    status: StringHandle,
    description: StringHandle,
    device: StringHandle,
    identity: StringHandle,
    receive_rate: StringHandle,
    send_rate: StringHandle,
    unpair_label: StringHandle,
    connected: bool,
    confirming: bool,
    unpairing: bool,
}

impl Page {
    pub fn new(mut platform: Platform, peer: connection::Peer) -> Self {
        Self {
            status: platform.create_string("Passport is connected"),
            description: platform
                .create_string("The encrypted relay is ready for desktop services."),
            device: platform.create_string(peer.name),
            identity: platform.create_string(peer.qid),
            receive_rate: platform.create_string("0 B/s"),
            send_rate: platform.create_string("0 B/s"),
            unpair_label: platform.create_string("Unpair Passport"),
            connected: true,
            confirming: false,
            unpairing: false,
        }
    }

    pub fn disconnected(&mut self) {
        self.status.replace("Passport is disconnected");
        self.description
            .replace("The secure session ended. Pair again to reconnect.");
        self.unpair_label.replace("Unpair and pair again");
        self.connected = false;
    }

    pub fn unpairing(&mut self) {
        self.status.replace("Clearing the pairing…");
        self.description
            .replace("Passport will be forgotten before pairing again.");
        self.confirming = false;
        self.unpairing = true;
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
        self.receive_rate.replace(format(receive));
        self.send_rate.replace(format(send));
    }

    pub fn render(&mut self, ui: &mut Ui) -> bool {
        let status_color = if self.unpairing {
            theme::ACCENT
        } else if self.connected {
            theme::POSITIVE
        } else {
            theme::NEGATIVE
        };
        let status_background = if self.unpairing {
            theme::ACCENT_SUBTLE
        } else if self.connected {
            theme::POSITIVE_SUBTLE
        } else {
            theme::NEGATIVE_SUBTLE
        };
        let content = render_page(ui, "Passport", "Secure desktop relay");
        let [summary, details] = Layout::default()
            .spacing(theme::SPACE_4)
            .constraints([Constraint::Fill(36), Constraint::Fill(64)])
            .areas(content);
        render_card(ui, summary);
        render_card(ui, details);

        let summary_content = theme::layout::padded(summary, theme::SPACE_5, theme::SPACE_4);
        let status_height = (summary_content.height * 0.2).clamp(72.0, 112.0);
        let [label, status, description, action] = Layout::default()
            .direction(Direction::Vertical)
            .spacing(theme::SPACE_3)
            .constraints([
                Constraint::Length(theme::SPACE_4),
                Constraint::Length(status_height),
                Constraint::Fill(1),
                Constraint::Length(theme::BUTTON_HEIGHT),
            ])
            .areas(summary_content);
        Text::new("STATUS")
            .color(status_color)
            .text_size(theme::TEXT_LABEL)
            .text_weight(600)
            .render(ui, label);
        Rectangle::new(status)
            .background(status_background)
            .border(theme::BORDER_WIDTH, status_color)
            .uniform_radius(theme::RADIUS_MEDIUM)
            .render(ui);
        Text::new(&self.status)
            .color(theme::TEXT)
            .text_size(theme::TEXT_BODY)
            .text_weight(600)
            .wrap(TextWrap::Word)
            .vertical_align(VerticalAlign::Center)
            .render(
                ui,
                theme::layout::padded(status, theme::SPACE_4, theme::SPACE_3),
            );
        Text::new(&self.description)
            .color(theme::TEXT_SECONDARY)
            .text_size(theme::TEXT_STATUS)
            .wrap(TextWrap::Word)
            .render(ui, description);

        let unpair = if self.unpairing {
            false
        } else if self.confirming {
            let [cancel, confirm] = Layout::default()
                .spacing(theme::SPACE_3)
                .constraints([Constraint::Fill(1); 2])
                .areas(action);
            if render_button(
                ui,
                "Cancel",
                "cancel unpair",
                cancel,
                theme::SURFACE_SUBTLE,
                theme::SURFACE_PRESSED,
            ) {
                self.confirming = false;
                ui.request_frame();
            }
            render_button(
                ui,
                "Confirm unpair",
                "confirm unpair",
                confirm,
                theme::NEGATIVE_SUBTLE,
                theme::NEGATIVE,
            )
        } else {
            let clicked = render_button(
                ui,
                &self.unpair_label,
                "unpair",
                action,
                theme::SURFACE_SUBTLE,
                theme::SURFACE_PRESSED,
            );
            if clicked {
                self.confirming = true;
                ui.request_frame();
            }
            false
        };

        let details_content = theme::layout::padded(details, theme::SPACE_5, theme::SPACE_4);
        let [label, rows] = Layout::default()
            .direction(Direction::Vertical)
            .spacing(theme::SPACE_3)
            .constraints([Constraint::Length(theme::SPACE_3), Constraint::Fill(1)])
            .areas(details_content);
        Text::new("CONNECTION DETAILS")
            .color(theme::ACCENT)
            .text_size(theme::TEXT_LABEL)
            .text_weight(600)
            .render(ui, label);
        let rows = Layout::default()
            .direction(Direction::Vertical)
            .spacing(theme::SPACE_2)
            .constraints([Constraint::Fill(1); 5])
            .areas(rows);
        let details: [(TextSource, TextSource); 5] = [
            ("DEVICE".into(), (&self.device).into()),
            ("IDENTITY".into(), (&self.identity).into()),
            ("RECEIVE".into(), (&self.receive_rate).into()),
            ("SEND".into(), (&self.send_rate).into()),
            ("SERVICES".into(), "Router + Foundation installed".into()),
        ];
        for ((label, value), area) in details.into_iter().zip(rows) {
            render_detail(ui, label, value, area);
        }

        unpair
    }
}

fn render_detail(
    ui: &mut Ui,
    label: impl Into<TextSource>,
    value: impl Into<TextSource>,
    area: blit::geometry::LogicalRect,
) {
    let [label_area, value_area] = Layout::default()
        .constraints([Constraint::Fill(32), Constraint::Fill(68)])
        .areas(area);
    Text::new(label)
        .color(theme::TEXT_MUTED)
        .text_size(theme::TEXT_LABEL)
        .text_weight(600)
        .vertical_align(VerticalAlign::Center)
        .render(ui, label_area);
    Text::new(value)
        .color(theme::TEXT)
        .text_size(theme::TEXT_STATUS)
        .vertical_align(VerticalAlign::Center)
        .render(ui, value_area);
}
