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
    status: Status,
    passport_name: StringHandle,
    passport_qid: StringHandle,
    desktop_qid: StringHandle,
    receive_rate: StringHandle,
    send_rate: StringHandle,
    confirming: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Status {
    Connected,
    Disconnected,
}

impl Page {
    pub fn new(mut platform: Platform, peer: connection::Peer) -> Self {
        Self {
            status: Status::Connected,
            passport_name: platform.create_string(peer.name),
            passport_qid: platform.create_string(peer.passport_qid),
            desktop_qid: platform.create_string(peer.desktop_qid),
            receive_rate: platform.create_string("0 B/s"),
            send_rate: platform.create_string("0 B/s"),
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
        self.receive_rate.replace(format(receive));
        self.send_rate.replace(format(send));
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
        let mut content = render_page(ui, "Passport", "Secure desktop relay");
        content.height = content.height.min(400.0);
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
        Text::new(status_message)
            .color(theme::TEXT)
            .text_size(theme::TEXT_BODY)
            .text_weight(600)
            .wrap(TextWrap::Word)
            .vertical_align(VerticalAlign::Center)
            .render(
                ui,
                theme::layout::padded(status, theme::SPACE_4, theme::SPACE_3),
            );
        Text::new(description_message)
            .color(theme::TEXT_SECONDARY)
            .text_size(theme::TEXT_STATUS)
            .wrap(TextWrap::Word)
            .render(ui, description);

        let unpair = if self.confirming {
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
                if self.status == Status::Disconnected {
                    "Unpair and pair again"
                } else {
                    "Unpair Passport"
                },
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
            ("PASSPORT NAME".into(), (&self.passport_name).into()),
            ("PASSPORT QID".into(), (&self.passport_qid).into()),
            ("DESKTOP QID".into(), (&self.desktop_qid).into()),
            ("RECEIVE".into(), (&self.receive_rate).into()),
            ("SEND".into(), (&self.send_rate).into()),
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
