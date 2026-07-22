use blit::{
    Ui,
    layout::{Constraint, Direction, Layout, LayoutAlign},
    paint::{Rectangle, TextWrap, VerticalAlign},
    widget::Text,
};

use super::{render_button, render_card, render_page};
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
        let content = render_page(
            ui,
            "Connecting to Passport",
            "Preparing the secure desktop relay",
        );
        let content_width = if content.width > 900.0 {
            content.width * 0.72
        } else {
            content.width
        };
        let content_height = if content.height > 500.0 {
            content.height * if failed { 0.7 } else { 0.62 }
        } else {
            content.height
        };
        let [content] = Layout::default()
            .align(LayoutAlign::Center)
            .constraints([Constraint::Length(content_width)])
            .areas(content);
        let [content] = Layout::default()
            .direction(Direction::Vertical)
            .align(LayoutAlign::Center)
            .constraints([Constraint::Length(content_height)])
            .areas(content);
        render_card(ui, content);
        let content = theme::layout::padded(content, theme::SPACE_5, theme::SPACE_4);
        let [label, status, steps, button] = Layout::default()
            .direction(Direction::Vertical)
            .spacing(theme::SPACE_3)
            .constraints([
                Constraint::Length(theme::SPACE_3),
                Constraint::Length(40.0),
                Constraint::Fill(1),
                Constraint::Length(if failed { theme::BUTTON_HEIGHT } else { 0.0 }),
            ])
            .areas(content);
        Text::new("CONNECTION PROGRESS")
            .color(theme::ACCENT)
            .text_size(theme::TEXT_LABEL)
            .text_weight(600)
            .render(ui, label);
        Text::new(status_message)
            .color(if failed { theme::NEGATIVE } else { theme::TEXT })
            .text_size(theme::TEXT_BODY)
            .wrap(TextWrap::Word)
            .vertical_align(VerticalAlign::Center)
            .render(ui, status);

        let step_areas = Layout::default()
            .direction(Direction::Vertical)
            .spacing(theme::SPACE_2)
            .constraints([Constraint::Fill(1); 4])
            .areas(steps);
        for (index, (label, area)) in [
            "Pairing QR code verified",
            "Bluetooth connection",
            "Secure QLv2 session",
            "Desktop services",
        ]
        .into_iter()
        .zip(step_areas)
        .enumerate()
        {
            let index = index as u8 + 1;
            render_step(
                ui,
                label,
                area,
                index <= self.completed,
                index == self.completed + 1,
                failed,
            );
        }

        failed
            && render_button(
                ui,
                "Start over",
                "start over",
                button,
                theme::NEGATIVE_SUBTLE,
                theme::NEGATIVE,
            )
    }
}

fn render_step(
    ui: &mut Ui,
    label: &'static str,
    area: blit::geometry::LogicalRect,
    completed: bool,
    active: bool,
    failed: bool,
) {
    const INDICATOR_SIZE: f32 = 8.0;

    let color = if completed {
        theme::POSITIVE
    } else if active && failed {
        theme::NEGATIVE
    } else if active {
        theme::ACCENT
    } else {
        theme::TEXT_MUTED
    };
    Rectangle::new(area)
        .background(if completed {
            theme::POSITIVE_SUBTLE
        } else if active && failed {
            theme::NEGATIVE_SUBTLE
        } else if active {
            theme::ACCENT_SUBTLE
        } else {
            theme::SURFACE_SUBTLE
        })
        .border(
            theme::BORDER_WIDTH,
            if active || completed {
                color
            } else {
                theme::BORDER
            },
        )
        .uniform_radius(theme::RADIUS_MEDIUM)
        .render(ui);
    let area = theme::layout::padded(area, theme::SPACE_3, theme::SPACE_2);
    let [indicator, text] = Layout::default()
        .spacing(theme::SPACE_3)
        .constraints([Constraint::Length(INDICATOR_SIZE), Constraint::Fill(1)])
        .areas(area);
    let [indicator] = Layout::default()
        .direction(Direction::Vertical)
        .align(LayoutAlign::Center)
        .constraints([Constraint::Length(INDICATOR_SIZE)])
        .areas(indicator);
    Rectangle::new(indicator)
        .background(color)
        .uniform_radius(INDICATOR_SIZE / 2.0)
        .render(ui);
    Text::new(label)
        .color(if completed || active {
            theme::TEXT
        } else {
            theme::TEXT_MUTED
        })
        .text_size(theme::TEXT_STATUS)
        .vertical_align(VerticalAlign::Center)
        .render(ui, text);
}
