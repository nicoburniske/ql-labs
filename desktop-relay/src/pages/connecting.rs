use blit::{
    Ui,
    layout::{Constraint, Direction, Layout, LayoutAlign},
    paint::{Rectangle, TextWrap, VerticalAlign},
    platform::Platform,
    resource::StringHandle,
    widget::Text,
};

use super::{render_button, render_card, render_page};
use crate::theme;

pub struct Page {
    title: StringHandle,
    subtitle: StringHandle,
    progress_label: StringHandle,
    status: StringHandle,
    qr_step: StringHandle,
    bluetooth_step: StringHandle,
    secure_step: StringHandle,
    services_step: StringHandle,
    start_over_label: StringHandle,
    completed: u8,
    failed: bool,
}

impl Page {
    pub fn new(mut platform: Platform) -> Self {
        Self {
            title: platform.create_string("Connecting to Prime"),
            subtitle: platform.create_string("Preparing the secure desktop relay"),
            progress_label: platform.create_string("CONNECTION PROGRESS"),
            status: platform.create_string("QR code verified. Preparing connection…"),
            qr_step: platform.create_string("Pairing QR code verified"),
            bluetooth_step: platform.create_string("Bluetooth connection"),
            secure_step: platform.create_string("Secure QLv2 session"),
            services_step: platform.create_string("Desktop services"),
            start_over_label: platform.create_string("Start over"),
            completed: 1,
            failed: false,
        }
    }

    pub fn searching(&mut self) {
        self.status.replace("Looking for Prime over Bluetooth…");
    }

    pub fn connecting(&mut self) {
        self.status
            .replace("Prime found. Opening Bluetooth connection…");
    }

    pub fn bluetooth_connected(&mut self) {
        self.completed = 2;
        self.status
            .replace("Bluetooth connected. Starting secure session…");
    }

    pub fn secure_session(&mut self) {
        self.completed = 2;
        self.status
            .replace("Establishing the encrypted QLv2 session…");
    }

    pub fn secure_connected(&mut self) {
        self.completed = 3;
        self.status.replace("Secure session established.");
    }

    pub fn provisioning(&mut self) {
        self.completed = 3;
        self.status
            .replace("Installing router and Foundation services…");
    }

    pub fn failed(&mut self, message: &str) {
        self.status.replace(message.to_owned());
        self.failed = true;
    }

    pub fn can_finish(&self) -> bool {
        !self.failed
    }

    pub fn render(&mut self, ui: &mut Ui) -> bool {
        let content = render_page(ui, &self.title, &self.subtitle);
        let content_width = if content.width > 900.0 {
            content.width * 0.72
        } else {
            content.width
        };
        let content_height = if content.height > 500.0 {
            content.height * if self.failed { 0.7 } else { 0.62 }
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
                Constraint::Length(if self.failed {
                    theme::BUTTON_HEIGHT
                } else {
                    0.0
                }),
            ])
            .areas(content);
        Text::new(&self.progress_label)
            .color(theme::ACCENT)
            .text_size(theme::TEXT_LABEL)
            .text_weight(600)
            .render(ui, label);
        Text::new(&self.status)
            .color(if self.failed {
                theme::NEGATIVE
            } else {
                theme::TEXT
            })
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
            &self.qr_step,
            &self.bluetooth_step,
            &self.secure_step,
            &self.services_step,
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
                self.failed,
            );
        }

        self.failed
            && render_button(
                ui,
                &self.start_over_label,
                "start over",
                button,
                theme::NEGATIVE_SUBTLE,
                theme::NEGATIVE,
            )
    }
}

fn render_step(
    ui: &mut Ui,
    label: &StringHandle,
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
