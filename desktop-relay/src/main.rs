mod connection;
mod pairing;
mod platform;
mod theme;

use blit::{Ui, paint::FontId, platform::Platform};
use blit_cpu::{Font, FontFace, RendererConfig};
use blit_desktop::{Application, Config, EventLoopProxy, Ops};

fn main() {
    let mut fonts = fontdb::Database::new();
    fonts.load_system_fonts();
    let id = fonts
        .query(&fontdb::Query {
            families: &[fontdb::Family::SansSerif],
            ..fontdb::Query::default()
        })
        .unwrap();
    let (font, index) = fonts
        .with_face_data(id, |data, index| (data.to_vec().into_boxed_slice(), index))
        .unwrap();
    assert_eq!(index, 0, "selected system font is a collection face");

    blit_desktop::run::<App>(Config {
        title: "QL Lab".into(),
        width: theme::WINDOW_WIDTH,
        height: theme::WINDOW_HEIGHT,
        renderer: RendererConfig {
            fonts: vec![FontFace {
                id: FontId::default(),
                weight: 400,
                font: Font::from_owned(font).unwrap(),
            }],
            font_metric_cache_capacity: 256,
            glyph_cache_capacity: 1024 * 1024,
            paragraph_cache_capacity: 1024 * 1024,
            shadow_cache_capacity: 32 * 1024 * 1024,
        },
    })
    .unwrap();
}

enum Event {
    CameraFrame,
    Connection(connection::Event),
}

struct App {
    connection: connection::Connection,
    pairing: pairing::Page,
}

impl Application for App {
    type Input = Event;

    fn new(platform: Platform, events: EventLoopProxy<Self::Input>, _ops: Ops<Self>) -> Self {
        Self {
            connection: connection::Connection::new(events.clone()),
            pairing: pairing::Page::new(platform, events),
        }
    }

    fn input(&mut self, event: Self::Input) {
        if let Event::Connection(event) = event {
            self.pairing.input(event);
        }
    }

    fn render(&mut self, ui: &mut Ui) {
        if let Some(action) = self.pairing.render(ui) {
            match action {
                pairing::Action::Pair(target) => self.connection.pair(target),
                pairing::Action::Reset => self.connection.reset(),
            }
        }
    }
}
