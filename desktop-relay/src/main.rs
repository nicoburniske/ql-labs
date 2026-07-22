mod connection;
mod page;
mod platform;
mod theme;

use blit::{Ui, paint::FontId, platform::Platform};
use blit_cpu::{Font, FontFace, RendererConfig};
use blit_desktop::{Application, Config, EventLoopProxy, Root};

fn main() {
    let log_level = std::env::var("QL_DESKTOP_LOG")
        .ok()
        .and_then(|level| level.parse::<tracing::Level>().ok())
        .unwrap_or(tracing::Level::INFO);
    tracing_subscriber::fmt().with_max_level(log_level).init();

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

struct App {
    platform: Platform,
    root: Root<App>,
    connection: connection::Connection,
    phase: connection::Phase,
    page: page::Page,
}

impl Application for App {
    type Input = ();

    fn new(platform: Platform, _events: EventLoopProxy<Self::Input>, mut root: Root<Self>) -> Self {
        let (connection, mut state) = connection::Connection::new();
        root.spawn(async move |cx| {
            loop {
                {
                    let state = state.borrow_and_update();
                    cx.app().update(&state);
                }
                if state.changed().await.is_err() {
                    break;
                }
            }
        });
        let page = page::Page::new(platform, &root);
        Self {
            platform,
            root,
            connection,
            phase: connection::Phase::Unpaired,
            page,
        }
    }

    fn input(&mut self, _: Self::Input) {}

    fn render(&mut self, ui: &mut Ui) {
        self.render_page(ui);
    }
}
