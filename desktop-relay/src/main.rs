mod connection;
mod page;
mod platform;
mod theme;

use blit::{Ui, paint::FontId, platform::Platform};
use blit_cpu::{Font, FontFace, RendererConfig};
use blit_desktop::{Application, Config, EventLoopProxy, Root};
use figment::{
    Figment,
    providers::{Env, Serialized},
};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
struct AppConfig {
    log: String,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self { log: "INFO".into() }
    }
}

fn main() -> anyhow::Result<()> {
    let config: AppConfig = Figment::from(Serialized::defaults(AppConfig::default()))
        .merge(Env::prefixed("QL_DESKTOP_"))
        .extract()?;
    let log_level: tracing::Level = config.log.parse()?;
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
    .map_err(|error| anyhow::anyhow!("{error}"))?;
    Ok(())
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
