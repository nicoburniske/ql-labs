use blit::color::Color;

pub const WINDOW_WIDTH: u32 = 800;
pub const WINDOW_HEIGHT: u32 = 480;

pub const SPACE_2: f32 = 8.0;
pub const SPACE_3: f32 = 12.0;
pub const SPACE_4: f32 = 16.0;
pub const SPACE_5: f32 = 20.0;
pub const SPACE_6: f32 = 24.0;

pub const RADIUS_MEDIUM: f32 = 12.0;
pub const RADIUS_LARGE: f32 = 18.0;
pub const BORDER_WIDTH: f32 = 1.0;
pub const GUIDE_WIDTH: f32 = 2.0;

pub const TEXT_LABEL: f32 = 12.0;
pub const TEXT_STATUS: f32 = 16.0;
pub const TEXT_BODY: f32 = 18.0;
pub const TEXT_TITLE: f32 = 28.0;
pub const BUTTON_HEIGHT: f32 = 52.0;

pub const BACKGROUND: Color = Color::from_rgba8(17, 17, 17, 255);
pub const SURFACE: Color = Color::from_rgba8(35, 31, 32, 255);
pub const SURFACE_SUBTLE: Color = Color::from_rgba8(90, 89, 90, 255);
pub const SURFACE_PRESSED: Color = Color::from_rgba8(116, 115, 116, 255);
pub const BORDER: Color = Color::from_rgba8(69, 68, 68, 255);
pub const TEXT: Color = Color::from_rgba8(255, 255, 255, 255);
pub const TEXT_SECONDARY: Color = Color::from_rgba8(227, 226, 226, 255);
pub const TEXT_MUTED: Color = Color::from_rgba8(149, 147, 148, 255);
pub const ACCENT: Color = Color::from_rgba8(51, 177, 199, 255);
pub const ACCENT_SUBTLE: Color = Color::from_rgba8(0, 86, 102, 255);
pub const POSITIVE: Color = Color::from_rgba8(46, 148, 131, 255);
pub const POSITIVE_SUBTLE: Color = Color::from_rgba8(19, 62, 55, 255);
pub const NEGATIVE: Color = Color::from_rgba8(255, 51, 51, 255);
pub const NEGATIVE_SUBTLE: Color = Color::from_rgba8(107, 21, 21, 255);
pub const PREVIEW: Color = Color::from_rgba8(0, 0, 0, 255);
pub const PREVIEW_GUIDE: Color = Color::from_rgba8(51, 177, 199, 220);
