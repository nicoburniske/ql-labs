pub mod protocol;

#[cfg(feature = "tokio")]
pub mod tokio;

pub const DEFAULT_ADDRESS: &str = "127.0.0.1:7447";
