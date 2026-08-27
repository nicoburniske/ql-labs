pub mod protocol;

#[cfg(feature = "tokio")]
pub mod tokio;

#[cfg(feature = "tokio")]
pub use tokio::{Receiver, Sender, attach, connect, receive, send};

pub use protocol::MAX_RECORD_SIZE;

pub const DEFAULT_ADDRESS: &str = "127.0.0.1:7447";
