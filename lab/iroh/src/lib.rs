//! iroh lab: the snippets of docs/notes/iroh-snippets.md as real modules, close to how the
//! repository will use them. Tests are in tests/lab.rs (M1) and tests/m2.rs (M2).
pub mod allowlist;
pub mod endpoint;
pub mod gate;
pub mod link;
pub mod locate;
pub mod lock;
pub mod lookup;
pub mod paths;
pub mod relay;
pub mod tls_tower;
pub mod tower;
#[cfg(feature = "verbatim")]
#[rustfmt::skip]
mod verbatim;

pub type Res<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
