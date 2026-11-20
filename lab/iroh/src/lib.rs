//! iroh lab: the snippets of docs/notes/iroh-snippets.md as real modules, close to how the
//! repository will use them. Tests are in tests/lab.rs.
pub mod endpoint;
pub mod gate;
pub mod link;
pub mod lock;
pub mod lookup;
pub mod paths;
pub mod relay;
pub mod tower;
#[cfg(feature = "verbatim")]
mod verbatim;

pub type Res<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
