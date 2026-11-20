//! `numbers-station`: the station core, a chief's node, as a library.
//!
//! It will hold `Station::start`: one actor owning the endpoint, the peer book and the message log,
//! behind an exclusive lock on the data dir, plus the local-socket server that frontends talk to.
//! It is usable in-process as a `chief` transport.

use std::path::{Path, PathBuf};

/// The data dir below the XDG data home.
pub const DATA_DIR_SUBPATH: &str = "thecompany/station";

/// The station's data dir when none is given: `$XDG_DATA_HOME/thecompany/station`, where an unset,
/// empty or relative `XDG_DATA_HOME` means `$HOME/.local/share`. `None` when neither is usable.
///
/// Takes the variables' values rather than reading the environment, so it stays pure.
pub fn default_data_dir(xdg_data_home: Option<&Path>, home: Option<&Path>) -> Option<PathBuf> {
    let base = match xdg_data_home {
        Some(p) if p.is_absolute() => p.to_path_buf(),
        _ => match home {
            Some(h) if h.is_absolute() => h.join(".local/share"),
            _ => return None,
        },
    };
    Some(base.join(DATA_DIR_SUBPATH))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_dir_follows_xdg() {
        let p = |s: &str| PathBuf::from(s);
        assert_eq!(
            default_data_dir(Some(&p("/x/data")), Some(&p("/home/a"))),
            Some(p("/x/data/thecompany/station"))
        );
        assert_eq!(
            default_data_dir(Some(&p("rel")), Some(&p("/home/a"))),
            Some(p("/home/a/.local/share/thecompany/station"))
        );
        assert_eq!(
            default_data_dir(None, Some(&p("/home/a"))),
            Some(p("/home/a/.local/share/thecompany/station"))
        );
        assert_eq!(default_data_dir(None, None), None);
        assert_eq!(default_data_dir(Some(&p("")), Some(&p("rel"))), None);
    }
}
