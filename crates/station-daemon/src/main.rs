//! The `station` binary: runs a chief's station in the foreground.

use std::process::ExitCode;

fn banner() -> String {
    format!(
        "station {}: not implemented yet (data dir would be $XDG_DATA_HOME/{})",
        env!("CARGO_PKG_VERSION"),
        numbers_station::DATA_DIR_SUBPATH
    )
}

fn main() -> ExitCode {
    eprintln!("{}", banner());
    ExitCode::FAILURE
}

#[cfg(test)]
mod tests {
    #[test]
    fn banner_names_the_binary() {
        let b = super::banner();
        assert!(b.starts_with("station "));
        assert!(b.contains("thecompany/station"));
    }
}
