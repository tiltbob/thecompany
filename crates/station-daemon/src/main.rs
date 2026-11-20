//! The `station` binary: runs a chief's station in the foreground.

use std::process::ExitCode;

fn banner() -> String {
    format!("station {}: not implemented yet", env!("CARGO_PKG_VERSION"))
}

fn main() -> ExitCode {
    eprintln!("{}", banner());
    ExitCode::FAILURE
}

#[cfg(test)]
mod tests {
    #[test]
    fn banner_names_the_binary_and_version() {
        assert_eq!(
            super::banner(),
            format!("station {}: not implemented yet", env!("CARGO_PKG_VERSION"))
        );
    }
}
