//! The `tower` binary.

use std::process::ExitCode;

fn banner() -> String {
    format!(
        "tower {}: not implemented yet (would listen on {})",
        env!("CARGO_PKG_VERSION"),
        numbers_tower::DEFAULT_BIND
    )
}

fn main() -> ExitCode {
    eprintln!("{}", banner());
    ExitCode::FAILURE
}

#[cfg(test)]
mod tests {
    #[test]
    fn banner_names_the_binary_and_address() {
        let b = super::banner();
        assert!(b.starts_with("tower "));
        assert!(b.contains("127.0.0.1:3340"));
    }
}
