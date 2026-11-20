//! The `chief` binary: the command-line frontend to a chief's station.

use std::process::ExitCode;

fn banner() -> String {
    format!(
        "chief {} (station API {}): not implemented yet",
        env!("CARGO_PKG_VERSION"),
        chief::API_VERSION
    )
}

fn main() -> ExitCode {
    eprintln!("{}", banner());
    ExitCode::FAILURE
}

#[cfg(test)]
mod tests {
    #[test]
    fn banner_names_the_binary_and_api() {
        let b = super::banner();
        assert!(b.starts_with("chief "));
        assert!(b.contains("station API 1"));
    }
}
