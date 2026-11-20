#![cfg(test)]
//! `chief::paths`: the default data dir, the socket path and the socket-path length check. Pure
//! functions; nothing touches the file system or sets the environment.

use chief::api::ApiError;
use chief::paths::*;
use std::path::{Path, PathBuf};

fn p(s: &str) -> PathBuf {
    PathBuf::from(s)
}

#[test]
fn the_data_dir_is_below_xdg_data_home() {
    assert_eq!(
        default_data_dir(Some(Path::new("/data")), Some(Path::new("/home/a"))),
        Some(p("/data/thecompany/station"))
    );
    assert_eq!(
        default_data_dir(Some(Path::new("/data")), None),
        Some(p("/data/thecompany/station"))
    );
}

#[test]
fn an_unset_empty_or_relative_xdg_data_home_falls_back_to_home() {
    let home = Some(Path::new("/home/a"));
    let expected = Some(p("/home/a/.local/share/thecompany/station"));
    assert_eq!(default_data_dir(None, home), expected);
    assert_eq!(default_data_dir(Some(Path::new("")), home), expected);
    assert_eq!(default_data_dir(Some(Path::new("data")), home), expected);
}

#[test]
fn no_usable_home_gives_no_data_dir() {
    assert_eq!(default_data_dir(None, None), None);
    assert_eq!(default_data_dir(None, Some(Path::new(""))), None);
    assert_eq!(default_data_dir(None, Some(Path::new("home/a"))), None);
    assert_eq!(
        default_data_dir(Some(Path::new("rel")), Some(Path::new("rel"))),
        None
    );
}

#[test]
fn the_default_data_dir_uses_the_runtime_dir() {
    let default = p("/home/a/.local/share/thecompany/station");
    assert_eq!(
        socket_path(&default, Some(&default), Some(Path::new("/run/user/1000"))),
        p("/run/user/1000/thecompany/station.sock")
    );
    // Compared by components: a trailing slash is the same directory.
    assert_eq!(
        socket_path(
            Path::new("/home/a/.local/share/thecompany/station/"),
            Some(&default),
            Some(Path::new("/run/user/1000"))
        ),
        p("/run/user/1000/thecompany/station.sock")
    );
}

#[test]
fn without_a_usable_runtime_dir_the_socket_is_in_the_data_dir() {
    let default = p("/home/a/.local/share/thecompany/station");
    let in_data_dir = p("/home/a/.local/share/thecompany/station/station.sock");
    assert_eq!(socket_path(&default, Some(&default), None), in_data_dir);
    assert_eq!(
        socket_path(&default, Some(&default), Some(Path::new(""))),
        in_data_dir
    );
    assert_eq!(
        socket_path(&default, Some(&default), Some(Path::new("run/user/1000"))),
        in_data_dir
    );
}

#[test]
fn a_custom_data_dir_has_its_own_socket() {
    let default = p("/home/a/.local/share/thecompany/station");
    let runtime = Some(Path::new("/run/user/1000"));
    assert_eq!(
        socket_path(Path::new("/srv/b"), Some(&default), runtime),
        p("/srv/b/station.sock")
    );
    assert_eq!(
        socket_path(Path::new("/srv/b"), None, runtime),
        p("/srv/b/station.sock")
    );
    assert_eq!(
        socket_path(Path::new("rel/dir"), Some(&default), runtime),
        p("rel/dir/station.sock")
    );
}

#[test]
fn path_env_combines_the_functions() {
    let env = PathEnv {
        xdg_data_home: None,
        xdg_runtime_dir: Some(p("/run/user/1000")),
        home: Some(p("/home/a")),
    };
    let default = p("/home/a/.local/share/thecompany/station");
    assert_eq!(env.default_data_dir(), Some(default.clone()));
    assert_eq!(
        env.socket_path(&default),
        p("/run/user/1000/thecompany/station.sock")
    );
    assert_eq!(
        env.socket_path(Path::new("/srv/b")),
        p("/srv/b/station.sock")
    );
    assert_eq!(PathEnv::default().default_data_dir(), None);
}

#[test]
fn path_env_reads_the_process_environment() {
    // Reads only; the test never sets a variable.
    let env = PathEnv::read();
    assert_eq!(env.home, std::env::var_os("HOME").map(PathBuf::from));
    assert_eq!(
        env.xdg_runtime_dir,
        std::env::var_os("XDG_RUNTIME_DIR").map(PathBuf::from)
    );
    assert_eq!(
        env.xdg_data_home,
        std::env::var_os("XDG_DATA_HOME").map(PathBuf::from)
    );
}

#[cfg(any(target_os = "linux", target_os = "android"))]
#[test]
fn the_limit_is_107_bytes_on_linux() {
    assert_eq!(SOCKET_PATH_MAX_BYTES, 107);
}

#[cfg(target_vendor = "apple")]
#[test]
fn the_limit_is_103_bytes_on_apple() {
    assert_eq!(SOCKET_PATH_MAX_BYTES, 103);
}

/// A path of exactly `len` bytes.
fn path_of(len: usize) -> PathBuf {
    let mut s = String::from("/");
    s.push_str(&"a".repeat(len - 1));
    PathBuf::from(s)
}

#[test]
fn a_path_up_to_the_limit_is_accepted() {
    assert_eq!(check_socket_path(&path_of(SOCKET_PATH_MAX_BYTES)), Ok(()));
    assert_eq!(check_socket_path(Path::new("/s")), Ok(()));
}

#[test]
fn a_path_over_the_limit_is_refused_naming_data_dir() {
    let path = path_of(SOCKET_PATH_MAX_BYTES + 1);
    let Err(ApiError::Invalid { field, reason }) = check_socket_path(&path) else {
        panic!("a path over the limit was accepted");
    };
    assert_eq!(field, "socket");
    assert!(reason.contains("--data-dir"), "{reason}");
    assert!(
        reason.contains(&format!("{} bytes long", SOCKET_PATH_MAX_BYTES + 1)),
        "{reason}"
    );
}

#[test]
fn an_empty_path_is_refused() {
    let Err(ApiError::Invalid { field, reason }) = check_socket_path(Path::new("")) else {
        panic!("an empty path was accepted");
    };
    assert_eq!(field, "socket");
    assert!(reason.contains("--data-dir"), "{reason}");
}

#[test]
fn the_constants_are_pinned() {
    assert_eq!(DATA_DIR_SUBPATH, "thecompany/station");
    assert_eq!(RUNTIME_SUBDIR, "thecompany");
    assert_eq!(SOCKET_FILE, "station.sock");
}

#[test]
fn environments_that_differ_only_in_xdg_data_home_share_the_socket_path() {
    // The documented path rule: each process's default data dir has the runtime socket. Two
    // stations of one user with different XDG_DATA_HOME therefore name the same socket; the
    // server's socket lock (`numbers_station::ipc`) is what keeps the second from taking it.
    let one = PathEnv {
        xdg_data_home: Some(p("/home/a/one")),
        xdg_runtime_dir: Some(p("/run/user/1000")),
        home: Some(p("/home/a")),
    };
    let two = PathEnv {
        xdg_data_home: Some(p("/home/a/two")),
        ..one.clone()
    };
    let (dir_one, dir_two) = (
        one.default_data_dir().unwrap(),
        two.default_data_dir().unwrap(),
    );
    assert_ne!(dir_one, dir_two);
    assert_eq!(one.socket_path(&dir_one), two.socket_path(&dir_two));
    assert_eq!(
        one.socket_path(&dir_one),
        p("/run/user/1000/thecompany/station.sock")
    );
}
