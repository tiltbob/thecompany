use super::{Args, RunError, RunningStation, ca, log_allowed, print_argument_exit, resolve};
use chief::api::{ApiError, Direction, Event, MessageStatus, TowerSource, TowerState};
use chief::client::{Client, SocketTransport, Transport};
use chief::paths::PathEnv;
use clap::Parser;
use numbers::testing::FakeDns;
use numbers::{DnsConfig, MAX_TOWERS, TlsRoots, TowerSet};
use numbers_station::ipc::IpcError;
use numbers_station::{IpTransport, StartError, StationConfig, TowerInput};
use numbers_tower::{TlsTower, Tower, TowerConfig};
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa,
    Issuer, KeyPair,
};
use serde_json::{Value, json};
use std::fs;
use std::net::{Ipv4Addr, SocketAddr};
use std::os::unix::fs::{FileTypeExt, MetadataExt, symlink};
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::mpsc;
use std::time::Duration;
use tokio::time::timeout;
use tracing::Level;

const TOWER: &str = "http://127.0.0.1:3340";
const PRIVATE_HOST: &str = "station-test.invalid";
const PRIVATE_MARKER: &str = "secret-marker";
const SYSTEMD_UNIT: &str = include_str!("../../../packaging/systemd/station.service");

fn parse(args: &[&str]) -> Args {
    Args::try_parse_from(std::iter::once("station").chain(args.iter().copied())).unwrap()
}

fn unit_values<'a>(unit: &'a str, section: &str, key: &str) -> Vec<&'a str> {
    let mut current = None;
    let mut values = Vec::new();
    for line in unit.lines().map(str::trim) {
        if line.is_empty() || line.starts_with(['#', ';']) {
            continue;
        }
        // The shipped unit has no continued lines. Splitting one would change its meaning.
        assert!(!line.ends_with('\\'), "a continued line in the unit");
        if let Some(name) = line
            .strip_prefix('[')
            .and_then(|line| line.strip_suffix(']'))
        {
            current = Some(name);
        } else if current == Some(section)
            && let Some((name, value)) = line.split_once('=')
            && name.trim() == key
        {
            values.push(value.trim());
        }
    }
    values
}

#[test]
fn systemd_exec_start_uses_remembered_towers_and_default_paths() {
    let values = unit_values(SYSTEMD_UNIT, "Service", "ExecStart");
    let [exec_start] = values.as_slice() else {
        panic!("expected exactly one [Service] ExecStart");
    };
    let argv: Vec<_> = exec_start.split_whitespace().collect();
    assert_eq!(argv.first().copied(), Some("%h/.local/bin/station"));
    // Only the executable uses a systemd specifier. The arguments are plain words.
    assert!(
        argv[1..]
            .iter()
            .all(|arg| !arg.contains(['"', '\'', '%', '\\']))
    );
    let args = checked(Args::try_parse_from(argv), "parse the systemd ExecStart");
    let env = PathEnv {
        home: Some("/home/user".into()),
        xdg_runtime_dir: Some("/run/user/1000".into()),
        ..PathEnv::default()
    };
    let (config, socket) = checked(resolve(args, &env), "resolve the systemd ExecStart");
    assert!(matches!(config.tower_input, TowerInput::Remembered));
    assert_eq!(
        config.data_dir,
        Path::new("/home/user/.local/share/thecompany/station")
    );
    assert_eq!(socket, Path::new("/run/user/1000/thecompany/station.sock"));
    assert!(matches!(config.tls_roots, TlsRoots::WebPki));
    assert!(matches!(config.dns, DnsConfig::System));
}

#[test]
fn systemd_station_unit_restarts_failures_and_keeps_private_permissions() {
    for (key, value) in [
        ("Type", "exec"),
        ("Restart", "on-failure"),
        ("RestartSec", "5s"),
        ("RestartPreventExitStatus", "2"),
        ("UMask", "0077"),
    ] {
        assert_eq!(unit_values(SYSTEMD_UNIT, "Service", key), [value], "{key}");
    }
    assert_eq!(
        unit_values(SYSTEMD_UNIT, "Install", "WantedBy"),
        ["default.target"]
    );
}

fn argument_exit(args: &[&str]) -> (ExitCode, String, String) {
    let Err(error) =
        Args::try_parse_from(std::iter::once(PRIVATE_MARKER).chain(args.iter().copied()))
    else {
        panic!("expected parsing to stop");
    };
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let status = print_argument_exit(error, &mut stdout, &mut stderr);
    (
        status,
        checked(String::from_utf8(stdout), "stdout should be UTF-8"),
        checked(String::from_utf8(stderr), "stderr should be UTF-8"),
    )
}

fn assert_private_values_omitted(text: &str) {
    assert!(!text.contains(PRIVATE_HOST), "output included a hostname");
    assert!(
        !text.contains(PRIVATE_MARKER),
        "output included a private value"
    );
}

#[test]
fn argument_errors_give_static_guidance_without_echoing_input() {
    let url = format!("https://{PRIVATE_HOST}/{PRIVATE_MARKER}");
    for args in [
        &[PRIVATE_MARKER][..],
        &["run", "--tower", &url, PRIVATE_MARKER],
        &["run", "--tower"],
        &["run", "--tower", &url, "--domain", PRIVATE_HOST],
    ] {
        let (status, stdout, stderr) = argument_exit(args);
        assert_eq!(status, ExitCode::from(2));
        assert!(stdout.is_empty());
        assert!(stderr.contains("station run --help"));
        assert_private_values_omitted(&stderr);
    }
}

#[test]
fn help_and_version_are_successful_and_use_the_fixed_command_name() {
    let url = format!("https://{PRIVATE_HOST}/{PRIVATE_MARKER}");
    for (args, expected) in [
        (&["--help"][..], "Usage: station"),
        (
            &["run", "--tower", &url, "--help"][..],
            "Usage: station run",
        ),
        (
            &["--version"][..],
            concat!("station ", env!("CARGO_PKG_VERSION")),
        ),
    ] {
        let (status, stdout, stderr) = argument_exit(args);
        assert_eq!(status, ExitCode::SUCCESS);
        assert!(stderr.is_empty());
        assert!(
            stdout.contains(expected),
            "missing normal help or version text"
        );
        assert_private_values_omitted(&stdout);
    }
}

#[test]
fn run_help_describes_public_tower_flags_and_hides_the_resolver_override() {
    let (exit, stdout, stderr) = argument_exit(&["run", "--help"]);
    assert_eq!(exit, ExitCode::SUCCESS);
    assert!(stderr.is_empty());
    for flag in ["--tower <URL>", "--domain <DOMAIN>", "--tower-ca <PEM>"] {
        assert!(stdout.contains(flag));
    }
    assert!(!stdout.contains("--dns-server"));
}

#[test]
fn resolves_remembered_domain_and_ordered_canonical_pins() {
    let env = PathEnv {
        home: Some("/home/user".into()),
        ..PathEnv::default()
    };
    let (config, _) = checked(resolve(parse(&["run"]), &env), "resolve remembered run");
    assert!(matches!(config.tower_input, TowerInput::Remembered));
    assert!(matches!(config.tls_roots, TlsRoots::WebPki));
    assert!(matches!(config.dns, DnsConfig::System));
    assert!(config.locator.is_none());

    let (config, _) = checked(
        resolve(parse(&["run", "--domain", "Team.SYNTHETIC.Invalid."]), &env),
        "resolve domain run",
    );
    assert!(
        matches!(config.tower_input, TowerInput::Domain(domain) if domain == "Team.SYNTHETIC.Invalid.")
    );
    assert!(config.locator.is_none());

    let (config, _) = checked(
        resolve(
            parse(&[
                "run",
                "--tower",
                "HTTPS://Tower.SYNTHETIC.INVALID:443",
                "--tower",
                TOWER,
            ]),
            &env,
        ),
        "resolve repeated pins",
    );
    let TowerInput::Pinned(pins) = config.tower_input else {
        panic!("expected a pinned run");
    };
    assert!(
        pins.urls()
            .eq(["https://tower.synthetic.invalid/", "http://127.0.0.1:3340/"])
    );
}

#[test]
fn invalid_or_excess_pins_report_static_diagnostics() {
    let env = PathEnv {
        home: Some("/home/user".into()),
        ..PathEnv::default()
    };
    let bad = format!("https://{PRIVATE_HOST}/{PRIVATE_MARKER}");
    let error = resolve(parse(&["run", "--tower", &bad]), &env).unwrap_err();
    assert!(matches!(error, RunError::TowerUrl));
    assert_private_values_omitted(&error.to_string());

    let duplicate = resolve(
        parse(&["run", "--tower", TOWER, "--tower", "HTTP://127.0.0.1:3340/"]),
        &env,
    )
    .unwrap_err();
    assert!(matches!(duplicate, RunError::TowerSet));
    assert!(duplicate.to_string().contains("distinct --tower URLs"));

    let urls: Vec<_> = (0..=MAX_TOWERS)
        .map(|index| format!("https://tower-{index}.{PRIVATE_HOST}/"))
        .collect();
    let mut args = vec!["run"];
    for url in &urls {
        args.extend(["--tower", url]);
    }
    let full = resolve(parse(&args), &env).unwrap_err();
    assert!(matches!(full, RunError::TowerSet));
    assert_private_values_omitted(&full.to_string());
}

#[test]
fn numeric_nameservers_preserve_the_port_and_ipv6_scope() {
    let env = PathEnv {
        home: Some("/home/user".into()),
        ..PathEnv::default()
    };
    for (input, expected) in [
        ("127.0.0.1:8053", SocketAddr::from(([127, 0, 0, 1], 8053))),
        (
            "[::1]:8053",
            SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 1], 8053)),
        ),
        (
            "[fe80::1%7]:8053",
            SocketAddr::V6(std::net::SocketAddrV6::new(
                "fe80::1".parse().unwrap(),
                8053,
                0,
                7,
            )),
        ),
    ] {
        let (config, _) = checked(
            resolve(parse(&["run", "--dns-server", input]), &env),
            "resolve literal DNS socket address",
        );
        assert!(matches!(config.dns, DnsConfig::Nameserver(addr) if addr == expected));
        assert!(config.locator.is_none());
    }

    for input in [
        "127.0.0.1",
        "::1:8053",
        "station-test.invalid:8053",
        "[fe80::1%secret-marker]:8053",
    ] {
        let error = resolve(parse(&["run", "--dns-server", input]), &env).unwrap_err();
        assert!(matches!(error, RunError::DnsServer));
        assert!(
            error
                .to_string()
                .contains("numeric IPv4 IP:PORT or bracketed IPv6 [IP]:PORT")
        );
        assert_private_values_omitted(&error.to_string());
    }
}

#[test]
fn logging_admits_only_the_cli_and_the_exact_core_tower_warning_target() {
    assert!(log_allowed("numbers_station_daemon::cli", Level::INFO));
    assert!(log_allowed("numbers_station::towers", Level::WARN));
    for level in [Level::TRACE, Level::DEBUG, Level::INFO, Level::ERROR] {
        assert!(!log_allowed("numbers_station::towers", level));
    }
    assert!(!log_allowed(
        "numbers_station::towers::dependency",
        Level::WARN
    ));
    assert!(!log_allowed("iroh::endpoint", Level::WARN));
}

#[test]
fn data_directory_and_socket_follow_the_shared_rules() {
    let env = PathEnv {
        xdg_data_home: Some(PathBuf::from("/data")),
        xdg_runtime_dir: Some(PathBuf::from("/run/user")),
        home: Some(PathBuf::from("/home/user")),
    };
    let (config, socket) = resolve(parse(&["run", "--tower", TOWER]), &env).unwrap();
    assert_eq!(config.data_dir, Path::new("/data/thecompany/station"));
    assert_eq!(socket, Path::new("/run/user/thecompany/station.sock"));
    let TowerInput::Pinned(towers) = config.tower_input else {
        panic!("the daemon must pin the supplied tower URL");
    };
    assert!(towers.urls().eq(["http://127.0.0.1:3340/"]));

    let (config, socket) = resolve(
        parse(&["run", "--tower", TOWER, "--data-dir", "/data/second"]),
        &env,
    )
    .unwrap();
    assert_eq!(config.data_dir, Path::new("/data/second"));
    assert_eq!(socket, Path::new("/data/second/station.sock"));

    let (_, socket) = resolve(
        parse(&[
            "run",
            "--tower",
            TOWER,
            "--data-dir",
            "/data/thecompany/station/",
        ]),
        &env,
    )
    .unwrap();
    assert_eq!(socket, Path::new("/run/user/thecompany/station.sock"));

    let home_only = PathEnv {
        home: env.home,
        ..PathEnv::default()
    };
    let (config, socket) = resolve(parse(&["run", "--tower", TOWER]), &home_only).unwrap();
    assert_eq!(
        config.data_dir,
        Path::new("/home/user/.local/share/thecompany/station")
    );
    assert_eq!(socket, config.data_dir.join("station.sock"));
}

#[test]
fn path_errors_point_to_data_dir() {
    let env = PathEnv::default();
    let missing = resolve(parse(&["run", "--tower", TOWER]), &env).unwrap_err();
    assert!(missing.to_string().contains("--data-dir DIR"));
    let long = format!("/{PRIVATE_MARKER}/{}", "d".repeat(200));
    let too_long =
        resolve(parse(&["run", "--tower", TOWER, "--data-dir", &long]), &env).unwrap_err();
    let message = too_long.to_string();
    assert!(message.contains("Unix socket path is too long"));
    assert!(message.contains("--data-dir"));
    assert_private_values_omitted(&message);
}

#[test]
fn startup_errors_omit_storage_details_and_socket_paths_and_sources() {
    let path = PathBuf::from(format!("/{PRIVATE_MARKER}/{PRIVATE_HOST}/station.sock"));
    let detail = format!("could not open {}", path.display());
    let errors = [
        (
            RunError::from(StartError::Storage {
                detail: detail.clone(),
            }),
            "owned by the current user",
        ),
        (
            RunError::from(IpcError::InUse { path: path.clone() }),
            "choose a separate --data-dir, or use a separate XDG_RUNTIME_DIR",
        ),
        (
            RunError::from(IpcError::PathTooLong {
                path: path.clone(),
                reason: detail.clone(),
            }),
            "choose a shorter --data-dir",
        ),
        (
            RunError::from(IpcError::Bind {
                path,
                source: std::io::Error::other(detail),
            }),
            "could not bind the Unix socket",
        ),
    ];
    for (error, guidance) in errors {
        let message = error.to_string();
        assert!(message.contains(guidance), "missing startup guidance");
        assert_private_values_omitted(&message);
    }
}

fn checked<T, E>(result: Result<T, E>, operation: &str) -> T {
    // API and network failures can include identities. Print only the operation on failure.
    result.unwrap_or_else(|_| panic!("{operation}"))
}

fn loopback_config(data_dir: &Path, tower: &str) -> StationConfig {
    let towers = checked(TowerSet::from_url(tower), "validate loopback tower URL");
    let mut config = StationConfig::new(data_dir, TowerInput::Pinned(towers));
    config.ip_transport = IpTransport::LoopbackOnly;
    config.connect_timeout = Duration::from_secs(3);
    config.send_deadline = Duration::from_secs(6);
    config
}

async fn wait_for_tower<T: Transport>(client: &Client<T>) {
    let mut events = checked(client.subscribe().await, "subscribe for tower readiness");
    let status = checked(client.status().await, "read the tower status");
    if status
        .towers
        .iter()
        .any(|tower| tower.state == TowerState::Connected)
    {
        return;
    }
    while let Some(event) = events.next().await {
        if let Event::Status { status } = event
            && status
                .towers
                .iter()
                .any(|tower| tower.state == TowerState::Connected)
        {
            return;
        }
    }
    panic!("station stopped while waiting for the loopback tower");
}

fn socket_identity(path: &Path) -> (u64, u64) {
    let metadata = std::fs::symlink_metadata(path).expect("inspect the socket");
    assert!(metadata.file_type().is_socket());
    (metadata.dev(), metadata.ino())
}

#[tokio::test]
async fn two_daemons_serve_separate_sockets_and_log_without_an_attached_client() {
    timeout(Duration::from_secs(75), async {
        let scratch = tempfile::tempdir().expect("create test directory");
        let tower = checked(
            Tower::spawn(TowerConfig::dev((Ipv4Addr::LOCALHOST, 0).into())).await,
            "start loopback tower",
        );
        let first_dir = scratch.path().join("first");
        let second_dir = scratch.path().join("second");
        let paths = PathEnv::default();
        let first_socket = paths.socket_path(&first_dir);
        let second_socket = paths.socket_path(&second_dir);
        let first_config = loopback_config(&first_dir, tower.url());
        let first = checked(
            RunningStation::start(first_config.clone(), &first_socket).await,
            "start first station and socket",
        );
        let second = checked(
            RunningStation::start(loopback_config(&second_dir, tower.url()), &second_socket).await,
            "start second station and socket",
        );
        let first_inode = socket_identity(&first_socket);
        assert_ne!(first_inode, socket_identity(&second_socket));

        let first_client = Client::new(SocketTransport::new(&first_socket));
        let first_status = checked(first_client.status().await, "call first station over its socket");
        assert!(matches!(
            RunningStation::start(first_config, &first_socket).await,
            Err(RunError::Station(StartError::AlreadyRunning))
        ));
        assert_eq!(socket_identity(&first_socket), first_inode);
        let current = checked(first_client.status().await, "call first station after a contender");
        assert!(current.instance_id == first_status.instance_id, "first instance changed");

        // Configure the receiver in process. It has no open subscription or socket connection
        // when the message arrives. Only the sender needs to observe the delivery event.
        let second_local = Client::new(second.station.handle());
        let second_status = checked(second_local.status().await, "read second station identity");
        assert!(first_status.station_id != second_status.station_id, "stations share an identity");
        checked(
            first_client.peer_add("second", &second_status.station_id).await,
            "authorize second station",
        );
        checked(
            second_local.peer_add("first", &first_status.station_id).await,
            "authorize first station",
        );
        tokio::join!(wait_for_tower(&first_client), wait_for_tower(&second_local));
        drop(second_local);

        let sent = checked(
            first_client.send_and_wait("second", "message while detached").await,
            "send to the detached receiver",
        );
        assert_eq!(sent.status, MessageStatus::Delivered);

        let second_client = Client::new(SocketTransport::new(&second_socket));
        let socket_status = checked(second_client.status().await, "call second station over its socket");
        assert!(socket_status.instance_id == second_status.instance_id, "second socket reached another station");
        let page = checked(second_client.log(None, None, 10).await, "read the detached receiver's log");
        assert!(matches!(page.messages.as_slice(), [message] if
            message.direction == Direction::Incoming && message.peer == "first" && message.body == "message while detached"
        ));

        checked(first.shutdown().await, "shut down first station");
        assert!(!first_socket.exists());
        assert!(matches!(first_client.status().await, Err(ApiError::NotRunning { .. })));
        checked(second_client.status().await, "second socket survives first shutdown");
        checked(second.shutdown().await, "shut down second station");
        assert!(!second_socket.exists());
        checked(tower.shutdown().await, "shut down loopback tower");
    })
    .await
    .expect("station daemon test timed out");
}

#[tokio::test]
async fn a_socket_failure_releases_the_station_data_lock() {
    timeout(Duration::from_secs(15), async {
        let scratch = tempfile::tempdir().expect("create test directory");
        let socket = scratch.path().join(format!("{PRIVATE_MARKER}.sock"));
        std::fs::write(&socket, b"keep this file").expect("block the socket path");
        let config = loopback_config(scratch.path(), TOWER);
        let Err(error @ RunError::Socket(IpcError::NotASocket { .. })) =
            RunningStation::start(config.clone(), &socket).await
        else {
            panic!("expected the socket bind to refuse the existing file");
        };
        let message = error.to_string();
        assert!(message.contains("choose another --data-dir, or change XDG_RUNTIME_DIR"));
        assert_private_values_omitted(&message);
        assert_eq!(std::fs::read(&socket).unwrap(), b"keep this file");
        std::fs::remove_file(&socket).expect("remove the fixture file");
        let running = checked(
            RunningStation::start(config, &socket).await,
            "restart the station after a socket error",
        );
        socket_identity(&socket);
        checked(running.shutdown().await, "shut down restarted station");
        assert!(!socket.exists());
    })
    .await
    .expect("station cleanup test timed out");
}

fn generate_ca() -> (Certificate, Issuer<'static, KeyPair>) {
    let mut params = CertificateParams::default();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params
        .distinguished_name
        .push(DnType::CommonName, "test CA");
    let key = checked(KeyPair::generate(), "generate test CA key");
    let cert = checked(params.self_signed(&key), "issue test CA certificate");
    (cert, Issuer::new(params, key))
}

#[test]
fn ca_bundle_extends_webpki_for_each_tower_input() {
    let directory = tempfile::tempdir().expect("create test CA directory");
    let file = directory.path().join("bundle.pem");
    let link = directory.path().join("bundle-link.pem");
    let (first, _) = generate_ca();
    let (second, _) = generate_ca();
    // PemObject skips surrounding text and unknown labels.
    let pem = format!(
        "test description\n-----BEGIN UNKNOWN-----\nAA==\n-----END UNKNOWN-----\n{}{}",
        first.pem(),
        second.pem()
    );
    checked(fs::write(&file, pem), "write multi-root CA fixture");
    checked(symlink(&file, &link), "link to regular CA fixture");

    let env = PathEnv {
        home: Some("/home/user".into()),
        ..PathEnv::default()
    };
    let ca_file = link.to_str().expect("test CA path is UTF-8");
    for (input, mode) in [
        (&[][..], "remembered"),
        (&["--domain", "team.synthetic.invalid"][..], "domain"),
        (&["--tower", TOWER][..], "pins"),
    ] {
        let mut args = vec!["run", "--tower-ca", ca_file];
        args.extend(input);
        let (config, _) = checked(
            resolve(parse(&args), &env),
            "configure additional TLS roots",
        );
        assert!(matches!(
            (config.tower_input, mode),
            (TowerInput::Remembered, "remembered")
                | (TowerInput::Domain(_), "domain")
                | (TowerInput::Pinned(_), "pins")
        ));
        assert!(matches!(config.tls_roots, TlsRoots::WebPkiPlus(roots)
            if roots.len() == 2 && roots[0] == first.der().as_ref() && roots[1] == second.der().as_ref()));
    }
}

#[test]
fn ca_loader_rejects_missing_and_non_regular_files_without_waiting_for_a_fifo_writer() {
    let directory = tempfile::tempdir().expect("create test CA directory");
    let missing = directory.path().join(PRIVATE_MARKER);
    for path in [directory.path(), missing.as_path()] {
        let error = ca::load(path).unwrap_err();
        assert!(matches!(error, ca::CaError::Read));
        assert_private_values_omitted(&error.to_string());
    }
    let socket_path = directory.path().join("ca.sock");
    let _socket = checked(UnixListener::bind(&socket_path), "bind test socket");
    assert!(matches!(ca::load(&socket_path), Err(ca::CaError::Read)));

    let fifo = directory.path().join("ca-fifo");
    checked(
        rustix::fs::mkfifoat(rustix::fs::CWD, &fifo, rustix::fs::Mode::RUSR),
        "create test FIFO",
    );
    let (finished, result) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        let _ = finished.send(ca::load(&fifo));
    });
    let loaded = result
        .recv_timeout(Duration::from_secs(5))
        .expect("FIFO opened without waiting for a writer");
    assert!(matches!(loaded, Err(ca::CaError::Read)));
    checked(worker.join(), "join CA loader test");
}

#[test]
fn ca_loader_rejects_malformed_or_mixed_bundles_and_validates_every_certificate() {
    let directory = tempfile::tempdir().expect("create test CA directory");
    let file = directory.path().join(PRIVATE_MARKER);
    let (valid, _) = generate_ca();
    let malformed =
        format!("-----BEGIN CERTIFICATE-----\n!{PRIVATE_MARKER}!\n-----END CERTIFICATE-----\n");
    let invalid_der = "-----BEGIN CERTIFICATE-----\nAQID\n-----END CERTIFICATE-----\n";
    let incomplete = "-----BEGIN CERTIFICATE-----\nAQID\n";
    let noncert = "-----BEGIN PRIVATE KEY-----\nAA==\n-----END PRIVATE KEY-----\n";
    for (contents, expected) in [
        ("".to_owned(), ca::CaError::Empty.to_string()),
        (
            "text without a bundle".to_owned(),
            ca::CaError::Empty.to_string(),
        ),
        (malformed, ca::CaError::Pem.to_string()),
        (incomplete.to_owned(), ca::CaError::Pem.to_string()),
        (
            format!("{}{noncert}", valid.pem()),
            ca::CaError::Pem.to_string(),
        ),
        (invalid_der.to_owned(), ca::CaError::Der.to_string()),
        (
            format!("{}{invalid_der}", valid.pem()),
            ca::CaError::Der.to_string(),
        ),
    ] {
        checked(fs::write(&file, contents), "write invalid CA fixture");
        let error = ca::load(&file).unwrap_err().to_string();
        assert_eq!(error, expected);
        assert_private_values_omitted(&error);
    }
}

#[test]
fn ca_size_limit_counts_the_whole_file_and_bad_ca_cannot_start_a_domain_run() {
    let directory = tempfile::tempdir().expect("create test CA directory");
    let file = directory.path().join(PRIVATE_MARKER);
    let (valid, _) = generate_ca();
    let mut contents = valid.pem().into_bytes();
    contents.resize(ca::MAX_CA_BYTES, b' ');
    checked(fs::write(&file, &contents), "write bundle at limit");
    assert!(matches!(ca::load(&file), Ok(TlsRoots::WebPkiPlus(roots)) if roots.len() == 1));
    contents.push(b' ');
    checked(fs::write(&file, contents), "write oversized CA bundle");
    let data = directory.path().join("station");
    let error = resolve(
        parse(&[
            "run",
            "--data-dir",
            data.to_str().expect("test station path is UTF-8"),
            "--domain",
            "team.synthetic.invalid",
            "--tower-ca",
            file.to_str().expect("test CA path is UTF-8"),
        ]),
        &PathEnv::default(),
    )
    .unwrap_err();
    assert!(matches!(error, RunError::TowerCa(ca::CaError::TooLarge)));
    assert_private_values_omitted(&error.to_string());
    assert!(!data.exists());
}

fn resolve_for_test(dir: &Path, flags: &[&str]) -> (StationConfig, PathBuf) {
    let mut args = vec![
        "run",
        "--data-dir",
        dir.to_str().expect("test path is UTF-8"),
    ];
    args.extend(flags);
    let (mut config, socket) = checked(
        resolve(parse(&args), &PathEnv::default()),
        "resolve test station flags",
    );
    config.ip_transport = IpTransport::RelayOnly;
    (config, socket)
}

async fn start_test_run(dir: &Path, flags: &[&str]) -> (RunningStation, Client<SocketTransport>) {
    let (config, socket) = resolve_for_test(dir, flags);
    let running = checked(
        RunningStation::start(config, &socket).await,
        "start station and serve its socket",
    );
    (running, Client::new(SocketTransport::new(socket)))
}

const TLS_HOST: &str = "tower.synthetic.invalid";
const DOMAIN: &str = "team.synthetic.invalid";
const SRV_NAME: &str = "_numbers._tcp.team.synthetic.invalid";

async fn tls_tower(dir: &Path) -> (Tower, PathBuf) {
    let (root, issuer) = generate_ca();
    let mut params = checked(
        CertificateParams::new(vec![TLS_HOST.to_owned()]),
        "set test tower certificate name",
    );
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let key = checked(KeyPair::generate(), "generate test tower key");
    let cert = checked(
        params.signed_by(&key, &issuer),
        "issue test tower certificate",
    );
    let ca_file = dir.join("root.pem");
    let cert_file = dir.join("cert.pem");
    let key_file = dir.join("key.pem");
    checked(fs::write(&ca_file, root.pem()), "write generated root");
    checked(fs::write(&cert_file, cert.pem()), "write generated leaf");
    checked(
        fs::write(&key_file, key.serialize_pem()),
        "write generated key",
    );

    let loopback = (Ipv4Addr::LOCALHOST, 0).into();
    let config = TlsTower::new(TLS_HOST, cert_file, key_file)
        .https_bind(loopback)
        .qad_bind(None);
    let tower = checked(
        Tower::spawn(TowerConfig::tls(config).bind(loopback)).await,
        "start loopback TLS tower",
    );
    (tower, ca_file)
}

#[tokio::test]
async fn domain_ca_and_dns_flags_drive_a_real_tls_bootstrap_and_remembered_runs() {
    timeout(Duration::from_secs(75), async {
        let scratch = tempfile::tempdir().expect("create test directory");
        let dir = scratch.path().join("station");
        let file = dir.join("towers.json");
        let (tower, ca_file) = tls_tower(scratch.path()).await;
        let port = tower
            .https_addr()
            .expect("test tower has a TLS listener")
            .port();
        let dns = checked(FakeDns::start().await, "start loopback DNS fixture");
        dns.set_srv(SRV_NAME, &[(TLS_HOST, port)]);
        dns.set_a(TLS_HOST);
        let server = dns.addr().to_string();
        let ca_file = ca_file.to_str().expect("test CA path is UTF-8");
        let common = ["--tower-ca", ca_file, "--dns-server", server.as_str()];
        let mut first_args = common.to_vec();
        first_args.extend(["--domain", "Team.SYNTHETIC.Invalid."]);
        let (running, client) = start_test_run(&dir, &first_args).await;
        wait_for_tower(&client).await;
        let status = checked(client.status().await, "read discovered TLS tower status");
        assert_eq!(status.bootstrap_domain.as_deref(), Some(DOMAIN));
        assert!(matches!(status.towers.as_slice(), [status] if
            status.url == tower.url() && status.state == TowerState::Connected
                && status.source == Some(TowerSource::Srv)));
        checked(running.shutdown().await, "shut down discovered run");
        let saved = checked(fs::read(&file), "read learned tower record");
        let srv_queries = dns.query_count_for(SRV_NAME, "SRV");
        assert!(srv_queries > 0);
        assert!(dns.query_count_for(TLS_HOST, "A") > 0);

        for extra in [&[][..], &["--domain", "TEAM.synthetic.invalid."][..]] {
            let mut args = common.to_vec();
            args.extend(extra);
            let (running, client) = start_test_run(&dir, &args).await;
            wait_for_tower(&client).await;
            let status = checked(client.status().await, "read remembered TLS tower status");
            assert_eq!(status.bootstrap_domain.as_deref(), Some(DOMAIN));
            assert!(matches!(status.towers.as_slice(), [status] if
                status.url == tower.url() && status.source == Some(TowerSource::Remembered)));
            checked(running.shutdown().await, "shut down remembered run");
            assert_eq!(dns.query_count_for(SRV_NAME, "SRV"), srv_queries);
            assert_eq!(
                checked(fs::read(&file), "read unchanged remembered record"),
                saved
            );
        }

        let pin = checked(
            Tower::spawn(TowerConfig::dev((Ipv4Addr::LOCALHOST, 0).into())).await,
            "start pinned loopback tower",
        );
        let (running, client) =
            start_test_run(&dir, &["--tower", pin.url(), "--dns-server", &server]).await;
        wait_for_tower(&client).await;
        let status = checked(client.status().await, "read pinned tower status");
        assert!(status.bootstrap_domain.is_none());
        let pin_url = checked(
            numbers::canonical_tower_url(pin.url()),
            "canonicalize fixture pin",
        );
        assert!(matches!(status.towers.as_slice(), [status] if
            status.url == pin_url && status.source == Some(TowerSource::Configured)));
        checked(running.shutdown().await, "shut down pinned run");
        checked(pin.shutdown().await, "stop pinned tower");
        assert_eq!(dns.query_count_for(SRV_NAME, "SRV"), srv_queries);
        assert_eq!(
            checked(fs::read(&file), "read unchanged pinned record"),
            saved
        );

        dns.set_silent(true);
        let (running, client) = start_test_run(
            &dir,
            &[
                "--domain",
                "New.SYNTHETIC.Invalid.",
                "--dns-server",
                &server,
            ],
        )
        .await;
        let status = checked(
            client.status().await,
            "read changed domain while DNS is pending",
        );
        assert_eq!(
            status.bootstrap_domain.as_deref(),
            Some("new.synthetic.invalid")
        );
        assert!(status.towers.is_empty());
        let cleared = checked(fs::read(&file), "read cleared remembered set");
        assert_eq!(
            checked(
                serde_json::from_slice::<Value>(&cleared),
                "decode cleared record"
            ),
            json!({"version": 1, "bootstrap_domain": "new.synthetic.invalid", "towers": []}),
        );
        timeout(Duration::from_secs(5), async {
            while dns.query_count_for("_numbers._tcp.new.synthetic.invalid", "SRV") == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("changed domain began its configured DNS lookup");
        checked(
            running.shutdown().await,
            "shut down while discovery is pending",
        );
        assert_eq!(
            checked(fs::read(&file), "read retained empty record"),
            cleared
        );
        checked(tower.shutdown().await, "stop the TLS test tower");
    })
    .await
    .expect("daemon tower flags test timed out");
}

#[tokio::test]
async fn first_run_without_configuration_exits_before_it_serves_ipc() {
    timeout(Duration::from_secs(15), async {
        let scratch = tempfile::tempdir().expect("create test directory");
        let dir = scratch.path().join("station");
        let (config, socket) = resolve_for_test(&dir, &[]);
        let Err(error @ RunError::Station(StartError::MissingTowerConfig)) =
            RunningStation::start(config, &socket).await
        else {
            panic!("expected a refusal for the first remembered run");
        };
        assert!(
            error
                .to_string()
                .contains("pass --tower URL or --domain DOMAIN")
        );
        assert!(!socket.exists());
        assert!(!dir.join("station.sock.lock").exists());
    })
    .await
    .expect("missing configuration test timed out");
}
