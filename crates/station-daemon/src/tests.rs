use super::{Args, RunError, RunningStation, print_argument_exit, resolve};
use chief::api::{ApiError, Direction, Event, MessageStatus, TowerState};
use chief::client::{Client, SocketTransport, Transport};
use chief::paths::PathEnv;
use clap::Parser;
use numbers_station::ipc::{IpcConfig, IpcError, IpcServer};
use numbers_station::{IpTransport, StartError, Station, StationConfig};
use numbers_tower::{Tower, TowerConfig};
use std::future::{pending, ready};
use std::net::Ipv4Addr;
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::sync::oneshot;
use tokio::time::timeout;

const TOWER: &str = "http://127.0.0.1:3340";
const PRIVATE_HOST: &str = "station-test.invalid";
const PRIVATE_MARKER: &str = "secret-marker";

fn parse(args: &[&str]) -> Args {
    Args::try_parse_from(std::iter::once("station").chain(args.iter().copied())).unwrap()
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
    let data_dir = format!("/{PRIVATE_MARKER}/{PRIVATE_HOST}");
    let (status, stdout, stderr) = argument_exit(&["run", "--data-dir", &data_dir]);
    assert_eq!(status, ExitCode::from(2));
    assert!(stdout.is_empty());
    assert!(
        stderr == "station: pass --tower URL to station run; see station run --help\n",
        "missing --tower guidance changed"
    );
    assert_private_values_omitted(&stderr);

    let url = format!("https://{PRIVATE_HOST}/{PRIVATE_MARKER}");
    for args in [
        &[PRIVATE_MARKER][..],
        &["run", "--tower", &url, PRIVATE_MARKER],
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
fn data_directory_and_socket_follow_the_shared_rules() {
    let env = PathEnv {
        xdg_data_home: Some(PathBuf::from("/data")),
        xdg_runtime_dir: Some(PathBuf::from("/run/user")),
        home: Some(PathBuf::from("/home/user")),
    };
    let (config, socket) = resolve(parse(&["run", "--tower", TOWER]), &env).unwrap();
    assert_eq!(config.data_dir, Path::new("/data/thecompany/station"));
    assert_eq!(socket, Path::new("/run/user/thecompany/station.sock"));
    assert_eq!(config.tower_url, TOWER);

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

#[tokio::test]
async fn invalid_tower_diagnostic_omits_the_url() {
    let scratch = tempfile::tempdir().expect("create test directory");
    let data_dir = scratch.path().join("station");
    let socket = data_dir.join("station.sock");
    let url = format!("https://{PRIVATE_MARKER}@{PRIVATE_HOST}/");
    let Err(error @ RunError::Station(StartError::InvalidTower(_))) =
        RunningStation::start(loopback_config(&data_dir, &url), &socket).await
    else {
        panic!("expected a tower URL with credentials to be rejected");
    };
    let message = error.to_string();
    assert_private_values_omitted(&message);
    assert!(message.starts_with("invalid --tower URL"));
    assert!(!data_dir.exists());
}

fn checked<T, E>(result: Result<T, E>, operation: &str) -> T {
    // API and network failures can include identities. Print only the operation on failure.
    result.unwrap_or_else(|_| panic!("{operation}"))
}

fn loopback_config(data_dir: &Path, tower: &str) -> StationConfig {
    let mut config = StationConfig::new(data_dir, tower);
    config.ip_transport = IpTransport::LoopbackOnly;
    config.connect_timeout = Duration::from_secs(3);
    config.send_deadline = Duration::from_secs(6);
    config
}

async fn wait_for_tower<T: Transport>(client: &Client<T>) {
    let mut events = checked(client.subscribe().await, "subscribe for tower readiness");
    let status = checked(client.status().await, "read the tower status");
    if status.towers[0].state == TowerState::Connected {
        return;
    }
    while let Some(event) = events.next().await {
        if let Event::Status { status } = event
            && status.towers[0].state == TowerState::Connected
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
            RunningStation::start(first_config.clone(), &first_socket).await,
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

        checked(first.run_until_stopped(ready(())).await, "shut down first station");
        assert!(!first_socket.exists());
        assert!(matches!(first_client.status().await, Err(ApiError::NotRunning { .. })));
        let restarted = checked(
            RunningStation::start(first_config, &first_socket).await,
            "restart first station after shutdown",
        );
        let restarted_status = checked(first_client.status().await, "call restarted station");
        assert!(restarted_status.station_id == first_status.station_id, "restart changed identity");
        assert!(restarted_status.instance_id != first_status.instance_id, "restart kept the instance");
        checked(restarted.shutdown().await, "shut down restarted station");
        assert!(!first_socket.exists());
        checked(second_client.status().await, "second socket survives first shutdown");
        checked(second.shutdown().await, "shut down second station");
        assert!(!second_socket.exists());
        checked(tower.shutdown().await, "shut down loopback tower");
    })
    .await
    .expect("station daemon test timed out");
}

#[tokio::test]
async fn an_unexpected_core_stop_removes_the_socket_and_returns_an_error() {
    timeout(Duration::from_secs(15), async {
        let scratch = tempfile::tempdir().expect("create test directory");
        let data_dir = scratch.path().join("station");
        let socket = data_dir.join("station.sock");
        let config = loopback_config(&data_dir, TOWER);
        let core_config = config.clone();
        let (started, station) = oneshot::channel();
        let (stop_runtime, stopped) = oneshot::channel();
        // Run the core on a separate runtime so its task can be cancelled while IPC stays live.
        let runtime = tokio::task::spawn_blocking(move || {
            let runtime = checked(
                Builder::new_current_thread().enable_all().build(),
                "create core runtime",
            );
            runtime.block_on(async {
                let station = checked(Station::start(core_config).await, "start station core");
                checked(started.send(station), "send station core");
                let _ = stopped.await;
            });
        });
        let station = checked(station.await, "receive station core");
        let ipc = checked(
            IpcServer::bind(station.handle(), &socket, IpcConfig::default()).await,
            "bind the station socket",
        );
        let running = RunningStation { station, ipc };
        let client = Client::new(SocketTransport::new(&socket));
        checked(client.status().await, "call station before the core stops");

        checked(stop_runtime.send(()), "stop core runtime without a signal");
        let result = running.run_until_stopped(pending()).await;
        let Err(error @ RunError::CoreStopped) = result else {
            panic!("expected an unexpected core stop to fail the daemon");
        };
        assert!(error.to_string() == "the station core stopped unexpectedly");
        assert!(!socket.exists());
        assert!(matches!(
            client.status().await,
            Err(ApiError::NotRunning { .. })
        ));
        checked(runtime.await, "join core runtime");

        let restarted = checked(
            RunningStation::start(config, &socket).await,
            "restart station after the core stops",
        );
        checked(restarted.shutdown().await, "shut down restarted station");
    })
    .await
    .expect("station daemon did not finish after its core stopped");
}

#[tokio::test]
async fn a_socket_failure_releases_the_station_data_lock() {
    timeout(Duration::from_secs(15), async {
        let scratch = tempfile::tempdir().expect("create test directory");
        let data_dir = scratch.path().join("station");
        numbers_base::ensure_private_dir(&data_dir).expect("create private station directory");
        let socket = data_dir.join(format!("{PRIVATE_MARKER}.sock"));
        std::fs::write(&socket, b"keep this file").expect("block the socket path");
        let config = loopback_config(&data_dir, TOWER);
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
