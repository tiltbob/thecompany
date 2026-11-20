#![cfg(test)]
#![cfg(unix)]

use std::fs::{self, File};
use std::future::{Future, ready};
use std::net::{Ipv4Addr, TcpListener};
use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use chief::api::{
    ApiError, Event, MessageStatus, TowerAddOutcome, TowerRemoveOutcome, TowerSource, TowerState,
};
use chief::client::{Client, Subscription};
use numbers::{LocateError, LocateFuture, Locator, MAX_TOWERS, TowerEntry, TowerSet};
use numbers_station::{IpTransport, StartError, Station, StationConfig, StationHandle, TowerInput};
use numbers_tower::{Tower, TowerConfig};
use serde_json::{Value, json};
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;

const CASE_BOUND: Duration = Duration::from_secs(60);
const START_BOUND: Duration = Duration::from_secs(5);
const DOMAIN: &str = "team.synthetic.invalid";
const TOWER_FILE: &str = "towers.json";

type Chief = Client<StationHandle>;
type LocateReply = Result<Vec<TowerEntry>, LocateError>;

fn checked<T, E>(result: Result<T, E>, operation: &str) -> T {
    // Station errors and replies can carry generated identities.
    result.unwrap_or_else(|_| panic!("{operation}"))
}

async fn bounded(task: impl Future<Output = ()>) {
    checked(timeout(CASE_BOUND, task).await, "tower test timed out");
}

struct Lookup {
    domain: String,
    record_at_lookup: Option<Vec<u8>>,
    reply: oneshot::Sender<LocateReply>,
}

struct GatedLocator {
    file: PathBuf,
    calls: mpsc::UnboundedSender<Lookup>,
}

impl Locator for GatedLocator {
    fn locate<'a>(&'a self, domain: &'a str) -> LocateFuture<'a> {
        Box::pin(async move {
            let (reply, answer) = oneshot::channel();
            self.calls
                .send(Lookup {
                    domain: domain.to_owned(),
                    record_at_lookup: fs::read(&self.file).ok(),
                    reply,
                })
                .map_err(|_| LocateError::Resolver)?;
            answer.await.unwrap_or(Err(LocateError::Resolver))
        })
    }
}

fn config(dir: &Path, input: TowerInput) -> (StationConfig, mpsc::UnboundedReceiver<Lookup>) {
    let (calls, lookups) = mpsc::unbounded_channel();
    let mut config = StationConfig::new(dir, input);
    config.ip_transport = IpTransport::RelayOnly;
    config.locator = Some(Arc::new(GatedLocator {
        file: dir.join(TOWER_FILE),
        calls,
    }));
    (config, lookups)
}

async fn next_lookup(lookups: &mut mpsc::UnboundedReceiver<Lookup>, domain: &str) -> Lookup {
    let lookup = lookups.recv().await.expect("station calls the locator");
    assert_eq!(lookup.domain, domain);
    let record = lookup
        .record_at_lookup
        .as_deref()
        .expect("tower record already exists when lookup starts");
    assert_eq!(decode(record), disk_record(domain, json!([])));
    lookup
}

async fn start(config: StationConfig) -> (Station, Chief) {
    let station = checked(
        checked(
            timeout(START_BOUND, Station::start(config)).await,
            "start awaited discovery",
        ),
        "start station",
    );
    let client = Client::new(station.handle());
    (station, client)
}

async fn stop(station: Station) {
    checked(station.shutdown().await, "shut down station");
}

async fn loopback_tower() -> Tower {
    checked(
        Tower::spawn(TowerConfig::dev((Ipv4Addr::LOCALHOST, 0).into())).await,
        "start loopback tower",
    )
}

fn pins(tower: &Tower) -> TowerInput {
    TowerInput::Pinned(checked(TowerSet::from_url(tower.url()), "build test pins"))
}

async fn wait_online(client: &Chief, url: &str, source: TowerSource) {
    let mut events = checked(client.subscribe().await, "subscribe to tower status");
    let mut status = checked(client.status().await, "read tower status");
    loop {
        if status.towers.iter().any(|tower| {
            tower.url == url && tower.state == TowerState::Connected && tower.source == Some(source)
        }) {
            return;
        }
        if let Event::Status { status: updated } = events.next().await.expect("station stays open")
        {
            status = updated;
        }
    }
}

async fn assert_no_queued_event(events: &mut Subscription) {
    tokio::select! {
        biased;
        _ = events.next() => panic!("unexpected event or closed subscription"),
        _ = ready(()) => {},
    }
}

fn disk_record(domain: &str, towers: Value) -> Value {
    json!({"version": 1, "bootstrap_domain": domain, "towers": towers})
}

fn decode(bytes: &[u8]) -> Value {
    checked(serde_json::from_slice(bytes), "decode tower record")
}

fn read_record(dir: &Path) -> Vec<u8> {
    checked(fs::read(dir.join(TOWER_FILE)), "read tower record")
}

fn write_private(path: &Path, bytes: &[u8]) {
    checked(fs::write(path, bytes), "write tower fixture");
    checked(
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)),
        "secure tower fixture",
    );
}

#[tokio::test]
async fn discovery_runs_after_start_and_a_remembered_set_skips_it() {
    bounded(async {
        let dir = tempfile::tempdir().expect("create station directory");
        let peer_dir = tempfile::tempdir().expect("create peer directory");
        let tower = loopback_tower().await;
        let entry = checked(TowerEntry::from_url(tower.url()), "build locator result");
        let url = entry.url().to_owned();
        let (settings, mut lookups) = config(
            dir.path(),
            TowerInput::Domain("Team.SYNTHETIC.Invalid.".into()),
        );
        let (station, chief) = start(settings).await;
        let lookup = next_lookup(&mut lookups, DOMAIN).await;
        for _ in 0..2 {
            let status = checked(chief.status().await, "read status while locator is gated");
            assert_eq!(status.bootstrap_domain.as_deref(), Some(DOMAIN));
            assert!(status.towers.is_empty());
        }
        assert!(
            lookups.try_recv().is_err(),
            "status must not restart discovery"
        );
        assert!(lookup.reply.send(Ok(vec![entry])).is_ok());
        wait_online(&chief, &url, TowerSource::Srv).await;

        let (peer_settings, mut peer_lookups) = config(peer_dir.path(), pins(&tower));
        let (peer_station, peer) = start(peer_settings).await;
        wait_online(&peer, &url, TowerSource::Configured).await;
        let chief_id = checked(chief.status().await, "read chief identity").station_id;
        let peer_id = checked(peer.status().await, "read peer identity").station_id;
        checked(chief.peer_add("other", &peer_id).await, "admit peer");
        checked(peer.peer_add("first", &chief_id).await, "admit chief");
        let delivered = checked(
            chief.send_and_wait("other", "after discovery").await,
            "send through the discovered tower",
        );
        assert_eq!(delivered.status, MessageStatus::Delivered);
        let log = checked(peer.log(Some("first"), None, 1).await, "read received chat");
        assert_eq!(log.messages.len(), 1);
        assert_eq!(log.messages[0].body, "after discovery");
        stop(peer_station).await;
        assert!(peer_lookups.try_recv().is_err(), "pins skip discovery");
        stop(station).await;

        let saved = read_record(dir.path());
        assert_eq!(
            decode(&saved),
            disk_record(
                DOMAIN,
                json!([{"url": url, "qad_port": null, "source": "srv"}])
            ),
        );
        for input in [
            TowerInput::Remembered,
            TowerInput::Domain("TEAM.synthetic.invalid.".into()),
        ] {
            let (settings, mut lookups) = config(dir.path(), input);
            let (station, chief) = start(settings).await;
            wait_online(&chief, &url, TowerSource::Remembered).await;
            let status = checked(chief.status().await, "read remembered tower status");
            assert_eq!(status.bootstrap_domain.as_deref(), Some(DOMAIN));
            stop(station).await;
            assert!(
                lookups.try_recv().is_err(),
                "remembered startup skips discovery"
            );
            assert_eq!(read_record(dir.path()), saved);
        }

        let pin = loopback_tower().await;
        let pin_url = checked(numbers::canonical_tower_url(pin.url()), "canonicalize pin");
        let (settings, mut lookups) = config(dir.path(), pins(&pin));
        let (station, chief) = start(settings).await;
        wait_online(&chief, &pin_url, TowerSource::Configured).await;
        let status = checked(chief.status().await, "read pinned tower status");
        assert!(status.bootstrap_domain.is_none());
        assert_eq!(status.towers.len(), 1);
        stop(station).await;
        assert!(lookups.try_recv().is_err(), "pin override skips discovery");
        assert_eq!(read_record(dir.path()), saved);
        checked(pin.shutdown().await, "shut down pinned tower");
        checked(tower.shutdown().await, "shut down discovered tower");
    })
    .await;
}

#[tokio::test]
async fn a_real_domain_change_replaces_the_record_before_lookup_and_shutdown_cancels_it() {
    bounded(async {
        let dir = tempfile::tempdir().expect("create station directory");
        let tower = loopback_tower().await;
        let entry = checked(TowerEntry::from_url(tower.url()), "build fixture tower");
        let old = disk_record(
            "old.synthetic.invalid",
            json!([{"url": entry.url(), "qad_port": null, "source": "srv"}]),
        );
        write_private(
            &dir.path().join(TOWER_FILE),
            &serde_json::to_vec(&old).unwrap(),
        );

        let (settings, mut lookups) = config(
            dir.path(),
            TowerInput::Domain("Team.SYNTHETIC.Invalid.".into()),
        );
        let (station, chief) = start(settings).await;
        let lookup = next_lookup(&mut lookups, DOMAIN).await;
        let status = checked(chief.status().await, "read changed-domain status");
        assert_eq!(status.bootstrap_domain.as_deref(), Some(DOMAIN));
        assert!(status.towers.is_empty());
        let empty = read_record(dir.path());
        assert_eq!(decode(&empty), disk_record(DOMAIN, json!([])));
        stop(station).await;
        assert!(
            lookup.reply.send(Ok(vec![entry])).is_err(),
            "shutdown drops the locator"
        );
        assert_eq!(read_record(dir.path()), empty);

        let (settings, mut lookups) = config(dir.path(), TowerInput::Remembered);
        let (station, chief) = start(settings).await;
        let lookup = next_lookup(&mut lookups, DOMAIN).await;
        assert!(
            checked(chief.status().await, "read empty remembered status")
                .towers
                .is_empty()
        );
        assert!(lookups.try_recv().is_err());
        stop(station).await;
        assert!(lookup.reply.is_closed());
        assert_eq!(read_record(dir.path()), empty);
        checked(tower.shutdown().await, "shut down loopback tower");
    })
    .await;
}

#[tokio::test]
async fn missing_or_invalid_configuration_preserves_the_file_and_explicit_domain_recovers_it() {
    bounded(async {
        let dir = tempfile::tempdir().expect("create station directory");
        let file = dir.path().join(TOWER_FILE);
        let (settings, mut lookups) = config(dir.path(), TowerInput::Remembered);
        assert!(matches!(
            Station::start(settings).await,
            Err(StartError::MissingTowerConfig)
        ));
        assert!(!file.exists());
        assert!(lookups.try_recv().is_err());

        let corrupt = b"damaged tower record";
        write_private(&file, corrupt);
        let (settings, mut lookups) = config(dir.path(), TowerInput::Remembered);
        assert!(matches!(
            Station::start(settings).await,
            Err(StartError::MissingTowerConfig)
        ));
        assert!(lookups.try_recv().is_err());
        assert_eq!(read_record(dir.path()), corrupt);

        let (settings, mut lookups) = config(
            dir.path(),
            TowerInput::Domain("https://team.synthetic.invalid/".into()),
        );
        assert!(matches!(
            Station::start(settings).await,
            Err(StartError::InvalidBootstrapDomain)
        ));
        assert!(lookups.try_recv().is_err());
        assert_eq!(read_record(dir.path()), corrupt);

        let (settings, mut lookups) = config(dir.path(), TowerInput::Domain(DOMAIN.into()));
        let (station, chief) = start(settings).await;
        let lookup = next_lookup(&mut lookups, DOMAIN).await;
        let status = checked(chief.status().await, "read recovered tower status");
        assert_eq!(status.bootstrap_domain.as_deref(), Some(DOMAIN));
        assert!(status.towers.is_empty());
        stop(station).await;
        assert!(lookup.reply.is_closed());
        assert_eq!(
            decode(&read_record(dir.path())),
            disk_record(DOMAIN, json!([]))
        );
    })
    .await;
}

#[tokio::test]
async fn pins_ignore_an_unsafe_tower_file_that_a_domain_run_cannot_replace() {
    bounded(async {
        let dir = tempfile::tempdir().expect("create station directory");
        let target = dir.path().join("outside-record");
        write_private(&target, b"untouched");
        let file = dir.path().join(TOWER_FILE);
        checked(symlink(&target, &file), "link unsafe tower fixture");
        let assert_link_untouched = || {
            assert!(
                checked(fs::read_link(&file), "inspect unsafe fixture") == target,
                "the symlink still names the original target"
            );
            assert_eq!(
                checked(fs::read(&target), "read unsafe fixture target"),
                b"untouched"
            );
        };

        let tower = loopback_tower().await;
        let url = checked(
            numbers::canonical_tower_url(tower.url()),
            "canonicalize pin",
        );
        let (settings, mut lookups) = config(dir.path(), pins(&tower));
        let (station, chief) = start(settings).await;
        wait_online(&chief, &url, TowerSource::Configured).await;
        let status = checked(chief.status().await, "read pinned tower status");
        assert!(status.bootstrap_domain.is_none());
        assert_eq!(status.towers.len(), 1);
        stop(station).await;
        assert!(lookups.try_recv().is_err());
        assert_link_untouched();

        let (settings, mut lookups) = config(dir.path(), TowerInput::Domain(DOMAIN.into()));
        assert!(matches!(
            Station::start(settings).await,
            Err(StartError::Storage { .. })
        ));
        assert!(lookups.try_recv().is_err());
        assert_link_untouched();
        checked(tower.shutdown().await, "shut down pinned tower");
    })
    .await;
}

#[tokio::test]
async fn explicit_changes_persist_and_cancel_an_older_lookup() {
    bounded(async {
        let dir = tempfile::tempdir().expect("create station directory");
        let tower = loopback_tower().await;
        let url = checked(numbers::canonical_tower_url(tower.url()), "canonicalize loopback tower");
        let typed = tower.url().replacen("http://", "HTTP://", 1);
        let (settings, mut lookups) = config(dir.path(), TowerInput::Domain(DOMAIN.into()));
        let (station, chief) = start(settings).await;
        let lookup = next_lookup(&mut lookups, DOMAIN).await;

        let added = checked(chief.tower_add(&typed).await, "add explicit tower");
        assert_eq!(added.outcome, TowerAddOutcome::Added);
        assert_eq!(added.tower.url, url);
        assert_eq!(added.tower.source, Some(TowerSource::Configured));
        assert!(lookup.reply.is_closed(), "explicit add cancels the older lookup");
        wait_online(&chief, &url, TowerSource::Configured).await;
        let saved = read_record(dir.path());
        assert_eq!(
            decode(&saved),
            disk_record(DOMAIN, json!([{"url": url, "qad_port": null, "source": "configured"}])),
        );
        let file = dir.path().join(TOWER_FILE);
        let inode = checked(fs::metadata(&file), "read record inode").ino();

        let mut events = checked(chief.subscribe().await, "subscribe before no-op mutations");
        let duplicate = checked(chief.tower_add(&typed).await, "add installed tower again");
        assert_eq!(duplicate.outcome, TowerAddOutcome::AlreadyInSet);
        assert_eq!(duplicate.tower.url, url);
        assert_eq!(duplicate.tower.source, Some(TowerSource::Configured));
        let absent = checked(
            chief.tower_remove("https://ABSENT.synthetic.invalid:443").await,
            "remove absent tower",
        );
        assert_eq!(absent.outcome, TowerRemoveOutcome::NotInSet);
        assert_eq!(absent.url, "https://absent.synthetic.invalid/");
        let status = checked(chief.status().await, "read tower status after no-op mutations");
        assert_eq!(status.towers, vec![duplicate.tower]);
        assert_no_queued_event(&mut events).await;
        assert_eq!(read_record(dir.path()), saved);
        assert_eq!(checked(fs::metadata(&file), "read unchanged record inode").ino(), inode);
        stop(station).await;

        let (settings, mut lookups) = config(dir.path(), TowerInput::Remembered);
        let (station, chief) = start(settings).await;
        wait_online(&chief, &url, TowerSource::Remembered).await;
        assert!(lookups.try_recv().is_err());
        assert_eq!(read_record(dir.path()), saved);
        let mut events = checked(chief.subscribe().await, "subscribe before removing last tower");
        let removed = checked(chief.tower_remove(&typed).await, "remove last tower");
        assert_eq!(removed.outcome, TowerRemoveOutcome::Removed);
        assert_eq!(removed.url, url);
        assert!(matches!(events.next().await, Some(Event::Status { status }) if status.towers.is_empty()));
        assert!(checked(chief.status().await, "read empty installed set").towers.is_empty());
        assert_eq!(decode(&read_record(dir.path())), disk_record(DOMAIN, json!([])));
        stop(station).await;
        checked(tower.shutdown().await, "shut down loopback tower");
    })
    .await;
}

#[tokio::test]
async fn the_first_no_op_after_restart_confirms_the_remembered_tower_record() {
    bounded(async {
        let dir = tempfile::tempdir().expect("create station directory");
        let tower = loopback_tower().await;
        let entry = checked(TowerEntry::from_url(tower.url()), "build locator result");
        let url = entry.url().to_owned();
        let (settings, mut lookups) = config(dir.path(), TowerInput::Domain(DOMAIN.into()));
        let (station, chief) = start(settings).await;
        let lookup = next_lookup(&mut lookups, DOMAIN).await;
        assert!(lookup.reply.send(Ok(vec![entry])).is_ok());
        wait_online(&chief, &url, TowerSource::Srv).await;
        stop(station).await;

        let file = dir.path().join(TOWER_FILE);
        let saved = read_record(dir.path());
        assert_eq!(
            decode(&saved),
            disk_record(DOMAIN, json!([{"url": url, "qad_port": null, "source": "srv"}])),
        );
        let published = checked(File::open(&file), "hold the original record open");
        let published_metadata = checked(published.metadata(), "stat the original record");
        let identity = |metadata: fs::Metadata| {
            (metadata.ino(), metadata.mtime(), metadata.mtime_nsec())
        };
        let on_disk = || identity(checked(fs::metadata(&file), "stat the current record"));
        let original = identity(published_metadata);

        let (settings, mut lookups) = config(dir.path(), TowerInput::Remembered);
        let (station, chief) = start(settings).await;
        wait_online(&chief, &url, TowerSource::Remembered).await;
        assert_eq!(on_disk(), original, "startup leaves the record in place");
        let mut events = checked(chief.subscribe().await, "subscribe before confirming the record");

        checked(
            fs::set_permissions(&file, fs::Permissions::from_mode(0o644)),
            "make the destination unsafe after loading it",
        );
        assert!(matches!(
            chief.tower_add(&url).await,
            Err(ApiError::Internal { detail })
                if detail == "could not use towers.json; it must be a regular file owned by this user with mode 0600"
        ), "the restarted actor cannot acknowledge a failed rewrite");
        assert_eq!(on_disk(), original);
        assert_eq!(read_record(dir.path()), saved);
        checked(
            fs::set_permissions(&file, fs::Permissions::from_mode(0o600)),
            "restore safe destination permissions",
        );

        let duplicate = checked(chief.tower_add(&url).await, "confirm the installed tower");
        assert_eq!(duplicate.outcome, TowerAddOutcome::AlreadyInSet);
        assert_eq!(duplicate.tower.source, Some(TowerSource::Remembered));
        let confirmed = checked(File::open(&file), "hold the confirmed record open");
        let rewritten = identity(checked(confirmed.metadata(), "stat the confirmed record"));
        assert_ne!(rewritten.0, original.0, "confirmation uses a checked replacement");
        assert_eq!(read_record(dir.path()), saved);

        let absent = checked(
            chief.tower_remove("https://absent.synthetic.invalid/").await,
            "remove an absent tower after confirmation",
        );
        assert_eq!(absent.outcome, TowerRemoveOutcome::NotInSet);
        assert_eq!(on_disk(), rewritten, "a confirmed record needs no second rewrite");
        assert_no_queued_event(&mut events).await;
        assert!(lookups.try_recv().is_err());
        stop(station).await;
        checked(tower.shutdown().await, "shut down loopback tower");
    })
    .await;
}

#[tokio::test]
async fn at_capacity_a_duplicate_succeeds_and_a_distinct_add_is_refused() {
    bounded(async {
        let dir = tempfile::tempdir().expect("create station directory");
        let (settings, mut lookups) = config(dir.path(), TowerInput::Domain(DOMAIN.into()));
        let (station, chief) = start(settings).await;
        let lookup = next_lookup(&mut lookups, DOMAIN).await;
        let listeners: Vec<_> = (0..=MAX_TOWERS)
            .map(|_| {
                checked(
                    TcpListener::bind((Ipv4Addr::LOCALHOST, 0)),
                    "reserve loopback port",
                )
            })
            .collect();
        let urls: Vec<_> = listeners
            .iter()
            .map(|listener| {
                format!(
                    "http://{}/",
                    checked(listener.local_addr(), "read reserved address")
                )
            })
            .collect();
        for url in &urls[..MAX_TOWERS] {
            assert_eq!(
                checked(chief.tower_add(url).await, "fill installed set").outcome,
                TowerAddOutcome::Added,
            );
        }
        assert!(lookup.reply.is_closed());
        assert_eq!(
            checked(
                chief.tower_add(&urls[0]).await,
                "add installed URL at capacity"
            )
            .outcome,
            TowerAddOutcome::AlreadyInSet,
        );
        assert_eq!(
            chief.tower_add(&urls[MAX_TOWERS]).await,
            Err(ApiError::TowerSetFull {
                max: MAX_TOWERS as u32
            }),
        );
        let status = checked(chief.status().await, "read full installed set");
        assert!(
            status
                .towers
                .iter()
                .map(|tower| &tower.url)
                .eq(&urls[..MAX_TOWERS])
        );
        stop(station).await;
    })
    .await;
}
