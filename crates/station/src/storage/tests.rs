use super::*;

use std::fs::{self, Permissions};
use std::future::ready;
use std::net::{Ipv4Addr, TcpListener};
use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};
use std::path::PathBuf;
use std::time::Duration;

use chief::api::{ApiError, Event};
use chief::client::{Client, Subscription};
use numbers::proto::CHAT_PROTOCOL;
use numbers::{Protocol, node_id};
use tempfile::TempDir;

use crate::{IpTransport, StartError, Station, StationConfig};

fn scratch() -> (TempDir, PathBuf) {
    let temp = tempfile::tempdir().expect("create scratch directory");
    let path = temp.path().join("station");
    (temp, path)
}

fn new_id() -> NodeId {
    node_id(&generate_node_key())
}

fn file_mode(path: &Path) -> u32 {
    fs::symlink_metadata(path).expect("stat fixture").mode() & 0o7777
}

fn write_file(path: &Path, bytes: &[u8], mode: u32) {
    fs::write(path, bytes).expect("write fixture");
    fs::set_permissions(path, Permissions::from_mode(mode)).expect("set fixture mode");
}

fn single_peer(name: &str, id: NodeId) -> PeerBook {
    PeerBook::from([(name.to_owned(), vec![id])])
}

fn disk_peer(name: &str, stations: &[NodeId]) -> DiskPeer {
    DiskPeer {
        name: name.to_owned(),
        stations: stations.iter().map(ToString::to_string).collect(),
    }
}

#[test]
fn lock_excludes_another_start_and_the_same_inode_is_reused() {
    let (temp, path) = scratch();
    let storage = Storage::lock(&path).expect("first lock");
    let lock_path = path.join(LOCK_FILE);
    let key_path = path.join(KEY_FILE);
    let inode = fs::metadata(&lock_path).expect("lock metadata").ino();
    assert_eq!(file_mode(&lock_path), 0o600);
    assert!(
        !key_path.exists(),
        "key creation is after acquiring the lock"
    );

    let alias = temp.path().join("alias");
    symlink(&path, &alias).expect("link directory alias");
    for contender in [&path, &alias] {
        assert!(matches!(
            Storage::lock(contender),
            Err(StorageError::AlreadyRunning)
        ));
    }
    assert!(!key_path.exists(), "failed starters never create the key");
    let key = storage.load_node_key().expect("create node key under lock");
    assert_eq!(file_mode(&key_path), 0o600);

    drop(storage);
    let reopened = Storage::lock(&alias).expect("lock after shutdown");
    assert_eq!(fs::metadata(lock_path).expect("lock metadata").ino(), inode);
    let same_key = reopened.load_node_key().expect("read existing node key");
    assert!(key == same_key, "existing key was preserved");
}

#[test]
fn startup_sync_runs_while_the_directory_is_locked_and_releases_the_lock_on_error() {
    let (_temp, path) = scratch();
    let result = Storage::lock_with_sync(&path, |dir| {
        let checked = dir.metadata().expect("stat the directory being synced");
        let expected = fs::metadata(&path).expect("stat the station directory");
        assert_eq!(
            (checked.dev(), checked.ino()),
            (expected.dev(), expected.ino())
        );
        assert!(matches!(
            Storage::lock(&path),
            Err(StorageError::AlreadyRunning)
        ));
        Err(io::Error::other("injected directory sync failure"))
    });
    let Err(error @ StorageError::SyncDataDir) = result else {
        panic!("startup must fail when the checked directory cannot be synced");
    };
    assert!(matches!(
        StartError::from(error),
        StartError::Storage { detail } if detail == "could not sync the station data directory"
    ));
    let _storage = Storage::lock(&path).expect("retry after the failed sync released the lock");
}

#[test]
fn retargeted_alias_cannot_change_the_directory_used_for_the_key() {
    let (temp, first) = scratch();
    let second = temp.path().join("other-station");
    let alias = temp.path().join("alias");
    ensure_private_dir(&first).expect("create first test directory");
    symlink(&first, &alias).expect("link the alias to the first directory");

    let first_storage = Storage::lock(&alias).expect("lock the first directory by alias");
    let first_key_path = first.join(KEY_FILE);
    assert!(!first_key_path.exists(), "first directory has no key yet");
    let second_storage = Storage::lock(&second).expect("lock the second directory");
    let second_key = second_storage
        .load_node_key()
        .expect("create the second key");

    fs::remove_file(&alias).expect("remove old alias");
    symlink(&second, &alias).expect("retarget the alias to the second directory");
    for locked_path in [&first, &alias] {
        assert!(matches!(
            Storage::lock(locked_path),
            Err(StorageError::AlreadyRunning)
        ));
    }

    let first_key = first_storage
        .load_node_key()
        .expect("create a key in the locked directory");
    assert!(
        first_key != second_key,
        "one lock cannot load another directory's key"
    );
    assert_eq!(file_mode(&first_key_path), 0o600);
    assert!(
        fs::read(first_key_path).expect("read the first key file") == first_key,
        "first key was written in its locked directory"
    );
    let reloaded_first = first_storage.load_node_key().expect("reload the first key");
    let reloaded_second = second_storage
        .load_node_key()
        .expect("reload the second key");
    assert!(
        reloaded_first == first_key,
        "retained lock still opens the first key"
    );
    assert!(
        reloaded_second == second_key,
        "the second key was preserved"
    );
}

#[test]
fn private_directory_and_lock_entries_are_checked() {
    let (_temp, path) = scratch();
    fs::create_dir(&path).expect("create fixture directory");
    fs::set_permissions(&path, Permissions::from_mode(0o755)).expect("set public mode");
    assert!(matches!(Storage::lock(&path), Err(StorageError::DataDir)));
    fs::set_permissions(&path, Permissions::from_mode(0o700)).expect("set private mode");

    let lock_path = path.join(LOCK_FILE);
    write_file(&lock_path, b"", 0o644);
    assert!(matches!(Storage::lock(&path), Err(StorageError::LockFile)));
    assert_eq!(file_mode(&lock_path), 0o644);
    fs::remove_file(&lock_path).expect("remove fixture lock");
    fs::create_dir(&lock_path).expect("create directory in place of lock");
    assert!(matches!(Storage::lock(&path), Err(StorageError::LockFile)));
    fs::remove_dir(&lock_path).expect("remove fixture lock directory");

    let target = path.join("target");
    write_file(&target, b"untouched", 0o600);
    symlink(&target, &lock_path).expect("link lock fixture");
    assert!(matches!(Storage::lock(&path), Err(StorageError::LockFile)));
    assert!(lock_path.is_symlink());
    assert!(fs::read(&target).expect("read fixture target") == b"untouched");
}

#[test]
fn private_file_validation_checks_the_owner_and_sets_mode_by_descriptor() {
    let (_temp, path) = scratch();
    let storage = Storage::lock(&path).expect("lock fixture");
    let fd = rustix::fs::openat(
        &storage.dir,
        "narrow-mode",
        OFlags::WRONLY | OPEN_PRIVATE | OFlags::CREATE | OFlags::EXCL,
        Mode::empty(),
    )
    .expect("create owner file without permission bits");
    let file = File::from(fd);
    assert_eq!(file_mode(&path.join("narrow-mode")), 0);
    set_private_mode(&file).expect("set permissions through the owned descriptor");
    assert_eq!(file_mode(&path.join("narrow-mode")), 0o600);

    let mut stat = rustix::fs::fstat(&file).expect("stat private fixture");
    stat.st_uid = stat.st_uid.wrapping_add(1);
    assert!(
        check_private_stat(&stat).is_err(),
        "wrong owner must be refused"
    );
}

#[test]
fn a_different_owner_is_refused_at_the_entry_and_descriptor() {
    use rustix::process::{Uid, geteuid};

    let (_temp, path) = scratch();
    let storage = Storage::lock(&path).expect("lock fixture");
    let own = new_id();
    let old = single_peer("alice", new_id());
    storage.save_peers(&old, own).expect("save old peer book");
    let peer_path = path.join(PEER_FILE);
    let original_bytes = fs::read(&peer_path).expect("read the original peer book");
    let mut file = File::open(&peer_path).expect("open peer book before changing owner");
    let other_owner = if geteuid().is_root() {
        Uid::from_raw(1)
    } else {
        Uid::ROOT
    };
    match rustix::fs::fchown(&file, Some(other_owner), None) {
        Ok(()) => {}
        // A user namespace can make the alternate UID unavailable even to its root user.
        Err(error @ (Errno::PERM | Errno::ACCESS | Errno::INVAL)) => {
            writeln!(
                io::stderr().lock(),
                "skipped storage ownership checks: could not change fixture owner ({error})"
            )
            .expect("report skipped ownership checks");
            return;
        }
        Err(error) => panic!("could not change fixture owner: {error}"),
    }

    let original = file.metadata().expect("stat the foreign-owned fixture");
    assert_eq!(original.uid(), other_owner.as_raw());
    assert_eq!(file_mode(&peer_path), 0o600);
    assert_eq!(
        check_private_file(&file)
            .expect_err("an open descriptor with the wrong owner must be refused")
            .kind(),
        io::ErrorKind::PermissionDenied
    );
    assert!(matches!(
        storage.load_peers(own),
        Err(StorageError::PrivateRecord(RecordFile::Peers))
    ));

    let next = single_peer("bob", new_id());
    assert!(
        matches!(
            storage.save_peers(&next, own),
            Err(StorageError::PrivateRecord(RecordFile::Peers))
        ),
        "a directory entry with the wrong owner must be refused before replacement"
    );
    let retained = fs::metadata(peer_path).expect("stat unchanged peer book");
    assert_eq!(retained.ino(), original.ino());
    assert_eq!(retained.uid(), original.uid());
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).expect("read retained fixture");
    assert!(
        bytes == original_bytes,
        "the foreign-owned record was modified"
    );
    assert_eq!(fs::read_dir(path).expect("list fixtures").count(), 2);
}

#[test]
fn damaged_or_symlinked_key_is_never_replaced() {
    let (_temp, path) = scratch();
    let storage = Storage::lock(&path).expect("lock fixture");
    let key_path = path.join(KEY_FILE);
    write_file(&key_path, b"invalid length", 0o600);
    assert!(matches!(
        storage.load_node_key(),
        Err(StorageError::NodeKey)
    ));
    assert!(fs::read(&key_path).expect("read fixture key file") == b"invalid length");

    fs::remove_file(&key_path).expect("remove fixture key file");
    symlink(path.join("missing"), &key_path).expect("link key fixture");
    assert!(matches!(
        storage.load_node_key(),
        Err(StorageError::NodeKey)
    ));
    assert!(key_path.is_symlink());
    assert!(!path.join("missing").exists());
}

#[test]
fn rejects_malformed_duplicate_empty_and_self_entries() {
    let (_temp, path) = scratch();
    let storage = Storage::lock(&path).expect("lock fixture");
    let own = new_id();
    let first = new_id();
    let second = new_id();
    let peer_path = path.join(PEER_FILE);
    assert!(storage.load_peers(own).expect("absent peer book").is_none());

    for invalid in [
        b"not json".as_slice(),
        br#"{"version":1,"version":1,"peers":[]}"#,
    ] {
        write_file(&peer_path, invalid, 0o600);
        assert!(matches!(
            storage.load_peers(own),
            Err(StorageError::InvalidPeers(_))
        ));
    }

    let cases = [
        (vec![disk_peer("bad name", &[first])], "invalid peer name"),
        (vec![disk_peer("alice", &[])], "peer has no stations"),
        (
            vec![disk_peer("alice", &[own])],
            "station lists its own ID as a peer",
        ),
        (
            vec![disk_peer("alice", &[first]), disk_peer("alice", &[second])],
            "duplicate peer name",
        ),
        (
            vec![disk_peer("alice", &[first, first])],
            "duplicate station ID",
        ),
        (
            vec![disk_peer("alice", &[first]), disk_peer("bob", &[first])],
            "duplicate station ID",
        ),
        (
            vec![DiskPeer {
                name: "alice".to_owned(),
                stations: vec!["invalid".to_owned()],
            }],
            "invalid station ID",
        ),
    ];
    for (peers, expected) in cases {
        let bytes = serde_json::to_vec(&DiskBook {
            version: PEER_FILE_VERSION,
            peers,
        })
        .expect("encode fixture book");
        write_file(&peer_path, &bytes, 0o600);
        assert!(
            matches!(storage.load_peers(own), Err(StorageError::InvalidPeers(reason)) if reason == expected)
        );
    }

    write_file(&peer_path, br#"{"version":2,"peers":[]}"#, 0o600);
    assert!(matches!(
        storage.load_peers(own),
        Err(StorageError::InvalidPeers("unsupported version"))
    ));
    write_file(&peer_path, &vec![b' '; PEER_FILE_MAX_BYTES + 1], 0o600);
    assert!(matches!(
        storage.load_peers(own),
        Err(StorageError::RecordTooLarge(RecordFile::Peers))
    ));
}

#[test]
fn peer_book_round_trips_in_canonical_order_and_replaces_atomically() {
    let (_temp, path) = scratch();
    let storage = Storage::lock(&path).expect("lock fixture");
    let own = new_id();
    let mut stations = [new_id(), new_id()];
    stations.sort_unstable_by(|left, right| right.cmp(left));
    let [first, second] = stations;
    let old = PeerBook::from([
        ("alice".to_owned(), stations.to_vec()),
        ("bob".to_owned(), vec![new_id()]),
    ]);
    let next = single_peer("bob", first);
    let peer_path = path.join(PEER_FILE);

    assert_eq!(
        storage.save_peers(&old, own).expect("save initial book"),
        SaveOutcome::Durable
    );
    assert_eq!(file_mode(&peer_path), 0o600);
    assert!(storage.load_peers(own).expect("load peer book").as_ref() == Some(&old));
    let mut opened = File::open(&peer_path).expect("hold old peer book open");
    assert_eq!(
        storage.save_peers(&next, own).expect("replace peer book"),
        SaveOutcome::Durable
    );
    assert!(storage.load_peers(own).expect("load replacement").as_ref() == Some(&next));

    let mut contents = Vec::new();
    opened
        .read_to_end(&mut contents)
        .expect("read old open descriptor");
    let disk: DiskBook = serde_json::from_slice(&contents).expect("read old disk schema");
    assert_eq!(disk.version, PEER_FILE_VERSION);
    assert_eq!(disk.peers[0].name, "alice");
    assert!(disk.peers[0].stations == [first.to_string(), second.to_string()]);
    assert!(disk.into_book(own).expect("read old version") == old);
    assert_eq!(fs::read_dir(path).expect("list fixtures").count(), 2);
}

#[test]
fn oversized_peer_book_is_refused_without_replacing_the_committed_file() {
    let (_temp, path) = scratch();
    let storage = Storage::lock(&path).expect("lock fixture");
    let own = new_id();
    let old = single_peer("alice", new_id());
    storage.save_peers(&old, own).expect("save old peer book");
    let peer_path = path.join(PEER_FILE);
    let committed = File::open(&peer_path).expect("hold committed peer book");
    let committed_inode = committed.metadata().expect("read committed inode").ino();
    let committed_bytes = fs::read(&peer_path).expect("read original peer book");

    let limit = 256 * 1024;
    let station_bytes = new_id().to_string().len() + 3; // JSON quotes and a comma.
    let stations = (0..=limit / station_bytes).map(|_| new_id()).collect();
    let next = PeerBook::from([("bob".to_owned(), stations)]);
    let bytes = serde_json::to_vec(&DiskBook::from_book(&next)).expect("encode oversized book");
    assert!((limit + 1..=limit + 128).contains(&bytes.len()));

    assert!(matches!(
        storage.save_peers(&next, own),
        Err(StorageError::RecordTooLarge(RecordFile::Peers))
    ));
    assert_eq!(
        fs::metadata(&peer_path)
            .expect("read unchanged inode")
            .ino(),
        committed_inode
    );
    assert!(
        fs::read(peer_path).expect("read retained peer book") == committed_bytes,
        "the refused save must leave the original bytes unchanged"
    );
    assert_eq!(fs::read_dir(path).expect("list fixtures").count(), 2);
}

#[test]
fn peer_storage_errors_have_fixed_diagnostics() {
    let peers = RecordFile::Peers;
    for (error, expected) in [
        (
            StorageError::DataDir,
            "could not use the station data directory; it must be private and owned by this user",
        ),
        (
            StorageError::LockFile,
            "could not open the station lock; it must be a regular file owned by this user with mode 0600",
        ),
        (
            StorageError::AlreadyRunning,
            "another station is already running with this data directory",
        ),
        (
            StorageError::TakeLock,
            "could not take the station data-directory lock",
        ),
        (
            StorageError::SyncDataDir,
            "could not sync the station data directory",
        ),
        (
            StorageError::NodeKey,
            "could not load or create node.key; check its owner, permissions and validity",
        ),
        (
            StorageError::PrivateRecord(peers),
            "could not use peers.json; it must be a regular file owned by this user with mode 0600",
        ),
        (StorageError::ReadRecord(peers), "could not read peers.json"),
        (
            StorageError::InvalidPeers("unsupported version"),
            "invalid peers.json: unsupported version",
        ),
        (
            StorageError::RecordTooLarge(peers),
            "peer book exceeds the 256 KiB limit",
        ),
        (
            StorageError::StageRecord(peers),
            "could not create a private staging file for peers.json",
        ),
        (
            StorageError::StagingNamesTaken(peers),
            "all staging names for peers.json are taken",
        ),
        (
            StorageError::WriteRecord(peers),
            "could not write or sync the staged peer book",
        ),
        (
            StorageError::ReplaceRecord(peers),
            "could not replace peers.json",
        ),
    ] {
        assert_eq!(error.to_string(), expected);
    }
}

#[test]
fn unsafe_targets_are_not_replaced_and_staging_file_is_cleaned() {
    let (_temp, path) = scratch();
    let storage = Storage::lock(&path).expect("lock fixture");
    let own = new_id();
    let book = single_peer("alice", new_id());
    let peer_path = path.join(PEER_FILE);

    write_file(&peer_path, b"keep", 0o644);
    assert!(matches!(
        storage.load_peers(own),
        Err(StorageError::PrivateRecord(RecordFile::Peers))
    ));
    assert!(matches!(
        storage.save_peers(&book, own),
        Err(StorageError::PrivateRecord(RecordFile::Peers))
    ));
    assert!(fs::read(&peer_path).expect("read fixture") == b"keep");
    assert_eq!(fs::read_dir(&path).expect("list fixtures").count(), 2);
    fs::remove_file(&peer_path).expect("remove fixture");

    fs::create_dir(&peer_path).expect("create directory in place of peer book");
    assert!(matches!(
        storage.load_peers(own),
        Err(StorageError::PrivateRecord(RecordFile::Peers))
    ));
    assert!(matches!(
        storage.save_peers(&book, own),
        Err(StorageError::PrivateRecord(RecordFile::Peers))
    ));
    assert!(peer_path.is_dir());
    assert_eq!(fs::read_dir(&path).expect("list fixtures").count(), 2);
    fs::remove_dir(&peer_path).expect("remove fixture directory");

    let target = path.join("target");
    write_file(&target, b"keep", 0o600);
    symlink(&target, &peer_path).expect("link peer book fixture");
    assert!(matches!(
        storage.load_peers(own),
        Err(StorageError::PrivateRecord(RecordFile::Peers))
    ));
    assert!(matches!(
        storage.save_peers(&book, own),
        Err(StorageError::PrivateRecord(RecordFile::Peers))
    ));
    assert!(peer_path.is_symlink());
    assert!(fs::read(&target).expect("read fixture target") == b"keep");
    assert_eq!(fs::read_dir(path).expect("list fixtures").count(), 3);
}

#[test]
fn production_staging_suffixes_are_fresh_per_draw() {
    let first = random_staging_suffix().expect("draw first suffix");
    let second = random_staging_suffix().expect("draw second suffix");
    assert!(first != second, "production staging suffixes must differ");
}

#[test]
fn all_legacy_staging_names_can_be_occupied_without_blocking_a_save() {
    let (_temp, path) = scratch();
    let storage = Storage::lock(&path).expect("lock fixture");
    let own = new_id();
    let book = single_peer("alice", new_id());
    let legacy_path = |index| path.join(format!(".{PEER_FILE}.{}.{index}.tmp", std::process::id()));
    for index in 0..STAGING_ATTEMPTS {
        write_file(&legacy_path(index), &[index], 0o600);
    }

    assert_eq!(
        storage
            .save_peers(&book, own)
            .expect("save with legacy stages present"),
        SaveOutcome::Durable
    );
    assert!(
        storage
            .load_peers(own)
            .expect("load committed peer book")
            .as_ref()
            == Some(&book)
    );
    for index in 0..STAGING_ATTEMPTS {
        assert!(fs::read(legacy_path(index)).expect("read the legacy stage") == [index]);
    }
    assert_eq!(
        fs::read_dir(path).expect("list fixtures").count(),
        usize::from(STAGING_ATTEMPTS) + 2
    );
}

#[test]
fn a_colliding_name_is_preserved_and_the_next_draw_commits() {
    let (_temp, path) = scratch();
    let storage = Storage::lock(&path).expect("lock fixture");
    let own = new_id();
    let book = single_peer("alice", new_id());
    let bytes = serde_json::to_vec(&DiskBook::from_book(&book)).expect("encode fixture book");
    let collision_suffix = 0x0011_2233_4455_6677_8899_aabb_ccdd_eeff;
    let collision_name = ".peers.json.00112233445566778899aabbccddeeff.tmp";
    assert_eq!(
        staging_name(RecordFile::Peers, collision_suffix),
        collision_name,
        "staging name format"
    );
    let collision = path.join(collision_name);
    write_file(&collision, b"keep", 0o644);
    let mut suffixes = [collision_suffix, 2].into_iter();

    assert_eq!(
        storage
            .replace_record_with_suffixes(
                RecordFile::Peers,
                &bytes,
                || Ok(suffixes.next().expect("exactly two draws")),
                File::sync_all,
            )
            .expect("commit under a fresh staging name"),
        SaveOutcome::Durable
    );
    assert!(
        suffixes.next().is_none(),
        "the first staging draw must collide and the second must commit"
    );
    assert!(
        storage
            .load_peers(own)
            .expect("load replacement peer book")
            .as_ref()
            == Some(&book)
    );
    assert!(fs::read(&collision).expect("read the original entry") == b"keep");
    assert_eq!(file_mode(&collision), 0o644);
    assert_eq!(fs::read_dir(&path).expect("list fixtures").count(), 3);
}

#[test]
fn sixteen_collisions_leave_the_book_and_all_stages_unchanged() {
    let (_temp, path) = scratch();
    let storage = Storage::lock(&path).expect("lock fixture");
    let own = new_id();
    let old = single_peer("alice", new_id());
    storage.save_peers(&old, own).expect("save old peer book");
    let peer_path = path.join(PEER_FILE);
    let committed = File::open(&peer_path).expect("hold committed peer book");
    let committed_inode = committed.metadata().expect("read committed inode").ino();
    let next = single_peer("bob", new_id());
    let bytes = serde_json::to_vec(&DiskBook::from_book(&next)).expect("encode replacement book");

    for index in 0..STAGING_ATTEMPTS {
        write_file(
            &path.join(staging_name(RecordFile::Peers, u128::from(index))),
            &[index],
            0o600,
        );
    }
    let mut suffixes = 0..u128::from(STAGING_ATTEMPTS);
    assert!(matches!(
        storage.replace_record_with_suffixes(
            RecordFile::Peers,
            &bytes,
            || Ok(suffixes.next().expect("draw no more than sixteen names")),
            |_| panic!("an exhausted save cannot reach directory sync"),
        ),
        Err(StorageError::StagingNamesTaken(RecordFile::Peers))
    ));
    assert!(suffixes.next().is_none());
    assert!(storage.load_peers(own).expect("load old book").as_ref() == Some(&old));
    assert_eq!(
        fs::metadata(peer_path).expect("read unchanged inode").ino(),
        committed_inode
    );
    for index in 0..STAGING_ATTEMPTS {
        assert!(
            fs::read(path.join(staging_name(RecordFile::Peers, u128::from(index))))
                .expect("read the original stage")
                == [index]
        );
    }
    assert_eq!(
        fs::read_dir(path).expect("list fixtures").count(),
        usize::from(STAGING_ATTEMPTS) + 2
    );
}

#[test]
fn entropy_failure_after_a_collision_preserves_the_existing_files() {
    let (_temp, path) = scratch();
    let storage = Storage::lock(&path).expect("lock fixture");
    let own = new_id();
    let old = single_peer("alice", new_id());
    storage.save_peers(&old, own).expect("save old peer book");
    let peer_path = path.join(PEER_FILE);
    let committed = File::open(&peer_path).expect("hold committed peer book");
    let committed_inode = committed.metadata().expect("read committed inode").ino();
    let next = single_peer("bob", new_id());
    let bytes = serde_json::to_vec(&DiskBook::from_book(&next)).expect("encode replacement book");

    let target = path.join("target");
    write_file(&target, b"keep", 0o600);
    let collision = path.join(staging_name(RecordFile::Peers, 1));
    symlink(&target, &collision).expect("link an existing stage name");
    let mut suffixes = [Ok(1), Err(getrandom::Error::UNSUPPORTED)].into_iter();
    assert!(matches!(
        storage.replace_record_with_suffixes(
            RecordFile::Peers,
            &bytes,
            || suffixes.next().expect("stop drawing after entropy failure"),
            |_| panic!("entropy failure cannot reach directory sync"),
        ),
        Err(StorageError::StageRecord(RecordFile::Peers))
    ));
    assert!(suffixes.next().is_none());
    assert!(storage.load_peers(own).expect("load old book").as_ref() == Some(&old));
    assert_eq!(
        fs::metadata(peer_path).expect("read unchanged inode").ino(),
        committed_inode
    );
    assert!(collision.is_symlink());
    assert!(fs::read(target).expect("read the symlink target") == b"keep");
    assert_eq!(fs::read_dir(path).expect("list fixtures").count(), 4);
}

#[test]
fn validation_failure_preserves_old_book_and_directory_sync_failure_commits() {
    let (_temp, path) = scratch();
    let storage = Storage::lock(&path).expect("lock fixture");
    let own = new_id();
    let old = single_peer("alice", new_id());
    storage.save_peers(&old, own).expect("save old book");
    let invalid = single_peer("alice", own);
    assert!(matches!(
        storage.save_peers(&invalid, own),
        Err(StorageError::InvalidPeers(_))
    ));
    assert!(
        storage
            .load_peers(own)
            .expect("load old book after rejection")
            .as_ref()
            == Some(&old)
    );

    let next = single_peer("bob", new_id());
    let bytes = serde_json::to_vec(&DiskBook::from_book(&next)).expect("encode next book");
    let outcome = storage
        .replace_record(RecordFile::Peers, &bytes, |_| {
            Err(io::ErrorKind::Other.into())
        })
        .expect("rename committed");
    assert_eq!(outcome, SaveOutcome::DurabilityUncertain);
    assert!(
        storage
            .load_peers(own)
            .expect("load committed replacement")
            .as_ref()
            == Some(&next)
    );
    assert_eq!(fs::read_dir(path).expect("list fixtures").count(), 2);
}

fn offline_config(path: &Path) -> (StationConfig, TcpListener) {
    let reservation = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("reserve loopback port");
    let address = reservation.local_addr().expect("read loopback port");
    let mut config = StationConfig::new(path, format!("http://{address}/"));
    config.ip_transport = IpTransport::RelayOnly;
    (config, reservation)
}

fn peer_file_inode(path: &Path) -> u64 {
    fs::metadata(path).expect("stat peer book").ino()
}

async fn assert_no_peer_event(events: &mut Subscription) {
    loop {
        let event = tokio::select! {
            biased;
            event = events.next() => event,
            () = ready(()) => return,
        };
        assert!(
            matches!(event, Some(Event::Status { .. })),
            "no peer event was published"
        );
    }
}

const CASE_TIMEOUT: Duration = Duration::from_secs(60);

#[tokio::test]
async fn station_start_refuses_a_failed_directory_sync_before_creating_the_key() {
    let (_temp, path) = scratch();
    let (config, _reservation) = offline_config(&path);
    tokio::time::timeout(CASE_TIMEOUT, async {
        FAIL_NEXT_STARTUP_SYNC.set(true);
        STARTUP_SYNCS.set(0);
        let result = Station::start(config.clone()).await;
        assert!(matches!(
            result,
            Err(StartError::Storage { detail }) if detail == "could not sync the station data directory"
        ), "Station::start must refuse a failed startup directory sync");
        assert!(!FAIL_NEXT_STARTUP_SYNC.get());
        assert_eq!(STARTUP_SYNCS.get(), 0);
        assert!(!path.join(KEY_FILE).exists());

        let station = Station::start(config).await.expect("retry after failed sync");
        assert_eq!(STARTUP_SYNCS.get(), 1);
        assert!(path.join(KEY_FILE).is_file());
        station.shutdown().await.expect("shut down after retry");
    })
    .await
    .expect("startup sync test timed out");
}

#[tokio::test]
async fn the_first_duplicate_add_after_restart_requires_a_durable_rewrite() {
    let (_temp, path) = scratch();
    let storage = Storage::lock(&path).expect("lock first station");
    let own = node_id(&storage.load_node_key().expect("load first station key"));
    let remote = new_id();
    let book = single_peer("alice", remote);
    storage.fail_next_peer_sync();
    assert_eq!(
        storage.save_peers(&book, own).expect("publish peer book"),
        SaveOutcome::DurabilityUncertain
    );
    let book_path = path.join(PEER_FILE);
    let published = File::open(&book_path).expect("hold published peer book open");
    let published_inode = published
        .metadata()
        .expect("stat published peer book")
        .ino();
    let contents = fs::read(&book_path).expect("read published peer book");
    drop(storage);

    let (config, _reservation) = offline_config(&path);
    tokio::time::timeout(CASE_TIMEOUT, async {
        STARTUP_SYNCS.set(0);
        let station = Station::start(config).await.expect("restart station");
        assert_eq!(STARTUP_SYNCS.get(), 1);
        assert_eq!(peer_file_inode(&book_path), published_inode);
        assert!(
            station
                .authorizer
                .authorize(&remote, Protocol::Stream(CHAT_PROTOCOL))
                .as_deref()
                == Some("alice"),
            "a peer loaded at startup must be authorized for chat"
        );
        let chief = Client::new(station.handle());
        let mut events = chief.subscribe().await.expect("subscribe to peer events");
        let remote_id = remote.to_string();
        fs::set_permissions(&book_path, Permissions::from_mode(0o644))
            .expect("make destination unsafe after loading the book");
        assert!(matches!(
            chief.peer_add("alice", &remote_id).await,
            Err(ApiError::Internal { detail })
                if detail == "could not use peers.json; it must be a regular file owned by this user with mode 0600"
        ), "the restarted actor cannot acknowledge a failed rewrite");
        assert_eq!(peer_file_inode(&book_path), published_inode);
        fs::set_permissions(&book_path, Permissions::from_mode(0o600))
            .expect("restore safe destination");

        let peer = chief
            .peer_add("alice", &remote_id)
            .await
            .unwrap_or_else(|_| panic!("duplicate add must succeed after the checked rewrite"));
        assert_eq!(peer.name, "alice");
        assert!(peer.stations.len() == 1 && peer.stations[0].station_id == remote_id);
        let rewritten = File::open(&book_path).expect("hold rewritten peer book open");
        let rewritten_inode = rewritten.metadata().expect("stat rewritten peer book").ino();
        assert_ne!(rewritten_inode, published_inode, "the first success follows a rewrite");
        assert!(fs::read(&book_path).expect("read confirmed peer book") == contents);
        assert!(chief.peer_add("alice", &remote_id).await.is_ok());
        assert_eq!(
            peer_file_inode(&book_path), rewritten_inode,
            "a confirmed book needs no second rewrite"
        );
        assert_no_peer_event(&mut events).await;
        station
            .shutdown()
            .await
            .expect("shut down restarted station");
    })
    .await
    .expect("station restarted and handled the duplicate add");
}

#[tokio::test]
async fn an_absent_remove_confirms_an_existing_empty_book_without_creating_a_missing_one() {
    tokio::time::timeout(CASE_TIMEOUT, async {
        for exists in [false, true] {
            let (_temp, path) = scratch();
            let book_path = path.join(PEER_FILE);
            let published = exists.then(|| {
                let storage = Storage::lock(&path).expect("lock for the empty book");
                assert_eq!(
                    storage
                        .save_peers(&PeerBook::new(), new_id())
                        .expect("save empty book"),
                    SaveOutcome::Durable
                );
                File::open(&book_path).expect("hold existing empty book open")
            });
            let (config, _reservation) = offline_config(&path);
            let station = Station::start(config).await.expect("start with no peers");
            let chief = Client::new(station.handle());
            let mut events = chief.subscribe().await.expect("subscribe to peer events");
            assert!(matches!(
                chief.peer_remove("absent").await,
                Err(ApiError::PeerNotFound { name }) if name == "absent"
            ));
            if let Some(mut published) = published {
                let original_inode = published
                    .metadata()
                    .expect("stat existing empty book")
                    .ino();
                assert_ne!(
                    peer_file_inode(&book_path),
                    original_inode,
                    "confirm the empty book"
                );
                let mut contents = Vec::new();
                published
                    .read_to_end(&mut contents)
                    .expect("read original empty book");
                assert!(fs::read(&book_path).expect("read confirmed empty book") == contents);
            } else {
                assert!(!book_path.exists());
            }
            assert_no_peer_event(&mut events).await;
            station.shutdown().await.expect("shut down with no peers");
        }
    })
    .await
    .expect("empty book confirmation test timed out");
}
