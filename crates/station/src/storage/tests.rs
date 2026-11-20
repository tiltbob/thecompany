use super::*;

use std::fs::{self, Permissions};
use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};
use std::path::PathBuf;

use numbers::{TowerEntry, node_id};
use serde_json::Value;
use tempfile::TempDir;

use crate::towers::TowerOrigin;

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
    assert!(
        storage
            .load_peers(own)
            .expect("absent peer book")
            .is_empty()
    );

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
    assert!(storage.load_peers(own).expect("load peer book") == old);
    let mut opened = File::open(&peer_path).expect("hold old peer book open");
    assert_eq!(
        storage.save_peers(&next, own).expect("replace peer book"),
        SaveOutcome::Durable
    );
    assert!(storage.load_peers(own).expect("load replacement") == next);

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
fn staging_name_collisions_preserve_every_preexisting_entry() {
    let (_temp, path) = scratch();
    let storage = Storage::lock(&path).expect("lock fixture");
    let own = new_id();
    let book = single_peer("alice", new_id());
    let stale = path.join(staging_name(RecordFile::Peers, 0));
    write_file(&stale, b"stale", 0o600);
    storage
        .save_peers(&book, own)
        .expect("skip stale stage name");
    assert!(fs::read(&stale).expect("read stale fixture") == b"stale");
    assert_eq!(fs::read_dir(&path).expect("list fixtures").count(), 3);

    for index in 1..STAGING_ATTEMPTS {
        write_file(
            &path.join(staging_name(RecordFile::Peers, index)),
            b"stale",
            0o600,
        );
    }
    let next = single_peer("bob", new_id());
    assert!(matches!(
        storage.save_peers(&next, own),
        Err(StorageError::StagingNamesTaken(RecordFile::Peers))
    ));
    assert!(storage.load_peers(own).expect("load old book") == book);
    for index in 0..STAGING_ATTEMPTS {
        assert!(
            fs::read(path.join(staging_name(RecordFile::Peers, index)))
                .expect("read existing stage fixture")
                == b"stale"
        );
    }
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
            == old
    );

    let next = single_peer("bob", new_id());
    let bytes = serde_json::to_vec(&DiskBook::from_book(&next)).expect("encode next book");
    let outcome = storage
        .replace_record(RecordFile::Peers, &bytes, |_| {
            Err(io::ErrorKind::Other.into())
        })
        .expect("rename committed");
    assert_eq!(outcome, SaveOutcome::DurabilityUncertain);
    assert!(storage.load_peers(own).expect("load committed replacement") == next);
    assert_eq!(fs::read_dir(path).expect("list fixtures").count(), 2);
}

fn tower_record(domain: &str) -> RememberedTowers {
    let entry = TowerEntry::from_url("http://127.0.0.1:3340").expect("make loopback tower fixture");
    RememberedTowers::new(domain, [(entry, TowerOrigin::Srv)]).expect("make tower record fixture")
}

#[test]
fn a_safe_corrupt_tower_record_can_be_replaced() {
    let (_temp, path) = scratch();
    let storage = Storage::lock(&path).expect("lock fixture");
    let tower_path = path.join(TOWER_FILE);
    assert!(
        storage
            .load_towers()
            .expect("read missing record")
            .is_none()
    );

    write_file(&tower_path, b"bad JSON", 0o600);
    assert!(matches!(
        storage.load_towers(),
        Err(StorageError::InvalidTowers(TowerRecordError::Malformed))
    ));
    let record = tower_record("First.Example.Invalid.");
    assert_eq!(
        storage.save_towers(&record).expect("replace safe record"),
        SaveOutcome::Durable
    );
    assert_eq!(file_mode(&tower_path), 0o600);
    let bytes = fs::read(&tower_path).expect("read saved record");
    let disk: Value = serde_json::from_slice(&bytes).expect("read v1 record");
    assert_eq!(disk["version"], 1);
    assert_eq!(disk["bootstrap_domain"], "first.example.invalid");
    let loaded = storage
        .load_towers()
        .expect("read saved record")
        .expect("saved record exists");
    assert_eq!(loaded.domain(), record.domain());
    assert!(loaded.entries().eq(record.entries()));
}

#[test]
fn tower_record_read_and_write_enforce_the_exact_size_limit() {
    let (_temp, path) = scratch();
    let storage = Storage::lock(&path).expect("lock fixture");
    let tower_path = path.join(TOWER_FILE);
    let record = tower_record("fixture.example.invalid");
    let mut bytes = record.encode().expect("encode fixture record");
    bytes.resize(TOWER_FILE_MAX_BYTES, b' ');
    assert_eq!(
        storage
            .replace_record(RecordFile::Towers, &bytes, File::sync_all)
            .expect("save tower record at the size limit"),
        SaveOutcome::Durable
    );
    assert!(matches!(storage.load_towers(), Ok(Some(_))));

    bytes.push(b' ');
    assert!(matches!(
        storage.replace_record(RecordFile::Towers, &bytes, |_| {
            panic!("oversize write must not reach directory sync")
        }),
        Err(StorageError::RecordTooLarge(RecordFile::Towers))
    ));
    assert!(fs::read(&tower_path).expect("read unchanged record") == bytes[..TOWER_FILE_MAX_BYTES]);
    assert_eq!(fs::read_dir(&path).expect("list fixtures").count(), 2);
    write_file(&tower_path, &bytes, 0o600);
    assert!(matches!(
        storage.load_towers(),
        Err(StorageError::RecordTooLarge(RecordFile::Towers))
    ));
}

#[test]
fn unsafe_tower_records_are_neither_read_nor_replaced() {
    let (_temp, path) = scratch();
    let storage = Storage::lock(&path).expect("lock fixture");
    let tower_path = path.join(TOWER_FILE);
    let record = tower_record("fixture.example.invalid");
    let assert_rejected = || {
        assert!(matches!(
            storage.load_towers(),
            Err(StorageError::PrivateRecord(RecordFile::Towers))
        ));
        assert!(matches!(
            storage.save_towers(&record),
            Err(StorageError::PrivateRecord(RecordFile::Towers))
        ));
    };

    write_file(&tower_path, b"keep", 0o644);
    assert_rejected();
    assert!(fs::read(&tower_path).expect("read original bytes") == b"keep");
    assert_eq!(file_mode(&tower_path), 0o644);
    fs::remove_file(&tower_path).expect("remove unsafe mode fixture");

    fs::create_dir(&tower_path).expect("create directory in place of tower record");
    assert_rejected();
    assert!(tower_path.is_dir());
    fs::remove_dir(&tower_path).expect("remove directory fixture");

    let target = path.join("target");
    write_file(&target, b"keep", 0o600);
    symlink(&target, &tower_path).expect("link tower fixture");
    assert_rejected();
    assert!(tower_path.is_symlink());
    assert!(fs::read(target).expect("read fixture target") == b"keep");
    assert_eq!(fs::read_dir(path).expect("list fixtures").count(), 3);
}

#[test]
fn tower_record_stays_with_the_locked_directory_when_an_alias_moves() {
    let (temp, first) = scratch();
    let second = temp.path().join("other-station");
    let alias = temp.path().join("alias");
    ensure_private_dir(&first).expect("create first fixture directory");
    symlink(&first, &alias).expect("link to first fixture directory");
    let first_storage = Storage::lock(&alias).expect("lock first directory through alias");
    let second_storage = Storage::lock(&second).expect("lock second directory");
    let second_record = tower_record("other.example.invalid");
    second_storage
        .save_towers(&second_record)
        .expect("save second fixture");

    fs::remove_file(&alias).expect("remove old alias");
    symlink(&second, &alias).expect("retarget alias to second directory");
    assert!(
        first_storage
            .load_towers()
            .expect("read first directory")
            .is_none()
    );
    let first_record = tower_record("first.example.invalid");
    first_storage
        .save_towers(&first_record)
        .expect("write through first directory handle");
    for (storage, record) in [
        (&first_storage, &first_record),
        (&second_storage, &second_record),
    ] {
        let saved = storage
            .load_towers()
            .expect("load from the locked directory")
            .expect("record exists");
        assert_eq!(saved.domain(), record.domain());
        assert!(saved.entries().eq(record.entries()));
    }
    assert_eq!(file_mode(&first.join(TOWER_FILE)), 0o600);
}

#[test]
fn tower_replacement_reports_uncertain_durability_only_after_rename() {
    let (_temp, path) = scratch();
    let storage = Storage::lock(&path).expect("lock fixture");
    let tower_path = path.join(TOWER_FILE);
    let old = tower_record("old.example.invalid");
    storage.save_towers(&old).expect("save original record");
    let original = fs::read(&tower_path).expect("read original record");
    let next = tower_record("next.example.invalid");
    let proposed = next.encode().expect("encode replacement");

    fs::set_permissions(&tower_path, Permissions::from_mode(0o644))
        .expect("make destination unsafe");
    assert!(matches!(
        storage.replace_record(RecordFile::Towers, &proposed, |_| {
            panic!("a failed replacement must not reach directory sync")
        }),
        Err(StorageError::PrivateRecord(RecordFile::Towers))
    ));
    assert!(fs::read(&tower_path).expect("read unchanged record") == original);
    fs::set_permissions(&tower_path, Permissions::from_mode(0o600))
        .expect("restore destination mode");

    let outcome = storage
        .replace_record(RecordFile::Towers, &proposed, |_| {
            assert!(fs::read(&tower_path).expect("read visible replacement") == proposed);
            Err(io::ErrorKind::Other.into())
        })
        .expect("rename committed");
    assert_eq!(outcome, SaveOutcome::DurabilityUncertain);
    assert!(fs::read(&tower_path).expect("read committed record") == proposed);
    assert_eq!(fs::read_dir(path).expect("list fixtures").count(), 2);
}

#[test]
fn peer_and_tower_staging_names_do_not_collide() {
    let (_temp, path) = scratch();
    let storage = Storage::lock(&path).expect("lock fixture");
    for index in 0..STAGING_ATTEMPTS {
        write_file(
            &path.join(staging_name(RecordFile::Peers, index)),
            b"keep",
            0o600,
        );
    }
    let stale = path.join(staging_name(RecordFile::Towers, 0));
    write_file(&stale, b"stale", 0o600);
    assert_eq!(
        storage
            .save_towers(&tower_record("example.invalid"))
            .expect("use the tower-specific namespace"),
        SaveOutcome::Durable
    );
    assert!(fs::read(stale).expect("read original stage fixture") == b"stale");
    for index in 0..STAGING_ATTEMPTS {
        assert!(
            fs::read(path.join(staging_name(RecordFile::Peers, index)))
                .expect("read existing peer stage fixture")
                == b"keep"
        );
    }
}
