use std::fs::{self, File, Permissions};
use std::future::ready;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::Path;

use numbers::link::LINK_ALPN;
use numbers::{Endpoint, EndpointConfig, generate_node_key, node_id};

use super::*;

mod delivery_deadline;

async fn queued_event(events: &mut Subscription) -> Option<Event> {
    tokio::select! {
        biased;
        next = events.next() => next,
        () = ready(()) => None,
    }
}

#[test]
fn empty_home_relay_observation_is_standby() {
    assert_eq!(tower_state(None), TowerState::Standby);
}

async fn actor_fixture(data_dir: &Path) -> (Actor, Link) {
    let storage = Storage::lock(data_dir).expect("lock actor test directory");
    let key = storage.load_node_key().expect("load actor key");
    let own_id = node_id(&key);
    let authorizer = Arc::new(Authorizer::new(&[LINK_ALPN], &[CHAT_PROTOCOL]));
    let endpoint = Endpoint::bind(
        &key,
        EndpointConfig {
            ip_transport: numbers::IpTransport::RelayOnly,
            ..EndpointConfig::default()
        },
        Arc::clone(&authorizer),
    )
    .await
    .unwrap_or_else(|_| panic!("bind actor fixture endpoint"));
    let budget = Duration::from_secs(2);
    let (link, _incoming) = Link::start(
        endpoint,
        numbers::link::LinkConfig {
            connect_timeout: budget,
            send_deadline: budget,
        },
    );
    let status = Status {
        station_id: own_id.to_string(),
        instance_id: "fixture".to_owned(),
        towers: Vec::new(),
        bootstrap_domain: None,
    };
    let actor = Actor::new(
        storage,
        own_id,
        PeerBook::new(),
        authorizer,
        status,
        budget,
        link.handle(),
    );
    (actor, link)
}

#[tokio::test]
async fn duplicate_peer_add_confirms_an_uncertain_book_without_another_event() {
    let dir = tempfile::tempdir().expect("create actor test directory");
    let data_dir = dir.path().join("station");
    let (mut actor, link) = actor_fixture(&data_dir).await;
    let own_id = actor.own_id;
    let (sink, mut events) = subscription(SUBSCRIPTION_CAPACITY);
    actor.sinks.push(sink);
    let remote = node_id(&generate_node_key());
    let remote_id = remote.to_string();
    let book_path = data_dir.join("peers.json");

    actor.storage.fail_next_peer_sync();
    assert!(matches!(
        actor.add_peer("peer", &remote_id),
        Err(ApiError::Internal { .. })
    ));
    assert!(actor.book_uncertain);
    let Some(Event::Peer { peer }) = queued_event(&mut events).await else {
        panic!("the initial visible add must publish a peer event");
    };
    let book = actor.book.clone();
    assert!(
        book.get("peer")
            .is_some_and(|stations| stations == &[remote])
    );
    assert!(
        actor
            .storage
            .load_peers(own_id)
            .expect("read visible peer book")
            == book
    );
    let original = File::open(&book_path).expect("hold the original peer book open");
    let original_inode = original.metadata().expect("read original inode").ino();

    // An unsafe destination makes replacement fail before rename. It must neither confirm the
    // book nor consume the one-shot sync failure meant for the next successful rename.
    actor.storage.fail_next_peer_sync();
    fs::set_permissions(&book_path, Permissions::from_mode(0o644))
        .expect("make destination unsafe");
    assert!(matches!(
        actor.add_peer("peer", &remote_id),
        Err(ApiError::Internal { .. })
    ));
    assert!(actor.book_uncertain);
    assert!(actor.book == book);
    fs::set_permissions(&book_path, Permissions::from_mode(0o600))
        .expect("restore destination mode");
    assert_eq!(
        fs::metadata(&book_path)
            .expect("read unchanged inode")
            .ino(),
        original_inode
    );
    assert!(queued_event(&mut events).await.is_none());

    assert!(matches!(
        actor.add_peer("peer", &remote_id),
        Err(ApiError::Internal { .. })
    ));
    assert!(actor.book_uncertain);
    assert!(
        actor
            .storage
            .load_peers(own_id)
            .expect("read uncertain rewrite")
            == book
    );
    let rewritten = File::open(&book_path).expect("hold the rewritten peer book open");
    let rewritten_inode = rewritten.metadata().expect("read rewritten inode").ino();
    assert_ne!(rewritten_inode, original_inode);
    assert!(queued_event(&mut events).await.is_none());

    assert!(
        matches!(actor.add_peer("peer", &remote_id), Ok(Response::Peer { peer: value }) if value == peer)
    );
    assert!(!actor.book_uncertain);
    assert!(
        actor
            .storage
            .load_peers(own_id)
            .expect("read confirmed peer book")
            == book
    );
    let confirmed_inode = fs::metadata(&book_path)
        .expect("read confirmed inode")
        .ino();
    assert_ne!(confirmed_inode, rewritten_inode);

    assert!(
        matches!(actor.add_peer("peer", &remote_id), Ok(Response::Peer { peer: value }) if value == peer)
    );
    assert_eq!(
        fs::metadata(&book_path)
            .expect("read clean duplicate inode")
            .ino(),
        confirmed_inode
    );
    assert!(queued_event(&mut events).await.is_none());
    link.shutdown().await.expect("shut down actor fixture link");
}

#[tokio::test]
async fn missing_peer_remove_confirms_an_uncertain_book_without_another_event() {
    let dir = tempfile::tempdir().expect("create actor test directory");
    let data_dir = dir.path().join("station");
    let book_path = data_dir.join("peers.json");
    let (mut actor, link) = actor_fixture(&data_dir).await;
    let own_id = actor.own_id;
    let removed = node_id(&generate_node_key());
    let retained = node_id(&generate_node_key());
    actor
        .add_peer("peer", &removed.to_string())
        .unwrap_or_else(|_| panic!("add peer to remove"));
    actor
        .add_peer("retained", &retained.to_string())
        .unwrap_or_else(|_| panic!("add peer to retain"));
    let (sink, mut events) = subscription(SUBSCRIPTION_CAPACITY);
    actor.sinks.push(sink);

    actor.storage.fail_next_peer_sync();
    assert!(matches!(
        actor.remove_peer("peer"),
        Err(ApiError::Internal { .. })
    ));
    assert!(actor.book_uncertain);
    assert!(matches!(
        queued_event(&mut events).await,
        Some(Event::PeerRemoved { name }) if name == "peer"
    ));
    let book = PeerBook::from([("retained".to_owned(), vec![retained])]);
    assert!(actor.book == book);
    assert_eq!(
        actor
            .authorizer
            .authorize(&removed, Protocol::Stream(CHAT_PROTOCOL)),
        None
    );
    assert!(actor.storage.load_peers(own_id).expect("read visible book") == book);
    let original = File::open(&book_path).expect("hold the visible peer book open");
    let original_inode = original.metadata().expect("read visible inode").ino();

    // Refuse the first replay before rename. The next replay should still hit the sync fault.
    actor.storage.fail_next_peer_sync();
    fs::set_permissions(&book_path, Permissions::from_mode(0o644))
        .expect("make destination unsafe");
    assert!(matches!(
        actor.remove_peer("peer"),
        Err(ApiError::Internal { .. })
    ));
    assert!(actor.book_uncertain);
    assert!(actor.book == book);
    fs::set_permissions(&book_path, Permissions::from_mode(0o600))
        .expect("restore destination mode");
    assert_eq!(
        fs::metadata(&book_path)
            .expect("read unchanged inode")
            .ino(),
        original_inode
    );

    assert!(matches!(
        actor.remove_peer("peer"),
        Err(ApiError::Internal { .. })
    ));
    assert!(actor.book_uncertain);
    assert!(
        actor
            .storage
            .load_peers(own_id)
            .expect("read rewritten book")
            == book
    );
    let rewritten = File::open(&book_path).expect("hold the rewritten peer book open");
    let rewritten_inode = rewritten.metadata().expect("read rewritten inode").ino();
    assert_ne!(rewritten_inode, original_inode);

    assert!(matches!(
        actor.remove_peer("peer"),
        Err(ApiError::PeerNotFound { name }) if name == "peer"
    ));
    assert!(!actor.book_uncertain);
    assert!(actor.book == book);
    assert!(
        actor
            .storage
            .load_peers(own_id)
            .expect("read confirmed book")
            == book
    );
    let confirmed_inode = fs::metadata(&book_path)
        .expect("read confirmed inode")
        .ino();
    assert_ne!(confirmed_inode, rewritten_inode);

    assert!(matches!(
        actor.remove_peer("peer"),
        Err(ApiError::PeerNotFound { name }) if name == "peer"
    ));
    assert_eq!(
        fs::metadata(&book_path)
            .expect("read clean missing-remove inode")
            .ino(),
        confirmed_inode
    );
    assert_eq!(
        actor
            .authorizer
            .authorize(&removed, Protocol::Stream(CHAT_PROTOCOL)),
        None
    );
    assert!(queued_event(&mut events).await.is_none());
    link.shutdown().await.expect("shut down actor fixture link");
}
