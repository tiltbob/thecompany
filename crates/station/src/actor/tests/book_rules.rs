use std::fs::{self, Permissions};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::PathBuf;

use super::*;

/// An actor with one peer `kept` already saved, so `peers.json` exists, and a subscription that
/// starts after that add.
struct BookFixture {
    _dir: tempfile::TempDir,
    actor: Actor,
    link: Link,
    events: Subscription,
    book_path: PathBuf,
    kept: NodeId,
    book: PeerBook,
    inode: u64,
}

impl BookFixture {
    async fn start() -> Self {
        let dir = tempfile::tempdir().expect("create actor test directory");
        let data_dir = dir.path().join("station");
        let (mut actor, link, _) = actor_fixture(&data_dir).await;
        let kept = node_id(&generate_node_key());
        actor
            .add_peer("kept", &kept.to_string())
            .unwrap_or_else(|_| panic!("add the kept peer"));
        let (sink, events) = subscription(SUBSCRIPTION_CAPACITY);
        actor.sinks.push(sink);
        let book_path = data_dir.join("peers.json");
        let inode = fs::metadata(&book_path).expect("read book inode").ino();
        let book = actor.book.clone();
        Self {
            _dir: dir,
            actor,
            link,
            events,
            book_path,
            kept,
            book,
            inode,
        }
    }

    fn authorized(&self, id: &NodeId) -> Option<Arc<str>> {
        self.actor
            .authorizer
            .authorize(id, Protocol::Stream(CHAT_PROTOCOL))
    }

    /// The book in memory and in the authorizer is the one saved at the start, the file was not
    /// replaced, and no event was pushed.
    async fn assert_unchanged(&mut self) {
        assert!(
            self.actor.book == self.book,
            "the book in memory is unchanged"
        );
        assert!(!self.actor.book_uncertain);
        assert_eq!(
            fs::metadata(&self.book_path)
                .expect("read book inode")
                .ino(),
            self.inode,
            "peers.json was not replaced"
        );
        assert!(self.authorized(&self.kept).as_deref() == Some("kept"));
        assert!(queued_event(&mut self.events).await.is_none());
    }

    /// The book on disk is the one saved at the start.
    fn assert_saved(&self) {
        let saved = self
            .actor
            .storage
            .load_peers(self.actor.own_id)
            .expect("read saved book");
        assert!(
            saved.as_ref() == Some(&self.book),
            "the book on disk is unchanged"
        );
    }

    async fn shutdown(self) {
        self.link
            .shutdown()
            .await
            .expect("shut down actor fixture link");
    }
}

#[tokio::test]
async fn a_station_listed_under_another_name_is_refused_without_a_write_or_event() {
    let mut fixture = BookFixture::start().await;
    let kept = fixture.kept;
    assert!(matches!(
        fixture.actor.add_peer("second", &kept.to_string()),
        Err(ApiError::StationTaken { station_id, peer })
            if station_id == kept.to_string() && peer == "kept"
    ));
    fixture.assert_unchanged().await;
    fixture.assert_saved();
    fixture.shutdown().await;
}

#[tokio::test]
async fn adding_the_own_station_is_refused_without_a_write_or_event() {
    let mut fixture = BookFixture::start().await;
    let own_id = fixture.actor.own_id;
    for name in ["self", "kept"] {
        assert!(matches!(
            fixture.actor.add_peer(name, &own_id.to_string()),
            Err(ApiError::Invalid { field, reason })
                if field == "station_id" && reason == "cannot add this station as a peer"
        ));
        fixture.assert_unchanged().await;
        fixture.assert_saved();
        assert!(fixture.authorized(&own_id).is_none());
    }
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_failed_save_changes_neither_the_book_nor_the_authorizer() {
    let mut fixture = BookFixture::start().await;
    // An unsafe destination makes the replacement fail before rename.
    fs::set_permissions(&fixture.book_path, Permissions::from_mode(0o644))
        .expect("make destination unsafe");

    let newcomer = node_id(&generate_node_key());
    assert!(matches!(
        fixture.actor.add_peer("newcomer", &newcomer.to_string()),
        Err(ApiError::Internal { .. })
    ));
    assert!(fixture.authorized(&newcomer).is_none());
    fixture.assert_unchanged().await;

    assert!(matches!(
        fixture.actor.remove_peer("kept"),
        Err(ApiError::Internal { .. })
    ));
    fixture.assert_unchanged().await;

    fs::set_permissions(&fixture.book_path, Permissions::from_mode(0o600))
        .expect("restore destination mode");
    fixture.assert_unchanged().await;
    fixture.assert_saved();
    fixture.shutdown().await;
}
