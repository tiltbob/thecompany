use std::time::Duration;

use iroh::EndpointAddr;
use numbers::IpTransport;
use numbers::link::LinkConfig;
use numbers::proto::StreamHeader;
use tokio::time::timeout;

use super::delivery_deadline::{bind, checked, dial_admitted, read, write};
use super::*;

const BUDGET: Duration = Duration::from_secs(5);
const CASE_BUDGET: Duration = Duration::from_secs(20);

/// The peer book decides, not the link: a chat the link admitted while the sender was still a
/// peer is refused once the actor's book no longer lists that station.
async fn chat_from_a_removed_peer_is_refused_and_not_logged() {
    let dir = checked(tempfile::tempdir(), "create actor test directory");
    let storage = checked(
        Storage::lock(&dir.path().join("station")),
        "lock actor test directory",
    );
    let key = checked(storage.load_node_key(), "load actor key");
    let own_id = node_id(&key);
    let peer_key = generate_node_key();
    let peer_id = node_id(&peer_key);
    let book = PeerBook::from([("peer".to_owned(), vec![peer_id])]);
    let authorizer = Arc::new(Authorizer::new(&[LINK_ALPN], &[CHAT_PROTOCOL]));
    authorize_book(&authorizer, &book);
    let endpoint = checked(
        Endpoint::bind(
            &key,
            EndpointConfig {
                ip_transport: IpTransport::LoopbackOnly,
                ..EndpointConfig::default()
            },
            Arc::clone(&authorizer),
        )
        .await,
        "bind actor endpoint",
    );
    let address = EndpointAddr::new(own_id).with_ip_addr(endpoint.iroh().bound_sockets()[0]);
    let config = LinkConfig {
        connect_timeout: BUDGET,
        send_deadline: BUDGET,
    };
    let (link, mut inbox) = Link::start(endpoint, config);
    let status = Status {
        station_id: own_id.to_string(),
        instance_id: "fixture".to_owned(),
        towers: Vec::new(),
        bootstrap_domain: None,
    };
    let mut actor = Actor::new(
        storage,
        own_id,
        Some(book),
        authorizer,
        status,
        BUDGET,
        link.handle(),
    );
    let (sink, mut events) = subscription(SUBSCRIPTION_CAPACITY);
    actor.sinks.push(sink);

    let peer = bind(&peer_key, [own_id]).await;
    let connection = dial_admitted(&peer, address, &link.handle()).await;
    let (mut send, mut recv) = checked(connection.open_bi().await, "open chat stream");
    write(&mut send, &StreamHeader::chat()).await;
    write(
        &mut send,
        &ChatMessage {
            id: "queued-before-removal".into(),
            sent_at_ms: 1,
            body: "from a removed peer".into(),
        },
    )
    .await;
    checked(send.finish(), "finish chat request");
    let chat = inbox.recv().await.expect("the link queues the chat");
    assert!(chat.station_id == peer_id, "the chat came from the peer");
    assert_eq!(chat.peer.as_ref(), "peer");

    // Change the book as `remove_peer` does, but leave the link open: the queued chat reaches the
    // actor after the removal, and its reply must still reach the sender to be observed. The
    // station is listed again under another name, so knowing the station is not enough.
    checked(
        actor.change_book(PeerBook::from([("renamed".to_owned(), vec![peer_id])])),
        "remove the peer",
    );
    actor.incoming_chat(chat);

    let reply: ChatReply = read(&mut recv).await;
    assert_eq!(reply, ChatReply::Nack(NackReason::Unavailable));
    let log = checked(actor.history.page(None, None, 10), "read message log");
    assert!(log.messages.is_empty(), "a refused chat is not logged");
    assert!(queued_event(&mut events).await.is_none());
    checked(link.shutdown().await, "shut down actor link");
    peer.iroh().close().await;
}

#[tokio::test]
async fn a_chat_queued_before_its_peer_was_removed_is_refused_and_not_logged() {
    checked(
        timeout(
            CASE_BUDGET,
            chat_from_a_removed_peer_is_refused_and_not_logged(),
        )
        .await,
        "removed-peer chat test timed out",
    );
}
