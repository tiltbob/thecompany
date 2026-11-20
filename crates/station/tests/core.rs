#![cfg(test)]

use std::future::{Future, poll_fn, ready};
use std::net::{Ipv4Addr, TcpListener};
use std::path::Path;
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::task::Poll;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chief::api::{
    ApiError, Direction, Event, FailReason, Link as ApiLink, Message, MessageStatus, TowerState,
    clean,
};
use chief::client::{Client, Subscription};
use numbers::proto::CHAT_PROTOCOL;
use numbers::proto::chat::{ChatMessage, ChatReply, NackReason};
use numbers::{
    Authorizer, Endpoint, EndpointConfig, IncomingChat, LINK_ALPN, Link, LinkConfig, LinkHandle,
    LinkPath, NodeId, TowerSet, generate_node_key,
};
use numbers_station::{
    IpTransport, MESSAGE_LOG_CAPACITY, StartError, Station, StationConfig, StationHandle,
};
use numbers_tower::{Tower, TowerConfig};
use tokio::sync::mpsc;
use tokio::task::yield_now;
use tokio::time::{Instant, sleep_until, timeout, timeout_at};

const CASE_TIMEOUT: Duration = Duration::from_secs(60);
const ONLINE_TIMEOUT: Duration = Duration::from_secs(35);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const SEND_DEADLINE: Duration = Duration::from_secs(4);
const HELD_SEND_DEADLINE: Duration = Duration::from_secs(25);
const PROBE_TIMEOUT: Duration = Duration::from_secs(6);

type Chief = Client<StationHandle>;

fn checked<T, E>(result: Result<T, E>, operation: &str) -> T {
    // Errors from the network or API can contain identities or peer input.
    result.unwrap_or_else(|_| panic!("{operation}"))
}

async fn within<F: Future>(duration: Duration, task: F, operation: &str) -> F::Output {
    checked(timeout(duration, task).await, operation)
}

struct Network {
    scratch: tempfile::TempDir,
    tower: Tower,
}

impl Network {
    async fn start() -> Self {
        let scratch = checked(tempfile::tempdir(), "create station test directory");
        let tower = checked(
            Tower::spawn(TowerConfig::dev((Ipv4Addr::LOCALHOST, 0).into())).await,
            "start loopback tower",
        );
        Self { scratch, tower }
    }

    fn config(&self, name: &str, ip_transport: IpTransport) -> StationConfig {
        config(
            &self.scratch.path().join(name),
            self.tower.url(),
            ip_transport,
        )
    }

    async fn station(&self, name: &str, ip_transport: IpTransport) -> (Station, Chief) {
        start(self.config(name, ip_transport)).await
    }

    async fn shutdown(self) {
        checked(self.tower.shutdown().await, "shut down loopback tower");
    }
}

fn config(path: &Path, tower_url: &str, ip_transport: IpTransport) -> StationConfig {
    let mut config = StationConfig::new(path, tower_url);
    config.ip_transport = ip_transport;
    config.connect_timeout = CONNECT_TIMEOUT;
    config.send_deadline = SEND_DEADLINE;
    config
}

async fn start(config: StationConfig) -> (Station, Chief) {
    let station = checked(Station::start(config).await, "start station");
    let client = Client::new(station.handle());
    (station, client)
}

async fn stop(station: Station) {
    checked(station.shutdown().await, "shut down station");
}

async fn subscribe(client: &Chief) -> Subscription {
    checked(client.subscribe().await, "subscribe to station events")
}

async fn next_event(events: &mut Subscription) -> Event {
    let event = events.next().await.expect("subscription stays open");
    assert!(
        !matches!(event, Event::Resync),
        "unexpected subscription lag"
    );
    event
}

async fn queued_event(events: &mut Subscription) -> Option<Event> {
    tokio::select! {
        biased;
        event = next_event(events) => Some(event),
        _ = ready(()) => None,
    }
}

async fn next_message(events: &mut Subscription) -> Message {
    loop {
        if let Event::Message { message } = next_event(events).await {
            return message;
        }
    }
}

async fn next_incoming(events: &mut Subscription) -> Message {
    loop {
        let message = next_message(events).await;
        if message.direction == Direction::Incoming {
            return message;
        }
    }
}

async fn wait_online(client: &Chief) {
    within(
        ONLINE_TIMEOUT,
        async {
            let mut events = subscribe(client).await;
            let status = checked(client.status().await, "read station status");
            if status.towers[0].state == TowerState::Connected {
                return;
            }
            loop {
                if let Event::Status { status } = next_event(&mut events).await
                    && status.towers[0].state == TowerState::Connected
                {
                    return;
                }
            }
        },
        "station comes online through the tower",
    )
    .await;
}

async fn add_peers(first: &Chief, second: &Chief) {
    let first_status = checked(first.status().await, "read first identity");
    let second_status = checked(second.status().await, "read second identity");
    checked(
        first.peer_add("second", &second_status.station_id).await,
        "authorize second station",
    );
    checked(
        second.peer_add("first", &first_status.station_id).await,
        "authorize first station",
    );
}

fn assert_delivered(
    sent: &Message,
    received: &Message,
    sender_name: &str,
    receiver_name: &str,
    body: &str,
) {
    assert_eq!(sent.direction, Direction::Outgoing);
    assert_eq!(sent.status, MessageStatus::Delivered);
    assert_eq!(sent.rev, 1);
    assert_eq!(sent.peer, receiver_name);
    assert!(sent.body == body, "outgoing body matches");
    assert_eq!(received.direction, Direction::Incoming);
    assert_eq!(received.status, MessageStatus::Received);
    assert_eq!(received.rev, 0);
    assert_eq!(received.peer, sender_name);
    assert!(received.id == sent.id, "message IDs match");
    assert!(
        received.body == clean(body).as_ref(),
        "incoming body matches after cleaning"
    );
}

async fn assert_log(client: &Chief, sent: &Message, received: &Message) {
    let log = checked(client.log(None, None, 10).await, "read chat log");
    assert_eq!(log.messages.len(), 2);
    assert_eq!(log.next_before, None);
    assert!(
        log.messages.contains(sent),
        "log contains final outgoing state"
    );
    assert!(
        log.messages.contains(received),
        "log contains incoming message"
    );
}

async fn wait_api_link(client: &Chief, expected: ApiLink) {
    wait_api_links(client, &[expected]).await;
}

async fn wait_api_links(client: &Chief, expected: &[ApiLink]) {
    let matches = |peer: &chief::api::Peer| {
        peer.stations
            .iter()
            .map(|station| station.link)
            .eq(expected.iter().copied())
    };
    within(
        PROBE_TIMEOUT,
        async {
            let mut events = subscribe(client).await;
            let peers = checked(client.peers().await, "read link snapshot");
            if matches(&peers[0]) {
                return;
            }
            loop {
                if let Event::Peer { peer } = next_event(&mut events).await
                    && matches(&peer)
                {
                    return;
                }
            }
        },
        "observe expected peer path in station API",
    )
    .await;
}

async fn round_trip(ip_transport: IpTransport, simultaneous: bool) {
    within(
        CASE_TIMEOUT,
        async {
            let network = Network::start().await;
            let (first, first_client) = network.station("first", ip_transport).await;
            let (second, second_client) = network.station("second", ip_transport).await;
            add_peers(&first_client, &second_client).await;
            tokio::join!(wait_online(&first_client), wait_online(&second_client));
            let mut first_events = subscribe(&first_client).await;
            let mut second_events = subscribe(&second_client).await;
            let first_body = "first\nhello\u{2028}\u{202e}there";
            let send_first = first_client.send_and_wait("second", first_body);
            let send_second = second_client.send_and_wait("first", "second greeting");
            let (sent_first, sent_second) = if simultaneous {
                tokio::join!(send_first, send_second)
            } else {
                (send_first.await, send_second.await)
            };
            let sent_first = checked(sent_first, "get first send outcome");
            let sent_second = checked(sent_second, "get second send outcome");
            let received_first = next_incoming(&mut first_events).await;
            let received_second = next_incoming(&mut second_events).await;
            assert_delivered(&sent_first, &received_second, "first", "second", first_body);
            assert_delivered(
                &sent_second,
                &received_first,
                "second",
                "first",
                "second greeting",
            );
            assert_log(&first_client, &sent_first, &received_first).await;
            assert_log(&second_client, &sent_second, &received_second).await;
            if matches!(ip_transport, IpTransport::RelayOnly) {
                tokio::join!(
                    wait_api_link(&first_client, ApiLink::Relayed),
                    wait_api_link(&second_client, ApiLink::Relayed)
                );
            }
            tokio::join!(stop(first), stop(second));
            network.shutdown().await;
        },
        "chat round trip timed out",
    )
    .await;
}

#[tokio::test]
async fn stations_exchange_chat_over_loopback() {
    round_trip(IpTransport::LoopbackOnly, false).await;
}

#[tokio::test]
async fn stations_exchange_chat_over_the_relay() {
    round_trip(IpTransport::RelayOnly, false).await;
}

#[tokio::test]
async fn simultaneous_initial_sends_work_over_loopback() {
    round_trip(IpTransport::LoopbackOnly, true).await;
}

#[tokio::test]
async fn simultaneous_initial_sends_work_over_the_relay() {
    round_trip(IpTransport::RelayOnly, true).await;
}

#[tokio::test]
async fn stranger_gets_no_answer_at_the_deadline() {
    within(
        CASE_TIMEOUT,
        async {
            let network = Network::start().await;
            let (caller, caller_client) = network.station("caller", IpTransport::RelayOnly).await;
            let (callee, callee_client) = network.station("callee", IpTransport::RelayOnly).await;
            let callee_status = checked(callee_client.status().await, "read callee identity");
            checked(
                caller_client
                    .peer_add("callee", &callee_status.station_id)
                    .await,
                "add callee to caller's peer book",
            );
            tokio::join!(wait_online(&caller_client), wait_online(&callee_client));
            let mut callee_events = subscribe(&callee_client).await;

            let begin = Instant::now();
            let outcome = checked(
                timeout(
                    PROBE_TIMEOUT,
                    caller_client.send_and_wait("callee", "uninvited message"),
                )
                .await,
                "observe stranger's send deadline",
            );
            let outcome = checked(outcome, "get stranger's send outcome");
            assert_eq!(
                outcome.status,
                MessageStatus::Failed {
                    reason: FailReason::NoAnswer
                }
            );
            assert_eq!(outcome.rev, 1);
            assert!(
                begin.elapsed() >= SEND_DEADLINE,
                "send did not fail before its deadline"
            );
            let current = checked(
                caller_client.message(outcome.seq).await,
                "read failed send from caller log",
            );
            assert!(current == outcome, "caller retains final failure");

            assert!(
                checked(callee_client.log(None, None, 10).await, "read callee log")
                    .messages
                    .is_empty()
            );
            assert!(checked(callee_client.peers().await, "read callee peers").is_empty());
            while let Some(event) = queued_event(&mut callee_events).await {
                assert!(
                    matches!(event, Event::Status { .. }),
                    "callee published a peer or message event"
                );
            }
            tokio::join!(stop(caller), stop(callee));
            network.shutdown().await;
        },
        "stranger test timed out",
    )
    .await;
}

#[tokio::test]
async fn restart_keeps_key_and_book_and_reconnects_on_next_send() {
    within(
        CASE_TIMEOUT,
        async {
            let network = Network::start().await;
            let (first, first_client) = network.station("first", IpTransport::RelayOnly).await;
            let second_config = network.config("second", IpTransport::RelayOnly);
            let (second, old_client) = start(second_config.clone()).await;
            add_peers(&first_client, &old_client).await;
            tokio::join!(wait_online(&first_client), wait_online(&old_client));
            let mut old_events = subscribe(&old_client).await;
            let before = checked(
                first_client.send_and_wait("second", "before restart").await,
                "send before restart",
            );
            let received = next_incoming(&mut old_events).await;
            assert_delivered(&before, &received, "first", "second", "before restart");
            assert_eq!(received.seq, 1);
            let old_status = checked(old_client.status().await, "read original station status");
            let old_peers = checked(old_client.peers().await, "read original peer book");
            assert_eq!(old_peers.len(), 1);
            assert!(matches!(
                Station::start(second_config.clone()).await,
                Err(StartError::AlreadyRunning)
            ));

            stop(second).await;
            let (restarted, client) = start(second_config).await;
            assert!(matches!(old_client.status().await, Err(ApiError::Stopping)));
            assert!(matches!(
                old_client.subscribe().await,
                Err(ApiError::Stopping)
            ));
            while old_events.next().await.is_some() {}
            assert!(old_events.next().await.is_none());
            let status = checked(client.status().await, "read restarted station status");
            assert!(
                status.station_id == old_status.station_id,
                "station key survives restart"
            );
            assert!(
                status.instance_id != old_status.instance_id,
                "restart creates a new instance"
            );
            let peers = checked(client.peers().await, "read book after restart");
            assert_eq!(peers.len(), 1);
            assert_eq!(peers[0].name, old_peers[0].name);
            assert_eq!(peers[0].stations.len(), 1);
            assert!(
                peers[0].stations[0].station_id == old_peers[0].stations[0].station_id,
                "book retains the authorized identity"
            );
            assert_eq!(peers[0].stations[0].link, ApiLink::Down);
            assert!(
                checked(client.log(None, None, 10).await, "read log after restart")
                    .messages
                    .is_empty()
            );

            let mut events = subscribe(&client).await;
            wait_online(&client).await;
            let sent = checked(
                first_client.send_and_wait("second", "after restart").await,
                "send to restarted station",
            );
            let received = next_incoming(&mut events).await;
            assert_delivered(&sent, &received, "first", "second", "after restart");
            assert_eq!(received.seq, 1);
            tokio::join!(stop(first), stop(restarted));
            network.shutdown().await;
        },
        "station restart timed out",
    )
    .await;
}

/// Polls `future` once without waiting.
async fn poll_now<F: Future + Unpin>(future: &mut F) -> Poll<F::Output> {
    poll_fn(|cx| Poll::Ready(Pin::new(&mut *future).poll(cx))).await
}

/// Whether the station has ended this subscription, after reading what was queued. Never waits.
async fn subscription_ended(events: &mut Subscription) -> bool {
    loop {
        let mut next = pin!(events.next());
        match poll_now(&mut next).await {
            Poll::Ready(Some(_)) => {}
            Poll::Ready(None) => return true,
            Poll::Pending => return false,
        }
    }
}

#[tokio::test]
async fn a_second_start_is_refused_until_shutdown_has_returned() {
    within(
        CASE_TIMEOUT,
        async {
            let network = Network::start().await;
            let config = network.config("station", IpTransport::RelayOnly);
            let (station, client) = start(config.clone()).await;
            let target = station_id(&client).await;
            let ((), peer) = tokio::join!(
                wait_online(&client),
                WirePeer::connect(&network.tower, &client, "peer")
            );
            // A live link gives the endpoint a peer connection to close besides the tower's. With
            // only the tower connection, the shutdown ended before any attempt could overlap it.
            peer.send(target, "open-link", "open the link", 0).await;
            wait_api_link(&client, ApiLink::Relayed).await;
            let mut events = subscribe(&client).await;

            // This test runs on a current-thread runtime, so the actor runs only while this task
            // waits. A refused start does not wait, so an attempt made between two polls that
            // both found the shutdown pending overlapped it. Only attempts made after the actor
            // left its loop count, since an earlier one proves nothing about the shutdown steps.
            let mut shutdown = pin!(stop(station));
            let mut refused_while_pending = 0_u32;
            loop {
                yield_now().await;
                // The actor drops every subscription after it left its loop and before it
                // closes its jobs and the endpoint.
                let stopping = subscription_ended(&mut events).await;
                if poll_now(&mut shutdown).await.is_ready() {
                    break;
                }
                assert!(
                    matches!(
                        Station::start(config.clone()).await,
                        Err(StartError::AlreadyRunning)
                    ),
                    "a second station started before the first one's shutdown returned"
                );
                if poll_now(&mut shutdown).await.is_ready() {
                    break;
                }
                if stopping {
                    refused_while_pending += 1;
                }
            }
            // This guard checks the test's power, not the station: it needs the endpoint close to
            // take more than one turn, which the open peer connection makes it do. If a later
            // shutdown finishes faster, move this test to a seam instead of deleting the guard.
            assert!(
                refused_while_pending > 0,
                "no start attempt overlapped the pending shutdown"
            );
            let (restarted, _client) = start(config).await;
            tokio::join!(stop(restarted), peer.shutdown());
            network.shutdown().await;
        },
        "shutdown lock test timed out",
    )
    .await;
}

#[tokio::test]
async fn pinned_station_lists_no_candidates_and_refuses_tower_changes() {
    within(
        CASE_TIMEOUT,
        async {
            let network = Network::start().await;
            let (station, client) = network.station("station", IpTransport::RelayOnly).await;
            wait_online(&client).await;
            let mut events = subscribe(&client).await;
            let before = checked(client.status().await, "read pinned tower status").towers;
            assert_eq!(before.len(), 1);
            let installed = checked(
                numbers_base::canonical_tower_url(network.tower.url()),
                "canonicalize the loopback tower URL",
            );
            assert_eq!(before[0].url, installed);

            for (after, limit) in [(None, 32), (Some("not a peer name"), 1), (None, 0)] {
                let page = checked(
                    client.tower_candidates(after, limit).await,
                    "read the M1 tower candidate page",
                );
                assert!(page.groups.is_empty());
                assert_eq!(page.next_after_peer, None);
            }

            let malformed = "https://tower.synthetic.invalid/?marker";
            let invalid = ApiError::Invalid {
                field: "url".into(),
                reason: "a tower URL must have a root path and no credentials, query, or fragment"
                    .into(),
            };
            assert_eq!(client.tower_add(malformed).await, Err(invalid.clone()));
            assert_eq!(client.tower_remove(malformed).await, Err(invalid));
            for url in [installed.as_str(), "https://tower.synthetic.invalid/"] {
                assert_eq!(client.tower_add(url).await, Err(ApiError::TowersPinned));
                assert_eq!(client.tower_remove(url).await, Err(ApiError::TowersPinned));
            }

            let after = checked(client.status().await, "read unchanged pinned tower status");
            assert_eq!(after.towers, before);
            assert!(
                queued_event(&mut events).await.is_none(),
                "candidate reads and refused changes do not publish status"
            );

            stop(station).await;
            network.shutdown().await;
        },
        "pinned tower API test timed out",
    )
    .await;
}

#[tokio::test]
async fn station_starts_while_the_tower_is_unavailable() {
    within(
        CASE_TIMEOUT,
        async {
            let scratch = checked(tempfile::tempdir(), "create station test directory");
            // This socket reserves a loopback port and never serves an HTTP response.
            let offline = checked(
                TcpListener::bind((Ipv4Addr::LOCALHOST, 0)),
                "reserve unavailable tower port",
            );
            let address = checked(offline.local_addr(), "read local tower address");
            let url = format!("HTTP://{address}/discard/../");
            let (station, client) = within(
                CONNECT_TIMEOUT,
                start(config(
                    &scratch.path().join("station"),
                    &url,
                    IpTransport::RelayOnly,
                )),
                "station start is independent of the tower",
            )
            .await;
            let status = checked(client.status().await, "read station status without a tower");
            assert_eq!(status.towers.len(), 1);
            assert!(
                status.towers[0].url == format!("http://{address}/"),
                "status uses the installed tower's canonical URL"
            );
            assert_eq!(status.bootstrap_domain, None);
            assert_eq!(status.towers[0].source, None);
            assert!(matches!(
                status.towers[0].state,
                TowerState::Standby | TowerState::Connecting | TowerState::Failing { .. }
            ));
            stop(station).await;
        },
        "offline startup test timed out",
    )
    .await;
}

#[tokio::test]
async fn zero_timeouts_are_refused_and_leave_the_data_directory_free() {
    within(
        CASE_TIMEOUT,
        async {
            let scratch = checked(tempfile::tempdir(), "create station test directory");
            // This socket reserves a loopback port and never serves an HTTP response.
            let offline = checked(
                TcpListener::bind((Ipv4Addr::LOCALHOST, 0)),
                "reserve unavailable tower port",
            );
            let address = checked(offline.local_addr(), "read local tower address");
            let url = format!("http://{address}/");
            let valid = config(
                &scratch.path().join("station"),
                &url,
                IpTransport::RelayOnly,
            );
            let mut zero_send = valid.clone();
            zero_send.send_deadline = Duration::ZERO;
            let mut zero_connect = valid.clone();
            zero_connect.connect_timeout = Duration::ZERO;
            for invalid in [zero_send, zero_connect] {
                assert!(matches!(
                    Station::start(invalid).await,
                    Err(StartError::InvalidTimeouts)
                ));
                let (station, _client) = start(valid.clone()).await;
                stop(station).await;
            }
        },
        "zero timeout test timed out",
    )
    .await;
}

#[tokio::test]
async fn a_tower_that_went_down_is_not_reported_connected_and_reaches_failing() {
    within(
        CASE_TIMEOUT,
        async {
            let Network { scratch, tower } = Network::start().await;
            let (station, client) = start(config(
                &scratch.path().join("station"),
                tower.url(),
                IpTransport::RelayOnly,
            ))
            .await;
            wait_online(&client).await;
            let mut events = subscribe(&client).await;
            checked(tower.shutdown().await, "shut down loopback tower");
            within(
                ONLINE_TIMEOUT,
                async {
                    // Reports queued before the shutdown can still say Connected. After each failed
                    // attempt iroh publishes the failure (Failing) for the backoff, then Connecting
                    // while it dials again; it publishes Connected only after a successful dial.
                    // This assumes no other test is given the freed tower port in the meantime.
                    let mut down = false;
                    loop {
                        let Event::Status { status } = next_event(&mut events).await else {
                            continue;
                        };
                        let state = &status.towers[0].state;
                        if *state == TowerState::Connected {
                            assert!(!down, "a tower that is down was reported connected");
                            continue;
                        }
                        down = true;
                        if matches!(state, TowerState::Failing { .. }) {
                            return;
                        }
                    }
                },
                "station reports the tower down, then failing",
            )
            .await;
            let status = checked(client.status().await, "read station status");
            assert!(status.towers[0].state != TowerState::Connected);
            stop(station).await;
        },
        "tower failure test timed out",
    )
    .await;
}

struct WirePeer {
    id: NodeId,
    link: Link,
    inbox: mpsc::Receiver<IncomingChat>,
}

impl WirePeer {
    async fn connect(tower: &Tower, station: &Chief, name: &str) -> Self {
        let station_id = station_id(station).await;
        let auth = Arc::new(Authorizer::new(&[LINK_ALPN], &[CHAT_PROTOCOL]));
        auth.replace_peers([(station_id, "station".into())]);
        let config = EndpointConfig {
            towers: checked(TowerSet::from_url(tower.url()), "parse loopback tower URL"),
            ip_transport: IpTransport::RelayOnly,
            ..EndpointConfig::default()
        };
        let endpoint = checked(
            Endpoint::bind(&generate_node_key(), config, auth).await,
            "bind fixture endpoint",
        );
        let id = endpoint.iroh().id();
        checked(
            station.peer_add(name, &id.to_string()).await,
            "register fixture identity",
        );
        checked(
            timeout(ONLINE_TIMEOUT, endpoint.iroh().online()).await,
            "fixture endpoint comes online",
        );
        let (link, inbox) = Link::start(
            endpoint,
            LinkConfig {
                connect_timeout: CONNECT_TIMEOUT,
                send_deadline: HELD_SEND_DEADLINE,
            },
        );
        Self { id, link, inbox }
    }

    fn handle(&self) -> LinkHandle {
        self.link.handle()
    }

    async fn receive(&mut self) -> IncomingChat {
        checked(
            timeout(PROBE_TIMEOUT, self.inbox.recv()).await,
            "fixture receives pending chat",
        )
        .expect("fixture inbox stays open")
    }

    async fn receive_before(&mut self, other: &mut Self) -> IncomingChat {
        tokio::select! {
            biased;
            _ = other.inbox.recv() => panic!("message reached the lower priority station first"),
            incoming = self.receive() => incoming,
        }
    }

    async fn send(&self, station: NodeId, id: &str, body: &str, sent_at_ms: u64) {
        let reply = checked(
            self.handle()
                .send(
                    station,
                    ChatMessage {
                        id: id.into(),
                        body: body.into(),
                        sent_at_ms,
                    },
                )
                .await,
            "send fixture chat over the wire",
        );
        assert!(
            matches!(reply, ChatReply::Ack),
            "fixture chat is acknowledged"
        );
    }

    async fn shutdown(self) {
        checked(self.link.shutdown().await, "shut down fixture endpoint");
    }
}

/// Returns both fixtures in the peer book's order, regardless of which registers first.
async fn delivery_peers(
    network: &Network,
    client: &Chief,
    warm_first: bool,
) -> (WirePeer, WirePeer) {
    let ((), first, second) = tokio::join!(
        wait_online(client),
        WirePeer::connect(&network.tower, client, "peer"),
        WirePeer::connect(&network.tower, client, "peer"),
    );
    let peers = checked(client.peers().await, "read delivery peer's book order");
    assert_eq!(peers.len(), 1);
    let stations = &peers[0].stations;
    assert_eq!(stations.len(), 2);
    let (first, second) = if stations[0].station_id == first.id.to_string() {
        (first, second)
    } else {
        (second, first)
    };
    assert!(stations[0].station_id == first.id.to_string());
    assert!(stations[1].station_id == second.id.to_string());
    let target = station_id(client).await;
    tokio::join!(
        async {
            if warm_first {
                first.send(target, "warm-first", "open first link", 0).await;
            }
        },
        second.send(target, "warm-second", "open second link", 0),
    );
    let first_path = if warm_first {
        ApiLink::Relayed
    } else {
        ApiLink::Down
    };
    wait_api_links(client, &[first_path, ApiLink::Relayed]).await;
    (first, second)
}

fn assert_same_chat(incoming: &IncomingChat, sent: &Message) {
    assert!(incoming.message.id == sent.id, "chat has the original ID");
    assert!(
        incoming.message.body == sent.body,
        "chat has the original body"
    );
    assert_eq!(incoming.message.sent_at_ms, sent.at_ms);
}

#[tokio::test]
async fn unavailable_station_falls_back_in_peer_book_order_with_the_same_chat() {
    within(
        CASE_TIMEOUT,
        async {
            let network = Network::start().await;
            let (station, client) = network.station("station", IpTransport::RelayOnly).await;
            let (mut first, mut second) = delivery_peers(&network, &client, true).await;
            let mut events = subscribe(&client).await;
            let sent = checked(
                client.send("peer", "deliver after unavailable").await,
                "accept outgoing chat",
            );
            let received = first.receive_before(&mut second).await;
            assert_same_chat(&received, &sent);
            received.respond(ChatReply::Nack(NackReason::Unavailable));

            let mut outcome = pin!(client.wait_for_outcome(&mut events, sent.clone()));
            let incoming = tokio::select! {
                incoming = second.receive() => incoming,
                result = outcome.as_mut() => {
                    let result = checked(result, "read early delivery outcome");
                    panic!("delivery ended before the second station's ACK: {:?}", result.status);
                }
            };
            assert_same_chat(&incoming, &sent);
            incoming.respond(ChatReply::Ack);
            let outcome = checked(outcome.await, "read delivery outcome");
            assert_eq!(outcome.status, MessageStatus::Delivered);
            assert_eq!(outcome.rev, 1);
            tokio::join!(stop(station), first.shutdown(), second.shutdown());
            network.shutdown().await;
        },
        "unavailable station fallback test timed out",
    )
    .await;
}

#[tokio::test]
async fn a_connected_station_is_tried_before_an_earlier_down_station() {
    within(
        CASE_TIMEOUT,
        async {
            let network = Network::start().await;
            let (station, client) = network.station("station", IpTransport::RelayOnly).await;
            let (mut first, mut second) = delivery_peers(&network, &client, false).await;
            let mut events = subscribe(&client).await;
            let sent = checked(
                client.send("peer", "prefer the connected station").await,
                "accept outgoing chat",
            );
            let received = second.receive_before(&mut first).await;
            assert_same_chat(&received, &sent);
            received.respond(ChatReply::Ack);
            let outcome = checked(
                client.wait_for_outcome(&mut events, sent).await,
                "read delivery outcome",
            );
            assert_eq!(outcome.status, MessageStatus::Delivered);
            assert!(matches!(
                first.inbox.try_recv(),
                Err(mpsc::error::TryRecvError::Empty)
            ));
            tokio::join!(stop(station), first.shutdown(), second.shutdown());
            network.shutdown().await;
        },
        "connected station priority test timed out",
    )
    .await;
}

#[tokio::test]
async fn trying_a_second_station_does_not_extend_the_send_deadline() {
    within(
        CASE_TIMEOUT,
        async {
            let network = Network::start().await;
            let mut config = network.config("station", IpTransport::RelayOnly);
            let deadline = Duration::from_secs(6);
            config.send_deadline = deadline;
            // Keep the link's earlier stale-path check out of this actor deadline test.
            config.connect_timeout = deadline;
            let (station, client) = start(config).await;
            let (mut first, mut second) = delivery_peers(&network, &client, true).await;
            let mut events = subscribe(&client).await;
            let started = Instant::now();
            let sent = checked(
                client.send("peer", "one deadline across both stations").await,
                "accept outgoing chat",
            );
            let accepted_by = Instant::now();
            let received = first.receive_before(&mut second).await;
            assert_same_chat(&received, &sent);
            sleep_until(accepted_by + deadline / 2).await;
            received.respond(ChatReply::Nack(NackReason::Unavailable));

            // The first reply spends three seconds of the budget. Two seconds of scheduling
            // slack still makes the outer bound shorter than a fresh six-second attempt.
            let outcome = checked(
                timeout_at(accepted_by + deadline + Duration::from_secs(2), async {
                    let mut outcome = pin!(client.wait_for_outcome(&mut events, sent.clone()));
                    let held = tokio::select! {
                        incoming = second.receive() => incoming,
                        result = outcome.as_mut() => {
                            let result = checked(result, "read early delivery outcome");
                            panic!("delivery ended before the second station held the chat: {:?}", result.status);
                        }
                    };
                    assert_same_chat(&held, &sent);
                    let outcome = checked(outcome.await, "read the unanswered chat's outcome");
                    // Keep the reply sender live until the actor declares NoAnswer.
                    drop(held);
                    outcome
                })
                .await,
                "both station attempts share the original deadline",
            );
            assert_eq!(outcome.status, MessageStatus::Failed { reason: FailReason::NoAnswer });
            assert!(started.elapsed() >= deadline, "send did not fail before its deadline");
            tokio::join!(stop(station), first.shutdown(), second.shutdown());
            network.shutdown().await;
        },
        "shared send deadline test timed out",
    )
    .await;
}

#[tokio::test]
async fn a_validation_refusal_stops_delivery_at_the_first_station() {
    within(
        CASE_TIMEOUT,
        async {
            let network = Network::start().await;
            let (station, client) = network.station("station", IpTransport::RelayOnly).await;
            let (mut first, mut second) = delivery_peers(&network, &client, true).await;
            let mut events = subscribe(&client).await;
            let sent = checked(
                client.send("peer", "stop after validation refusal").await,
                "accept outgoing chat",
            );
            let received = first.receive_before(&mut second).await;
            assert_same_chat(&received, &sent);
            received.respond(ChatReply::Nack(NackReason::InvalidBody));

            let outcome = tokio::select! {
                biased;
                _ = second.inbox.recv() => panic!("validation refusal must stop the send"),
                outcome = client.wait_for_outcome(&mut events, sent) => checked(outcome, "read refusal outcome"),
            };
            assert_eq!(outcome.status, MessageStatus::Failed { reason: FailReason::Refused });
            tokio::join!(stop(station), first.shutdown(), second.shutdown());
            network.shutdown().await;
        },
        "validation refusal test timed out",
    )
    .await;
}

async fn station_id(client: &Chief) -> NodeId {
    checked(
        checked(client.status().await, "read station identity")
            .station_id
            .parse(),
        "parse station identity",
    )
}

fn unix_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock is after Unix epoch")
            .as_millis(),
    )
    .expect("clock fits Unix milliseconds")
}

#[tokio::test]
async fn receipt_window_deduplicates_across_stations_and_resets_on_restart() {
    within(
        CASE_TIMEOUT,
        async {
            let network = Network::start().await;
            let config = network.config("station", IpTransport::RelayOnly);
            let (station, client) = start(config.clone()).await;
            let target = station_id(&client).await;
            let ((), primary, replica, other) = tokio::join!(
                wait_online(&client),
                WirePeer::connect(&network.tower, &client, "sender"),
                WirePeer::connect(&network.tower, &client, "sender"),
                WirePeer::connect(&network.tower, &client, "other"),
            );
            let mut events = subscribe(&client).await;

            let before = unix_ms();
            primary
                .send(
                    target,
                    "retry",
                    "Grüße\n\u{001b}\t\u{0085}\u{2028}\u{202e}\u{200b}bye",
                    1,
                )
                .await;
            let after = unix_ms();
            let received = next_message(&mut events).await;
            assert_eq!(received.seq, 1);
            assert_eq!(received.rev, 0);
            assert_eq!(received.status, MessageStatus::Received);
            assert_eq!(received.direction, Direction::Incoming);
            assert_eq!(received.peer, "sender");
            assert!(received.id == "retry", "receipt retains the wire ID");
            assert!(
                received.body == "Grüße      \u{200b}bye",
                "station replaces the API rule 9 characters with spaces"
            );
            assert!(
                (before..=after).contains(&received.at_ms),
                "station records local arrival time"
            );
            primary
                .send(target, "retry", "same station retry", u64::MAX)
                .await;
            replica
                .send(target, "retry", "different station retry", u64::MAX)
                .await;
            let log = checked(
                client.log(None, None, 10).await,
                "read log after duplicate ACKs",
            );
            assert!(
                log.messages.as_slice() == [received],
                "duplicate ACKs leave one log entry"
            );
            while let Some(event) = queued_event(&mut events).await {
                assert!(
                    !matches!(event, Event::Message { .. }),
                    "duplicate ACK emitted a message event"
                );
            }

            // The same ID under another local peer name is a different receipt. Filling the window
            // from that peer must eventually evict the oldest receipt from the original sender.
            let mut lagging = subscribe(&client).await;
            for index in 0..999 {
                let id = if index == 0 {
                    "retry".into()
                } else {
                    format!("other-{index}")
                };
                other.send(target, &id, "fill receipt window", 0).await;
                let message = next_message(&mut events).await;
                assert_eq!(message.peer, "other");
                assert!(message.id == id, "each distinct receipt produces an event");
            }
            assert!(matches!(lagging.next().await, Some(Event::Resync)));
            replica
                .send(target, "retry", "still in the window", 0)
                .await;
            other
                .send(target, "evict-oldest", "evict one receipt", 0)
                .await;
            let eviction = next_message(&mut events).await;
            assert!(
                eviction.id == "evict-oldest",
                "retry did not produce a message event"
            );
            assert!(
                next_message(&mut lagging).await == eviction,
                "lagging subscription resumes after resync"
            );
            primary
                .send(target, "retry", "accepted again after eviction", 0)
                .await;
            let after_eviction = next_message(&mut events).await;
            assert_eq!(after_eviction.seq, 1002);
            assert_eq!(after_eviction.peer, "sender");
            assert!(
                after_eviction.id == "retry",
                "duplicate ACK did not refresh FIFO order"
            );

            stop(station).await;
            let (restarted, client) = start(config).await;
            wait_online(&client).await;
            let mut events = subscribe(&client).await;
            replica
                .send(target, "retry", "accepted again after restart", 0)
                .await;
            let after_restart = next_message(&mut events).await;
            assert_eq!(after_restart.seq, 1);
            assert_eq!(after_restart.peer, "sender");
            assert!(after_restart.id == "retry", "restart clears the window");
            tokio::join!(
                stop(restarted),
                primary.shutdown(),
                replica.shutdown(),
                other.shutdown()
            );
            network.shutdown().await;
        },
        "receipt window test timed out",
    )
    .await;
}

#[tokio::test]
async fn removing_a_peer_closes_the_link_and_finishes_every_accepted_send_once() {
    within(
        CASE_TIMEOUT,
        async {
            let network = Network::start().await;
            let mut config = network.config("station", IpTransport::RelayOnly);
            config.send_deadline = HELD_SEND_DEADLINE;
            let (station, client) = start(config).await;
            let target = station_id(&client).await;
            let ((), mut peer) = tokio::join!(
                wait_online(&client),
                WirePeer::connect(&network.tower, &client, "peer")
            );
            let mut events = subscribe(&client).await;
            let first = {
                let mut call = pin!(client.send("peer", "accepted before call cancellation"));
                poll_fn(|cx| {
                    assert!(
                        call.as_mut().poll(cx).is_pending(),
                        "actor has not answered the send call"
                    );
                    Poll::Ready(())
                })
                .await;
                // Observe acceptance while keeping the caller's future suspended, then drop it.
                next_message(&mut events).await
            };
            assert_eq!(first.status, MessageStatus::Sending);
            let first_held = peer.receive().await;
            assert!(
                first_held.message.id == first.id,
                "cancelled call still reached peer"
            );
            let second = checked(
                client.send("peer", "second outstanding send").await,
                "accept second send",
            );
            let second_held = peer.receive().await;
            assert!(
                second_held.message.id == second.id,
                "second send reached peer"
            );
            assert!(
                next_message(&mut events).await == second,
                "initial event precedes the send result"
            );
            let peer_link = peer.handle();
            let mut paths = peer_link.subscribe_paths();
            assert_eq!(peer_link.path(target), LinkPath::Relayed);
            wait_api_link(&client, ApiLink::Relayed).await;

            checked(
                client.peer_remove("peer").await,
                "remove peer with two sends pending",
            );
            let failed = MessageStatus::Failed {
                reason: FailReason::PeerRemoved,
            };
            let mut finished = Vec::new();
            loop {
                match next_event(&mut events).await {
                    Event::Message { message } => {
                        assert_eq!(message.rev, 1);
                        assert_eq!(message.status, failed);
                        assert!([first.seq, second.seq].contains(&message.seq));
                        assert!(!finished.contains(&message.seq), "one final event per send");
                        finished.push(message.seq);
                    }
                    Event::PeerRemoved { name } => {
                        assert_eq!(name, "peer");
                        assert_eq!(finished.len(), 2, "all final events precede peer removal");
                        break;
                    }
                    _ => {}
                }
            }
            within(
                PROBE_TIMEOUT,
                async {
                    while peer_link.path(target) != LinkPath::Down {
                        checked(paths.recv().await, "observe peer-side link change");
                    }
                },
                "removal closes the transport link",
            )
            .await;
            // A late peer reply cannot change the outcome after revocation.
            first_held.respond(ChatReply::Ack);
            second_held.respond(ChatReply::Ack);
            assert!(checked(client.peers().await, "read peers after removal").is_empty());
            for seq in finished {
                let message = checked(client.message(seq).await, "read failed send from log");
                assert_eq!(message.status, failed);
                assert_eq!(message.rev, 1);
            }
            stop(station).await;
            while let Some(event) = events.next().await {
                assert!(
                    matches!(event, Event::Status { .. }),
                    "removed peer emitted an additional event"
                );
            }
            peer.shutdown().await;
            network.shutdown().await;
        },
        "peer removal test timed out",
    )
    .await;
}

#[tokio::test]
async fn an_evicted_send_still_completes_for_a_waiting_frontend() {
    within(CASE_TIMEOUT, async {
        let network = Network::start().await;
        let mut config = network.config("station", IpTransport::RelayOnly);
        config.send_deadline = HELD_SEND_DEADLINE;
        let (station, client) = start(config).await;
        let target = station_id(&client).await;
        let ((), mut peer, flood_peer) = tokio::join!(
            wait_online(&client),
            WirePeer::connect(&network.tower, &client, "peer"),
            WirePeer::connect(&network.tower, &client, "flooder"),
        );
        let mut events = subscribe(&client).await;
        let mut outcome = pin!(client.send_and_wait("peer", "awaiting ACK through eviction"));
        let held = tokio::select! {
            held = peer.receive() => held,
            _ = &mut outcome => panic!("send completed before the peer answered"),
        };
        let sent = next_message(&mut events).await;
        assert_eq!(sent.status, MessageStatus::Sending);
        assert!(held.message.id == sent.id, "fixture holds this send's ACK");
        let flood = async {
            for index in 0..MESSAGE_LOG_CAPACITY {
                let id = format!("flood-{index}");
                flood_peer.send(target, &id, "fill message log", 0).await;
                let incoming = next_message(&mut events).await;
                assert!(incoming.id == id, "subscriber kept up with the flood");
                assert_eq!(incoming.direction, Direction::Incoming);
            }
        };
        tokio::select! {
            () = flood => {},
            _ = &mut outcome => panic!("waiter lost a pending send during eviction"),
        }
        assert!(matches!(client.message(sent.seq).await, Err(ApiError::MessageNotFound { seq }) if seq == sent.seq));
        let original = held.message.clone();
        held.respond(ChatReply::Ack);
        let outcome = within(PROBE_TIMEOUT, async {
            // A held ACK can exceed the link's first attempt budget while the log fills.
            let retry = tokio::select! {
                result = &mut outcome => return result,
                retry = peer.inbox.recv() => retry.expect("fixture inbox stays open"),
            };
            assert!(retry.message == original, "link retries the same pending message");
            retry.respond(ChatReply::Ack);
            tokio::select! {
                result = &mut outcome => result,
                _ = peer.inbox.recv() => panic!("link attempts another retry"),
            }
        }, "waiter receives final event after eviction").await;
        let outcome = checked(outcome, "waiter completes after eviction");
        assert_eq!(outcome.seq, sent.seq);
        assert!(outcome.id == sent.id, "final event preserves message identity");
        assert_eq!(outcome.rev, 1);
        assert_eq!(outcome.status, MessageStatus::Delivered);
        assert!(next_message(&mut events).await == outcome, "subscription receives the outcome after eviction");
        assert!(matches!(client.message(sent.seq).await, Err(ApiError::MessageNotFound { seq }) if seq == sent.seq));
        tokio::join!(stop(station), peer.shutdown(), flood_peer.shutdown());
        network.shutdown().await;
    }, "evicted send test timed out").await;
}
