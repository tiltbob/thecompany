use std::net::{Ipv4Addr, UdpSocket};
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use chief::api::{FailReason, MessageStatus};
use iroh::EndpointAddr;
use iroh::address_lookup::memory::MemoryLookup;
use iroh::endpoint::{Connection, RecvStream, SendStream};
use numbers::link::{LINK_ALPN, Link, LinkConfig, LinkHandle, LinkPath};
use numbers::proto::chat::{ChatMessage, ChatReply, NackReason};
use numbers::proto::{CHAT_PROTOCOL, Hello, HelloEcho, StreamHeader};
use numbers::{
    Authorizer, Endpoint, EndpointConfig, IpTransport, NodeId, generate_node_key, node_id,
};
use numbers_base::{WIRE_FRAME_MAX, read_frame, write_frame};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::time::{Instant, advance, pause, resume, timeout};

use super::super::deliver;

const SEND_BUDGET: Duration = Duration::from_secs(3);
const CASE_BUDGET: Duration = Duration::from_secs(15);

pub(super) fn checked<T, E>(result: Result<T, E>, operation: &str) -> T {
    result.unwrap_or_else(|_| panic!("{operation}"))
}

pub(super) async fn bind(key: &[u8; 32], peers: impl IntoIterator<Item = NodeId>) -> Endpoint {
    let authorizer = Arc::new(Authorizer::new(&[LINK_ALPN], &[CHAT_PROTOCOL]));
    authorizer.replace_peers(peers.into_iter().map(|peer| (peer, "peer".into())));
    checked(
        Endpoint::bind(
            key,
            EndpointConfig {
                ip_transport: IpTransport::LoopbackOnly,
                ..EndpointConfig::default()
            },
            authorizer,
        )
        .await,
        "bind delivery test endpoint",
    )
}

pub(super) async fn write(send: &mut SendStream, value: &impl Serialize) {
    checked(
        write_frame(send, value, WIRE_FRAME_MAX).await,
        "write frame",
    );
}

pub(super) async fn read<T: DeserializeOwned>(recv: &mut RecvStream) -> T {
    checked(read_frame(recv, WIRE_FRAME_MAX, None).await, "read frame")
}

async fn connect() -> (Link, Endpoint, Connection) {
    let local_key = generate_node_key();
    let peer_key = generate_node_key();
    let local_id = node_id(&local_key);
    let peer_id = node_id(&peer_key);
    let endpoint = bind(&local_key, [peer_id]).await;
    let peer = bind(&peer_key, [local_id]).await;
    let address = EndpointAddr::new(local_id).with_ip_addr(endpoint.iroh().bound_sockets()[0]);
    let (link, _inbox) = Link::start(
        endpoint,
        LinkConfig {
            send_deadline: SEND_BUDGET,
            connect_timeout: SEND_BUDGET,
        },
    );
    let connection = dial_admitted(&peer, address, &link.handle()).await;
    (link, peer, connection)
}

/// Dials the station from a raw peer endpoint, completes the hello by hand and waits until the
/// station's link has admitted the connection.
pub(super) async fn dial_admitted(
    peer: &Endpoint,
    address: EndpointAddr,
    station: &LinkHandle,
) -> Connection {
    let peer_id = peer.iroh().id();
    let mut paths = station.subscribe_paths();
    let connection = checked(
        peer.iroh().connect(address, LINK_ALPN).await,
        "dial station",
    );
    let (mut send, mut recv) = checked(connection.open_bi().await, "open hello stream");
    let hello = Hello::local([1; 16], [2; 16]);
    write(&mut send, &hello).await;
    let response: Hello = read(&mut recv).await;
    assert_eq!(response.validate(), Ok(()));
    assert!(response.echo == Some(hello.challenge));
    write(
        &mut send,
        &HelloEcho {
            echo: response.challenge,
        },
    )
    .await;
    checked(send.finish(), "finish hello");
    while station.path(peer_id) != LinkPath::Direct {
        checked(paths.recv().await, "observe admitted direct link");
    }
    connection
}

async fn ready_reply_survives_deadline(reply: ChatReply, expected: MessageStatus) {
    let (link, peer, connection) = connect().await;
    let message = ChatMessage {
        id: "deadline-test".into(),
        sent_at_ms: 123,
        body: "test chat".into(),
    };
    let request = checked(serde_json::to_value(&message), "encode expected chat");
    let deadline = Instant::now() + SEND_BUDGET;
    let mut delivery = pin!(deliver(
        link.handle(),
        vec![peer.iroh().id()],
        message,
        deadline,
        SEND_BUDGET,
    ));
    let mut reply_stream = tokio::select! {
        biased;
        status = delivery.as_mut() => panic!("delivery ended before the peer replied: {status:?}"),
        stream = async {
            let (send, mut recv) = checked(connection.accept_bi().await, "accept chat stream");
            assert!(read::<StreamHeader>(&mut recv).await == StreamHeader::chat());
            assert!(read::<serde_json::Value>(&mut recv).await == request);
            assert!(checked(recv.read_to_end(0).await, "read chat stream end").is_empty());
            send
        } => stream,
    };

    // Leave delivery parked while the station's QUIC driver acknowledges the complete reply.
    write(&mut reply_stream, &reply).await;
    checked(reply_stream.finish(), "finish chat reply");
    assert!(checked(reply_stream.stopped().await, "transport acknowledges reply").is_none());
    pause();
    let ready_at = Instant::now();
    assert!(
        ready_at < deadline,
        "reply reached the station before its deadline"
    );
    advance(deadline.duration_since(ready_at) + Duration::from_millis(1)).await;
    assert!(Instant::now() > deadline);
    let status = delivery.await;
    resume();

    assert_eq!(status, expected);
    checked(link.shutdown().await, "shut down delivery test link");
    peer.iroh().close().await;
}

#[tokio::test]
async fn an_ack_ready_before_the_deadline_still_delivers_after_a_late_poll() {
    checked(
        timeout(
            CASE_BUDGET,
            ready_reply_survives_deadline(ChatReply::Ack, MessageStatus::Delivered),
        )
        .await,
        "late poll after ACK timed out",
    );
}

#[tokio::test]
async fn a_refusal_ready_before_the_deadline_still_refuses_after_a_late_poll() {
    checked(
        timeout(
            CASE_BUDGET,
            ready_reply_survives_deadline(
                ChatReply::Nack(NackReason::InvalidBody),
                MessageStatus::Failed {
                    reason: FailReason::Refused,
                },
            ),
        )
        .await,
        "late poll after refusal timed out",
    );
}

#[tokio::test]
async fn a_third_station_can_deliver_after_two_silent_stations_with_default_budgets() {
    checked(
        timeout(Duration::from_secs(25), async {
            let local_key = generate_node_key();
            let peer_key = generate_node_key();
            let local_id = node_id(&local_key);
            let peer_id = node_id(&peer_key);
            let [first, second] = [(); 2].map(|()| {
                let socket = checked(
                    UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)),
                    "bind silent station socket",
                );
                checked(
                    socket.set_nonblocking(true),
                    "set silent socket nonblocking",
                );
                (node_id(&generate_node_key()), socket)
            });
            let endpoint = bind(&local_key, [first.0, second.0, peer_id]).await;
            let peer = bind(&peer_key, [local_id]).await;
            let addresses = [
                EndpointAddr::new(first.0)
                    .with_ip_addr(checked(first.1.local_addr(), "read first silent address")),
                EndpointAddr::new(second.0)
                    .with_ip_addr(checked(second.1.local_addr(), "read second silent address")),
                EndpointAddr::new(peer_id).with_ip_addr(peer.iroh().bound_sockets()[0]),
            ];
            checked(
                endpoint.iroh().address_lookup(),
                "get station address lookup",
            )
            .add(MemoryLookup::from_endpoint_info(addresses));

            let config = LinkConfig {
                connect_timeout: Duration::from_secs(5),
                send_deadline: Duration::from_secs(15),
            };
            let (link, _inbox) = Link::start(endpoint, config);
            let (peer, mut received) = Link::start(peer, config);
            let message = ChatMessage {
                id: "failover-test".into(),
                sent_at_ms: 123,
                body: "deliver to the third station".into(),
            };
            let mut delivery = pin!(deliver(
                link.handle(),
                vec![first.0, second.0, peer_id],
                message.clone(),
                Instant::now() + config.send_deadline,
                config.connect_timeout,
            ));
            let status = tokio::select! {
                status = delivery.as_mut() => status,
                incoming = received.recv() => {
                    let incoming = incoming.expect("live station inbox stays open");
                    assert!(incoming.station_id == local_id && incoming.message == message);
                    incoming.respond(ChatReply::Ack);
                    delivery.await
                }
            };
            assert_eq!(status, MessageStatus::Delivered);
            // Both earlier stations were tried. Keep their sockets bound and silent throughout.
            for (_, socket) in [first, second] {
                checked(socket.recv(&mut [0; 1]), "silent station receives a dial");
            }
            let (local, remote) = tokio::join!(link.shutdown(), peer.shutdown());
            checked(local, "shut down sending station link");
            checked(remote, "shut down receiving station link");
        })
        .await,
        "default station budget failover timed out",
    );
}
