#![cfg(test)]
//! The wire form of `chief::api`: round trips, the exact JSON text, what decoding accepts and
//! refuses, `Display` texts and `check_peer_name`. Ids are placeholders (AGENTS.md, hard rule 9).

use chief::api::*;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::fmt::Debug;

// Samples.

const AT_MS: u64 = 1_767_225_600_000;

fn outgoing(seq: u64, rev: u32, status: MessageStatus) -> Message {
    Message {
        seq,
        rev,
        id: format!("MSG-{seq}"),
        peer: "bob".into(),
        direction: Direction::Outgoing,
        body: "on my way".into(),
        at_ms: AT_MS,
        status,
    }
}

fn incoming(seq: u64) -> Message {
    Message {
        seq,
        rev: 0,
        id: format!("MSG-{seq}"),
        peer: "bob".into(),
        direction: Direction::Incoming,
        body: "where are you?".into(),
        at_ms: AT_MS - 100_000,
        status: MessageStatus::Received,
    }
}

fn tower(port: u16, state: TowerState) -> TowerStatus {
    TowerStatus {
        url: format!("http://127.0.0.1:{port}/"),
        state,
        source: None,
    }
}

fn status(towers: Vec<TowerStatus>) -> Status {
    Status {
        station_id: "STATION-A".into(),
        instance_id: "INSTANCE-1".into(),
        towers,
        bootstrap_domain: None,
    }
}

fn peer(stations: &[(&str, Link)]) -> Peer {
    Peer {
        name: "bob".into(),
        stations: stations
            .iter()
            .map(|(id, link)| PeerStation {
                station_id: (*id).into(),
                link: *link,
            })
            .collect(),
    }
}

fn delivered() -> Message {
    outgoing(42, 1, MessageStatus::Delivered)
}

/// One value of every `Request` variant (`Log` twice: without and with its options), each with its
/// exact JSON text.
fn requests() -> Vec<(Request, &'static str)> {
    vec![
        (Request::Status, r#"{"type":"status"}"#),
        (Request::Peers, r#"{"type":"peers"}"#),
        (
            Request::PeerAdd {
                name: "bob".into(),
                station_id: "STATION-B".into(),
            },
            r#"{"type":"peer_add","name":"bob","station_id":"STATION-B"}"#,
        ),
        (
            Request::PeerRemove { name: "bob".into() },
            r#"{"type":"peer_remove","name":"bob"}"#,
        ),
        (
            Request::Send {
                peer: "bob".into(),
                body: "on my way".into(),
            },
            r#"{"type":"send","peer":"bob","body":"on my way"}"#,
        ),
        (
            Request::Message { seq: 42 },
            r#"{"type":"message","seq":42}"#,
        ),
        (
            Request::Log {
                peer: None,
                before: None,
                limit: 50,
            },
            r#"{"type":"log","limit":50}"#,
        ),
        (
            Request::Log {
                peer: Some("bob".into()),
                before: Some(42),
                limit: 50,
            },
            r#"{"type":"log","peer":"bob","before":42,"limit":50}"#,
        ),
    ]
}

/// One value of every `Response` variant, each with its exact JSON text.
fn responses() -> Vec<(Response, &'static str)> {
    vec![
        (
            Response::Status {
                status: status(vec![
                    tower(3340, TowerState::Connected),
                    tower(
                        3341,
                        TowerState::Failing {
                            error: "connection refused".into(),
                        },
                    ),
                ]),
            },
            concat!(
                r#"{"type":"status","status":{"station_id":"STATION-A","instance_id":"INSTANCE-1","#,
                r#""towers":[{"url":"http://127.0.0.1:3340/","state":{"type":"connected"}},"#,
                r#"{"url":"http://127.0.0.1:3341/","state":{"type":"failing","error":"connection refused"}}]}}"#,
            ),
        ),
        (
            Response::Peers {
                peers: vec![peer(&[
                    ("STATION-B", Link::Relayed),
                    ("STATION-C", Link::Down),
                ])],
            },
            concat!(
                r#"{"type":"peers","peers":[{"name":"bob","stations":["#,
                r#"{"station_id":"STATION-B","link":"relayed"},"#,
                r#"{"station_id":"STATION-C","link":"down"}]}]}"#,
            ),
        ),
        (
            Response::Peer {
                peer: peer(&[("STATION-B", Link::Relayed)]),
            },
            r#"{"type":"peer","peer":{"name":"bob","stations":[{"station_id":"STATION-B","link":"relayed"}]}}"#,
        ),
        (
            Response::Message {
                message: outgoing(42, 0, MessageStatus::Sending),
            },
            concat!(
                r#"{"type":"message","message":{"seq":42,"rev":0,"id":"MSG-42","peer":"bob","#,
                r#""direction":"outgoing","body":"on my way","at_ms":1767225600000,"#,
                r#""status":{"type":"sending"}}}"#,
            ),
        ),
        (
            Response::Log {
                page: LogPage {
                    messages: vec![
                        incoming(41),
                        outgoing(
                            42,
                            1,
                            MessageStatus::Failed {
                                reason: FailReason::NoAnswer,
                            },
                        ),
                    ],
                    next_before: Some(41),
                },
            },
            concat!(
                r#"{"type":"log","page":{"messages":["#,
                r#"{"seq":41,"rev":0,"id":"MSG-41","peer":"bob","direction":"incoming","#,
                r#""body":"where are you?","at_ms":1767225500000,"status":{"type":"received"}},"#,
                r#"{"seq":42,"rev":1,"id":"MSG-42","peer":"bob","direction":"outgoing","#,
                r#""body":"on my way","at_ms":1767225600000,"#,
                r#""status":{"type":"failed","reason":"no_answer"}}],"next_before":41}}"#,
            ),
        ),
        (Response::Done, r#"{"type":"done"}"#),
    ]
}

/// One value of every `Event` variant, each with its exact JSON text.
fn events() -> Vec<(Event, &'static str)> {
    vec![
        (Event::Resync, r#"{"type":"resync"}"#),
        (
            Event::Message {
                message: delivered(),
            },
            concat!(
                r#"{"type":"message","message":{"seq":42,"rev":1,"id":"MSG-42","peer":"bob","#,
                r#""direction":"outgoing","body":"on my way","at_ms":1767225600000,"#,
                r#""status":{"type":"delivered"}}}"#,
            ),
        ),
        (
            Event::Peer {
                peer: peer(&[("STATION-B", Link::Direct)]),
            },
            r#"{"type":"peer","peer":{"name":"bob","stations":[{"station_id":"STATION-B","link":"direct"}]}}"#,
        ),
        (
            Event::PeerRemoved { name: "bob".into() },
            r#"{"type":"peer_removed","name":"bob"}"#,
        ),
        (
            Event::Status {
                status: status(vec![
                    tower(3340, TowerState::Connecting),
                    tower(
                        3341,
                        TowerState::Denied {
                            reason: "not on the allowlist".into(),
                        },
                    ),
                ]),
            },
            concat!(
                r#"{"type":"status","status":{"station_id":"STATION-A","instance_id":"INSTANCE-1","#,
                r#""towers":[{"url":"http://127.0.0.1:3340/","state":{"type":"connecting"}},"#,
                r#"{"url":"http://127.0.0.1:3341/","state":{"type":"denied","reason":"not on the allowlist"}}]}}"#,
            ),
        ),
    ]
}

/// One value of every `ApiError` variant.
fn errors() -> Vec<ApiError> {
    vec![
        ApiError::PeerNotFound { name: "bob".into() },
        ApiError::StationTaken {
            station_id: "STATION-B".into(),
            peer: "robert".into(),
        },
        ApiError::MessageNotFound { seq: 7 },
        ApiError::Invalid {
            field: "body".into(),
            reason: "must not be empty".into(),
        },
        ApiError::Stopping,
        ApiError::Internal {
            detail: "writing the peer book failed".into(),
        },
        ApiError::BadRequest {
            detail: "unknown variant `frobnicate`".into(),
        },
        ApiError::NotRunning {
            socket: "/tmp/test/station.sock".into(),
        },
        ApiError::IncompatibleApi {
            station: 2,
            client: 1,
        },
        ApiError::Disconnected {
            detail: "connection reset".into(),
        },
        ApiError::Timeout { after_ms: 10_000 },
        ApiError::BadReply {
            detail: "frame too large".into(),
        },
    ]
}

fn tower_states() -> Vec<TowerState> {
    vec![
        TowerState::Connecting,
        TowerState::Connected,
        TowerState::Failing {
            error: "connection refused".into(),
        },
        TowerState::Denied {
            reason: "not on the allowlist".into(),
        },
        TowerState::Standby,
    ]
}

fn message_statuses() -> Vec<MessageStatus> {
    let mut all = vec![
        MessageStatus::Sending,
        MessageStatus::Delivered,
        MessageStatus::Received,
    ];
    all.extend(fail_reasons().map(|reason| MessageStatus::Failed { reason }));
    all
}

fn links() -> [Link; 3] {
    [Link::Down, Link::Relayed, Link::Direct]
}

fn directions() -> [Direction; 2] {
    [Direction::Incoming, Direction::Outgoing]
}

fn fail_reasons() -> [FailReason; 3] {
    [
        FailReason::NoAnswer,
        FailReason::Refused,
        FailReason::PeerRemoved,
    ]
}

// Exhaustive matches: a new variant does not compile until it is numbered here, and the coverage
// tests then fail until the samples above hold a value of it.

const REQUEST_VARIANTS: usize = 7;
fn request_variant(value: &Request) -> usize {
    match value {
        Request::Status => 0,
        Request::Peers => 1,
        Request::PeerAdd { .. } => 2,
        Request::PeerRemove { .. } => 3,
        Request::Send { .. } => 4,
        Request::Message { .. } => 5,
        Request::Log { .. } => 6,
    }
}

const RESPONSE_VARIANTS: usize = 6;
fn response_variant(value: &Response) -> usize {
    match value {
        Response::Status { .. } => 0,
        Response::Peers { .. } => 1,
        Response::Peer { .. } => 2,
        Response::Message { .. } => 3,
        Response::Log { .. } => 4,
        Response::Done => 5,
    }
}

const EVENT_VARIANTS: usize = 5;
fn event_variant(value: &Event) -> usize {
    match value {
        Event::Resync => 0,
        Event::Message { .. } => 1,
        Event::Peer { .. } => 2,
        Event::PeerRemoved { .. } => 3,
        Event::Status { .. } => 4,
    }
}

const ERROR_VARIANTS: usize = 12;
fn error_variant(value: &ApiError) -> usize {
    match value {
        ApiError::PeerNotFound { .. } => 0,
        ApiError::StationTaken { .. } => 1,
        ApiError::MessageNotFound { .. } => 2,
        ApiError::Invalid { .. } => 3,
        ApiError::Stopping => 4,
        ApiError::Internal { .. } => 5,
        ApiError::BadRequest { .. } => 6,
        ApiError::NotRunning { .. } => 7,
        ApiError::IncompatibleApi { .. } => 8,
        ApiError::Disconnected { .. } => 9,
        ApiError::Timeout { .. } => 10,
        ApiError::BadReply { .. } => 11,
    }
}

const TOWER_STATE_VARIANTS: usize = 5;
fn tower_state_variant(value: &TowerState) -> usize {
    match value {
        TowerState::Connecting => 0,
        TowerState::Connected => 1,
        TowerState::Failing { .. } => 2,
        TowerState::Denied { .. } => 3,
        TowerState::Standby => 4,
    }
}

/// Counts `Failed` once per reason.
const MESSAGE_STATUS_VARIANTS: usize = 6;
fn message_status_variant(value: &MessageStatus) -> usize {
    match value {
        MessageStatus::Sending => 0,
        MessageStatus::Delivered => 1,
        MessageStatus::Received => 2,
        MessageStatus::Failed { reason } => match reason {
            FailReason::NoAnswer => 3,
            FailReason::Refused => 4,
            FailReason::PeerRemoved => 5,
        },
    }
}

const LINK_VARIANTS: usize = 3;
fn link_variant(value: &Link) -> usize {
    match value {
        Link::Down => 0,
        Link::Relayed => 1,
        Link::Direct => 2,
    }
}

const DIRECTION_VARIANTS: usize = 2;
fn direction_variant(value: &Direction) -> usize {
    match value {
        Direction::Incoming => 0,
        Direction::Outgoing => 1,
    }
}

const FAIL_REASON_VARIANTS: usize = 3;
fn fail_reason_variant(value: &FailReason) -> usize {
    match value {
        FailReason::NoAnswer => 0,
        FailReason::Refused => 1,
        FailReason::PeerRemoved => 2,
    }
}

// Helpers.

/// Serializes `value`, reads it back, checks it is equal, and returns the JSON text.
fn round_trip<T: Serialize + DeserializeOwned + PartialEq + Debug>(value: &T) -> String {
    let text = serde_json::to_string(value).unwrap();
    let back: T = serde_json::from_str(&text).unwrap();
    assert_eq!(&back, value, "round trip through {text}");
    text
}

/// Round-trips every sample and checks that the samples hold every variant.
fn round_trip_all<T: Serialize + DeserializeOwned + PartialEq + Debug>(
    samples: &[T],
    variant: fn(&T) -> usize,
    variants: usize,
) {
    let mut seen = vec![false; variants];
    for sample in samples {
        round_trip(sample);
        seen[variant(sample)] = true;
    }
    let missing: Vec<usize> = (0..variants).filter(|i| !seen[*i]).collect();
    assert!(missing.is_empty(), "no sample of variants {missing:?}");
}

fn values<T: Clone>(pairs: &[(T, &str)]) -> Vec<T> {
    pairs.iter().map(|(value, _)| value.clone()).collect()
}

// 1. Round trips.

#[test]
fn every_variant_round_trips() {
    round_trip_all(&values(&requests()), request_variant, REQUEST_VARIANTS);
    round_trip_all(&values(&responses()), response_variant, RESPONSE_VARIANTS);
    round_trip_all(&values(&events()), event_variant, EVENT_VARIANTS);
    round_trip_all(&errors(), error_variant, ERROR_VARIANTS);
}

#[test]
fn every_state_value_round_trips() {
    round_trip_all(&tower_states(), tower_state_variant, TOWER_STATE_VARIANTS);
    round_trip_all(
        &message_statuses(),
        message_status_variant,
        MESSAGE_STATUS_VARIANTS,
    );
    round_trip_all(&links(), link_variant, LINK_VARIANTS);
    round_trip_all(&directions(), direction_variant, DIRECTION_VARIANTS);
    round_trip_all(&fail_reasons(), fail_reason_variant, FAIL_REASON_VARIANTS);
}

#[test]
fn numbers_keep_their_full_range() {
    let request = Request::Message { seq: u64::MAX };
    assert_eq!(
        round_trip(&request),
        r#"{"type":"message","seq":18446744073709551615}"#
    );
    let error = ApiError::IncompatibleApi {
        station: u32::MAX,
        client: 0,
    };
    round_trip(&error);
}

// 2. Golden JSON.

fn assert_golden<T: Serialize + DeserializeOwned + PartialEq + Debug>(pairs: &[(T, &str)]) {
    for (value, text) in pairs {
        assert_eq!(serde_json::to_string(value).unwrap(), *text);
        assert_eq!(&serde_json::from_str::<T>(text).unwrap(), value);
    }
}

#[test]
fn requests_have_this_json() {
    assert_golden(&requests());
}

#[test]
fn responses_have_this_json() {
    assert_golden(&responses());
}

#[test]
fn events_have_this_json() {
    assert_golden(&events());
}

#[test]
fn errors_have_this_json() {
    assert_golden(&[
        (
            ApiError::PeerNotFound { name: "bob".into() },
            r#"{"type":"peer_not_found","name":"bob"}"#,
        ),
        (
            ApiError::Invalid {
                field: "body".into(),
                reason: "must not be empty".into(),
            },
            r#"{"type":"invalid","field":"body","reason":"must not be empty"}"#,
        ),
        (ApiError::Stopping, r#"{"type":"stopping"}"#),
        (
            ApiError::IncompatibleApi {
                station: 2,
                client: 1,
            },
            r#"{"type":"incompatible_api","station":2,"client":1}"#,
        ),
    ]);
}

#[test]
fn label_enums_are_bare_strings() {
    let texts: Vec<String> = links().iter().map(round_trip).collect();
    assert_eq!(texts, [r#""down""#, r#""relayed""#, r#""direct""#]);
    let texts: Vec<String> = directions().iter().map(round_trip).collect();
    assert_eq!(texts, [r#""incoming""#, r#""outgoing""#]);
    let texts: Vec<String> = fail_reasons().iter().map(round_trip).collect();
    assert_eq!(
        texts,
        [r#""no_answer""#, r#""refused""#, r#""peer_removed""#]
    );
    assert_golden(&[
        (TowerSource::Configured, r#""configured""#),
        (TowerSource::Remembered, r#""remembered""#),
        (TowerSource::Srv, r#""srv""#),
        (TowerSource::Peer, r#""peer""#),
    ]);
}

#[test]
fn status_has_observed_and_standby_towers_with_optional_metadata() {
    let mut observed = tower(3340, TowerState::Connected);
    observed.source = Some(TowerSource::Configured);
    let unobserved = TowerStatus {
        url: "https://relay.team.example.invalid/".into(),
        state: TowerState::Standby,
        source: Some(TowerSource::Srv),
    };
    let mut snapshot = status(vec![observed, unobserved]);
    snapshot.bootstrap_domain = Some("team.example.invalid".into());
    let response = Response::Status { status: snapshot };
    assert_golden(&[(
        response,
        concat!(
            r#"{"type":"status","status":{"station_id":"STATION-A","instance_id":"INSTANCE-1","towers":["#,
            r#"{"url":"http://127.0.0.1:3340/","state":{"type":"connected"},"source":"configured"},"#,
            r#"{"url":"https://relay.team.example.invalid/","state":{"type":"standby"},"source":"srv"}],"#,
            r#""bootstrap_domain":"team.example.invalid"}}"#,
        ),
    )]);
}

// 3. What decoding accepts and refuses.

#[test]
fn unknown_fields_are_ignored_at_every_level() {
    let request: Request =
        serde_json::from_value(json!({"type":"send","peer":"bob","body":"x","urgent":true}))
            .unwrap();
    assert_eq!(
        request,
        Request::Send {
            peer: "bob".into(),
            body: "x".into()
        }
    );

    let response: Response = serde_json::from_value(json!({
        "type": "status",
        "extra": 1,
        "status": {
            "station_id": "STATION-A",
            "instance_id": "INSTANCE-1",
            "started_at_ms": 5,
            "towers": [{
                "url": "http://127.0.0.1:3340/",
                "rtt_ms": 12,
                "state": {"type": "failing", "error": "e", "since_ms": 3}
            }]
        }
    }))
    .unwrap();
    assert_eq!(
        response,
        Response::Status {
            status: status(vec![tower(3340, TowerState::Failing { error: "e".into() })])
        }
    );

    let event: Event = serde_json::from_value(json!({
        "type": "message",
        "extra": [1, 2],
        "message": {
            "seq": 42, "rev": 1, "id": "MSG-42", "peer": "bob", "direction": "outgoing",
            "body": "on my way", "at_ms": AT_MS, "channel": "general",
            "status": {"type": "delivered", "at_ms": 7}
        }
    }))
    .unwrap();
    assert_eq!(
        event,
        Event::Message {
            message: delivered()
        }
    );

    let event: Event = serde_json::from_value(json!({
        "type": "peer",
        "peer": {"name": "bob", "chief_id": "CHIEF-1",
                 "stations": [{"station_id": "STATION-B", "link": "direct", "rtt_ms": 3}]}
    }))
    .unwrap();
    assert_eq!(
        event,
        Event::Peer {
            peer: peer(&[("STATION-B", Link::Direct)])
        }
    );

    let page: Response = serde_json::from_value(json!({
        "type": "log", "page": {"messages": [], "next_before": 3, "next": 3}
    }))
    .unwrap();
    assert_eq!(
        page,
        Response::Log {
            page: LogPage {
                messages: vec![],
                next_before: Some(3)
            }
        }
    );

    let error: ApiError =
        serde_json::from_value(json!({"type":"timeout","after_ms":5,"hint":"x"})).unwrap();
    assert_eq!(error, ApiError::Timeout { after_ms: 5 });
}

#[test]
fn an_unknown_type_does_not_decode() {
    let unknown = json!({"type": "frobnicate"});
    let with_fields = json!({"type": "frobnicate", "name": "bob"});
    for value in [&unknown, &with_fields] {
        assert!(serde_json::from_value::<Request>(value.clone()).is_err());
        assert!(serde_json::from_value::<Response>(value.clone()).is_err());
        assert!(serde_json::from_value::<Event>(value.clone()).is_err());
        assert!(serde_json::from_value::<ApiError>(value.clone()).is_err());
        assert!(serde_json::from_value::<TowerState>(value.clone()).is_err());
        assert!(serde_json::from_value::<MessageStatus>(value.clone()).is_err());
    }
    assert!(serde_json::from_value::<Link>(json!("connecting")).is_err());
    assert!(serde_json::from_value::<Direction>(json!("sideways")).is_err());
    assert!(serde_json::from_value::<FailReason>(json!("quota")).is_err());
    assert!(serde_json::from_value::<TowerSource>(json!("unknown")).is_err());

    // The decoder's message names the unknown variant: the socket server passes it on in
    // `ApiError::BadRequest`.
    let error = serde_json::from_value::<Request>(unknown).unwrap_err();
    assert!(error.to_string().contains("frobnicate"), "{error}");
}

#[test]
fn a_missing_tag_or_required_field_does_not_decode() {
    assert!(serde_json::from_value::<Request>(json!({"peer": "bob", "body": "x"})).is_err());
    assert!(serde_json::from_value::<Request>(json!({"type": "send", "peer": "bob"})).is_err());
    assert!(serde_json::from_value::<Request>(json!({"type": "log"})).is_err());
    assert!(serde_json::from_value::<Request>(json!({"type": "message", "seq": -1})).is_err());
}

#[test]
fn a_missing_option_reads_as_none_and_none_is_not_written() {
    let none = Request::Log {
        peer: None,
        before: None,
        limit: 50,
    };
    let read: Request = serde_json::from_value(json!({"type": "log", "limit": 50})).unwrap();
    assert_eq!(read, none);
    let read: Request =
        serde_json::from_value(json!({"type": "log", "limit": 50, "peer": null, "before": null}))
            .unwrap();
    assert_eq!(read, none);
    let written: Value = serde_json::to_value(&none).unwrap();
    assert_eq!(written, json!({"type": "log", "limit": 50}));
}

#[test]
fn a_page_without_a_cursor_reads_as_none_and_none_is_not_written() {
    let last = Response::Log {
        page: LogPage {
            messages: vec![],
            next_before: None,
        },
    };
    let read: Response =
        serde_json::from_value(json!({"type": "log", "page": {"messages": []}})).unwrap();
    assert_eq!(read, last);
    let written: Value = serde_json::to_value(&last).unwrap();
    assert_eq!(written, json!({"type": "log", "page": {"messages": []}}));
}

#[test]
fn legacy_status_and_null_metadata_read_as_none() {
    let legacy = json!({
        "station_id": "STATION-A",
        "instance_id": "INSTANCE-1",
        "towers": [{"url": "http://127.0.0.1:3340/", "state": {"type": "connected"}}]
    });
    let expected = status(vec![tower(3340, TowerState::Connected)]);
    assert_eq!(
        serde_json::from_value::<Status>(legacy.clone()).unwrap(),
        expected
    );

    let mut with_null = legacy.clone();
    with_null["bootstrap_domain"] = Value::Null;
    with_null["towers"][0]["source"] = Value::Null;
    assert_eq!(
        serde_json::from_value::<Status>(with_null).unwrap(),
        expected
    );
    assert_eq!(serde_json::to_value(&expected).unwrap(), legacy);
}

// Display texts, which frontends show as they are.

#[test]
fn errors_display_as_sentences() {
    let texts: Vec<String> = errors().iter().map(ToString::to_string).collect();
    assert_eq!(
        texts,
        [
            "no peer named bob",
            "station STATION-B already belongs to peer robert",
            "message 7 is not in the station's log",
            "invalid body: must not be empty",
            "the station is shutting down",
            "station error: writing the peer book failed",
            "the station could not read the request: unknown variant `frobnicate`",
            "no station is running (nothing listens on /tmp/test/station.sock)",
            "the station speaks API version 2, this chief speaks 1",
            "lost the connection to the station: connection reset",
            "the station did not answer within 10000 ms",
            "unexpected reply from the station: frame too large",
        ]
    );
}

#[test]
fn fail_reasons_follow_the_peer_name() {
    let texts: Vec<String> = fail_reasons()
        .iter()
        .map(|reason| format!("bob {reason}"))
        .collect();
    assert_eq!(
        texts,
        [
            "bob appears offline, or has not added you",
            "bob refused the message",
            "bob was removed before the message was delivered",
        ]
    );
}

#[test]
fn api_version_is_two_and_exported_at_the_root() {
    assert_eq!(chief::API_VERSION, 2);
    assert_eq!(chief::API_VERSION, chief::api::API_VERSION);
}

// 4. Peer names.

#[test]
fn peer_names_that_pass() {
    let longest = "a".repeat(PEER_NAME_MAX_BYTES);
    for name in [
        "bob",
        "a",
        "7",
        "Bob",
        "bob.laptop",
        "bob_2",
        "bob-work",
        "0.a_b-c",
        "bob-",
        "bob.",
        longest.as_str(),
    ] {
        assert_eq!(check_peer_name(name), Ok(()), "{name:?}");
    }
}

#[test]
fn peer_names_that_fail() {
    let too_long = "a".repeat(PEER_NAME_MAX_BYTES + 1);
    // 32 bytes, but not ASCII.
    let non_ascii_at_the_limit = "\u{e9}".repeat(PEER_NAME_MAX_BYTES / 2);
    for name in [
        "",
        too_long.as_str(),
        "-bob",
        ".bob",
        "_bob",
        "bo b",
        " bob",
        "bob\n",
        "bo\u{7}b",
        "bo\u{1b}[31mb",
        "b\u{f6}b",
        non_ascii_at_the_limit.as_str(),
        "bob/x",
        "bob:x",
        "bob@x",
    ] {
        match check_peer_name(name) {
            Err(ApiError::Invalid { field, .. }) => assert_eq!(field, "name", "{name:?}"),
            other => panic!("{name:?} gave {other:?}"),
        }
    }
}

#[test]
fn peer_name_errors_say_what_is_wrong() {
    let reason = |name: &str| match check_peer_name(name) {
        Err(ApiError::Invalid { reason, .. }) => reason,
        other => panic!("{name:?} gave {other:?}"),
    };
    assert_eq!(reason(""), "must be 1 to 32 characters");
    assert_eq!(reason("-bob"), "must start with a letter or a digit");
    assert_eq!(
        reason("bo b"),
        "may contain only letters, digits, '.', '_' and '-'"
    );
}
