#![cfg(test)]
#![cfg(unix)]
//! The local socket's envelope (`chief::socket::wire`): the exact JSON of every variant, round
//! trips, and what decoding accepts and refuses. Ids are placeholders (AGENTS.md, hard rule 9).

use chief::api::{ApiError, Request, Response};
use chief::socket::wire::*;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::fmt::Debug;

/// Asserts the exact JSON text of `value` and that it reads back as `value`.
fn golden<T: Serialize + DeserializeOwned + PartialEq + Debug>(value: &T, json: &str) {
    assert_eq!(serde_json::to_string(value).unwrap(), json);
    assert_eq!(&serde_json::from_str::<T>(json).unwrap(), value);
}

#[test]
fn hello_has_this_json() {
    golden(&Hello { api: 1 }, r#"{"api":1}"#);
}

#[test]
fn hello_reply_has_this_json() {
    golden(
        &HelloReply {
            api: 1,
            station_id: "STATION-A".into(),
            instance_id: "INSTANCE-1".into(),
        },
        r#"{"api":1,"station_id":"STATION-A","instance_id":"INSTANCE-1"}"#,
    );
}

#[test]
fn open_has_this_json() {
    golden(
        &Open::Call {
            request: Request::Status,
        },
        r#"{"type":"call","request":{"type":"status"}}"#,
    );
    golden(
        &Open::Call {
            request: Request::Log {
                peer: None,
                before: Some(7),
                limit: 50,
            },
        },
        r#"{"type":"call","request":{"type":"log","before":7,"limit":50}}"#,
    );
    golden(&Open::Subscribe, r#"{"type":"subscribe"}"#);
}

#[test]
fn reply_has_this_json() {
    golden(
        &Reply::Ok {
            response: Response::Done,
        },
        r#"{"type":"ok","response":{"type":"done"}}"#,
    );
    golden(&Reply::Subscribed, r#"{"type":"subscribed"}"#);
    golden(
        &Reply::Err {
            error: ApiError::PeerNotFound { name: "bob".into() },
        },
        r#"{"type":"err","error":{"type":"peer_not_found","name":"bob"}}"#,
    );
}

#[test]
fn unknown_fields_are_ignored() {
    assert_eq!(
        serde_json::from_str::<Hello>(r#"{"api":2,"features":["x"]}"#).unwrap(),
        Hello { api: 2 }
    );
    assert_eq!(
        serde_json::from_str::<Open>(r#"{"type":"subscribe","from":3}"#).unwrap(),
        Open::Subscribe
    );
    assert_eq!(
        serde_json::from_str::<Reply>(r#"{"type":"subscribed","extra":null}"#).unwrap(),
        Reply::Subscribed
    );
}

#[test]
fn unknown_variants_and_missing_fields_are_refused() {
    assert!(serde_json::from_str::<Open>(r#"{"type":"stream"}"#).is_err());
    assert!(serde_json::from_str::<Open>(r#"{"type":"call"}"#).is_err());
    assert!(serde_json::from_str::<Open>(r#"{"type":"call","request":{"type":"nope"}}"#).is_err());
    assert!(serde_json::from_str::<Reply>(r#"{"type":"ok"}"#).is_err());
    assert!(serde_json::from_str::<Hello>(r#"{}"#).is_err());
    assert!(serde_json::from_str::<HelloReply>(r#"{"api":1}"#).is_err());
}

#[test]
fn a_decode_error_detail_never_quotes_the_input() {
    let json = r#"{"type":"ok","response":{"type":"message","message":"MARKER-SECRET"}}"#;
    let error = serde_json::from_str::<Reply>(json).unwrap_err();
    // serde's own text quotes the value: the reason for the helper.
    assert!(error.to_string().contains("MARKER"), "{error}");
    let detail = chief::socket::json_error_detail(&error);
    assert!(!detail.contains("MARKER"), "{detail}");
    assert!(detail.contains("expected fields or types"), "{detail}");
    let syntax = serde_json::from_str::<Reply>("{not json").unwrap_err();
    let detail = chief::socket::json_error_detail(&syntax);
    assert!(detail.contains("not valid JSON"), "{detail}");
    assert!(detail.contains("(line 1, column 2)"), "{detail}");
    let eof = serde_json::from_str::<Reply>(r#"{"type":"#).unwrap_err();
    assert!(chief::socket::json_error_detail(&eof).contains("ends in the middle"));
}
