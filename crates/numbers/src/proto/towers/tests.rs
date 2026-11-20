use numbers_base::{FrameError, WIRE_FRAME_MAX, encode_frame, read_frame};
use serde_json::json;

use super::{TowerAnnouncement, WireTowerAnnouncement};
use crate::{MAX_TOWERS, Qad, TowerEntry, TowerSet};

#[tokio::test]
async fn frames_round_trip_empty_and_full_ordered_announcements() {
    let entries = (0..MAX_TOWERS)
        .rev()
        .map(|index| {
            let entry = TowerEntry::from_url(&format!("https://tower-{index}.invalid/")).unwrap();
            match index {
                0 => entry.with_qad_port(u16::MAX).unwrap(),
                1 => entry.without_qad(),
                _ => entry,
            }
        })
        .collect::<Vec<_>>();
    let full = TowerSet::from_entries(entries.clone()).unwrap();
    for (set, expected) in [(&TowerSet::default(), &[][..]), (&full, entries.as_slice())] {
        let outgoing = TowerAnnouncement::from_working_set(set);
        let wire = WireTowerAnnouncement::from(&outgoing);
        let bytes = encode_frame(&wire, WIRE_FRAME_MAX).unwrap();
        let mut reader = bytes.as_slice();
        let incoming: WireTowerAnnouncement =
            read_frame(&mut reader, WIRE_FRAME_MAX, None).await.unwrap();
        let decoded = TowerAnnouncement::try_from(incoming).unwrap();
        assert_eq!(decoded.entries(), expected);
        assert!(reader.is_empty());
    }

    let wire = WireTowerAnnouncement::from(&TowerAnnouncement::from_working_set(&full));
    let json = serde_json::to_value(wire).unwrap();
    assert_eq!(json["towers"][0]["qad_port"], json!(7842));
    assert_eq!(json["towers"][MAX_TOWERS - 2]["qad_port"], json!(null));
    assert_eq!(json["towers"][MAX_TOWERS - 1]["qad_port"], json!(65535));
}

#[test]
fn working_set_projection_keeps_only_https_entries_in_order() {
    let first = TowerEntry::from_url("https://first.invalid/").unwrap();
    let plain = TowerEntry::from_url("http://127.0.0.1:3340/").unwrap();
    let second = TowerEntry::from_url("https://second.invalid/")
        .unwrap()
        .without_qad();
    let set = TowerSet::from_entries([first.clone(), plain, second.clone()]).unwrap();
    assert_eq!(
        TowerAnnouncement::from_working_set(&set).entries(),
        &[first, second]
    );
}

#[test]
fn omitted_and_null_qad_are_off_and_extra_fields_are_tolerated() {
    let wire: WireTowerAnnouncement = serde_json::from_value(json!({
        "towers": [
            {"url": "https://first.invalid/", "extra": [1, 2]},
            {"url": "https://second.invalid/", "qad_port": null}
        ],
        "extra": {"future": true}
    }))
    .unwrap();
    let decoded = TowerAnnouncement::try_from(wire).unwrap();
    assert_eq!(
        decoded
            .entries()
            .iter()
            .map(TowerEntry::qad)
            .collect::<Vec<_>>(),
        [Qad::Off; 2]
    );
}

#[test]
fn one_invalid_entry_rejects_the_whole_announcement() {
    for (url, port) in [
        ("https://SECOND.invalid/", None),
        ("https://second.invalid:443/", None),
        ("https://first.invalid/", None),
        ("http://127.0.0.1:3340/", None),
        ("https://second.invalid/child", None),
        ("https://second.invalid/", Some(0)),
        ("not a URL", None),
    ] {
        let wire: WireTowerAnnouncement = serde_json::from_value(json!({
            "towers": [
                {"url": "https://first.invalid/", "qad_port": 7842},
                {"url": url, "qad_port": port}
            ]
        }))
        .unwrap();
        assert!(TowerAnnouncement::try_from(wire).is_err());
    }

    let towers = (0..=MAX_TOWERS)
        .map(|index| json!({"url": format!("https://tower-{index}.invalid/")}))
        .collect::<Vec<_>>();
    let wire: WireTowerAnnouncement = serde_json::from_value(json!({"towers": towers})).unwrap();
    assert!(TowerAnnouncement::try_from(wire).is_err());
}

#[test]
fn malformed_and_out_of_range_values_do_not_decode() {
    for json in [
        r#"{"towers":["#,
        r#"[[]]"#,
        r#"{"towers":{}}"#,
        r#"{"towers":[["https://first.invalid/",7842]]}"#,
        r#"{"towers":[{"qad_port":7842}]}"#,
        r#"{"towers":[{"url":"https://first.invalid/","qad_port":"7842"}]}"#,
        r#"{"towers":[{"url":"https://first.invalid/","qad_port":-1}]}"#,
        r#"{"towers":[{"url":"https://first.invalid/","qad_port":65536}]}"#,
    ] {
        assert!(serde_json::from_str::<WireTowerAnnouncement>(json).is_err());
    }
}

#[tokio::test]
async fn wire_cap_rejects_the_next_byte_before_reading_a_body() {
    let mut at_cap = u32::try_from(WIRE_FRAME_MAX)
        .unwrap()
        .to_be_bytes()
        .to_vec();
    at_cap.extend_from_slice(br#"{"towers":[]}"#);
    at_cap.resize(4 + WIRE_FRAME_MAX, b' ');
    let wire: WireTowerAnnouncement = read_frame(&mut at_cap.as_slice(), WIRE_FRAME_MAX, None)
        .await
        .unwrap();
    assert!(
        TowerAnnouncement::try_from(wire)
            .unwrap()
            .entries()
            .is_empty()
    );

    let length_only = u32::try_from(WIRE_FRAME_MAX + 1).unwrap().to_be_bytes();
    assert!(matches!(
        read_frame::<_, WireTowerAnnouncement>(&mut length_only.as_slice(), WIRE_FRAME_MAX, None)
            .await,
        Err(FrameError::TooLarge {
            cap: WIRE_FRAME_MAX,
            ..
        })
    ));
}
