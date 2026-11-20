use serde_json::json;

use super::WireHelloEcho;
use crate::proto::HelloEcho;

#[test]
fn an_absent_or_false_claim_uses_the_existing_echo() {
    let echo = [1; 16];
    let old = serde_json::to_value(HelloEcho { echo }).unwrap();
    let new = serde_json::to_value(WireHelloEcho {
        echo,
        no_open_lower_initiated_link: false,
    })
    .unwrap();
    assert_eq!(new, old);

    for value in [
        old,
        json!({"echo": echo, "no_open_lower_initiated_link": false, "future": [1, 2]}),
    ] {
        let response: WireHelloEcho = serde_json::from_value(value).unwrap();
        assert_eq!(response.echo, echo);
        assert!(!response.no_open_lower_initiated_link);
    }
}

#[test]
fn an_affirmative_is_read_by_new_peers_and_ignored_by_old_peers() {
    let echo = [1; 16];
    let mut value = serde_json::to_value(WireHelloEcho {
        echo,
        no_open_lower_initiated_link: true,
    })
    .unwrap();
    assert_eq!(
        value,
        json!({"echo": echo, "no_open_lower_initiated_link": true})
    );
    value["future"] = json!({"extra": true});

    let old: HelloEcho = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(old.echo, echo);
    let new: WireHelloEcho = serde_json::from_value(value).unwrap();
    assert_eq!(new.echo, echo);
    assert!(new.no_open_lower_initiated_link);
}
