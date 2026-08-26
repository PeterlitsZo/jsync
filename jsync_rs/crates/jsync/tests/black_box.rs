use jsync::{Action, Consumer, Message, PathSegment, Producer};
use serde_json::{Value, json};

#[test]
fn producer_messages_keep_consumer_in_sync() {
    let initial = json!({
        "revision": 0,
        "profile": {"name": "Ada", "active": true},
        "items": ["alpha", "beta"],
        "obsolete": "remove me",
    });

    struct UpdateCase {
        to_update: Value,
        expected_message: Message,
        expected_message_bytes_len: usize,
    }

    let updates: Vec<UpdateCase> = vec![
        UpdateCase {
            to_update: json!({
                "revision": 1,
                "profile": {"name": "Ada Lovelace", "active": true},
                "items": ["alpha", "beta", "gamma"],
                "obsolete": "remove me",
            }),
            expected_message: Message::new(vec![
                Action::Add {
                    path: vec![PathSegment::Key("items".to_string()), PathSegment::Index(2)],
                    value: json!("gamma"),
                },
                Action::Append {
                    path: vec![
                        PathSegment::Key("profile".to_string()),
                        PathSegment::Key("name".to_string()),
                    ],
                    text: " Lovelace".to_string(),
                },
                Action::Replace {
                    path: vec![PathSegment::Key("revision".to_string())],
                    value: json!(1),
                },
            ]),
            expected_message_bytes_len: 59,
        },
        UpdateCase {
            to_update: json!({
                "revision": 2,
                "profile": {"name": "Countess Ada Lovelace", "active": false},
                "items": ["alpha", "beta", "gamma"],
                "tags": ["math", "programming"],
            }),
            expected_message: Message::new(vec![
                Action::Remove {
                    path: vec![PathSegment::Key("obsolete".to_string())],
                },
                Action::Replace {
                    path: vec![
                        PathSegment::Key("profile".to_string()),
                        PathSegment::Key("active".to_string()),
                    ],
                    value: json!(false),
                },
                Action::Prepend {
                    path: vec![
                        PathSegment::Key("profile".to_string()),
                        PathSegment::Key("name".to_string()),
                    ],
                    text: "Countess ".to_string(),
                },
                Action::Replace {
                    path: vec![PathSegment::Key("revision".to_string())],
                    value: json!(2),
                },
                Action::Add {
                    path: vec![PathSegment::Key("tags".to_string())],
                    value: json!(["math", "programming"]),
                },
            ]),
            expected_message_bytes_len: 100,
        },
        UpdateCase {
            to_update: json!({
                "revision": 3,
                "profile": {"name": "Countess Ada Lovelace", "active": false},
                "items": ["gamma"],
                "tags": ["math"],
            }),
            expected_message: Message::new(vec![
                Action::Replace {
                    path: vec![PathSegment::Key("items".to_string())],
                    value: json!(["gamma"]),
                },
                Action::Replace {
                    path: vec![PathSegment::Key("revision".to_string())],
                    value: json!(3),
                },
                Action::Remove {
                    path: vec![PathSegment::Key("tags".to_string()), PathSegment::Index(1)],
                },
            ]),
            expected_message_bytes_len: 42,
        },
        UpdateCase {
            to_update: json!(["root replacement", {"revision": 4}, [1, 2, 3]]),
            expected_message: Message::new(vec![Action::Replace {
                path: vec![],
                value: json!(["root replacement", {"revision": 4}, [1, 2, 3]]),
            }]),
            expected_message_bytes_len: 40,
        },
        UpdateCase {
            to_update: json!({
                "revision": 5,
                "profile": {"name": "Grace", "active": true},
                "items": ["delta", "epsilon"],
                "tags": ["systems"],
            }),
            expected_message: Message::new(vec![Action::Replace {
                path: vec![],
                value: json!({
                    "revision": 5,
                    "profile": {"name": "Grace", "active": true},
                    "items": ["delta", "epsilon"],
                    "tags": ["systems"],
                }),
            }]),
            expected_message_bytes_len: 81,
        },
    ];

    let mut producer = Producer::new(initial.clone());
    let mut consumer = Consumer::new();

    let initial_message = producer
        .get_message()
        .expect("initial message should be produced")
        .expect("initial snapshot should exist");
    assert_eq!(
        Message::from_bytes(initial_message.clone()).expect("initial message should decode"),
        Message::new(vec![Action::Snapshot {
            value: initial.clone(),
        }])
    );
    consumer
        .consume(&initial_message)
        .expect("consumer should accept the initial message");

    for update in updates {
        producer.update(update.to_update);
        let message = producer
            .get_message()
            .expect("producer message should be encoded")
            .expect("must have message to sync");
        assert_eq!(
            Message::from_bytes(message.clone()).expect("producer message should decode"),
            update.expected_message
        );
        assert_eq!(
            message.len(),
            update.expected_message_bytes_len,
            "message length should match expected length"
        );
        consumer
            .consume(&message)
            .expect("consumer should accept the producer message");
    }

    assert_eq!(consumer.document(), Some(producer.document()));
}

#[test]
fn producer_replaces_object_subtree_when_it_is_smaller() {
    let initial = json!({
        "wrapper": {"a": 0, "b": 0, "c": 0, "d": 0, "e": 0},
        "unchanged": true,
    });
    let mut producer = Producer::new(initial);
    producer
        .get_message()
        .expect("initial message should be encoded")
        .expect("initial snapshot should exist");

    producer.update(json!({
        "wrapper": {"a": 1, "b": 1, "c": 1, "d": 1, "e": 1},
        "unchanged": true,
    }));
    let message = producer
        .get_message()
        .expect("producer message should be encoded")
        .expect("must have message to sync");

    assert_eq!(
        Message::from_bytes(message).expect("producer message should decode"),
        Message::new(vec![Action::Replace {
            path: vec![PathSegment::Key("wrapper".to_string())],
            value: json!({"a": 1, "b": 1, "c": 1, "d": 1, "e": 1}),
        }])
    );
}

#[test]
fn copy_and_move_messages_round_trip() {
    let message = Message::new(vec![
        Action::Copy {
            from: vec![key("source")],
            path: vec![key("target")],
        },
        Action::Move {
            from: vec![key("old")],
            path: vec![key("new")],
        },
    ]);
    let move_bytes = message.to_bytes().expect("copy/move message should encode");
    assert_eq!(&move_bytes[..3], &[0xd9, 0xff, 0x01]);
    assert_eq!(
        Message::from_bytes(move_bytes).expect("copy/move message should decode"),
        message
    );
}

#[test]
fn consumer_applies_copy_and_move_actions() {
    let mut consumer = Consumer::new();
    let message = Message::new(vec![
        Action::Snapshot {
            value: json!({
                "source": {"nested": [1, 2]},
                "items": ["a", "b", "c"],
                "keep": true,
            }),
        },
        Action::Copy {
            from: vec![key("source")],
            path: vec![key("target")],
        },
        Action::Move {
            from: vec![key("items"), PathSegment::Index(0)],
            path: vec![key("items"), PathSegment::Index(2)],
        },
    ])
    .to_bytes()
    .expect("copy/move message should encode");

    consumer
        .consume(&message)
        .expect("copy/move message should apply");
    assert_eq!(
        consumer.document(),
        Some(&json!({
            "source": {"nested": [1, 2]},
            "target": {"nested": [1, 2]},
            "items": ["b", "c", "a"],
            "keep": true,
        }))
    );
}

#[test]
fn producer_emits_copy_and_move_actions_for_reused_object_values() {
    let shared = json!({
        "name": "large repeated payload",
        "items": [1, 2, 3, 4, 5],
        "flags": {"active": true, "visible": false},
    });
    let mut producer = Producer::new(json!({
        "old": shared.clone(),
        "source": shared.clone(),
        "keep": true,
    }));
    producer
        .get_message()
        .expect("initial message should encode")
        .expect("initial snapshot should exist");

    producer.update(json!({
        "new": shared.clone(),
        "source": shared.clone(),
        "target": shared,
        "keep": true,
    }));
    let message = producer
        .get_message()
        .expect("copy message should encode")
        .expect("copy message should exist");

    assert_eq!(
        Message::from_bytes(message).expect("producer message should decode"),
        Message::new(vec![
            Action::Move {
                from: vec![key("old")],
                path: vec![key("new")],
            },
            Action::Copy {
                from: vec![key("source")],
                path: vec![key("target")],
            },
        ])
    );
}

fn key(value: &str) -> PathSegment {
    PathSegment::Key(value.to_string())
}
