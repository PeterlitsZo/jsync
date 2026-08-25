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
                Action::Replace {
                    path: vec![
                        PathSegment::Key("profile".to_string()),
                        PathSegment::Key("name".to_string()),
                    ],
                    value: json!("Ada Lovelace"),
                },
                Action::Replace {
                    path: vec![PathSegment::Key("revision".to_string())],
                    value: json!(1),
                },
            ]),
            expected_message_bytes_len: 62,
        },
        UpdateCase {
            to_update: json!({
                "revision": 2,
                "profile": {"name": "Ada Lovelace", "active": false},
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
                Action::Replace {
                    path: vec![PathSegment::Key("revision".to_string())],
                    value: json!(2),
                },
                Action::Add {
                    path: vec![PathSegment::Key("tags".to_string())],
                    value: json!(["math", "programming"]),
                },
            ]),
            expected_message_bytes_len: 74,
        },
        UpdateCase {
            to_update: json!({
                "revision": 3,
                "profile": {"name": "Ada Lovelace", "active": false},
                "items": ["gamma"],
                "tags": ["math"],
            }),
            expected_message: Message::new(vec![
                Action::Replace {
                    path: vec![PathSegment::Key("items".to_string()), PathSegment::Index(0)],
                    value: json!("gamma"),
                },
                Action::Remove {
                    path: vec![PathSegment::Key("items".to_string()), PathSegment::Index(2)],
                },
                Action::Remove {
                    path: vec![PathSegment::Key("items".to_string()), PathSegment::Index(1)],
                },
                Action::Replace {
                    path: vec![PathSegment::Key("revision".to_string())],
                    value: json!(3),
                },
                Action::Remove {
                    path: vec![PathSegment::Key("tags".to_string()), PathSegment::Index(1)],
                },
            ]),
            expected_message_bytes_len: 62,
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
