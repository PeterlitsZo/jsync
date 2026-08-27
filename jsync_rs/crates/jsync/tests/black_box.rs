use jsync::{
    Action, ArrayPatchEdit, Consumer, Message, PathSegment, Producer, ProducerPathSegmentPool,
    StringPatchEdit,
};
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
                Action::StringAppend {
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
            expected_message_bytes_len: 67,
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
                Action::StringPrepend {
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
            expected_message_bytes_len: 80,
        },
        UpdateCase {
            to_update: json!({
                "revision": 3,
                "profile": {"name": "Countess Ada Lovelace", "active": false},
                "items": ["gamma"],
                "tags": ["math"],
            }),
            expected_message: Message::new(vec![
                Action::ArrayPatch {
                    path: vec![PathSegment::Key("items".to_string())],
                    edits: vec![ArrayPatchEdit {
                        start: 0,
                        delete_count: 2,
                        values: Vec::new(),
                    }],
                },
                Action::Replace {
                    path: vec![PathSegment::Key("revision".to_string())],
                    value: json!(3),
                },
                Action::Remove {
                    path: vec![PathSegment::Key("tags".to_string()), PathSegment::Index(1)],
                },
            ]),
            expected_message_bytes_len: 27,
        },
        UpdateCase {
            to_update: json!(["root replacement", {"revision": 4}, [1, 2, 3]]),
            expected_message: Message::new(vec![Action::Replace {
                path: vec![],
                value: json!(["root replacement", {"revision": 4}, [1, 2, 3]]),
            }]),
            expected_message_bytes_len: 43,
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
            expected_message_bytes_len: 84,
        },
        UpdateCase {
            to_update: json!({
                "revision": 6,
                "profile": {"name": "Peterlits", "active": false},
                "items": ["delta", "epsilon"],
                "tags": ["person", "male"],
            }),
            expected_message: Message::new(vec![
                Action::Replace {
                    path: vec![
                        PathSegment::Key("profile".to_string()),
                        PathSegment::Key("active".to_string()),
                    ],
                    value: json!(false),
                },
                Action::Replace {
                    path: vec![
                        PathSegment::Key("profile".to_string()),
                        PathSegment::Key("name".to_string()),
                    ],
                    value: json!("Peterlits"),
                },
                Action::Replace {
                    path: vec![PathSegment::Key("revision".to_string())],
                    value: json!(6),
                },
                Action::Replace {
                    path: vec![PathSegment::Key("tags".to_string())],
                    value: json!(["person", "male"]),
                },
            ]),
            expected_message_bytes_len: 50,
        },
        UpdateCase {
            to_update: json!({
                "revision": 7,
                "profile": {"name": "Peterlits Zo", "active": false},
                "items": ["delta", "epsilon"],
                "tags": ["person", "male"],
            }),
            expected_message: Message::new(vec![
                Action::StringAppend {
                    path: vec![
                        PathSegment::Key("profile".to_string()),
                        PathSegment::Key("name".to_string()),
                    ],
                    text: " Zo".to_string(),
                },
                Action::Replace {
                    path: vec![PathSegment::Key("revision".to_string())],
                    value: json!(7),
                },
            ]),
            expected_message_bytes_len: 21,
        },
        UpdateCase {
            to_update: json!({
                "revision": 7,
                "profile": {"name": "Peterlits The Zo", "active": false},
                "items": ["delta", "epsilon"],
                "tags": ["person", "red", "me", "male"],
            }),
            expected_message: Message::new(vec![
                Action::StringPatch {
                    path: vec![
                        PathSegment::Key("profile".to_string()),
                        PathSegment::Key("name".to_string()),
                    ],
                    edits: vec![StringPatchEdit {
                        start: 10,
                        delete_count: 0,
                        text: "The ".to_string(),
                    }],
                },
                Action::ArrayPatch {
                    path: vec![PathSegment::Key("tags".to_string())],
                    edits: vec![ArrayPatchEdit {
                        start: 1,
                        delete_count: 0,
                        values: vec![json!("red"), json!("me")],
                    }],
                },
            ]),
            expected_message_bytes_len: 37,
        },
    ];

    let mut producer = Producer::new(initial.clone());
    let mut consumer = Consumer::new();

    let initial_message = producer
        .get_message()
        .expect("initial message should be produced")
        .expect("initial snapshot should exist");
    assert_eq!(
        consumer
            .decode_message_dry_run(&initial_message)
            .expect("initial message should decode"),
        Message::new(vec![Action::Snapshot {
            value: initial.clone(),
        }])
    );
    consumer
        .consume(&initial_message)
        .expect("consumer should accept the initial message");

    for (index, update) in updates.iter().enumerate() {
        producer.update(update.to_update.clone());
        let message = producer
            .get_message()
            .expect("producer message should be encoded")
            .expect("must have message to sync");
        assert_eq!(
            consumer
                .decode_message_dry_run(&message)
                .expect("producer message should decode"),
            update.expected_message
        );
        assert_eq!(
            message.len(),
            update.expected_message_bytes_len,
            "message length should match expected length for update {}",
            index
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
    let mut inspector = Consumer::new();
    let initial_message = producer
        .get_message()
        .expect("initial message should be encoded")
        .expect("initial snapshot should exist");
    inspector
        .consume(&initial_message)
        .expect("inspector should accept the initial message");

    producer.update(json!({
        "wrapper": {"a": 1, "b": 1, "c": 1, "d": 1, "e": 1},
        "unchanged": true,
    }));
    let message = producer
        .get_message()
        .expect("producer message should be encoded")
        .expect("must have message to sync");

    assert_eq!(
        inspector
            .decode_message_dry_run(&message)
            .expect("producer message should decode"),
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
    let mut encode_pool = ProducerPathSegmentPool::new();
    let mut encode_txn = encode_pool.transaction();
    let move_bytes = message
        .to_bytes_with_pool_txn(&mut encode_txn)
        .expect("copy/move message should encode");
    encode_txn.commit();
    let mut inspector = Consumer::new();
    assert_eq!(&move_bytes[..3], &[0xd9, 0xff, 0x01]);
    assert_eq!(
        inspector
            .decode_message_dry_run(&move_bytes)
            .expect("copy/move message should decode"),
        message
    );
}

#[test]
fn consumer_applies_copy_and_move_actions() {
    let mut consumer = Consumer::new();
    let mut encode_pool = ProducerPathSegmentPool::new();
    let mut encode_txn = encode_pool.transaction();
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
    .to_bytes_with_pool_txn(&mut encode_txn)
    .expect("copy/move message should encode");
    encode_txn.commit();

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
fn array_patch_message_round_trips_and_applies() {
    let message = Message::new(vec![
        Action::Snapshot {
            value: json!({"items": ["a", "b", "c", "d", "e"]}),
        },
        Action::ArrayPatch {
            path: vec![key("items")],
            edits: vec![
                ArrayPatchEdit {
                    start: 3,
                    delete_count: 1,
                    values: vec![json!("D")],
                },
                ArrayPatchEdit {
                    start: 1,
                    delete_count: 1,
                    values: vec![json!("B"), json!("BB")],
                },
            ],
        },
    ]);
    let mut encode_pool = ProducerPathSegmentPool::new();
    let bytes = encode_message(&mut encode_pool, &message);
    let mut consumer = Consumer::new();

    assert_eq!(
        consumer
            .decode_message_dry_run(&bytes)
            .expect("array patch message should decode"),
        message
    );
    consumer
        .consume(&bytes)
        .expect("array patch message should apply");
    assert_eq!(
        consumer.document(),
        Some(&json!({"items": ["a", "B", "BB", "c", "D", "e"]}))
    );
}

#[test]
fn array_patch_can_target_root_array() {
    let message = Message::new(vec![
        Action::Snapshot {
            value: json!(["a", "b", "c"]),
        },
        Action::ArrayPatch {
            path: vec![],
            edits: vec![ArrayPatchEdit {
                start: 1,
                delete_count: 1,
                values: vec![json!("B"), json!("BB")],
            }],
        },
    ]);
    let mut encode_pool = ProducerPathSegmentPool::new();
    let bytes = encode_message(&mut encode_pool, &message);
    let mut consumer = Consumer::new();

    consumer
        .consume(&bytes)
        .expect("root array patch should apply");
    assert_eq!(consumer.document(), Some(&json!(["a", "B", "BB", "c"])));
}

#[test]
fn array_patch_fixture_bytes_are_cross_language_compatible() {
    let message = Message::new(vec![
        Action::Snapshot {
            value: json!(["a", "b", "c", "d"]),
        },
        Action::ArrayPatch {
            path: vec![],
            edits: vec![
                ArrayPatchEdit {
                    start: 2,
                    delete_count: 1,
                    values: vec![json!("C")],
                },
                ArrayPatchEdit {
                    start: 0,
                    delete_count: 0,
                    values: vec![json!("A")],
                },
            ],
        },
    ]);
    let expected_bytes = vec![
        217, 255, 1, 130, 129, 128, 130, 130, 0, 132, 97, 97, 97, 98, 97, 99, 97, 100, 131, 9, 128,
        130, 131, 2, 1, 129, 97, 67, 131, 0, 0, 129, 97, 65,
    ];
    let mut encode_pool = ProducerPathSegmentPool::new();
    let bytes = encode_message(&mut encode_pool, &message);
    let mut consumer = Consumer::new();

    assert_eq!(bytes, expected_bytes);
    assert_eq!(
        consumer
            .decode_message_dry_run(&expected_bytes)
            .expect("cross-language fixture should decode"),
        message
    );
    consumer
        .consume(&expected_bytes)
        .expect("cross-language fixture should apply");
    assert_eq!(consumer.document(), Some(&json!(["A", "a", "b", "C", "d"])));
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
    let mut inspector = Consumer::new();
    let initial_message = producer
        .get_message()
        .expect("initial message should encode")
        .expect("initial snapshot should exist");
    inspector
        .consume(&initial_message)
        .expect("inspector should accept the initial message");

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
        inspector
            .decode_message_dry_run(&message)
            .expect("producer message should decode"),
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

#[test]
fn producer_emits_string_patch_for_middle_insert() {
    let old = format!("{}{}", "a".repeat(80), "b".repeat(80));
    let new = format!("{}XYZ{}", "a".repeat(80), "b".repeat(80));
    let decoded = producer_update_message(json!({"text": old}), json!({"text": new}));

    assert_eq!(
        decoded,
        Message::new(vec![Action::StringPatch {
            path: vec![key("text")],
            edits: vec![StringPatchEdit {
                start: 80,
                delete_count: 0,
                text: "XYZ".to_string(),
            }],
        }])
    );
}

#[test]
fn producer_emits_string_patch_for_middle_delete() {
    let old = format!("{}XYZ{}", "a".repeat(80), "b".repeat(80));
    let new = format!("{}{}", "a".repeat(80), "b".repeat(80));
    let decoded = producer_update_message(json!({"text": old}), json!({"text": new}));

    assert_eq!(
        decoded,
        Message::new(vec![Action::StringPatch {
            path: vec![key("text")],
            edits: vec![StringPatchEdit {
                start: 80,
                delete_count: 3,
                text: String::new(),
            }],
        }])
    );
}

#[test]
fn producer_emits_string_patch_with_multiple_myers_edits() {
    let old = format!("{}x{}y{}", "a".repeat(80), "b".repeat(80), "c".repeat(80));
    let new = format!("{}X{}Y{}", "a".repeat(80), "b".repeat(80), "c".repeat(80));
    let decoded = producer_update_message(json!({"text": old}), json!({"text": new}));

    assert_eq!(
        decoded,
        Message::new(vec![Action::StringPatch {
            path: vec![key("text")],
            edits: vec![
                StringPatchEdit {
                    start: 161,
                    delete_count: 1,
                    text: "Y".to_string(),
                },
                StringPatchEdit {
                    start: 80,
                    delete_count: 1,
                    text: "X".to_string(),
                },
            ],
        }])
    );
}

#[test]
fn producer_string_patch_uses_unicode_scalar_offsets() {
    let prefix = "😀".repeat(40);
    let old = format!("{prefix}middle{}", "🚀".repeat(40));
    let new = format!("{prefix}XYZmiddle{}", "🚀".repeat(40));
    let decoded = producer_update_message(json!({"text": old}), json!({"text": new}));

    assert_eq!(
        decoded,
        Message::new(vec![Action::StringPatch {
            path: vec![key("text")],
            edits: vec![StringPatchEdit {
                start: 40,
                delete_count: 0,
                text: "XYZ".to_string(),
            }],
        }])
    );
}

#[test]
fn producer_replaces_completely_different_large_strings() {
    let old = "a".repeat(12_000);
    let new = "b".repeat(12_000);
    let decoded = producer_update_message(json!({"text": old}), json!({"text": new.clone()}));

    assert_eq!(
        decoded,
        Message::new(vec![Action::Replace {
            path: vec![key("text")],
            value: json!(new),
        }])
    );
}

#[test]
fn producer_emits_array_patch_for_middle_insert() {
    let anchor_a = "a".repeat(80);
    let anchor_b = "b".repeat(80);
    let decoded = producer_update_message(
        json!({"items": [anchor_a, anchor_b]}),
        json!({"items": [anchor_a, "inserted", anchor_b]}),
    );

    assert_eq!(
        decoded,
        Message::new(vec![Action::ArrayPatch {
            path: vec![key("items")],
            edits: vec![ArrayPatchEdit {
                start: 1,
                delete_count: 0,
                values: vec![json!("inserted")],
            }],
        }])
    );
}

#[test]
fn producer_emits_array_patch_with_multiple_myers_edits() {
    let anchor_a = "a".repeat(80);
    let anchor_c = "c".repeat(80);
    let anchor_e = "e".repeat(80);
    let decoded = producer_update_message(
        json!({"items": [anchor_a, anchor_c, anchor_e]}),
        json!({"items": [anchor_a, "b", anchor_c, "d", anchor_e]}),
    );

    assert_eq!(
        decoded,
        Message::new(vec![Action::ArrayPatch {
            path: vec![key("items")],
            edits: vec![
                ArrayPatchEdit {
                    start: 2,
                    delete_count: 0,
                    values: vec![json!("d")],
                },
                ArrayPatchEdit {
                    start: 1,
                    delete_count: 0,
                    values: vec![json!("b")],
                },
            ],
        }])
    );
}

#[test]
fn producer_keeps_recursive_array_diff_when_it_is_smaller() {
    let old = format!("{}{}", "a".repeat(80), "b".repeat(80));
    let new = format!("{}XYZ{}", "a".repeat(80), "b".repeat(80));
    let decoded = producer_update_message(json!({"items": [old]}), json!({"items": [new]}));

    assert_eq!(
        decoded,
        Message::new(vec![Action::StringPatch {
            path: vec![key("items"), PathSegment::Index(0)],
            edits: vec![StringPatchEdit {
                start: 80,
                delete_count: 0,
                text: "XYZ".to_string(),
            }],
        }])
    );
}

#[test]
fn string_patch_message_round_trips_and_applies() {
    let message = Message::new(vec![
        Action::Snapshot {
            value: json!({"text": "abc def ghi"}),
        },
        Action::StringPatch {
            path: vec![key("text")],
            edits: vec![
                StringPatchEdit {
                    start: 9,
                    delete_count: 2,
                    text: "Y".to_string(),
                },
                StringPatchEdit {
                    start: 2,
                    delete_count: 1,
                    text: "X".to_string(),
                },
            ],
        },
    ]);
    let mut encode_pool = ProducerPathSegmentPool::new();
    let bytes = encode_message(&mut encode_pool, &message);
    let mut consumer = Consumer::new();

    assert_eq!(
        consumer
            .decode_message_dry_run(&bytes)
            .expect("string patch message should decode"),
        message
    );
    consumer
        .consume(&bytes)
        .expect("string patch message should apply");
    assert_eq!(consumer.document(), Some(&json!({"text": "abX def gY"})));
}

#[test]
fn invalid_string_patch_edits_do_not_commit_document_or_path_pool() {
    for edits in [
        Vec::<StringPatchEdit>::new(),
        vec![StringPatchEdit {
            start: 1,
            delete_count: 0,
            text: String::new(),
        }],
        vec![StringPatchEdit {
            start: 7,
            delete_count: 0,
            text: "x".to_string(),
        }],
        vec![
            StringPatchEdit {
                start: 1,
                delete_count: 1,
                text: "x".to_string(),
            },
            StringPatchEdit {
                start: 3,
                delete_count: 1,
                text: "y".to_string(),
            },
        ],
        vec![
            StringPatchEdit {
                start: 3,
                delete_count: 2,
                text: String::new(),
            },
            StringPatchEdit {
                start: 2,
                delete_count: 2,
                text: String::new(),
            },
        ],
    ] {
        assert_invalid_string_patch_rolls_back(edits);
    }
}

#[test]
fn invalid_array_patch_edits_do_not_commit_document_or_path_pool() {
    for edits in [
        Vec::<ArrayPatchEdit>::new(),
        vec![ArrayPatchEdit {
            start: 1,
            delete_count: 0,
            values: Vec::new(),
        }],
        vec![ArrayPatchEdit {
            start: 7,
            delete_count: 0,
            values: vec![json!("x")],
        }],
        vec![
            ArrayPatchEdit {
                start: 1,
                delete_count: 1,
                values: vec![json!("x")],
            },
            ArrayPatchEdit {
                start: 3,
                delete_count: 1,
                values: vec![json!("y")],
            },
        ],
        vec![
            ArrayPatchEdit {
                start: 3,
                delete_count: 2,
                values: Vec::new(),
            },
            ArrayPatchEdit {
                start: 2,
                delete_count: 2,
                values: Vec::new(),
            },
        ],
    ] {
        assert_invalid_array_patch_rolls_back(edits);
    }
}

fn producer_update_message(initial: Value, update: Value) -> Message {
    let mut producer = Producer::new(initial);
    let mut inspector = Consumer::new();
    let initial_message = producer
        .get_message()
        .expect("initial message should encode")
        .expect("initial snapshot should exist");
    inspector
        .consume(&initial_message)
        .expect("inspector should accept initial snapshot");

    producer.update(update);
    let message = producer
        .get_message()
        .expect("update message should encode")
        .expect("update message should exist");
    let decoded = inspector
        .decode_message_dry_run(&message)
        .expect("update message should decode");
    inspector
        .consume(&message)
        .expect("inspector should consume update message");
    assert_eq!(inspector.document(), Some(producer.document()));
    decoded
}

fn assert_invalid_string_patch_rolls_back(edits: Vec<StringPatchEdit>) {
    let mut consumer = Consumer::new();
    let mut encode_pool = ProducerPathSegmentPool::new();
    let initial = Message::new(vec![Action::Snapshot {
        value: json!({"text": "abcdef"}),
    }]);
    let initial_bytes = encode_message(&mut encode_pool, &initial);
    consumer
        .consume(&initial_bytes)
        .expect("initial snapshot should apply");

    let invalid = Message::new(vec![Action::StringPatch {
        path: vec![key("text")],
        edits,
    }]);
    let invalid_bytes = encode_message(&mut encode_pool, &invalid);
    assert!(consumer.consume(&invalid_bytes).is_err());
    assert_eq!(consumer.document(), Some(&json!({"text": "abcdef"})));

    let pooled_path_followup = Message::new(vec![Action::StringAppend {
        path: vec![key("text")],
        text: "!".to_string(),
    }]);
    let pooled_path_followup_bytes = encode_message(&mut encode_pool, &pooled_path_followup);
    assert!(consumer.consume(&pooled_path_followup_bytes).is_err());
    assert_eq!(consumer.document(), Some(&json!({"text": "abcdef"})));
}

fn assert_invalid_array_patch_rolls_back(edits: Vec<ArrayPatchEdit>) {
    let mut consumer = Consumer::new();
    let mut encode_pool = ProducerPathSegmentPool::new();
    let initial = Message::new(vec![Action::Snapshot {
        value: json!({"items": ["a", "b", "c", "d", "e", "f"]}),
    }]);
    let initial_bytes = encode_message(&mut encode_pool, &initial);
    consumer
        .consume(&initial_bytes)
        .expect("initial snapshot should apply");

    let invalid = Message::new(vec![Action::ArrayPatch {
        path: vec![key("items")],
        edits,
    }]);
    let invalid_bytes = encode_message(&mut encode_pool, &invalid);
    assert!(consumer.consume(&invalid_bytes).is_err());
    assert_eq!(
        consumer.document(),
        Some(&json!({"items": ["a", "b", "c", "d", "e", "f"]}))
    );

    let pooled_path_followup = Message::new(vec![Action::StringAppend {
        path: vec![key("items"), PathSegment::Index(0)],
        text: "!".to_string(),
    }]);
    let pooled_path_followup_bytes = encode_message(&mut encode_pool, &pooled_path_followup);
    assert!(consumer.consume(&pooled_path_followup_bytes).is_err());
    assert_eq!(
        consumer.document(),
        Some(&json!({"items": ["a", "b", "c", "d", "e", "f"]}))
    );
}

fn encode_message(encode_pool: &mut ProducerPathSegmentPool, message: &Message) -> Vec<u8> {
    let mut encode_txn = encode_pool.transaction();
    let bytes = message
        .to_bytes_with_pool_txn(&mut encode_txn)
        .expect("message should encode");
    encode_txn.commit();
    bytes
}

fn key(value: &str) -> PathSegment {
    PathSegment::Key(value.to_string())
}
