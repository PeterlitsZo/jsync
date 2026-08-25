use std::io::Cursor;

use ciborium::{Value as CborValue, de::from_reader};
use jsync::{Consumer, Producer};
use serde_json::json;

const HEADER: [u8; 3] = [0xd9, 0xff, 0x01];

fn payload(message: &[u8]) -> Vec<CborValue> {
    assert!(message.starts_with(&HEADER));
    match from_reader(Cursor::new(&message[HEADER.len()..])).expect("producer message must decode")
    {
        CborValue::Array(actions) => actions,
        value => panic!("expected action array, got {value:?}"),
    }
}

fn opcode(action: &CborValue) -> i128 {
    match action {
        CborValue::Array(elements) => elements
            .first()
            .and_then(CborValue::as_integer)
            .map(i128::from)
            .expect("action must have an integer opcode"),
        value => panic!("expected action array, got {value:?}"),
    }
}

fn action_path(action: &CborValue) -> &CborValue {
    match action {
        CborValue::Array(elements) => elements.get(1).expect("action must have a path"),
        value => panic!("expected action array, got {value:?}"),
    }
}

#[test]
fn first_message_snapshots_latest_document() {
    let mut producer = Producer::new(json!({"count": 0}));
    producer.update(json!({"count": 1}));

    let message = producer
        .get_message()
        .expect("first message must encode")
        .expect("first message must exist");
    let actions = payload(&message);

    assert_eq!(actions.len(), 1);
    assert_eq!(opcode(&actions[0]), 0);
    assert_eq!(actions[0], to_cbor(json!([0, {"count": 1}])));

    let mut consumer = Consumer::new();
    consumer.consume(&message).unwrap();
    assert_eq!(consumer.document(), Some(&json!({"count": 1})));
}

#[test]
fn emits_incremental_actions_from_last_emitted_document() {
    let mut producer = Producer::new(json!({"count": 0, "items": []}));
    let snapshot = producer.get_message().unwrap().unwrap();

    producer.update(json!({"count": 1, "items": ["a"]}));
    let patch = producer.get_message().unwrap().unwrap();
    let actions = payload(&patch);

    assert_eq!(actions.len(), 2);
    assert_eq!(opcode(&actions[0]), 3);
    assert_eq!(action_path(&actions[0]), &to_cbor(json!(["count"])));
    assert_eq!(opcode(&actions[1]), 1);
    assert_eq!(action_path(&actions[1]), &to_cbor(json!(["items", 0])));

    let mut consumer = Consumer::new();
    consumer.consume(&snapshot).unwrap();
    consumer.consume(&patch).unwrap();
    assert_eq!(
        consumer.document(),
        Some(&json!({"count": 1, "items": ["a"]}))
    );
}

#[test]
fn coalesces_multiple_updates_before_get_message() {
    let mut producer = Producer::new(json!({"count": 0}));
    producer.get_message().unwrap();

    producer.update(json!({"count": 1}));
    producer.update(json!({"count": 2}));
    let patch = producer.get_message().unwrap().unwrap();
    let actions = payload(&patch);

    assert_eq!(actions.len(), 1);
    assert_eq!(opcode(&actions[0]), 3);
    assert_eq!(actions[0], to_cbor(json!([3, ["count"], 2])));
}

#[test]
fn returns_none_when_document_did_not_change() {
    let mut producer = Producer::new(json!({"count": 0}));
    producer.get_message().unwrap().unwrap();

    assert!(producer.get_message().unwrap().is_none());
    producer.update(json!({"count": 0}));
    assert!(producer.get_message().unwrap().is_none());
}

#[test]
fn replaces_root_when_root_value_changes() {
    let mut producer = Producer::new(json!({"count": 0}));
    producer.get_message().unwrap().unwrap();
    producer.update(json!(["new-root"]));

    let patch = producer.get_message().unwrap().unwrap();
    assert_eq!(payload(&patch), vec![to_cbor(json!([3, [], ["new-root"]]))]);

    let mut consumer = Consumer::new();
    consumer
        .consume(&producer_message(json!({"count": 0})))
        .unwrap();
    consumer.consume(&patch).unwrap();
    assert_eq!(consumer.document(), Some(&json!(["new-root"])));
}

#[test]
fn removes_object_keys_and_array_tail_in_valid_order() {
    let mut producer = Producer::new(json!({
        "gone": true,
        "list": ["A", "B", "C"],
    }));
    producer.get_message().unwrap().unwrap();
    producer.update(json!({"list": ["A"]}));

    let patch = producer.get_message().unwrap().unwrap();
    let actions = payload(&patch);

    assert_eq!(actions.len(), 3);
    assert_eq!(
        actions.iter().map(opcode).collect::<Vec<_>>(),
        vec![2, 2, 2]
    );
    assert_eq!(action_path(&actions[0]), &to_cbor(json!(["gone"])));
    assert_eq!(action_path(&actions[1]), &to_cbor(json!(["list", 2])));
    assert_eq!(action_path(&actions[2]), &to_cbor(json!(["list", 1])));

    let mut consumer = Consumer::new();
    consumer
        .consume(&producer_message(json!({
            "gone": true,
            "list": ["A", "B", "C"],
        })))
        .unwrap();
    consumer.consume(&patch).unwrap();
    assert_eq!(consumer.document(), Some(&json!({"list": ["A"]})));
}

fn producer_message(document: serde_json::Value) -> Vec<u8> {
    let mut producer = Producer::new(document);
    producer.get_message().unwrap().unwrap()
}

fn to_cbor(value: serde_json::Value) -> CborValue {
    match value {
        serde_json::Value::Null => CborValue::Null,
        serde_json::Value::Bool(value) => CborValue::Bool(value),
        serde_json::Value::Number(number) => {
            if let Some(value) = number.as_i64() {
                CborValue::Integer(value.into())
            } else {
                CborValue::Integer(number.as_u64().expect("test number must be integer").into())
            }
        }
        serde_json::Value::String(value) => CborValue::Text(value),
        serde_json::Value::Array(values) => {
            CborValue::Array(values.into_iter().map(to_cbor).collect())
        }
        serde_json::Value::Object(object) => CborValue::Map(
            object
                .into_iter()
                .map(|(key, value)| (CborValue::Text(key), to_cbor(value)))
                .collect(),
        ),
    }
}
