use ciborium::{Value as CborValue, ser::into_writer};
use jsync::{Consumer, JsyncError, JsyncErrorKind};
use serde_json::{Value, json};

const HEADER: [u8; 3] = [0xd9, 0xff, 0x01];

fn message(payload: Value) -> Vec<u8> {
    let mut bytes = HEADER.to_vec();
    let payload = to_cbor(payload);
    into_writer(&payload, &mut bytes).expect("test payload must encode");
    bytes
}

fn to_cbor(value: Value) -> CborValue {
    match value {
        Value::Null => CborValue::Null,
        Value::Bool(value) => CborValue::Bool(value),
        Value::Number(number) => {
            if let Some(value) = number.as_i64() {
                CborValue::Integer(value.into())
            } else if let Some(value) = number.as_u64() {
                CborValue::Integer(value.into())
            } else {
                CborValue::Float(number.as_f64().expect("test number must be finite"))
            }
        }
        Value::String(value) => CborValue::Text(value),
        Value::Array(values) => CborValue::Array(values.into_iter().map(to_cbor).collect()),
        Value::Object(object) => CborValue::Map(
            object
                .into_iter()
                .map(|(key, value)| (CborValue::Text(key), to_cbor(value)))
                .collect(),
        ),
    }
}

fn code(error: JsyncError) -> JsyncErrorKind {
    error.kind
}

fn assert_code(result: Result<(), JsyncError>, expected: JsyncErrorKind) {
    let error = result.expect_err("operation should fail");
    assert_eq!(code(error), expected);
}

#[test]
fn renders_error_kind_message_and_context() {
    let error = JsyncError::new(
        JsyncErrorKind::UnsupportedVersion,
        "The version is unsupported.",
    )
    .with_metadata("version", "3")
    .with_metadata("expected", "1")
    .with_context("while decoding the payload")
    .with_context("while consuming a Jsync message")
    .with_source(anyhow::anyhow!("balabala"));
    let rendered = error.to_string();
    assert_eq!(
        rendered,
        "while consuming a Jsync message: while decoding the payload: (UnsupportedVersion) The version is unsupported. (expected=1, version=3) Source: balabala"
    );
    assert_eq!(error.metadata.get("version"), Some(&"3".to_string()));
    assert_eq!(
        error.context,
        vec![
            "while decoding the payload",
            "while consuming a Jsync message"
        ]
    );
    assert!(error.source.is_some());
}

#[test]
fn consumes_snapshot_and_object_adds() {
    let mut consumer = Consumer::new();
    consumer.consume(&message(json!([[0, {"a": 1}]]))).unwrap();
    consumer
        .consume(&message(json!([[1, ["b"], 2], [1, ["a"], 3]])))
        .unwrap();
    assert_eq!(consumer.document(), Some(&json!({"a": 3, "b": 2})));
}

#[test]
fn replaces_root_with_snapshot_or_empty_path_add() {
    let mut consumer = Consumer::new();
    consumer.consume(&message(json!([[0, {"a": 1}]]))).unwrap();
    consumer
        .consume(&message(json!([[1, [], ["new-root"]]])))
        .unwrap();
    assert_eq!(consumer.document(), Some(&json!(["new-root"])));
    consumer.consume(&message(json!([[0, null]]))).unwrap();
    assert_eq!(consumer.document(), Some(&Value::Null));
}

#[test]
fn inserts_array_values_at_index_and_end() {
    let mut consumer = Consumer::new();
    consumer
        .consume(&message(json!([[0, {"list": ["A", "B", "C"]}]])))
        .unwrap();
    consumer
        .consume(&message(json!([
            [1, ["list", 0], "first"],
            [1, ["list", 2], "middle"],
            [1, ["list", "-"], "last"]
        ])))
        .unwrap();
    assert_eq!(
        consumer.document(),
        Some(&json!({"list": ["first", "A", "middle", "B", "C", "last"]}))
    );
}

#[test]
fn requires_initial_snapshot_and_allows_empty_followup() {
    let mut consumer = Consumer::new();
    assert_code(
        consumer.consume(&message(json!([[1, ["a"], 1]]))),
        JsyncErrorKind::InitialSnapshotRequired,
    );
    assert_eq!(consumer.document(), None);
    consumer.consume(&message(json!([[0, {}]]))).unwrap();
    consumer.consume(&message(json!([]))).unwrap();
    assert_eq!(consumer.document(), Some(&json!({})));
}

#[test]
fn rolls_back_failed_messages_including_failed_first_message() {
    let mut consumer = Consumer::new();
    assert_code(
        consumer.consume(&message(json!([[0, {}], [1, ["missing", 0], 1]]))),
        JsyncErrorKind::PathParentMissing,
    );
    assert_eq!(consumer.document(), None);

    consumer
        .consume(&message(json!([[0, {"a": 1, "list": []}]])))
        .unwrap();
    assert_code(
        consumer.consume(&message(json!([[1, ["a"], 2], [1, ["missing", 0], 3]]))),
        JsyncErrorKind::PathParentMissing,
    );
    assert_eq!(consumer.document(), Some(&json!({"a": 1, "list": []})));
}

#[test]
fn validates_headers_shapes_and_action_lengths() {
    let mut consumer = Consumer::new();
    assert_code(
        consumer.consume(&[0xd9, 0xff]),
        JsyncErrorKind::InvalidHeader,
    );
    assert_code(
        consumer.consume(&[0xd9, 0xfe, 1]),
        JsyncErrorKind::InvalidHeader,
    );
    assert_code(
        consumer.consume(&[0xd9, 0xff, 2]),
        JsyncErrorKind::UnsupportedVersion,
    );
    assert_code(
        consumer.consume(&message(json!({}))),
        JsyncErrorKind::MessageNotArray,
    );
    assert_code(
        consumer.consume(&message(json!([1]))),
        JsyncErrorKind::ActionNotArray,
    );
    assert_code(
        consumer.consume(&message(json!([[]]))),
        JsyncErrorKind::InvalidActionLength,
    );
    assert_code(
        consumer.consume(&message(json!([[0]]))),
        JsyncErrorKind::InvalidActionLength,
    );
    assert_code(
        consumer.consume(&message(json!([[1, [], 1, 2]]))),
        JsyncErrorKind::InvalidActionLength,
    );
    assert_code(
        consumer.consume(&message(json!([[9, null]]))),
        JsyncErrorKind::UnknownAction,
    );
}

#[test]
fn renders_path_values_as_metadata_and_context_as_readable_location() {
    let mut consumer = Consumer::new();
    consumer
        .consume(&message(json!([[0, {"list": []}]])))
        .unwrap();

    let error = consumer
        .consume(&message(json!([[1, ["list", "not-an-index"], "x"]])))
        .expect_err("an array path must reject a string index");

    assert_eq!(error.kind, JsyncErrorKind::InvalidPath);
    assert_eq!(
        error.context,
        vec![
            "while applying the final ADD path segment",
            "while applying a Jsync action",
        ]
    );
    assert_eq!(error.metadata.get("action_index"), Some(&"0".to_string()));
    assert_eq!(
        error.metadata.get("segment"),
        Some(&"not-an-index".to_string())
    );
    assert_eq!(
        error.to_string(),
        "while applying a Jsync action: while applying the final ADD path segment: \
(InvalidPath) An array final segment must be a non-negative integer or '-'. (action_index=0, segment=not-an-index, segment_index=1)"
    );
}

#[test]
fn renders_decoding_location_as_context_and_value_path_as_metadata() {
    let mut consumer = Consumer::new();
    consumer
        .consume(&message(json!([[0, {"nested": [null]}]])))
        .expect("valid JSON values should decode");

    let mut bytes = HEADER.to_vec();
    let payload = CborValue::Array(vec![CborValue::Array(vec![
        CborValue::Integer(0.into()),
        CborValue::Map(vec![(
            CborValue::Text("nested".to_string()),
            CborValue::Array(vec![CborValue::Bytes(vec![1])]),
        )]),
    ])]);
    into_writer(&payload, &mut bytes).unwrap();

    let error = Consumer::new()
        .consume(&bytes)
        .expect_err("CBOR byte strings are not JSON values");
    assert_eq!(error.kind, JsyncErrorKind::InvalidJsonValue);
    assert_eq!(
        error.context,
        vec![
            "while decoding the SNAPSHOT value",
            "while parsing a Jsync action",
        ]
    );
    assert_eq!(error.metadata.get("action_index"), Some(&"0".to_string()));
    assert!(!error.metadata.contains_key("value_path"));
}

#[test]
fn validates_paths_and_array_bounds() {
    let cases = [
        (
            json!([[0, {"list": ["A"]}], [1, ["list", 2], "x"]]),
            JsyncErrorKind::ArrayIndexOutOfBounds,
        ),
        (
            json!([[0, {"list": ["A"]}], [1, ["list", -1], "x"]]),
            JsyncErrorKind::InvalidPath,
        ),
        (
            json!([[0, {"list": ["A"]}], [1, ["list", "0"], "x"]]),
            JsyncErrorKind::InvalidPath,
        ),
        (
            json!([[0, {}], [1, ["missing", 0], "x"]]),
            JsyncErrorKind::PathParentMissing,
        ),
        (
            json!([[0, {"value": 1}], [1, ["value", "x"], "x"]]),
            JsyncErrorKind::PathParentNotContainer,
        ),
    ];
    for (payload, expected) in cases {
        let mut consumer = Consumer::new();
        assert_code(consumer.consume(&message(payload)), expected);
    }
}

#[test]
fn rejects_non_json_cbor_values_and_trailing_bytes() {
    let mut bytes = HEADER.to_vec();
    let payload = CborValue::Array(vec![CborValue::Array(vec![
        CborValue::Integer(0.into()),
        CborValue::Bytes(vec![1, 2]),
    ])]);
    into_writer(&payload, &mut bytes).unwrap();
    let mut consumer = Consumer::new();
    assert_code(consumer.consume(&bytes), JsyncErrorKind::InvalidJsonValue);

    let mut valid = message(json!([[0, {}]]));
    valid.push(0);
    assert_code(consumer.consume(&valid), JsyncErrorKind::TrailingBytes);
}

#[test]
fn removes_and_replaces_object_keys_and_root() {
    let mut consumer = Consumer::new();
    consumer
        .consume(&message(json!([[0, {"a": 1, "b": 2, "nullable": null}]])))
        .unwrap();

    consumer
        .consume(&message(json!([
            [3, ["a"], {"nested": true}],
            [2, ["b"]],
            [3, ["nullable"], "present"]
        ])))
        .unwrap();
    assert_eq!(
        consumer.document(),
        Some(&json!({"a": {"nested": true}, "nullable": "present"}))
    );

    consumer
        .consume(&message(json!([[3, [], ["new-root"]]])))
        .unwrap();
    assert_eq!(consumer.document(), Some(&json!(["new-root"])));
}

#[test]
fn removes_and_replaces_array_elements_in_order() {
    let mut consumer = Consumer::new();
    consumer
        .consume(&message(json!([[0, {"list": ["A", "B", "C"]}]])))
        .unwrap();

    consumer
        .consume(&message(json!([
            [2, ["list", 1]],
            [3, ["list", 1], "D"],
            [3, ["list", 0], "first"]
        ])))
        .unwrap();
    assert_eq!(consumer.document(), Some(&json!({"list": ["first", "D"]})));
}

#[test]
fn validates_remove_and_replace_action_shapes_and_values() {
    let mut consumer = Consumer::new();
    assert_code(
        consumer.consume(&message(json!([[2]]))),
        JsyncErrorKind::InvalidActionLength,
    );
    assert_code(
        consumer.consume(&message(json!([[2, [], 1]]))),
        JsyncErrorKind::InvalidActionLength,
    );
    assert_code(
        consumer.consume(&message(json!([[3, []]]))),
        JsyncErrorKind::InvalidActionLength,
    );
    assert_code(
        consumer.consume(&message(json!([[3, [], 1, 2]]))),
        JsyncErrorKind::InvalidActionLength,
    );

    let mut bytes = HEADER.to_vec();
    let payload = CborValue::Array(vec![CborValue::Array(vec![
        CborValue::Integer(3.into()),
        CborValue::Array(Vec::new()),
        CborValue::Bytes(vec![1]),
    ])]);
    into_writer(&payload, &mut bytes).unwrap();
    let error = Consumer::new()
        .consume(&bytes)
        .expect_err("REPLACE values must be legal JSON values");
    assert_eq!(error.kind, JsyncErrorKind::InvalidJsonValue);
    assert_eq!(
        error.context,
        vec![
            "while decoding the REPLACE value",
            "while parsing a Jsync action",
        ]
    );
}

#[test]
fn validates_remove_and_replace_paths_and_targets() {
    let cases = [
        (
            json!([[0, {"obj": {"present": 1}, "list": ["A"], "scalar": 1}], [2, []]]),
            JsyncErrorKind::InvalidPath,
        ),
        (
            json!([[0, {"obj": {"present": 1}, "list": ["A"], "scalar": 1}], [2, ["obj", "missing"]]]),
            JsyncErrorKind::PathParentMissing,
        ),
        (
            json!([[0, {"obj": {"present": 1}, "list": ["A"], "scalar": 1}], [2, ["list", 1]]]),
            JsyncErrorKind::ArrayIndexOutOfBounds,
        ),
        (
            json!([[0, {"obj": {"present": 1}, "list": ["A"], "scalar": 1}], [2, ["list", "-"]]]),
            JsyncErrorKind::InvalidPath,
        ),
        (
            json!([[0, {"obj": {"present": 1}, "list": ["A"], "scalar": 1}], [2, ["obj", 0]]]),
            JsyncErrorKind::InvalidPath,
        ),
        (
            json!([[0, {"obj": {"present": 1}, "list": ["A"], "scalar": 1}], [3, ["obj", "missing"], 1]]),
            JsyncErrorKind::PathParentMissing,
        ),
        (
            json!([[0, {"obj": {"present": 1}, "list": ["A"], "scalar": 1}], [3, ["list", 1], 1]]),
            JsyncErrorKind::ArrayIndexOutOfBounds,
        ),
        (
            json!([[0, {"obj": {"present": 1}, "list": ["A"], "scalar": 1}], [3, ["list", "-"], 1]]),
            JsyncErrorKind::InvalidPath,
        ),
        (
            json!([[0, {"obj": {"present": 1}, "list": ["A"], "scalar": 1}], [3, ["obj", 0], 1]]),
            JsyncErrorKind::InvalidPath,
        ),
        (
            json!([[0, {"obj": {"present": 1}, "list": ["A"], "scalar": 1}], [3, ["scalar", "x"], 1]]),
            JsyncErrorKind::PathParentNotContainer,
        ),
    ];

    for (payload, expected) in cases {
        let mut consumer = Consumer::new();
        assert_code(consumer.consume(&message(payload)), expected);
    }
}

#[test]
fn rolls_back_remove_and_replace_failures() {
    let mut consumer = Consumer::new();
    consumer
        .consume(&message(json!([[0, {"a": 1, "b": 2, "list": ["A"]}]])))
        .unwrap();

    assert_code(
        consumer.consume(&message(json!([
            [2, ["a"]],
            [3, ["b"], 3],
            [2, ["missing"]]
        ]))),
        JsyncErrorKind::PathParentMissing,
    );
    assert_eq!(
        consumer.document(),
        Some(&json!({"a": 1, "b": 2, "list": ["A"]}))
    );

    let mut first_message = Consumer::new();
    assert_code(
        first_message.consume(&message(json!([
            [0, {"a": 1}],
            [2, ["missing"]]
        ]))),
        JsyncErrorKind::PathParentMissing,
    );
    assert_eq!(first_message.document(), None);
    first_message
        .consume(&message(json!([[0, {"ready": true}]])))
        .unwrap();
    assert_eq!(first_message.document(), Some(&json!({"ready": true})));
}
