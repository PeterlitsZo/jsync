use std::io::Cursor;

use ciborium::Value as CborValue;
use serde_json::{Map, Number, Value};

use crate::error::{JsyncError, JsyncErrorKind};

const HEADER: [u8; 3] = [0xd9, 0xff, 0x01];

/// A structured Jsync message.
#[derive(Debug, Clone, PartialEq)]
pub struct Message {
    /// Ordered actions contained in the message.
    pub actions: Vec<Action>,
}

/// A structured Jsync action.
#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    /// Replaces the current document with the given JSON value.
    Snapshot {
        /// The snapshot value to replace the current document with.
        value: Value,
    },
    /// Inserts a JSON value at the given path.
    Add {
        /// The validated destination path.
        path: Vec<PathSegment>,
        /// The value to insert or overwrite.
        value: Value,
    },
    /// Removes the value at the given path.
    Remove {
        /// The validated path of the value to remove.
        path: Vec<PathSegment>,
    },
    /// Replaces the value at the given path.
    Replace {
        /// The validated path of the value to replace.
        path: Vec<PathSegment>,
        /// The replacement JSON value.
        value: Value,
    },
    /// Appends text to an existing string value at the given path.
    Append {
        /// The validated path of the string to append to.
        path: Vec<PathSegment>,
        /// The text to append.
        text: String,
    },
    /// Prepends text to an existing string value at the given path.
    Prepend {
        /// The validated path of the string to prepend to.
        path: Vec<PathSegment>,
        /// The text to prepend.
        text: String,
    },
}

/// One segment in a validated Jsync action path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathSegment {
    /// Selects an object property by key.
    Key(String),
    /// Selects an array element by non-negative index.
    Index(usize),
}

impl Message {
    /// Creates a message from already structured actions.
    pub fn new(actions: Vec<Action>) -> Self {
        Self { actions }
    }

    /// Decodes and validates one complete Jsync binary message.
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self, JsyncError> {
        let payload = decode_payload(&bytes)?;
        Ok(Self {
            actions: parse_actions(payload)?,
        })
    }

    /// Encodes this message as Jsync version 1 bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>, JsyncError> {
        let payload = CborValue::Array(
            self.actions
                .iter()
                .map(action_to_cbor)
                .collect::<Result<Vec<_>, _>>()?,
        );
        let mut bytes = HEADER.to_vec();
        ciborium::ser::into_writer(&payload, &mut bytes).map_err(|error| {
            JsyncError::new(
                JsyncErrorKind::ApplyFailed,
                "The Jsync message could not be encoded.",
            )
            .with_source(anyhow::Error::new(error))
        })?;
        Ok(bytes)
    }
}

/// Decodes the one CBOR payload after validating and removing the Jsync header.
fn decode_payload(message: &[u8]) -> Result<CborValue, JsyncError> {
    if message.get(..HEADER.len()) != Some(HEADER.as_slice()) {
        if message.len() >= 3 && message[0..2] == [0xd9, 0xff] && message[2] > 1 {
            return Err(JsyncError::new(
                JsyncErrorKind::UnsupportedVersion,
                "The Jsync version is unsupported.",
            )
            .with_metadata("version", message[2].to_string())
            .with_metadata("expected", "1"));
        }
        return Err(JsyncError::new(
            JsyncErrorKind::InvalidHeader,
            "The message is not a valid Jsync message or its version is newer.",
        )
        .with_metadata("expected", "0xd9ff01"));
    }

    let payload = &message[HEADER.len()..];
    let mut cursor = Cursor::new(payload);
    let value = ciborium::de::from_reader::<CborValue, _>(&mut cursor).map_err(|error| {
        JsyncError::new(
            JsyncErrorKind::CborDecode,
            "The Jsync payload could not be decoded as CBOR.",
        )
        .with_source(anyhow::Error::new(error))
    })?;
    if cursor.position() != payload.len() as u64 {
        return Err(JsyncError::new(
            JsyncErrorKind::TrailingBytes,
            "The Jsync payload contains trailing bytes.",
        )
        .with_metadata(
            "remaining",
            (payload.len() as u64 - cursor.position()).to_string(),
        ));
    }
    Ok(value)
}

/// Validates and converts a decoded CBOR message into structured actions.
fn parse_actions(value: CborValue) -> Result<Vec<Action>, JsyncError> {
    let actions = match value {
        CborValue::Array(actions) => actions,
        _ => {
            return Err(JsyncError::new(
                JsyncErrorKind::MessageNotArray,
                "The Jsync message payload must be an array.",
            ));
        }
    };

    actions
        .into_iter()
        .enumerate()
        .map(|(index, action)| {
            parse_action(action).map_err(|error| {
                error
                    .with_metadata("action_index", index.to_string())
                    .with_context("while parsing a Jsync action")
            })
        })
        .collect()
}

/// Validates one raw CBOR action without relying on its position in the message.
fn parse_action(value: CborValue) -> Result<Action, JsyncError> {
    let action = match value {
        CborValue::Array(action) => action,
        _ => {
            return Err(JsyncError::new(
                JsyncErrorKind::ActionNotArray,
                "The Jsync action must be an array.",
            ));
        }
    };
    if action.is_empty() {
        return Err(JsyncError::new(
            JsyncErrorKind::InvalidActionLength,
            "The Jsync action has no opcode.",
        )
        .with_metadata("expected", "at least 1")
        .with_metadata("actual", "0"));
    }

    let opcode = action
        .first()
        .and_then(CborValue::as_integer)
        .map(i128::from)
        .ok_or_else(|| {
            JsyncError::new(
                JsyncErrorKind::UnknownAction,
                "The Jsync action opcode must be an integer.",
            )
        })?;

    match opcode {
        0 => {
            require_action_length(action.len(), 2)?;
            let snapshot = action
                .into_iter()
                .nth(1)
                .expect("validated snapshot length");
            let value = to_json(snapshot)
                .map_err(|error| error.with_context("while decoding the SNAPSHOT value"))?;
            Ok(Action::Snapshot { value })
        }
        1 => {
            require_action_length(action.len(), 3)?;
            let mut elements = action.into_iter();
            let _opcode = elements.next();
            let raw_path = elements.next().expect("validated add length");
            let path = parse_path(raw_path)
                .map_err(|error| error.with_context("while parsing the ADD path"))?;
            let raw_value = elements.next().expect("validated add length");
            let value = to_json(raw_value)
                .map_err(|error| error.with_context("while decoding the ADD value"))?;
            Ok(Action::Add { path, value })
        }
        2 => {
            require_action_length(action.len(), 2)?;
            let mut elements = action.into_iter();
            let _opcode = elements.next();
            let path = parse_path(elements.next().expect("validated remove length"))
                .map_err(|error| error.with_context("while parsing the REMOVE path"))?;
            Ok(Action::Remove { path })
        }
        3 => {
            require_action_length(action.len(), 3)?;
            let mut elements = action.into_iter();
            let _opcode = elements.next();
            let path = parse_path(elements.next().expect("validated replace length"))
                .map_err(|error| error.with_context("while parsing the REPLACE path"))?;
            let value = to_json(elements.next().expect("validated replace length"))
                .map_err(|error| error.with_context("while decoding the REPLACE value"))?;
            Ok(Action::Replace { path, value })
        }
        4 => {
            require_action_length(action.len(), 3)?;
            let mut elements = action.into_iter();
            let _opcode = elements.next();
            let path = parse_path(elements.next().expect("validated append length"))
                .map_err(|error| error.with_context("while parsing the APPEND path"))?;
            let text = parse_text(elements.next().expect("validated append length"))
                .map_err(|error| error.with_context("while decoding the APPEND text"))?;
            Ok(Action::Append { path, text })
        }
        5 => {
            require_action_length(action.len(), 3)?;
            let mut elements = action.into_iter();
            let _opcode = elements.next();
            let path = parse_path(elements.next().expect("validated prepend length"))
                .map_err(|error| error.with_context("while parsing the PREPEND path"))?;
            let text = parse_text(elements.next().expect("validated prepend length"))
                .map_err(|error| error.with_context("while decoding the PREPEND text"))?;
            Ok(Action::Prepend { path, text })
        }
        opcode => Err(JsyncError::new(
            JsyncErrorKind::UnknownAction,
            "The Jsync action opcode is not supported.",
        )
        .with_metadata("opcode", opcode.to_string())),
    }
}

/// Validates the exact arity required by an action opcode.
fn require_action_length(actual: usize, expected: usize) -> Result<(), JsyncError> {
    if actual == expected {
        Ok(())
    } else {
        Err(JsyncError::new(
            JsyncErrorKind::InvalidActionLength,
            "The Jsync action has an invalid number of elements.",
        )
        .with_metadata("expected", expected.to_string())
        .with_metadata("actual", actual.to_string()))
    }
}

/// Converts a raw CBOR path array into validated path segments.
fn parse_path(value: CborValue) -> Result<Vec<PathSegment>, JsyncError> {
    let path = match value {
        CborValue::Array(path) => path,
        _ => {
            return Err(JsyncError::new(
                JsyncErrorKind::InvalidPath,
                "The path must be an array.",
            ));
        }
    };

    path.into_iter()
        .enumerate()
        .map(|(segment_index, segment)| match segment {
            CborValue::Text(key) => Ok(PathSegment::Key(key)),
            CborValue::Integer(integer) => {
                let integer = i128::from(integer);
                if integer < 0 {
                    return Err(JsyncError::new(
                        JsyncErrorKind::InvalidPath,
                        "A path index must be non-negative.",
                    )
                    .with_metadata("segment", integer.to_string())
                    .with_metadata("segment_index", segment_index.to_string()));
                }
                usize::try_from(integer)
                    .map(PathSegment::Index)
                    .map_err(|_| {
                        JsyncError::new(JsyncErrorKind::InvalidPath, "A path index is too large.")
                            .with_metadata("segment", integer.to_string())
                            .with_metadata("segment_index", segment_index.to_string())
                    })
            }
            _ => Err(JsyncError::new(
                JsyncErrorKind::InvalidPath,
                "A path segment must be a string or non-negative integer.",
            )
            .with_metadata("segment_index", segment_index.to_string())),
        })
        .collect()
}

fn action_to_cbor(action: &Action) -> Result<CborValue, JsyncError> {
    match action {
        Action::Snapshot { value } => Ok(CborValue::Array(vec![integer(0), json_to_cbor(value)?])),
        Action::Add { path, value } => Ok(CborValue::Array(vec![
            integer(1),
            path_to_cbor(path),
            json_to_cbor(value)?,
        ])),
        Action::Remove { path } => Ok(CborValue::Array(vec![integer(2), path_to_cbor(path)])),
        Action::Replace { path, value } => Ok(CborValue::Array(vec![
            integer(3),
            path_to_cbor(path),
            json_to_cbor(value)?,
        ])),
        Action::Append { path, text } => Ok(CborValue::Array(vec![
            integer(4),
            path_to_cbor(path),
            CborValue::Text(text.clone()),
        ])),
        Action::Prepend { path, text } => Ok(CborValue::Array(vec![
            integer(5),
            path_to_cbor(path),
            CborValue::Text(text.clone()),
        ])),
    }
}

fn path_to_cbor(path: &[PathSegment]) -> CborValue {
    CborValue::Array(
        path.iter()
            .map(|segment| match segment {
                PathSegment::Key(key) => CborValue::Text(key.clone()),
                PathSegment::Index(index) => integer(*index as u64),
            })
            .collect(),
    )
}

fn parse_text(value: CborValue) -> Result<String, JsyncError> {
    match value {
        CborValue::Text(text) => Ok(text),
        _ => Err(JsyncError::new(
            JsyncErrorKind::InvalidJsonValue,
            "The string patch text must be a CBOR text string.",
        )),
    }
}

fn to_json(value: CborValue) -> Result<Value, JsyncError> {
    match value {
        CborValue::Null => Ok(Value::Null),
        CborValue::Bool(value) => Ok(Value::Bool(value)),
        CborValue::Integer(integer) => {
            let text = i128::from(integer).to_string();
            serde_json::from_str(&text).map_err(|error| {
                JsyncError::new(
                    JsyncErrorKind::InvalidJsonValue,
                    "The integer is not representable as a JSON number.",
                )
                .with_source(anyhow::Error::new(error))
            })
        }
        CborValue::Float(value) if value.is_finite() => {
            Number::from_f64(value).map(Value::Number).ok_or_else(|| {
                JsyncError::new(
                    JsyncErrorKind::InvalidJsonValue,
                    "The float is not representable as a JSON number.",
                )
            })
        }
        CborValue::Float(_) => Err(JsyncError::new(
            JsyncErrorKind::InvalidJsonValue,
            "A non-finite float is not allowed in JSON.",
        )),
        CborValue::Text(value) => Ok(Value::String(value)),
        CborValue::Array(values) => values
            .into_iter()
            .map(to_json)
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array),
        CborValue::Map(entries) => {
            let mut object = Map::new();
            for (key, value) in entries {
                let CborValue::Text(key) = key else {
                    return Err(JsyncError::new(
                        JsyncErrorKind::InvalidJsonValue,
                        "JSON object keys must be strings.",
                    ));
                };
                object.insert(key, to_json(value)?);
            }
            Ok(Value::Object(object))
        }
        CborValue::Bytes(_) => Err(JsyncError::new(
            JsyncErrorKind::InvalidJsonValue,
            "CBOR byte strings are not allowed in JSON.",
        )),
        CborValue::Tag(_, _) => Err(JsyncError::new(
            JsyncErrorKind::InvalidJsonValue,
            "CBOR tags are not allowed in JSON.",
        )),
        _ => Err(JsyncError::new(
            JsyncErrorKind::InvalidJsonValue,
            "This CBOR value type is not allowed in JSON.",
        )),
    }
}

fn json_to_cbor(value: &Value) -> Result<CborValue, JsyncError> {
    match value {
        Value::Null => Ok(CborValue::Null),
        Value::Bool(value) => Ok(CborValue::Bool(*value)),
        Value::Number(number) => number_to_cbor(number),
        Value::String(value) => Ok(CborValue::Text(value.clone())),
        Value::Array(values) => Ok(CborValue::Array(
            values
                .iter()
                .map(json_to_cbor)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        Value::Object(object) => Ok(CborValue::Map(
            object
                .iter()
                .map(|(key, value)| Ok((CborValue::Text(key.clone()), json_to_cbor(value)?)))
                .collect::<Result<Vec<_>, JsyncError>>()?,
        )),
    }
}

fn number_to_cbor(number: &Number) -> Result<CborValue, JsyncError> {
    if let Some(value) = number.as_i64() {
        return Ok(integer(value));
    }
    if let Some(value) = number.as_u64() {
        return Ok(integer(value));
    }

    let text = number.to_string();
    if !text.contains('.') && !text.contains('e') && !text.contains('E') {
        return Err(JsyncError::new(
            JsyncErrorKind::InvalidJsonValue,
            "The JSON integer is outside the supported CBOR integer range.",
        ));
    }
    if let Some(value) = number.as_f64() {
        return Ok(CborValue::Float(value));
    }

    Err(JsyncError::new(
        JsyncErrorKind::InvalidJsonValue,
        "The JSON number cannot be encoded as a CBOR number.",
    ))
}

fn integer<T>(value: T) -> CborValue
where
    ciborium::value::Integer: From<T>,
{
    CborValue::Integer(value.into())
}
