use ciborium::Value as CborValue;
use serde_json::{Map, Number, Value};

use crate::error::{JsyncError, JsyncErrorKind};

/// Represents one segment in an already validated ADD path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PathSegment {
    /// Selects an object property by key.
    Key(String),
    /// Selects an array element by non-negative index.
    Index(usize),
}

/// Represents one validated action ready to be applied.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Action {
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
}

/// Validates and converts a decoded CBOR message into executable actions.
pub(crate) fn parse_actions(value: CborValue) -> Result<Vec<Action>, JsyncError> {
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
                "The ADD path must be an array.",
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
                        "An ADD path index must be non-negative.",
                    )
                    .with_metadata("segment", integer.to_string())
                    .with_metadata("segment_index", segment_index.to_string()));
                }
                usize::try_from(integer)
                    .map(PathSegment::Index)
                    .map_err(|_| {
                        JsyncError::new(
                            JsyncErrorKind::InvalidPath,
                            "An ADD path index is too large.",
                        )
                        .with_metadata("segment", integer.to_string())
                        .with_metadata("segment_index", segment_index.to_string())
                    })
            }
            _ => Err(JsyncError::new(
                JsyncErrorKind::InvalidPath,
                "An ADD path segment must be a string or non-negative integer.",
            )
            .with_metadata("segment_index", segment_index.to_string())),
        })
        .collect()
}

/// Converts a raw CBOR value into the supported JSON data model.
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
