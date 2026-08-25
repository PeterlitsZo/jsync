use std::io::Cursor;

use ciborium::Value as CborValue;
use serde_json::Value;

use crate::error::{JsyncError, JsyncErrorKind};
use crate::value::{Action, PathSegment, parse_actions};

const HEADER: [u8; 3] = [0xd9, 0xff, 0x01];

/// Consumes Jsync messages and maintains the current JSON document.
#[derive(Debug, Default)]
pub struct Consumer {
    document: Option<Value>,
    initialized: bool,
}

impl Consumer {
    /// Creates an empty consumer that is waiting for its initial SNAPSHOT.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the current document, or `None` before the first successful message.
    pub fn document(&self) -> Option<&Value> {
        self.document.as_ref()
    }

    /// Decodes and atomically applies one Jsync message.
    pub fn consume(&mut self, message: &[u8]) -> Result<(), JsyncError> {
        let actions = parse_actions(decode_payload(message)?)?;
        if !self.initialized && !matches!(actions.first(), Some(Action::Snapshot { .. })) {
            return Err(JsyncError::new(
                JsyncErrorKind::InitialSnapshotRequired,
                "The first Jsync message must start with SNAPSHOT.",
            ));
        }

        let mut working = self.document.clone().unwrap_or(Value::Null);
        for (index, action) in actions.into_iter().enumerate() {
            apply_action(&mut working, action).map_err(|error| {
                error
                    .with_metadata("action_index", index.to_string())
                    .with_context("while applying a Jsync action")
            })?;
        }

        self.document = Some(working);
        self.initialized = true;
        Ok(())
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

/// Applies one validated action to a working document.
fn apply_action(root: &mut Value, action: Action) -> Result<(), JsyncError> {
    match action {
        Action::Snapshot { value } => {
            *root = value;
            Ok(())
        }
        Action::Add { path, value } => apply_add(root, &path, value),
    }
}

/// Applies an ADD action to an object, array, or root document.
fn apply_add(root: &mut Value, path: &[PathSegment], value: Value) -> Result<(), JsyncError> {
    if path.is_empty() {
        *root = value;
        return Ok(());
    }

    let (parent_path, final_segment) = path.split_at(path.len() - 1);
    let parent = resolve_container(root, parent_path)?;
    match (parent, &final_segment[0]) {
        (Value::Object(object), PathSegment::Key(key)) => {
            object.insert(key.clone(), value);
            Ok(())
        }
        (Value::Array(array), PathSegment::Index(index)) => {
            if *index > array.len() {
                return Err(JsyncError::new(
                    JsyncErrorKind::ArrayIndexOutOfBounds,
                    "The ADD index is greater than the array length.",
                )
                .with_metadata("index", index.to_string())
                .with_metadata("length", array.len().to_string())
                .with_metadata("segment_index", (path.len() - 1).to_string())
                .with_context("while applying the final ADD path segment"));
            }
            array.insert(*index, value);
            Ok(())
        }
        (Value::Array(array), PathSegment::Key(key)) if key == "-" => {
            array.push(value);
            Ok(())
        }
        (Value::Array(_), PathSegment::Key(key)) => Err(JsyncError::new(
            JsyncErrorKind::InvalidPath,
            "An array final segment must be a non-negative integer or '-'.",
        )
        .with_metadata("segment", key.clone())
        .with_metadata("segment_index", (path.len() - 1).to_string())
        .with_context("while applying the final ADD path segment")),
        (Value::Object(_), PathSegment::Index(index)) => Err(JsyncError::new(
            JsyncErrorKind::InvalidPath,
            "An object final segment must be a string.",
        )
        .with_metadata("index", index.to_string())
        .with_metadata("segment_index", (path.len() - 1).to_string())
        .with_context("while applying the final ADD path segment")),
        (Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_), _) => {
            Err(JsyncError::new(
                JsyncErrorKind::PathParentNotContainer,
                "The ADD path parent is a scalar instead of an object or array.",
            )
            .with_metadata("segment_index", (path.len() - 1).to_string())
            .with_context("while applying the final ADD path segment"))
        }
    }
}

/// Resolves an existing object or array container for an ADD parent path.
fn resolve_container<'a>(
    mut current: &'a mut Value,
    path: &[PathSegment],
) -> Result<&'a mut Value, JsyncError> {
    for (segment_index, segment) in path.iter().enumerate() {
        current = match (current, segment) {
            (Value::Object(object), PathSegment::Key(key)) => {
                object.get_mut(key).ok_or_else(|| {
                    JsyncError::new(
                        JsyncErrorKind::PathParentMissing,
                        "The ADD path object key does not exist.",
                    )
                    .with_metadata("key", key.clone())
                    .with_metadata("segment_index", segment_index.to_string())
                    .with_context("while resolving an ADD path parent")
                })?
            }
            (Value::Array(array), PathSegment::Index(index)) => {
                let length = array.len();
                array.get_mut(*index).ok_or_else(|| {
                    JsyncError::new(
                        JsyncErrorKind::ArrayIndexOutOfBounds,
                        "The ADD path index is outside the array.",
                    )
                    .with_metadata("index", index.to_string())
                    .with_metadata("length", length.to_string())
                    .with_metadata("segment_index", segment_index.to_string())
                    .with_context("while resolving an ADD path parent")
                })?
            }
            (Value::Array(_), PathSegment::Key(key)) => {
                return Err(JsyncError::new(
                    JsyncErrorKind::InvalidPath,
                    "An array intermediate segment must be an index.",
                )
                .with_metadata("segment", key.clone())
                .with_metadata("segment_index", segment_index.to_string())
                .with_context("while resolving an intermediate ADD path segment"));
            }
            (Value::Object(_), PathSegment::Index(index)) => {
                return Err(JsyncError::new(
                    JsyncErrorKind::InvalidPath,
                    "An object intermediate segment must be a string.",
                )
                .with_metadata("index", index.to_string())
                .with_metadata("segment_index", segment_index.to_string())
                .with_context("while resolving an intermediate ADD path segment"));
            }
            (Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_), _) => {
                return Err(JsyncError::new(
                    JsyncErrorKind::PathParentNotContainer,
                    "The ADD path traversed a scalar.",
                )
                .with_metadata("segment_index", segment_index.to_string())
                .with_context("while resolving an ADD path parent"));
            }
        };
    }
    Ok(current)
}
