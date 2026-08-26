use std::collections::HashMap;
use std::io::Cursor;

use ciborium::Value as CborValue;
use serde_json::{Map, Number, Value};

use crate::error::{JsyncError, JsyncErrorKind};

const HEADER: [u8; 3] = [0xd9, 0xff, 0x01];
const MAX_SAFE_JSON_INTEGER: i128 = 9_007_199_254_740_991;

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
    /// Copies an existing JSON value to another path.
    Copy {
        /// The validated source path.
        from: Vec<PathSegment>,
        /// The validated destination path.
        path: Vec<PathSegment>,
    },
    /// Moves an existing JSON value to another path.
    Move {
        /// The validated source path.
        from: Vec<PathSegment>,
        /// The validated destination path.
        path: Vec<PathSegment>,
    },
}

/// One segment in a validated Jsync action path.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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

    /// Decodes a message using a caller-owned consumer path segment pool transaction.
    pub fn from_bytes_with_pool_txn(
        bytes: Vec<u8>,
        txn: &mut ConsumerPathSegmentPoolTransaction<'_>,
    ) -> Result<Self, JsyncError> {
        decode_payload(&bytes)
            .and_then(|payload| parse_message(payload, txn))
            .map(|actions| Self { actions })
    }

    /// Encodes this message using a caller-owned producer path segment pool
    /// transaction.
    pub fn to_bytes_with_pool_txn(
        &self,
        txn: &mut ProducerPathSegmentPoolTransaction<'_>,
    ) -> Result<Vec<u8>, JsyncError> {
        // Encode the actions into CBOR. It will interact with the txn to append
        // path segments as needed.
        let actions = self
            .actions
            .iter()
            .map(|action| action_to_cbor(action, txn))
            .collect::<Result<Vec<_>, _>>()?;

        // Encode the metadata into CBOR (we get the appended segments from the
        // txn).
        let metadata = CborValue::Array(vec![CborValue::Array(
            txn.appended_segments()
                .iter()
                .map(|segment| match segment {
                    PathSegment::Key(key) => CborValue::Text(key.clone()),
                    PathSegment::Index(index) => integer(*index as u64),
                })
                .collect(),
        )]);

        // Encode the payload into CBOR.
        let payload = CborValue::Array(vec![metadata, CborValue::Array(actions)]);

        // Serialize the payload into bytes with the header.
        {
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
}

/// Producer-side path segment pool with stable indexes and O(1) segment lookup.
#[derive(Debug, Clone, Default)]
pub struct ProducerPathSegmentPool {
    segments: Vec<PathSegment>,
    indexes: HashMap<PathSegment, usize>,
}

impl ProducerPathSegmentPool {
    /// Creates an empty producer path segment pool.
    pub fn new() -> Self {
        Self::default()
    }

    /// Starts an atomic producer pool update.
    pub fn transaction(&mut self) -> ProducerPathSegmentPoolTransaction<'_> {
        let checkpoint = self.segments.len();
        ProducerPathSegmentPoolTransaction {
            pool: self,
            checkpoint,
            committed: false,
        }
    }

    fn index_for(&mut self, segment: &PathSegment) -> usize {
        if let Some(index) = self.indexes.get(segment) {
            return *index;
        }

        let index = self.segments.len();
        let segment = segment.clone();
        self.segments.push(segment.clone());
        self.indexes.insert(segment, index);
        index
    }

    pub(crate) fn index_of(&self, segment: &PathSegment) -> Option<usize> {
        self.indexes.get(segment).copied()
    }

    /// Returns the committed pool size so producer-side estimators can simulate
    /// future appended segment indexes without mutating the real pool.
    pub(crate) fn len(&self) -> usize {
        self.segments.len()
    }

    fn rollback_to(&mut self, len: usize) {
        if len >= self.segments.len() {
            return;
        }

        for segment in self.segments.drain(len..) {
            self.indexes.remove(&segment);
        }
    }
}

/// Producer-side path segment pool transaction.
#[derive(Debug)]
pub struct ProducerPathSegmentPoolTransaction<'a> {
    pool: &'a mut ProducerPathSegmentPool,
    checkpoint: usize,
    committed: bool,
}

impl ProducerPathSegmentPoolTransaction<'_> {
    /// Returns the segments appended since this transaction started.
    pub fn appended_segments(&self) -> &[PathSegment] {
        &self.pool.segments[self.checkpoint..]
    }

    /// Commits this transaction.
    pub fn commit(mut self) {
        self.committed = true;
    }

    /// Aborts this transaction and rolls the pool back to its checkpoint.
    pub fn abort(self) {}
}

impl Drop for ProducerPathSegmentPoolTransaction<'_> {
    fn drop(&mut self) {
        if !self.committed {
            self.pool.rollback_to(self.checkpoint);
        }
    }
}

/// Consumer-side path segment pool with stable indexes.
#[derive(Debug, Clone, Default)]
pub struct ConsumerPathSegmentPool {
    segments: Vec<PathSegment>,
}

impl ConsumerPathSegmentPool {
    /// Creates an empty consumer path segment pool.
    pub fn new() -> Self {
        Self::default()
    }

    /// Starts an atomic consumer pool update.
    pub fn transaction(&mut self) -> ConsumerPathSegmentPoolTransaction<'_> {
        let checkpoint = self.segments.len();
        ConsumerPathSegmentPoolTransaction {
            pool: self,
            checkpoint,
            committed: false,
        }
    }

    fn rollback_to(&mut self, len: usize) {
        self.segments.truncate(len);
    }
}

/// Consumer-side path segment pool transaction.
#[derive(Debug)]
pub struct ConsumerPathSegmentPoolTransaction<'a> {
    pool: &'a mut ConsumerPathSegmentPool,
    checkpoint: usize,
    committed: bool,
}

impl ConsumerPathSegmentPoolTransaction<'_> {
    /// Appends path segments declared by message metadata.
    pub fn append_segments(&mut self, segments: Vec<PathSegment>) {
        self.pool.segments.extend(segments);
    }

    /// Commits this transaction.
    pub fn commit(mut self) {
        self.committed = true;
    }

    /// Aborts this transaction and rolls the pool back to its checkpoint.
    pub fn abort(self) {}
}

impl Drop for ConsumerPathSegmentPoolTransaction<'_> {
    fn drop(&mut self) {
        if !self.committed {
            self.pool.rollback_to(self.checkpoint);
        }
    }
}

/// Decodes the one CBOR payload after validating and removing the Jsync header.
fn decode_payload(message: &[u8]) -> Result<CborValue, JsyncError> {
    // Check the header and version.
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

    // Parse it as a CBOR value.
    let payload = &message[HEADER.len()..];
    let mut cursor = Cursor::new(payload);
    let value = ciborium::de::from_reader::<CborValue, _>(&mut cursor).map_err(|error| {
        JsyncError::new(
            JsyncErrorKind::CborDecode,
            "The Jsync payload could not be decoded as CBOR.",
        )
        .with_source(anyhow::Error::new(error))
    })?;

    // Make sure there are no trailing bytes.
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
fn parse_message(
    value: CborValue,
    txn: &mut ConsumerPathSegmentPoolTransaction<'_>,
) -> Result<Vec<Action>, JsyncError> {
    // Check the schema of the message -- it must be an array with two elements.
    let message = match value {
        CborValue::Array(message) => message,
        _ => {
            return Err(JsyncError::new(
                JsyncErrorKind::MessageNotArray,
                "The Jsync message payload must be an array.",
            ));
        }
    };
    if message.len() != 2 {
        return Err(JsyncError::new(
            JsyncErrorKind::InvalidActionLength,
            "The Jsync message payload must contain metadata and actions.",
        )
        .with_metadata("expected", "2")
        .with_metadata("actual", message.len().to_string()));
    }

    // Parse the metadata and append the path segments in metadata to the path
    // segment pool.
    let mut elements = message.into_iter();
    let to_append_path_segment_pool =
        parse_metadata(elements.next().expect("validated message length"))?;
    txn.append_segments(to_append_path_segment_pool);

    // Parse the actions from the message payload.
    let actions = match elements.next().expect("validated message length") {
        CborValue::Array(actions) => actions,
        _ => {
            return Err(JsyncError::new(
                JsyncErrorKind::MessageNotArray,
                "The Jsync actions payload must be an array.",
            ));
        }
    };
    let actions = actions
        .into_iter()
        .enumerate()
        .map(|(index, action)| {
            parse_action(action, txn).map_err(|error| {
                error
                    .with_metadata("action_index", index.to_string())
                    .with_context("while parsing a Jsync action")
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(actions)
}

fn parse_metadata(value: CborValue) -> Result<Vec<PathSegment>, JsyncError> {
    // Check the schema of the metadata.
    let metadata = match value {
        CborValue::Array(metadata) => metadata,
        _ => {
            return Err(JsyncError::new(
                JsyncErrorKind::MessageNotArray,
                "The Jsync metadata must be an array.",
            ));
        }
    };
    if metadata.len() != 1 {
        return Err(JsyncError::new(
            JsyncErrorKind::InvalidActionLength,
            "The Jsync metadata must contain the path segment pool append list.",
        )
        .with_metadata("expected", "1")
        .with_metadata("actual", metadata.len().to_string()));
    }

    // Parse the path segments from the metadata.
    parse_path_segments(
        metadata
            .into_iter()
            .next()
            .expect("validated metadata length"),
    )
}

/// Validates one raw CBOR action without relying on its position in the message.
fn parse_action(
    value: CborValue,
    txn: &ConsumerPathSegmentPoolTransaction<'_>,
) -> Result<Action, JsyncError> {
    // Check the schema of the action.
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

    // Parse the opcode from the action.
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

    // Parse the action based on the opcode.
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
            let path = parse_pooled_path_with_txn(raw_path, txn)
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
            let path =
                parse_pooled_path_with_txn(elements.next().expect("validated remove length"), txn)
                    .map_err(|error| error.with_context("while parsing the REMOVE path"))?;
            Ok(Action::Remove { path })
        }
        3 => {
            require_action_length(action.len(), 3)?;
            let mut elements = action.into_iter();
            let _opcode = elements.next();
            let path =
                parse_pooled_path_with_txn(elements.next().expect("validated replace length"), txn)
                    .map_err(|error| error.with_context("while parsing the REPLACE path"))?;
            let value = to_json(elements.next().expect("validated replace length"))
                .map_err(|error| error.with_context("while decoding the REPLACE value"))?;
            Ok(Action::Replace { path, value })
        }
        4 => {
            require_action_length(action.len(), 3)?;
            let mut elements = action.into_iter();
            let _opcode = elements.next();
            let path =
                parse_pooled_path_with_txn(elements.next().expect("validated append length"), txn)
                    .map_err(|error| error.with_context("while parsing the APPEND path"))?;
            let text = parse_text(elements.next().expect("validated append length"))
                .map_err(|error| error.with_context("while decoding the APPEND text"))?;
            Ok(Action::Append { path, text })
        }
        5 => {
            require_action_length(action.len(), 3)?;
            let mut elements = action.into_iter();
            let _opcode = elements.next();
            let path =
                parse_pooled_path_with_txn(elements.next().expect("validated prepend length"), txn)
                    .map_err(|error| error.with_context("while parsing the PREPEND path"))?;
            let text = parse_text(elements.next().expect("validated prepend length"))
                .map_err(|error| error.with_context("while decoding the PREPEND text"))?;
            Ok(Action::Prepend { path, text })
        }
        6 => {
            require_action_length(action.len(), 3)?;
            let mut elements = action.into_iter();
            let _opcode = elements.next();
            let from =
                parse_pooled_path_with_txn(elements.next().expect("validated copy length"), txn)
                    .map_err(|error| error.with_context("while parsing the COPY from path"))?;
            let path =
                parse_pooled_path_with_txn(elements.next().expect("validated copy length"), txn)
                    .map_err(|error| error.with_context("while parsing the COPY path"))?;
            Ok(Action::Copy { from, path })
        }
        7 => {
            require_action_length(action.len(), 3)?;
            let mut elements = action.into_iter();
            let _opcode = elements.next();
            let from =
                parse_pooled_path_with_txn(elements.next().expect("validated move length"), txn)
                    .map_err(|error| error.with_context("while parsing the MOVE from path"))?;
            let path =
                parse_pooled_path_with_txn(elements.next().expect("validated move length"), txn)
                    .map_err(|error| error.with_context("while parsing the MOVE path"))?;
            Ok(Action::Move { from, path })
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

/// Converts raw path segment metadata into validated path segments.
fn parse_path_segments(value: CborValue) -> Result<Vec<PathSegment>, JsyncError> {
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

/// Converts a raw CBOR path index array into validated path segments.
fn parse_pooled_path_with_txn(
    value: CborValue,
    txn: &ConsumerPathSegmentPoolTransaction<'_>,
) -> Result<Vec<PathSegment>, JsyncError> {
    // Check the schema of the path.
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
        .map(|(segment_index, segment)| {
            // Parse and check the segment.
            let CborValue::Integer(integer) = segment else {
                return Err(JsyncError::new(
                    JsyncErrorKind::InvalidPath,
                    "A path segment pool index must be a non-negative integer.",
                )
                .with_metadata("segment_index", segment_index.to_string()));
            };
            let integer = i128::from(integer);
            if integer < 0 {
                return Err(JsyncError::new(
                    JsyncErrorKind::InvalidPath,
                    "A path segment pool index must be non-negative.",
                )
                .with_metadata("segment", integer.to_string())
                .with_metadata("segment_index", segment_index.to_string()));
            }
            let index = usize::try_from(integer).map_err(|_| {
                JsyncError::new(
                    JsyncErrorKind::InvalidPath,
                    "A path segment pool index is too large.",
                )
                .with_metadata("segment", integer.to_string())
                .with_metadata("segment_index", segment_index.to_string())
            })?;

            // Get the segment from the pool .
            txn.pool.segments.get(index).cloned().ok_or_else(|| {
                JsyncError::new(
                    JsyncErrorKind::InvalidPath,
                    "A path segment pool index is outside the current pool.",
                )
                .with_metadata("index", index.to_string())
                .with_metadata("length", txn.pool.segments.len().to_string())
                .with_metadata("segment_index", segment_index.to_string())
            })
        })
        .collect()
}

fn action_to_cbor(
    action: &Action,
    txn: &mut ProducerPathSegmentPoolTransaction<'_>,
) -> Result<CborValue, JsyncError> {
    fn pooled_path_to_cbor(
        txn: &mut ProducerPathSegmentPoolTransaction<'_>,
        path: &[PathSegment],
    ) -> CborValue {
        CborValue::Array(
            path.iter()
                .map(|segment| txn.pool.index_for(segment))
                .map(|index| integer(index as u64))
                .collect(),
        )
    }

    match action {
        Action::Snapshot { value } => Ok(CborValue::Array(vec![integer(0), json_to_cbor(value)?])),
        Action::Add { path, value } => Ok(CborValue::Array(vec![
            integer(1),
            pooled_path_to_cbor(txn, path),
            json_to_cbor(value)?,
        ])),
        Action::Remove { path } => Ok(CborValue::Array(vec![
            integer(2),
            pooled_path_to_cbor(txn, path),
        ])),
        Action::Replace { path, value } => Ok(CborValue::Array(vec![
            integer(3),
            pooled_path_to_cbor(txn, path),
            json_to_cbor(value)?,
        ])),
        Action::Append { path, text } => Ok(CborValue::Array(vec![
            integer(4),
            pooled_path_to_cbor(txn, path),
            CborValue::Text(text.clone()),
        ])),
        Action::Prepend { path, text } => Ok(CborValue::Array(vec![
            integer(5),
            pooled_path_to_cbor(txn, path),
            CborValue::Text(text.clone()),
        ])),
        Action::Copy { from, path } => Ok(CborValue::Array(vec![
            integer(6),
            pooled_path_to_cbor(txn, from),
            pooled_path_to_cbor(txn, path),
        ])),
        Action::Move { from, path } => Ok(CborValue::Array(vec![
            integer(7),
            pooled_path_to_cbor(txn, from),
            pooled_path_to_cbor(txn, path),
        ])),
    }
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
            let integer = i128::from(integer);
            validate_safe_json_integer(integer)?;
            let text = integer.to_string();
            serde_json::from_str(&text).map_err(|error| {
                JsyncError::new(
                    JsyncErrorKind::InvalidJsonValue,
                    "The integer is not representable as a JSON number.",
                )
                .with_source(anyhow::Error::new(error))
            })
        }
        CborValue::Float(value) if value.is_finite() => {
            validate_safe_json_float(value)?;
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
        validate_safe_json_integer(value as i128)?;
        return Ok(integer(value));
    }
    if let Some(value) = number.as_u64() {
        validate_safe_json_integer(value as i128)?;
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
        validate_safe_json_float(value)?;
        return Ok(CborValue::Float(value));
    }

    Err(JsyncError::new(
        JsyncErrorKind::InvalidJsonValue,
        "The JSON number cannot be encoded as a CBOR number.",
    ))
}

fn validate_safe_json_integer(value: i128) -> Result<(), JsyncError> {
    if (-MAX_SAFE_JSON_INTEGER..=MAX_SAFE_JSON_INTEGER).contains(&value) {
        Ok(())
    } else {
        Err(JsyncError::new(
            JsyncErrorKind::InvalidJsonValue,
            "The JSON integer is outside the cross-language safe integer range.",
        )
        .with_metadata("minimum", (-MAX_SAFE_JSON_INTEGER).to_string())
        .with_metadata("maximum", MAX_SAFE_JSON_INTEGER.to_string())
        .with_metadata("value", value.to_string()))
    }
}

fn validate_safe_json_float(value: f64) -> Result<(), JsyncError> {
    if value.fract() != 0.0 || value.abs() <= MAX_SAFE_JSON_INTEGER as f64 {
        Ok(())
    } else {
        Err(JsyncError::new(
            JsyncErrorKind::InvalidJsonValue,
            "The JSON integer is outside the cross-language safe integer range.",
        )
        .with_metadata("minimum", (-MAX_SAFE_JSON_INTEGER).to_string())
        .with_metadata("maximum", MAX_SAFE_JSON_INTEGER.to_string())
        .with_metadata("value", value.to_string()))
    }
}

fn integer<T>(value: T) -> CborValue
where
    ciborium::value::Integer: From<T>,
{
    CborValue::Integer(value.into())
}
