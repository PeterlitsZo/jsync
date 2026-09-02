use std::collections::HashSet;
use std::io::Cursor;

use ciborium::Value as CborValue;

use super::json_cbor::{integer, json_to_cbor, to_json};
use super::opcode::{
    OPCODE_ADD, OPCODE_ARRAY_PATCH, OPCODE_COPY, OPCODE_MOVE, OPCODE_REMOVE, OPCODE_REPLACE,
    OPCODE_SNAPSHOT, OPCODE_STRING_APPEND, OPCODE_STRING_PATCH, OPCODE_STRING_PREPEND,
};
use super::{
    Action, ArrayPatchEdit, ConsumerPathSegmentPoolTransaction, PathSegment,
    ProducerPathSegmentPoolTransaction, StringPatchEdit,
};
use crate::error::{JsyncError, JsyncErrorKind};

const HEADER: [u8; 3] = [0xd9, 0xff, 0x01];
const METADATA_PATH_SEGMENT_POOL_APPEND: i128 = 0;

pub(super) fn from_bytes_with_pool_txn(
    bytes: &[u8],
    txn: &mut ConsumerPathSegmentPoolTransaction<'_>,
) -> Result<Vec<Action>, JsyncError> {
    decode_payload(bytes).and_then(|payload| parse_message(payload, txn))
}

pub(super) fn to_bytes_with_pool_txn(
    message_actions: &[Action],
    txn: &mut ProducerPathSegmentPoolTransaction<'_>,
) -> Result<Vec<u8>, JsyncError> {
    let actions = message_actions
        .iter()
        .map(|action| action_to_cbor(action, txn))
        .collect::<Result<Vec<_>, _>>()?;

    let appended_segments = txn
        .appended_segments()
        .iter()
        .map(|segment| match segment {
            PathSegment::Key(key) => CborValue::Text(key.clone()),
            PathSegment::Index(index) => integer(*index as u64),
        })
        .collect::<Vec<_>>();
    let actions = CborValue::Array(actions);
    let payload = if appended_segments.is_empty() {
        CborValue::Array(vec![actions])
    } else {
        CborValue::Array(vec![
            integer(METADATA_PATH_SEGMENT_POOL_APPEND as u64),
            CborValue::Array(appended_segments),
            actions,
        ])
    };

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

fn parse_message(
    value: CborValue,
    txn: &mut ConsumerPathSegmentPoolTransaction<'_>,
) -> Result<Vec<Action>, JsyncError> {
    let message = match value {
        CborValue::Array(message) => message,
        _ => {
            return Err(JsyncError::new(
                JsyncErrorKind::MessageNotArray,
                "The Jsync message payload must be an array.",
            ));
        }
    };
    if message.is_empty() || message.len() % 2 == 0 {
        return Err(JsyncError::new(
            JsyncErrorKind::InvalidActionLength,
            "The Jsync message payload must contain metadata tag/value pairs followed by actions.",
        )
        .with_metadata("expected", "a non-empty odd length")
        .with_metadata("actual", message.len().to_string()));
    }

    let mut message = message;
    let body = message.pop().expect("validated non-empty message");
    parse_metadata_pairs(message, txn)?;

    let actions = match body {
        CborValue::Array(actions) => actions,
        _ => {
            return Err(JsyncError::new(
                JsyncErrorKind::MessageNotArray,
                "The Jsync actions payload must be an array.",
            ));
        }
    };
    actions
        .into_iter()
        .enumerate()
        .map(|(index, action)| {
            parse_action(action, txn).map_err(|error| {
                error
                    .with_metadata("action_index", index.to_string())
                    .with_context("while parsing a Jsync action")
            })
        })
        .collect()
}

fn parse_metadata_pairs(
    metadata: Vec<CborValue>,
    txn: &mut ConsumerPathSegmentPoolTransaction<'_>,
) -> Result<(), JsyncError> {
    let mut seen_known_tags = HashSet::new();
    for pair in metadata.chunks_exact(2) {
        let tag = pair[0].as_integer().map(i128::from).ok_or_else(|| {
            JsyncError::new(
                JsyncErrorKind::InvalidJsonValue,
                "A Jsync metadata tag must be an integer.",
            )
        })?;

        match tag {
            METADATA_PATH_SEGMENT_POOL_APPEND => {
                if !seen_known_tags.insert(tag) {
                    return Err(JsyncError::new(
                        JsyncErrorKind::InvalidActionLength,
                        "A Jsync metadata tag must not appear more than once.",
                    )
                    .with_metadata("tag", tag.to_string()));
                }
                txn.append_segments(parse_path_segments(pair[1].clone())?);
            }
            _ => {}
        }
    }
    Ok(())
}

fn parse_action(
    value: CborValue,
    txn: &ConsumerPathSegmentPoolTransaction<'_>,
) -> Result<Action, JsyncError> {
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
        opcode if opcode == i128::from(OPCODE_SNAPSHOT) => {
            require_action_length(action.len(), 2)?;
            let snapshot = action
                .into_iter()
                .nth(1)
                .expect("validated snapshot length");
            let value = to_json(snapshot)
                .map_err(|error| error.with_context("while decoding the SNAPSHOT value"))?;
            Ok(Action::Snapshot { value })
        }
        opcode if opcode == i128::from(OPCODE_ADD) => {
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
        opcode if opcode == i128::from(OPCODE_REMOVE) => {
            require_action_length(action.len(), 2)?;
            let mut elements = action.into_iter();
            let _opcode = elements.next();
            let path =
                parse_pooled_path_with_txn(elements.next().expect("validated remove length"), txn)
                    .map_err(|error| error.with_context("while parsing the REMOVE path"))?;
            Ok(Action::Remove { path })
        }
        opcode if opcode == i128::from(OPCODE_REPLACE) => {
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
        opcode if opcode == i128::from(OPCODE_STRING_APPEND) => {
            require_action_length(action.len(), 3)?;
            let mut elements = action.into_iter();
            let _opcode = elements.next();
            let path =
                parse_pooled_path_with_txn(elements.next().expect("validated append length"), txn)
                    .map_err(|error| error.with_context("while parsing the APPEND path"))?;
            let text = parse_text(elements.next().expect("validated append length"))
                .map_err(|error| error.with_context("while decoding the APPEND text"))?;
            Ok(Action::StringAppend { path, text })
        }
        opcode if opcode == i128::from(OPCODE_STRING_PREPEND) => {
            require_action_length(action.len(), 3)?;
            let mut elements = action.into_iter();
            let _opcode = elements.next();
            let path =
                parse_pooled_path_with_txn(elements.next().expect("validated prepend length"), txn)
                    .map_err(|error| error.with_context("while parsing the PREPEND path"))?;
            let text = parse_text(elements.next().expect("validated prepend length"))
                .map_err(|error| error.with_context("while decoding the PREPEND text"))?;
            Ok(Action::StringPrepend { path, text })
        }
        opcode if opcode == i128::from(OPCODE_STRING_PATCH) => {
            require_action_length(action.len(), 3)?;
            let mut elements = action.into_iter();
            let _opcode = elements.next();
            let path = parse_pooled_path_with_txn(
                elements.next().expect("validated string patch length"),
                txn,
            )
            .map_err(|error| error.with_context("while parsing the STRING_PATCH path"))?;
            let edits =
                parse_string_patch_edits(elements.next().expect("validated string patch length"))
                    .map_err(|error| error.with_context("while decoding the STRING_PATCH edits"))?;
            Ok(Action::StringPatch { path, edits })
        }
        opcode if opcode == i128::from(OPCODE_ARRAY_PATCH) => {
            require_action_length(action.len(), 3)?;
            let mut elements = action.into_iter();
            let _opcode = elements.next();
            let path = parse_pooled_path_with_txn(
                elements.next().expect("validated array patch length"),
                txn,
            )
            .map_err(|error| error.with_context("while parsing the ARRAY_PATCH path"))?;
            let edits =
                parse_array_patch_edits(elements.next().expect("validated array patch length"))
                    .map_err(|error| error.with_context("while decoding the ARRAY_PATCH edits"))?;
            Ok(Action::ArrayPatch { path, edits })
        }
        opcode if opcode == i128::from(OPCODE_COPY) => {
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
        opcode if opcode == i128::from(OPCODE_MOVE) => {
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

fn parse_pooled_path_with_txn(
    value: CborValue,
    txn: &ConsumerPathSegmentPoolTransaction<'_>,
) -> Result<Vec<PathSegment>, JsyncError> {
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

            txn.path_segment_at(index).ok_or_else(|| {
                JsyncError::new(
                    JsyncErrorKind::InvalidPath,
                    "A path segment pool index is outside the current pool.",
                )
                .with_metadata("index", index.to_string())
                .with_metadata("length", txn.pool_len().to_string())
                .with_metadata("segment_index", segment_index.to_string())
            })
        })
        .collect()
}

fn parse_string_patch_edits(value: CborValue) -> Result<Vec<StringPatchEdit>, JsyncError> {
    let edits = match value {
        CborValue::Array(edits) => edits,
        _ => {
            return Err(JsyncError::new(
                JsyncErrorKind::InvalidJsonValue,
                "The STRING_PATCH edits must be an array.",
            ));
        }
    };

    edits
        .into_iter()
        .enumerate()
        .map(|(edit_index, edit)| {
            parse_string_patch_edit(edit).map_err(|error| {
                error
                    .with_metadata("edit_index", edit_index.to_string())
                    .with_context("while decoding a STRING_PATCH edit")
            })
        })
        .collect()
}

fn parse_string_patch_edit(value: CborValue) -> Result<StringPatchEdit, JsyncError> {
    let edit = match value {
        CborValue::Array(edit) => edit,
        _ => {
            return Err(JsyncError::new(
                JsyncErrorKind::InvalidJsonValue,
                "A STRING_PATCH edit must be an array.",
            ));
        }
    };
    require_action_length(edit.len(), 3)?;

    let mut elements = edit.into_iter();
    let start = parse_usize(elements.next().expect("validated string patch edit length"))
        .map_err(|error| error.with_context("while decoding the STRING_PATCH edit start"))?;
    let delete_count = parse_usize(elements.next().expect("validated string patch edit length"))
        .map_err(|error| error.with_context("while decoding the STRING_PATCH edit delete count"))?;
    let text = parse_text(elements.next().expect("validated string patch edit length"))
        .map_err(|error| error.with_context("while decoding the STRING_PATCH edit text"))?;

    Ok(StringPatchEdit {
        start,
        delete_count,
        text,
    })
}

fn parse_array_patch_edits(value: CborValue) -> Result<Vec<ArrayPatchEdit>, JsyncError> {
    let edits = match value {
        CborValue::Array(edits) => edits,
        _ => {
            return Err(JsyncError::new(
                JsyncErrorKind::InvalidJsonValue,
                "The ARRAY_PATCH edits must be an array.",
            ));
        }
    };
    if edits.is_empty() {
        return Err(JsyncError::new(
            JsyncErrorKind::InvalidJsonValue,
            "The ARRAY_PATCH edits cannot be empty.",
        ));
    }

    edits
        .into_iter()
        .enumerate()
        .map(|(edit_index, edit)| {
            parse_array_patch_edit(edit).map_err(|error| {
                error
                    .with_metadata("edit_index", edit_index.to_string())
                    .with_context("while decoding an ARRAY_PATCH edit")
            })
        })
        .collect()
}

fn parse_array_patch_edit(value: CborValue) -> Result<ArrayPatchEdit, JsyncError> {
    let edit = match value {
        CborValue::Array(edit) => edit,
        _ => {
            return Err(JsyncError::new(
                JsyncErrorKind::InvalidJsonValue,
                "An ARRAY_PATCH edit must be an array.",
            ));
        }
    };
    require_action_length(edit.len(), 3)?;

    let mut elements = edit.into_iter();
    let start = parse_usize(elements.next().expect("validated array patch edit length"))
        .map_err(|error| error.with_context("while decoding the ARRAY_PATCH edit start"))?;
    let delete_count = parse_usize(elements.next().expect("validated array patch edit length"))
        .map_err(|error| error.with_context("while decoding the ARRAY_PATCH edit delete count"))?;
    let values = match elements.next().expect("validated array patch edit length") {
        CborValue::Array(values) => values
            .into_iter()
            .map(to_json)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| error.with_context("while decoding the ARRAY_PATCH edit values"))?,
        _ => {
            return Err(JsyncError::new(
                JsyncErrorKind::InvalidJsonValue,
                "The ARRAY_PATCH edit values must be an array.",
            ));
        }
    };
    if delete_count == 0 && values.is_empty() {
        return Err(JsyncError::new(
            JsyncErrorKind::InvalidJsonValue,
            "An ARRAY_PATCH edit must delete or insert values.",
        ));
    }

    Ok(ArrayPatchEdit {
        start,
        delete_count,
        values,
    })
}

fn parse_usize(value: CborValue) -> Result<usize, JsyncError> {
    let CborValue::Integer(integer) = value else {
        return Err(JsyncError::new(
            JsyncErrorKind::InvalidJsonValue,
            "The value must be a non-negative integer.",
        ));
    };
    let integer = i128::from(integer);
    if integer < 0 {
        return Err(JsyncError::new(
            JsyncErrorKind::InvalidJsonValue,
            "The value must be non-negative.",
        )
        .with_metadata("value", integer.to_string()));
    }
    usize::try_from(integer).map_err(|_| {
        JsyncError::new(JsyncErrorKind::InvalidJsonValue, "The value is too large.")
            .with_metadata("value", integer.to_string())
    })
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
        Action::Snapshot { value } => Ok(CborValue::Array(vec![
            integer(OPCODE_SNAPSHOT),
            json_to_cbor(value)?,
        ])),
        Action::Add { path, value } => Ok(CborValue::Array(vec![
            integer(OPCODE_ADD),
            pooled_path_to_cbor(txn, path),
            json_to_cbor(value)?,
        ])),
        Action::Remove { path } => Ok(CborValue::Array(vec![
            integer(OPCODE_REMOVE),
            pooled_path_to_cbor(txn, path),
        ])),
        Action::Replace { path, value } => Ok(CborValue::Array(vec![
            integer(OPCODE_REPLACE),
            pooled_path_to_cbor(txn, path),
            json_to_cbor(value)?,
        ])),
        Action::StringAppend { path, text } => Ok(CborValue::Array(vec![
            integer(OPCODE_STRING_APPEND),
            pooled_path_to_cbor(txn, path),
            CborValue::Text(text.clone()),
        ])),
        Action::StringPrepend { path, text } => Ok(CborValue::Array(vec![
            integer(OPCODE_STRING_PREPEND),
            pooled_path_to_cbor(txn, path),
            CborValue::Text(text.clone()),
        ])),
        Action::StringPatch { path, edits } => Ok(CborValue::Array(vec![
            integer(OPCODE_STRING_PATCH),
            pooled_path_to_cbor(txn, path),
            CborValue::Array(
                edits
                    .iter()
                    .map(|edit| {
                        CborValue::Array(vec![
                            integer(edit.start as u64),
                            integer(edit.delete_count as u64),
                            CborValue::Text(edit.text.clone()),
                        ])
                    })
                    .collect(),
            ),
        ])),
        Action::ArrayPatch { path, edits } => Ok(CborValue::Array(vec![
            integer(OPCODE_ARRAY_PATCH),
            pooled_path_to_cbor(txn, path),
            CborValue::Array(
                edits
                    .iter()
                    .map(|edit| {
                        Ok(CborValue::Array(vec![
                            integer(edit.start as u64),
                            integer(edit.delete_count as u64),
                            CborValue::Array(
                                edit.values
                                    .iter()
                                    .map(json_to_cbor)
                                    .collect::<Result<Vec<_>, _>>()?,
                            ),
                        ]))
                    })
                    .collect::<Result<Vec<_>, JsyncError>>()?,
            ),
        ])),
        Action::Copy { from, path } => Ok(CborValue::Array(vec![
            integer(OPCODE_COPY),
            pooled_path_to_cbor(txn, from),
            pooled_path_to_cbor(txn, path),
        ])),
        Action::Move { from, path } => Ok(CborValue::Array(vec![
            integer(OPCODE_MOVE),
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
