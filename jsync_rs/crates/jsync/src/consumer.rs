use serde_json::Value;

use crate::error::{JsyncError, JsyncErrorKind};
use crate::message::{Action, ConsumerPathSegmentPool, Message, PathSegment, StringPatchEdit};

/// Consumes Jsync messages and maintains the current JSON document.
#[derive(Debug, Default)]
pub struct Consumer {
    document: Option<Value>,
    initialized: bool,
    path_segment_pool: ConsumerPathSegmentPool,
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

    /// Decodes one Jsync message without committing path segment pool changes.
    ///
    /// Very helpful for decoding and inspecting the message without modifying
    /// the consumer's state (we ask for `&mut self` but we do not change it).
    pub fn decode_message_dry_run(&mut self, message: &[u8]) -> Result<Message, JsyncError> {
        let mut transaction = self.path_segment_pool.transaction();
        let result = Message::from_bytes_with_pool_txn(message.to_vec(), &mut transaction);
        transaction.abort();
        result
    }

    /// Decodes and atomically applies one Jsync message.
    ///
    /// If it fails, the state of the consumer will not be modified.
    pub fn consume(&mut self, message: &[u8]) -> Result<(), JsyncError> {
        let mut transaction = self.path_segment_pool.transaction();
        let message = Message::from_bytes_with_pool_txn(message.to_vec(), &mut transaction)?;
        let actions = message.actions;
        if !self.initialized && !matches!(actions.first(), Some(Action::Snapshot { .. })) {
            return Err(JsyncError::new(
                JsyncErrorKind::InitialSnapshotRequired,
                "The first Jsync message must start with SNAPSHOT.",
            ));
        }

        let mut working = self.document.clone().unwrap_or(Value::Null);
        for (index, action) in actions.into_iter().enumerate() {
            if let Err(error) = apply_action(&mut working, action) {
                return Err(error
                    .with_metadata("action_index", index.to_string())
                    .with_context("while applying a Jsync action"));
            }
        }

        self.document = Some(working);
        self.initialized = true;
        transaction.commit();
        Ok(())
    }
}

/// Applies one validated action to a working document.
fn apply_action(root: &mut Value, action: Action) -> Result<(), JsyncError> {
    match action {
        Action::Snapshot { value } => {
            *root = value;
            Ok(())
        }
        Action::Add { path, value } => apply_add(root, &path, value),
        Action::Remove { path } => apply_remove(root, &path),
        Action::Replace { path, value } => apply_replace(root, &path, value),
        Action::StringAppend { path, text } => apply_string_append(root, &path, &text),
        Action::StringPrepend { path, text } => apply_string_prepend(root, &path, &text),
        Action::StringPatch { path, edits } => apply_string_patch(root, &path, &edits),
        Action::Copy { from, path } => apply_copy(root, &from, &path),
        Action::Move { from, path } => apply_move(root, &from, &path),
    }
}

/// Applies an ADD action to an object, array, or root document.
fn apply_add(root: &mut Value, path: &[PathSegment], value: Value) -> Result<(), JsyncError> {
    if path.is_empty() {
        *root = value;
        return Ok(());
    }

    let (parent_path, final_segment) = path.split_at(path.len() - 1);
    let parent = resolve_container(root, parent_path)
        .map_err(|error| error.with_context("while resolving an ADD path parent"))?;
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

/// Removes an existing value from an object, array, or root path.
fn apply_remove(root: &mut Value, path: &[PathSegment]) -> Result<(), JsyncError> {
    if path.is_empty() {
        return Err(JsyncError::new(
            JsyncErrorKind::InvalidPath,
            "The REMOVE path cannot target the root document.",
        )
        .with_context("while applying the final REMOVE path segment"));
    }

    let (parent_path, final_segment) = path.split_at(path.len() - 1);
    let parent = resolve_container(root, parent_path)
        .map_err(|error| error.with_context("while resolving a REMOVE path parent"))?;
    match (parent, &final_segment[0]) {
        (Value::Object(object), PathSegment::Key(key)) => {
            if object.remove(key).is_some() {
                Ok(())
            } else {
                Err(JsyncError::new(
                    JsyncErrorKind::PathParentMissing,
                    "The REMOVE object key does not exist.",
                )
                .with_metadata("key", key.clone())
                .with_metadata("segment_index", (path.len() - 1).to_string())
                .with_context("while applying the final REMOVE path segment"))
            }
        }
        (Value::Array(array), PathSegment::Index(index)) => {
            if *index >= array.len() {
                return Err(JsyncError::new(
                    JsyncErrorKind::ArrayIndexOutOfBounds,
                    "The REMOVE index is outside the array.",
                )
                .with_metadata("index", index.to_string())
                .with_metadata("length", array.len().to_string())
                .with_metadata("segment_index", (path.len() - 1).to_string())
                .with_context("while applying the final REMOVE path segment"));
            }
            array.remove(*index);
            Ok(())
        }
        (Value::Array(_), PathSegment::Key(key)) => Err(JsyncError::new(
            JsyncErrorKind::InvalidPath,
            "A REMOVE array final segment must be a non-negative integer; '-' is only valid for ADD.",
        )
        .with_metadata("segment", key.clone())
        .with_metadata("segment_index", (path.len() - 1).to_string())
        .with_context("while applying the final REMOVE path segment")),
        (Value::Object(_), PathSegment::Index(index)) => Err(JsyncError::new(
            JsyncErrorKind::InvalidPath,
            "A REMOVE object final segment must be a string.",
        )
        .with_metadata("index", index.to_string())
        .with_metadata("segment_index", (path.len() - 1).to_string())
        .with_context("while applying the final REMOVE path segment")),
        (Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_), _) => Err(
            JsyncError::new(
                JsyncErrorKind::PathParentNotContainer,
                "The REMOVE path parent is a scalar instead of an object or array.",
            )
            .with_metadata("segment_index", (path.len() - 1).to_string())
            .with_context("while applying the final REMOVE path segment"),
        ),
    }
}

/// Replaces an existing value at an object, array, or root path.
fn apply_replace(root: &mut Value, path: &[PathSegment], value: Value) -> Result<(), JsyncError> {
    if path.is_empty() {
        *root = value;
        return Ok(());
    }

    let (parent_path, final_segment) = path.split_at(path.len() - 1);
    let parent = resolve_container(root, parent_path)
        .map_err(|error| error.with_context("while resolving a REPLACE path parent"))?;
    match (parent, &final_segment[0]) {
        (Value::Object(object), PathSegment::Key(key)) => {
            if !object.contains_key(key) {
                return Err(JsyncError::new(
                    JsyncErrorKind::PathParentMissing,
                    "The REPLACE object key does not exist.",
                )
                .with_metadata("key", key.clone())
                .with_metadata("segment_index", (path.len() - 1).to_string())
                .with_context("while applying the final REPLACE path segment"));
            }
            object.insert(key.clone(), value);
            Ok(())
        }
        (Value::Array(array), PathSegment::Index(index)) => {
            if *index >= array.len() {
                return Err(JsyncError::new(
                    JsyncErrorKind::ArrayIndexOutOfBounds,
                    "The REPLACE index is outside the array.",
                )
                .with_metadata("index", index.to_string())
                .with_metadata("length", array.len().to_string())
                .with_metadata("segment_index", (path.len() - 1).to_string())
                .with_context("while applying the final REPLACE path segment"));
            }
            array[*index] = value;
            Ok(())
        }
        (Value::Array(_), PathSegment::Key(key)) => Err(JsyncError::new(
            JsyncErrorKind::InvalidPath,
            "A REPLACE array final segment must be a non-negative integer; '-' is only valid for ADD.",
        )
        .with_metadata("segment", key.clone())
        .with_metadata("segment_index", (path.len() - 1).to_string())
        .with_context("while applying the final REPLACE path segment")),
        (Value::Object(_), PathSegment::Index(index)) => Err(JsyncError::new(
            JsyncErrorKind::InvalidPath,
            "A REPLACE object final segment must be a string.",
        )
        .with_metadata("index", index.to_string())
        .with_metadata("segment_index", (path.len() - 1).to_string())
        .with_context("while applying the final REPLACE path segment")),
        (Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_), _) => Err(
            JsyncError::new(
                JsyncErrorKind::PathParentNotContainer,
                "The REPLACE path parent is a scalar instead of an object or array.",
            )
            .with_metadata("segment_index", (path.len() - 1).to_string())
            .with_context("while applying the final REPLACE path segment"),
        ),
    }
}

/// Appends text to an existing string at an object, array, or root path.
fn apply_string_append(
    root: &mut Value,
    path: &[PathSegment],
    text: &str,
) -> Result<(), JsyncError> {
    let target = resolve_value(root, path)
        .map_err(|error| error.with_context("while resolving an APPEND path target"))?;
    let Value::String(target) = target else {
        return Err(JsyncError::new(
            JsyncErrorKind::ApplyFailed,
            "The APPEND path target is not a string.",
        )
        .with_context("while applying the APPEND action"));
    };
    target.push_str(text);
    Ok(())
}

/// Prepends text to an existing string at an object, array, or root path.
fn apply_string_prepend(
    root: &mut Value,
    path: &[PathSegment],
    text: &str,
) -> Result<(), JsyncError> {
    let target = resolve_value(root, path)
        .map_err(|error| error.with_context("while resolving a PREPEND path target"))?;
    let Value::String(target) = target else {
        return Err(JsyncError::new(
            JsyncErrorKind::ApplyFailed,
            "The PREPEND path target is not a string.",
        )
        .with_context("while applying the PREPEND action"));
    };
    target.insert_str(0, text);
    Ok(())
}

/// Applies local edits to an existing string at an object, array, or root path.
fn apply_string_patch(
    root: &mut Value,
    path: &[PathSegment],
    edits: &[StringPatchEdit],
) -> Result<(), JsyncError> {
    let target = resolve_value(root, path)
        .map_err(|error| error.with_context("while resolving a STRING_PATCH path target"))?;
    let Value::String(target) = target else {
        return Err(JsyncError::new(
            JsyncErrorKind::ApplyFailed,
            "The STRING_PATCH path target is not a string.",
        )
        .with_context("while applying the STRING_PATCH action"));
    };

    let scalar_to_byte = scalar_to_byte_offsets(target);
    validate_string_patch_edits(edits, scalar_to_byte.len() - 1)?;

    for edit in edits {
        let start_byte = scalar_to_byte[edit.start];
        let end_byte = scalar_to_byte[edit.start + edit.delete_count];
        target.replace_range(start_byte..end_byte, &edit.text);
    }
    Ok(())
}

fn scalar_to_byte_offsets(value: &str) -> Vec<usize> {
    value
        .char_indices()
        .map(|(index, _)| index)
        .chain(std::iter::once(value.len()))
        .collect()
}

fn validate_string_patch_edits(
    edits: &[StringPatchEdit],
    scalar_len: usize,
) -> Result<(), JsyncError> {
    if edits.is_empty() {
        return Err(JsyncError::new(
            JsyncErrorKind::InvalidJsonValue,
            "The STRING_PATCH edits cannot be empty.",
        ));
    }

    let mut previous_start = None;
    for (edit_index, edit) in edits.iter().enumerate() {
        if edit.delete_count == 0 && edit.text.is_empty() {
            return Err(JsyncError::new(
                JsyncErrorKind::InvalidJsonValue,
                "A STRING_PATCH edit must delete text or insert text.",
            )
            .with_metadata("edit_index", edit_index.to_string()));
        }
        let end = edit.start.checked_add(edit.delete_count).ok_or_else(|| {
            JsyncError::new(
                JsyncErrorKind::InvalidJsonValue,
                "A STRING_PATCH edit range is too large.",
            )
            .with_metadata("edit_index", edit_index.to_string())
        })?;
        if edit.start > scalar_len || end > scalar_len {
            return Err(JsyncError::new(
                JsyncErrorKind::InvalidJsonValue,
                "A STRING_PATCH edit range is outside the target string.",
            )
            .with_metadata("edit_index", edit_index.to_string())
            .with_metadata("start", edit.start.to_string())
            .with_metadata("delete_count", edit.delete_count.to_string())
            .with_metadata("length", scalar_len.to_string()));
        }
        if let Some(previous_start) = previous_start {
            if end > previous_start {
                return Err(JsyncError::new(
                    JsyncErrorKind::InvalidJsonValue,
                    "STRING_PATCH edits must be in descending, non-overlapping order.",
                )
                .with_metadata("edit_index", edit_index.to_string()));
            }
        }
        previous_start = Some(edit.start);
    }

    Ok(())
}

/// Copies an existing JSON value to an object, array, or root path.
fn apply_copy(
    root: &mut Value,
    from: &[PathSegment],
    path: &[PathSegment],
) -> Result<(), JsyncError> {
    validate_from_path(from).map_err(|error| error.with_context("while validating COPY paths"))?;
    let value = resolve_value(root, from)
        .map_err(|error| error.with_context("while resolving a COPY from path"))?
        .clone();
    apply_add(root, path, value).map_err(|error| error.with_context("while applying COPY path"))
}

/// Moves an existing JSON value to an object, array, or root path.
fn apply_move(
    root: &mut Value,
    from: &[PathSegment],
    path: &[PathSegment],
) -> Result<(), JsyncError> {
    validate_from_path(from).map_err(|error| error.with_context("while validating MOVE paths"))?;
    if from == path {
        return Ok(());
    }
    validate_move_paths(from, path)
        .map_err(|error| error.with_context("while validating MOVE paths"))?;

    let value = remove_and_return(root, from)
        .map_err(|error| error.with_context("while resolving a MOVE from path"))?;
    apply_add(root, path, value).map_err(|error| error.with_context("while applying MOVE path"))
}

fn validate_move_paths(from: &[PathSegment], path: &[PathSegment]) -> Result<(), JsyncError> {
    if from.is_empty() {
        return Err(JsyncError::new(
            JsyncErrorKind::InvalidPath,
            "The MOVE from path cannot target the root document.",
        ));
    }
    if is_descendant_path(from, path) {
        return Err(JsyncError::new(
            JsyncErrorKind::InvalidPath,
            "The MOVE path cannot target a child of the MOVE from path.",
        ));
    }
    Ok(())
}

fn validate_from_path(from: &[PathSegment]) -> Result<(), JsyncError> {
    for (segment_index, segment) in from.iter().enumerate() {
        if matches!(segment, PathSegment::Key(key) if key == "-") {
            return Err(JsyncError::new(
                JsyncErrorKind::InvalidPath,
                "The from path cannot contain '-'.",
            )
            .with_metadata("segment", "-")
            .with_metadata("segment_index", segment_index.to_string()));
        }
    }
    Ok(())
}

fn is_descendant_path(parent: &[PathSegment], child: &[PathSegment]) -> bool {
    child.len() > parent.len() && child.starts_with(parent)
}

/// Removes an existing value and returns it for a MOVE action.
fn remove_and_return(root: &mut Value, path: &[PathSegment]) -> Result<Value, JsyncError> {
    if path.is_empty() {
        return Err(JsyncError::new(
            JsyncErrorKind::InvalidPath,
            "The MOVE from path cannot target the root document.",
        )
        .with_context("while applying the final MOVE from path segment"));
    }

    let (parent_path, final_segment) = path.split_at(path.len() - 1);
    let parent = resolve_container(root, parent_path)
        .map_err(|error| error.with_context("while resolving a MOVE from path parent"))?;
    match (parent, &final_segment[0]) {
        (Value::Object(object), PathSegment::Key(key)) => object.remove(key).ok_or_else(|| {
            JsyncError::new(
                JsyncErrorKind::PathParentMissing,
                "The MOVE object key does not exist.",
            )
            .with_metadata("key", key.clone())
            .with_metadata("segment_index", (path.len() - 1).to_string())
            .with_context("while applying the final MOVE from path segment")
        }),
        (Value::Array(array), PathSegment::Index(index)) => {
            if *index >= array.len() {
                return Err(JsyncError::new(
                    JsyncErrorKind::ArrayIndexOutOfBounds,
                    "The MOVE index is outside the array.",
                )
                .with_metadata("index", index.to_string())
                .with_metadata("length", array.len().to_string())
                .with_metadata("segment_index", (path.len() - 1).to_string())
                .with_context("while applying the final MOVE from path segment"));
            }
            Ok(array.remove(*index))
        }
        (Value::Array(_), PathSegment::Key(key)) => Err(JsyncError::new(
            JsyncErrorKind::InvalidPath,
            "A MOVE array from segment must be a non-negative integer.",
        )
        .with_metadata("segment", key.clone())
        .with_metadata("segment_index", (path.len() - 1).to_string())
        .with_context("while applying the final MOVE from path segment")),
        (Value::Object(_), PathSegment::Index(index)) => Err(JsyncError::new(
            JsyncErrorKind::InvalidPath,
            "A MOVE object from segment must be a string.",
        )
        .with_metadata("index", index.to_string())
        .with_metadata("segment_index", (path.len() - 1).to_string())
        .with_context("while applying the final MOVE from path segment")),
        (Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_), _) => {
            Err(JsyncError::new(
                JsyncErrorKind::PathParentNotContainer,
                "The MOVE from path parent is a scalar instead of an object or array.",
            )
            .with_metadata("segment_index", (path.len() - 1).to_string())
            .with_context("while applying the final MOVE from path segment"))
        }
    }
}

/// Resolves an existing object or array container for an action parent path.
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
                        "The path object key does not exist.",
                    )
                    .with_metadata("key", key.clone())
                    .with_metadata("segment_index", segment_index.to_string())
                    .with_context("while resolving a path parent")
                })?
            }
            (Value::Array(array), PathSegment::Index(index)) => {
                let length = array.len();
                array.get_mut(*index).ok_or_else(|| {
                    JsyncError::new(
                        JsyncErrorKind::ArrayIndexOutOfBounds,
                        "The path index is outside the array.",
                    )
                    .with_metadata("index", index.to_string())
                    .with_metadata("length", length.to_string())
                    .with_metadata("segment_index", segment_index.to_string())
                    .with_context("while resolving a path parent")
                })?
            }
            (Value::Array(_), PathSegment::Key(key)) => {
                return Err(JsyncError::new(
                    JsyncErrorKind::InvalidPath,
                    "An array intermediate segment must be an index.",
                )
                .with_metadata("segment", key.clone())
                .with_metadata("segment_index", segment_index.to_string())
                .with_context("while resolving an intermediate path segment"));
            }
            (Value::Object(_), PathSegment::Index(index)) => {
                return Err(JsyncError::new(
                    JsyncErrorKind::InvalidPath,
                    "An object intermediate segment must be a string.",
                )
                .with_metadata("index", index.to_string())
                .with_metadata("segment_index", segment_index.to_string())
                .with_context("while resolving an intermediate path segment"));
            }
            (Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_), _) => {
                return Err(JsyncError::new(
                    JsyncErrorKind::PathParentNotContainer,
                    "The path traversed a scalar.",
                )
                .with_metadata("segment_index", segment_index.to_string())
                .with_context("while resolving a path parent"));
            }
        };
    }
    Ok(current)
}

/// Resolves an existing value for an action path.
fn resolve_value<'a>(
    mut current: &'a mut Value,
    path: &[PathSegment],
) -> Result<&'a mut Value, JsyncError> {
    for (segment_index, segment) in path.iter().enumerate() {
        current = match (current, segment) {
            (Value::Object(object), PathSegment::Key(key)) => {
                object.get_mut(key).ok_or_else(|| {
                    JsyncError::new(
                        JsyncErrorKind::PathParentMissing,
                        "The path object key does not exist.",
                    )
                    .with_metadata("key", key.clone())
                    .with_metadata("segment_index", segment_index.to_string())
                    .with_context("while resolving a path target")
                })?
            }
            (Value::Array(array), PathSegment::Index(index)) => {
                let length = array.len();
                array.get_mut(*index).ok_or_else(|| {
                    JsyncError::new(
                        JsyncErrorKind::ArrayIndexOutOfBounds,
                        "The path index is outside the array.",
                    )
                    .with_metadata("index", index.to_string())
                    .with_metadata("length", length.to_string())
                    .with_metadata("segment_index", segment_index.to_string())
                    .with_context("while resolving a path target")
                })?
            }
            (Value::Array(_), PathSegment::Key(key)) => {
                return Err(JsyncError::new(
                    JsyncErrorKind::InvalidPath,
                    "An array path segment must be an index.",
                )
                .with_metadata("segment", key.clone())
                .with_metadata("segment_index", segment_index.to_string())
                .with_context("while resolving a path target"));
            }
            (Value::Object(_), PathSegment::Index(index)) => {
                return Err(JsyncError::new(
                    JsyncErrorKind::InvalidPath,
                    "An object path segment must be a string.",
                )
                .with_metadata("index", index.to_string())
                .with_metadata("segment_index", segment_index.to_string())
                .with_context("while resolving a path target"));
            }
            (Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_), _) => {
                return Err(JsyncError::new(
                    JsyncErrorKind::PathParentNotContainer,
                    "The path traversed a scalar.",
                )
                .with_metadata("segment_index", segment_index.to_string())
                .with_context("while resolving a path target"));
            }
        };
    }
    Ok(current)
}
