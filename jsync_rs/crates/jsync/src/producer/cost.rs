use std::collections::HashMap;

use serde_json::{Number, Value};

use super::diff::DiffPlan;
use crate::error::{JsyncError, JsyncErrorKind};
#[cfg(debug_assertions)]
use crate::message::Message;
use crate::message::{Action, PathSegment, ProducerPathSegmentPool};
use crate::message::{
    OPCODE_ADD, OPCODE_COPY, OPCODE_MOVE, OPCODE_REMOVE, OPCODE_REPLACE, OPCODE_SNAPSHOT,
    OPCODE_STRING_APPEND, OPCODE_STRING_PATCH, OPCODE_STRING_PREPEND,
};

pub(super) fn plan(
    actions: Vec<Action>,
    path_segment_pool: &ProducerPathSegmentPool,
) -> Result<DiffPlan, JsyncError> {
    // Cost is used only to choose between equivalent patch plans. Estimating it
    // here avoids serializing every candidate message during recursive diffing.
    let cost = estimate_plan_cost(&actions, path_segment_pool)?;
    Ok(DiffPlan { actions, cost })
}

fn estimate_plan_cost(
    actions: &[Action],
    path_segment_pool: &ProducerPathSegmentPool,
) -> Result<usize, JsyncError> {
    if actions.is_empty() {
        return Ok(0);
    }

    let mut estimator = CostEstimator::new(path_segment_pool);
    let actions_cost = actions
        .iter()
        .map(|action| estimator.estimate_action(action))
        .try_fold(0usize, |total, cost| Ok(total + cost?))?;
    let metadata_segments_cost = estimator.metadata_segments_cost()?;

    // Wire payload shape is: HEADER + [metadata, actions], where metadata is a
    // one-element array containing the path segment pool append list.
    Ok(3 // Jsync header.
        + cbor_array_header_len(2)
        + cbor_array_header_len(1)
        + cbor_array_header_len(estimator.appended_len())
        + metadata_segments_cost
        + cbor_array_header_len(actions.len())
        + actions_cost)
}

#[cfg(debug_assertions)]
#[allow(dead_code)]
fn encoded_plan_cost_for_debug(
    actions: &[Action],
    path_segment_pool: &ProducerPathSegmentPool,
) -> Result<usize, JsyncError> {
    // Keep the real encoder path available for local assertions when changing
    // estimator rules. Normal diffing should not call this helper.
    if actions.is_empty() {
        return Ok(0);
    }

    let mut path_segment_pool = path_segment_pool.clone();
    let mut txn = path_segment_pool.transaction();
    let len = Message::new(actions.to_vec())
        .to_bytes_with_pool_txn(&mut txn)?
        .len();
    txn.commit();
    Ok(len)
}

struct CostEstimator<'a> {
    path_segment_pool: &'a ProducerPathSegmentPool,
    // Segments first seen by this candidate plan. They contribute both to path
    // indexes inside actions and to metadata appended at the front of the message.
    appended_segments: Vec<PathSegment>,
    appended_indexes: HashMap<PathSegment, usize>,
}

impl<'a> CostEstimator<'a> {
    fn new(path_segment_pool: &'a ProducerPathSegmentPool) -> Self {
        Self {
            path_segment_pool,
            appended_segments: Vec::new(),
            appended_indexes: HashMap::new(),
        }
    }

    fn appended_len(&self) -> usize {
        self.appended_segments.len()
    }

    fn estimate_action(&mut self, action: &Action) -> Result<usize, JsyncError> {
        match action {
            Action::Snapshot { value } => Ok(cbor_array_header_len(2)
                + cbor_uint_len(OPCODE_SNAPSHOT)
                + estimate_json_value_len(value)?),
            Action::Add { path, value } => Ok(cbor_array_header_len(3)
                + cbor_uint_len(OPCODE_ADD)
                + self.estimate_path_len(path)
                + estimate_json_value_len(value)?),
            Action::Remove { path } => Ok(cbor_array_header_len(2)
                + cbor_uint_len(OPCODE_REMOVE)
                + self.estimate_path_len(path)),
            Action::Replace { path, value } => Ok(cbor_array_header_len(3)
                + cbor_uint_len(OPCODE_REPLACE)
                + self.estimate_path_len(path)
                + estimate_json_value_len(value)?),
            Action::StringAppend { path, text } => Ok(cbor_array_header_len(3)
                + cbor_uint_len(OPCODE_STRING_APPEND)
                + self.estimate_path_len(path)
                + cbor_text_len(text)),
            Action::StringPrepend { path, text } => Ok(cbor_array_header_len(3)
                + cbor_uint_len(OPCODE_STRING_PREPEND)
                + self.estimate_path_len(path)
                + cbor_text_len(text)),
            Action::StringPatch { path, edits } => Ok(cbor_array_header_len(3)
                + cbor_uint_len(OPCODE_STRING_PATCH)
                + self.estimate_path_len(path)
                + cbor_array_header_len(edits.len())
                + edits
                    .iter()
                    .map(|edit| {
                        cbor_array_header_len(3)
                            + cbor_uint_len(edit.start as u64)
                            + cbor_uint_len(edit.delete_count as u64)
                            + cbor_text_len(&edit.text)
                    })
                    .sum::<usize>()),
            Action::Copy { from, path } => Ok(cbor_array_header_len(3)
                + cbor_uint_len(OPCODE_COPY)
                + self.estimate_path_len(from)
                + self.estimate_path_len(path)),
            Action::Move { from, path } => Ok(cbor_array_header_len(3)
                + cbor_uint_len(OPCODE_MOVE)
                + self.estimate_path_len(from)
                + self.estimate_path_len(path)),
        }
    }

    fn estimate_path_len(&mut self, path: &[PathSegment]) -> usize {
        cbor_array_header_len(path.len())
            + path
                .iter()
                .map(|segment| cbor_uint_len(self.index_for(segment) as u64))
                .sum::<usize>()
    }

    fn index_for(&mut self, segment: &PathSegment) -> usize {
        // Match ProducerPathSegmentPool::index_for without mutating the real
        // pool: committed indexes win, then indexes appended by this plan.
        if let Some(index) = self.path_segment_pool.index_of(segment) {
            return index;
        }
        if let Some(index) = self.appended_indexes.get(segment) {
            return *index;
        }

        let index = self.path_segment_pool.len() + self.appended_segments.len();
        let segment = segment.clone();
        self.appended_segments.push(segment.clone());
        self.appended_indexes.insert(segment, index);
        index
    }

    fn metadata_segments_cost(&self) -> Result<usize, JsyncError> {
        self.appended_segments
            .iter()
            .map(estimate_path_segment_len)
            .try_fold(0usize, |total, cost| Ok(total + cost?))
    }
}

fn estimate_path_segment_len(segment: &PathSegment) -> Result<usize, JsyncError> {
    match segment {
        PathSegment::Key(key) => Ok(cbor_text_len(key)),
        PathSegment::Index(index) => Ok(cbor_uint_len(*index as u64)),
    }
}

fn estimate_json_value_len(value: &Value) -> Result<usize, JsyncError> {
    match value {
        Value::Null | Value::Bool(_) => Ok(1),
        Value::Number(number) => estimate_json_number_len(number),
        Value::String(value) => Ok(cbor_text_len(value)),
        Value::Array(values) => values
            .iter()
            .map(estimate_json_value_len)
            .try_fold(cbor_array_header_len(values.len()), |total, cost| {
                Ok(total + cost?)
            }),
        Value::Object(object) => object
            .iter()
            .try_fold(cbor_map_header_len(object.len()), |total, (key, value)| {
                Ok(total + cbor_text_len(key) + estimate_json_value_len(value)?)
            }),
    }
}

fn estimate_json_number_len(number: &Number) -> Result<usize, JsyncError> {
    // Mirror message::number_to_cbor enough to preserve the same validation
    // failures and major type choice before the final encoder sees the value.
    if let Some(value) = number.as_i64() {
        validate_safe_json_integer(value as i128)?;
        return Ok(cbor_int_len(value));
    }
    if let Some(value) = number.as_u64() {
        validate_safe_json_integer(value as i128)?;
        return Ok(cbor_uint_len(value));
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
        return Ok(9);
    }

    Err(JsyncError::new(
        JsyncErrorKind::InvalidJsonValue,
        "The JSON number cannot be encoded as a CBOR number.",
    ))
}

fn validate_safe_json_integer(value: i128) -> Result<(), JsyncError> {
    const MAX_SAFE_JSON_INTEGER: i128 = 9_007_199_254_740_991;
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
    const MAX_SAFE_JSON_INTEGER: f64 = 9_007_199_254_740_991.0;
    if value.fract() != 0.0 || value.abs() <= MAX_SAFE_JSON_INTEGER {
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

fn cbor_int_len(value: i64) -> usize {
    if value >= 0 {
        cbor_uint_len(value as u64)
    } else {
        cbor_uint_len((-1 - value) as u64)
    }
}

fn cbor_uint_len(value: u64) -> usize {
    cbor_argument_len(value)
}

fn cbor_text_len(value: &str) -> usize {
    // CBOR text lengths are counted in UTF-8 bytes, not Unicode scalar values.
    cbor_argument_len(value.len() as u64) + value.len()
}

fn cbor_array_header_len(len: usize) -> usize {
    cbor_argument_len(len as u64)
}

fn cbor_map_header_len(len: usize) -> usize {
    cbor_argument_len(len as u64)
}

fn cbor_argument_len(value: u64) -> usize {
    match value {
        0..=23 => 1,
        24..=0xff => 2,
        0x100..=0xffff => 3,
        0x1_0000..=0xffff_ffff => 5,
        _ => 9,
    }
}
