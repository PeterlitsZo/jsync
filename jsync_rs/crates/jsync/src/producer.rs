//! Jsync message producer.

use std::collections::HashMap;

use serde_json::{Map, Number, Value};

use crate::error::{JsyncError, JsyncErrorKind};
use crate::message::{Action, Message, PathSegment, ProducerPathSegmentPool};

/// Produces Jsync snapshots and incremental messages for a JSON document.
#[derive(Debug, Clone)]
pub struct Producer {
    current_document: Value,
    last_emitted_document: Option<Value>,
    path_segment_pool: ProducerPathSegmentPool,
}

impl Producer {
    /// Creates a producer with the initial JSON document.
    pub fn new(initial_document: Value) -> Self {
        Self {
            current_document: initial_document,
            last_emitted_document: None,
            path_segment_pool: ProducerPathSegmentPool::new(),
        }
    }

    /// Replaces the current JSON document without producing a message yet.
    pub fn update(&mut self, document: Value) {
        self.current_document = document;
    }

    /// Returns the current JSON document.
    pub fn document(&self) -> &Value {
        &self.current_document
    }

    /// Produces the next Jsync message, or `None` when there is no change.
    ///
    /// The first successful call always emits a SNAPSHOT of the latest current
    /// document, even if `update` was called before that first call.
    pub fn get_message(&mut self) -> Result<Option<Vec<u8>>, JsyncError> {
        let actions = match &self.last_emitted_document {
            None => vec![Action::Snapshot {
                value: self.current_document.clone(),
            }],
            Some(previous) if previous == &self.current_document => return Ok(None),
            Some(previous) => {
                let mut path = Vec::new();
                let actions = build_diff(
                    previous,
                    &self.current_document,
                    &mut path,
                    &self.path_segment_pool,
                )?
                .actions;
                if actions.is_empty() {
                    return Err(JsyncError::new(
                        JsyncErrorKind::ApplyFailed,
                        "The Jsync producer generated an empty diff for changed documents.",
                    ));
                }
                actions
            }
        };

        let mut txn = self.path_segment_pool.transaction();
        let message = Message::new(actions).to_bytes_with_pool_txn(&mut txn)?;
        txn.commit();
        self.last_emitted_document = Some(self.current_document.clone());
        Ok(Some(message))
    }
}

#[derive(Debug)]
struct DiffPlan {
    actions: Vec<Action>,
    cost: usize,
}

type ValueDigest = [u8; 32];

fn build_diff(
    from: &Value,
    to: &Value,
    path: &mut Vec<PathSegment>,
    path_segment_pool: &ProducerPathSegmentPool,
) -> Result<DiffPlan, JsyncError> {
    if from == to {
        return Ok(DiffPlan {
            actions: Vec::new(),
            cost: 0,
        });
    }

    // A simple plan to just replace the value.
    let replace = replace_plan(path, to, path_segment_pool)?;

    // A structural plan to patch the value.
    let structural = match (from, to) {
        (Value::Object(old), Value::Object(new)) => diff_objects(old, new, path, path_segment_pool),
        (Value::Array(old), Value::Array(new)) => diff_arrays(old, new, path, path_segment_pool),
        (Value::String(old), Value::String(new)) => {
            return diff_strings(old, new, path, replace, path_segment_pool);
        }
        _ => return Ok(replace),
    }?;

    // Return the plan that is cheaper to execute.
    Ok(choose_smaller(structural, replace))
}

fn diff_objects(
    old: &Map<String, Value>,
    new: &Map<String, Value>,
    path: &mut Vec<PathSegment>,
    path_segment_pool: &ProducerPathSegmentPool,
) -> Result<DiffPlan, JsyncError> {
    let mut actions = Vec::new();

    // Find the keys that are removed and added, to determine the plan.
    let mut removed = old
        .keys()
        .filter(|key| !new.contains_key(*key))
        .cloned()
        .collect::<Vec<_>>();
    removed.sort();
    let mut added = new
        .keys()
        .filter(|key| !old.contains_key(*key))
        .cloned()
        .collect::<Vec<_>>();
    added.sort();
    let mut added_by_digest = HashMap::<ValueDigest, Vec<String>>::new();
    let mut added_digests = HashMap::<String, ValueDigest>::new();
    for key in &added {
        let digest = digest_value(&new[key.as_str()])?;
        added_digests.insert(key.clone(), digest);
        added_by_digest.entry(digest).or_default().push(key.clone());
    }

    // Handle the key deletion.
    let mut move_actions = Vec::new();
    let mut remaining_removed = Vec::new();
    for key in removed {
        let old_digest = digest_value(&old[key.as_str()])?;
        if let Some(added_key) = added_by_digest
            .get(&old_digest)
            .and_then(|bucket| bucket.first())
            .cloned()
        {
            // If the old value matches the added value, move it instead of
            // removing it.

            let move_action = Action::Move {
                from: child_path(path, &key),
                path: child_path(path, &added_key),
            };
            let fallback = vec![
                Action::Remove {
                    path: child_path(path, &key),
                },
                Action::Add {
                    path: child_path(path, &added_key),
                    value: new[&added_key].clone(),
                },
            ];
            if plan(vec![move_action.clone()], path_segment_pool)?.cost
                < plan(fallback.clone(), path_segment_pool)?.cost
            {
                remove_sorted_key(&mut added, &added_key);
                added_digests.remove(&added_key);
                if let Some(bucket) = added_by_digest.get_mut(&old_digest) {
                    bucket.remove(0);
                }
                move_actions.push(move_action);
            } else {
                remaining_removed.push(key);
            }
        } else {
            remaining_removed.push(key);
        }
    }
    actions.extend(move_actions);
    for key in remaining_removed {
        let mut target = path.clone();
        target.push(PathSegment::Key(key.clone()));
        actions.push(Action::Remove { path: target });
    }

    // Find the common keys and unchanged keys.
    let mut common = old
        .keys()
        .filter(|key| new.contains_key(*key))
        .cloned()
        .collect::<Vec<_>>();
    common.sort();
    let mut unchanged_by_digest = HashMap::<ValueDigest, Vec<String>>::new();
    for key in &common {
        let old_digest = digest_value(&old[key.as_str()])?;
        if old_digest == digest_value(&new[key.as_str()])? {
            unchanged_by_digest
                .entry(old_digest)
                .or_default()
                .push(key.clone());
        }
    }

    // Handle the key addition.
    let mut remaining_added = Vec::new();
    for key in added {
        let new_digest = added_digests[&key];
        if let Some(source) = unchanged_by_digest
            .get(&new_digest)
            .and_then(|bucket| bucket.first())
        {
            let copy_action = Action::Copy {
                from: child_path(path, source),
                path: child_path(path, &key),
            };
            let fallback = Action::Add {
                path: child_path(path, &key),
                value: new[&key].clone(),
            };
            if plan(vec![copy_action.clone()], path_segment_pool)?.cost
                < plan(vec![fallback.clone()], path_segment_pool)?.cost
            {
                actions.push(copy_action);
            } else {
                remaining_added.push(key);
            }
        } else {
            remaining_added.push(key);
        }
    }
    for key in common {
        path.push(PathSegment::Key(key.clone()));
        actions.extend(
            build_diff(
                &old[key.as_str()],
                &new[key.as_str()],
                path,
                path_segment_pool,
            )?
            .actions,
        );
        path.pop();
    }
    for key in remaining_added {
        let mut target = path.clone();
        target.push(PathSegment::Key(key.clone()));
        actions.push(Action::Add {
            path: target,
            value: new[&key].clone(),
        });
    }

    // Return the plan.
    plan(actions, path_segment_pool)
}

fn child_path(path: &[PathSegment], key: &str) -> Vec<PathSegment> {
    let mut target = path.to_vec();
    target.push(PathSegment::Key(key.to_string()));
    target
}

fn remove_sorted_key(keys: &mut Vec<String>, key: &str) {
    if let Ok(index) = keys.binary_search_by(|candidate| candidate.as_str().cmp(key)) {
        keys.remove(index);
    }
}

fn digest_value(value: &Value) -> Result<ValueDigest, JsyncError> {
    let mut hasher = blake3::Hasher::new();
    update_digest_value(&mut hasher, value)?;
    Ok(hasher.finalize().into())
}

fn update_digest_value(hasher: &mut blake3::Hasher, value: &Value) -> Result<(), JsyncError> {
    match value {
        Value::Null => hasher.update(b"N"),
        Value::Bool(false) => hasher.update(b"B0"),
        Value::Bool(true) => hasher.update(b"B1"),
        Value::Number(number) => return update_digest_number(hasher, number),
        Value::String(value) => {
            hasher.update(b"S");
            update_digest_bytes(hasher, value.as_bytes())
        }
        Value::Array(values) => {
            hasher.update(b"A");
            update_digest_len(hasher, values.len());
            for value in values {
                update_digest_value(hasher, value)?;
            }
            hasher
        }
        Value::Object(object) => {
            hasher.update(b"O");
            update_digest_len(hasher, object.len());
            let mut keys = object.keys().collect::<Vec<_>>();
            keys.sort();
            for key in keys {
                hasher.update(b"K");
                update_digest_bytes(hasher, key.as_bytes());
                update_digest_value(hasher, &object[key])?;
            }
            hasher
        }
    };
    Ok(())
}

fn update_digest_number(hasher: &mut blake3::Hasher, number: &Number) -> Result<(), JsyncError> {
    if let Some(value) = number.as_i64() {
        validate_safe_json_integer(value as i128)?;
        update_digest_integer(hasher, value as i128);
        return Ok(());
    }
    if let Some(value) = number.as_u64() {
        validate_safe_json_integer(value as i128)?;
        update_digest_integer(hasher, value as i128);
        return Ok(());
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
        if value.fract() == 0.0 {
            update_digest_integer(hasher, value as i128);
        } else {
            hasher.update(b"F");
            hasher.update(&value.to_be_bytes());
        }
        return Ok(());
    }

    Err(JsyncError::new(
        JsyncErrorKind::InvalidJsonValue,
        "The JSON number cannot be encoded as a CBOR number.",
    ))
}

fn update_digest_integer(hasher: &mut blake3::Hasher, value: i128) {
    hasher.update(b"I");
    update_digest_bytes(hasher, value.to_string().as_bytes());
}

fn update_digest_bytes<'a>(hasher: &'a mut blake3::Hasher, bytes: &[u8]) -> &'a mut blake3::Hasher {
    update_digest_len(hasher, bytes.len());
    hasher.update(bytes)
}

fn update_digest_len(hasher: &mut blake3::Hasher, len: usize) {
    hasher.update(&(len as u64).to_be_bytes());
}

fn diff_arrays(
    old: &[Value],
    new: &[Value],
    path: &mut Vec<PathSegment>,
    path_segment_pool: &ProducerPathSegmentPool,
) -> Result<DiffPlan, JsyncError> {
    let mut actions = Vec::new();

    for index in 0..old.len().min(new.len()) {
        path.push(PathSegment::Index(index));
        actions.extend(build_diff(&old[index], &new[index], path, path_segment_pool)?.actions);
        path.pop();
    }

    for index in (new.len()..old.len()).rev() {
        let mut target = path.clone();
        target.push(PathSegment::Index(index));
        actions.push(Action::Remove { path: target });
    }

    for (index, value) in new.iter().enumerate().skip(old.len()) {
        let mut target = path.clone();
        target.push(PathSegment::Index(index));
        actions.push(Action::Add {
            path: target,
            value: value.clone(),
        });
    }

    plan(actions, path_segment_pool)
}

fn diff_strings(
    old: &str,
    new: &str,
    path: &[PathSegment],
    replace: DiffPlan,
    path_segment_pool: &ProducerPathSegmentPool,
) -> Result<DiffPlan, JsyncError> {
    let mut best = replace;

    if let Some(suffix) = new.strip_prefix(old)
        && !suffix.is_empty()
    {
        let append = plan(
            vec![Action::Append {
                path: path.to_vec(),
                text: suffix.to_string(),
            }],
            path_segment_pool,
        )?;
        if append.cost < best.cost {
            best = append;
        }
    }

    if let Some(prefix) = new.strip_suffix(old)
        && !prefix.is_empty()
    {
        let prepend = plan(
            vec![Action::Prepend {
                path: path.to_vec(),
                text: prefix.to_string(),
            }],
            path_segment_pool,
        )?;
        if prepend.cost < best.cost {
            best = prepend;
        }
    }

    Ok(best)
}

fn replace_plan(
    path: &[PathSegment],
    value: &Value,
    path_segment_pool: &ProducerPathSegmentPool,
) -> Result<DiffPlan, JsyncError> {
    plan(
        vec![Action::Replace {
            path: path.to_vec(),
            value: value.clone(),
        }],
        path_segment_pool,
    )
}

fn plan(
    actions: Vec<Action>,
    path_segment_pool: &ProducerPathSegmentPool,
) -> Result<DiffPlan, JsyncError> {
    // Cost is used only to choose between equivalent patch plans. Estimating it
    // here avoids serializing every candidate message during recursive diffing.
    let cost = estimate_plan_cost(&actions, path_segment_pool)?;
    Ok(DiffPlan { actions, cost })
}

fn choose_smaller(structural: DiffPlan, replace: DiffPlan) -> DiffPlan {
    if replace.cost < structural.cost {
        replace
    } else {
        structural
    }
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
            Action::Snapshot { value } => {
                Ok(cbor_array_header_len(2) + cbor_uint_len(0) + estimate_json_value_len(value)?)
            }
            Action::Add { path, value } => Ok(cbor_array_header_len(3)
                + cbor_uint_len(1)
                + self.estimate_path_len(path)
                + estimate_json_value_len(value)?),
            Action::Remove { path } => {
                Ok(cbor_array_header_len(2) + cbor_uint_len(2) + self.estimate_path_len(path))
            }
            Action::Replace { path, value } => Ok(cbor_array_header_len(3)
                + cbor_uint_len(3)
                + self.estimate_path_len(path)
                + estimate_json_value_len(value)?),
            Action::Append { path, text } => Ok(cbor_array_header_len(3)
                + cbor_uint_len(4)
                + self.estimate_path_len(path)
                + cbor_text_len(text)),
            Action::Prepend { path, text } => Ok(cbor_array_header_len(3)
                + cbor_uint_len(5)
                + self.estimate_path_len(path)
                + cbor_text_len(text)),
            Action::Copy { from, path } => Ok(cbor_array_header_len(3)
                + cbor_uint_len(6)
                + self.estimate_path_len(from)
                + self.estimate_path_len(path)),
            Action::Move { from, path } => Ok(cbor_array_header_len(3)
                + cbor_uint_len(7)
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
