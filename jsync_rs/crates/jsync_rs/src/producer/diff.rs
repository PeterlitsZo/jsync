use std::collections::HashMap;

use serde_json::{Map, Value};

use super::cost::plan;
use super::digest::{ValueDigest, digest_value};
use crate::error::JsyncError;
use crate::message::{
    Action, ArrayPatchEdit, PathSegment, ProducerPathSegmentPool, StringPatchEdit,
};

type DigestIndex = HashMap<ValueDigest, Vec<String>>;
type KeyDigestIndex = HashMap<String, ValueDigest>;
const MYERS_MIDDLE_PRODUCT_THRESHOLD: usize = 100_000;
const MYERS_TRACE_CELL_THRESHOLD: usize = 2_000_000;
const MYERS_ARRAY_MIDDLE_PRODUCT_THRESHOLD: usize = 100_000;
const MYERS_ARRAY_TRACE_CELL_THRESHOLD: usize = 2_000_000;

#[derive(Debug)]
pub(super) struct DiffPlan {
    pub(super) actions: Vec<Action>,
    pub(super) cost: usize,
}

pub(super) fn build_diff(
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

    let removed = sorted_removed_keys(old, new);
    let mut added = sorted_added_keys(old, new);
    let (mut added_by_digest, mut added_digests) = index_added_values_by_digest(&added, new)?;

    let (move_actions, remaining_removed) = extract_move_actions(
        removed,
        &mut added,
        &mut added_by_digest,
        &mut added_digests,
        old,
        new,
        path,
        path_segment_pool,
    )?;
    actions.extend(move_actions);
    for key in remaining_removed {
        let mut target = path.clone();
        target.push(PathSegment::Key(key.clone()));
        actions.push(Action::Remove { path: target });
    }

    let common = sorted_common_keys(old, new);
    let unchanged_by_digest = index_unchanged_values_by_digest(&common, old, new)?;

    let (copy_actions, remaining_added) = extract_copy_actions(
        added,
        &added_digests,
        &unchanged_by_digest,
        new,
        path,
        path_segment_pool,
    )?;
    actions.extend(copy_actions);

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

fn sorted_removed_keys(old: &Map<String, Value>, new: &Map<String, Value>) -> Vec<String> {
    let mut keys = old
        .keys()
        .filter(|key| !new.contains_key(*key))
        .cloned()
        .collect::<Vec<_>>();
    keys.sort();
    keys
}

fn sorted_added_keys(old: &Map<String, Value>, new: &Map<String, Value>) -> Vec<String> {
    let mut keys = new
        .keys()
        .filter(|key| !old.contains_key(*key))
        .cloned()
        .collect::<Vec<_>>();
    keys.sort();
    keys
}

fn sorted_common_keys(old: &Map<String, Value>, new: &Map<String, Value>) -> Vec<String> {
    let mut keys = old
        .keys()
        .filter(|key| new.contains_key(*key))
        .cloned()
        .collect::<Vec<_>>();
    keys.sort();
    keys
}

fn index_added_values_by_digest(
    added: &[String],
    new: &Map<String, Value>,
) -> Result<(DigestIndex, KeyDigestIndex), JsyncError> {
    let mut by_digest = DigestIndex::new();
    let mut digests = KeyDigestIndex::new();
    for key in added {
        let digest = digest_value(&new[key.as_str()])?;
        digests.insert(key.clone(), digest);
        by_digest.entry(digest).or_default().push(key.clone());
    }
    Ok((by_digest, digests))
}

fn index_unchanged_values_by_digest(
    common: &[String],
    old: &Map<String, Value>,
    new: &Map<String, Value>,
) -> Result<DigestIndex, JsyncError> {
    let mut unchanged_by_digest = DigestIndex::new();
    for key in common {
        let old_digest = digest_value(&old[key.as_str()])?;
        if old_digest == digest_value(&new[key.as_str()])? && old[key.as_str()] == new[key.as_str()]
        {
            unchanged_by_digest
                .entry(old_digest)
                .or_default()
                .push(key.clone());
        }
    }
    Ok(unchanged_by_digest)
}

fn extract_move_actions(
    removed: Vec<String>,
    added: &mut Vec<String>,
    added_by_digest: &mut DigestIndex,
    added_digests: &mut KeyDigestIndex,
    old: &Map<String, Value>,
    new: &Map<String, Value>,
    path: &[PathSegment],
    path_segment_pool: &ProducerPathSegmentPool,
) -> Result<(Vec<Action>, Vec<String>), JsyncError> {
    let mut move_actions = Vec::new();
    let mut remaining_removed = Vec::new();
    for key in removed {
        let old_digest = digest_value(&old[key.as_str()])?;
        let Some(added_key) = added_by_digest
            .get(&old_digest)
            .and_then(|bucket| bucket.first())
            .cloned()
        else {
            remaining_removed.push(key);
            continue;
        };
        if old[key.as_str()] != new[added_key.as_str()] {
            remaining_removed.push(key);
            continue;
        }

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
            remove_sorted_key(added, &added_key);
            added_digests.remove(&added_key);
            if let Some(bucket) = added_by_digest.get_mut(&old_digest) {
                bucket.remove(0);
            }
            move_actions.push(move_action);
        } else {
            remaining_removed.push(key);
        }
    }
    Ok((move_actions, remaining_removed))
}

fn extract_copy_actions(
    added: Vec<String>,
    added_digests: &KeyDigestIndex,
    unchanged_by_digest: &DigestIndex,
    new: &Map<String, Value>,
    path: &[PathSegment],
    path_segment_pool: &ProducerPathSegmentPool,
) -> Result<(Vec<Action>, Vec<String>), JsyncError> {
    let mut copy_actions = Vec::new();
    let mut remaining_added = Vec::new();
    for key in added {
        let new_digest = added_digests[&key];
        let Some(source) = unchanged_by_digest
            .get(&new_digest)
            .and_then(|bucket| bucket.first())
        else {
            remaining_added.push(key);
            continue;
        };
        if new[key.as_str()] != new[source.as_str()] {
            remaining_added.push(key);
            continue;
        }

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
            copy_actions.push(copy_action);
        } else {
            remaining_added.push(key);
        }
    }
    Ok((copy_actions, remaining_added))
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

fn diff_arrays(
    old: &[Value],
    new: &[Value],
    path: &mut Vec<PathSegment>,
    path_segment_pool: &ProducerPathSegmentPool,
) -> Result<DiffPlan, JsyncError> {
    let mut best = legacy_array_plan(old, new, path, path_segment_pool)?;

    if let Some(single_patch) = single_array_patch_plan(old, new, path, path_segment_pool)? {
        if single_patch.cost < best.cost {
            best = single_patch;
        }
    }

    if should_run_myers_array_diff(old, new) {
        if let Some(myers_patch) = myers_array_patch_plan(old, new, path, path_segment_pool)? {
            if myers_patch.cost < best.cost {
                best = myers_patch;
            }
        }
    }

    Ok(best)
}

fn legacy_array_plan(
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

fn single_array_patch_plan(
    old: &[Value],
    new: &[Value],
    path: &[PathSegment],
    path_segment_pool: &ProducerPathSegmentPool,
) -> Result<Option<DiffPlan>, JsyncError> {
    let prefix_len = common_array_prefix_len(old, new);
    let suffix_len = common_array_suffix_len(old, new, prefix_len);
    let old_mid_end = old.len() - suffix_len;
    let new_mid_end = new.len() - suffix_len;
    let delete_count = old_mid_end - prefix_len;
    let values = new[prefix_len..new_mid_end].to_vec();
    if delete_count == 0 && values.is_empty() {
        return Ok(None);
    }

    plan(
        vec![Action::ArrayPatch {
            path: path.to_vec(),
            edits: vec![ArrayPatchEdit {
                start: prefix_len,
                delete_count,
                values,
            }],
        }],
        path_segment_pool,
    )
    .map(Some)
}

fn myers_array_patch_plan(
    old: &[Value],
    new: &[Value],
    path: &[PathSegment],
    path_segment_pool: &ProducerPathSegmentPool,
) -> Result<Option<DiffPlan>, JsyncError> {
    let prefix_len = common_array_prefix_len(old, new);
    let suffix_len = common_array_suffix_len(old, new, prefix_len);
    let old_mid_end = old.len() - suffix_len;
    let new_mid_end = new.len() - suffix_len;
    let old_middle = &old[prefix_len..old_mid_end];
    let new_middle = &new[prefix_len..new_mid_end];
    if old_middle.is_empty() || new_middle.is_empty() {
        return Ok(None);
    }

    let old_digests = old_middle
        .iter()
        .map(digest_value)
        .collect::<Result<Vec<_>, _>>()?;
    let new_digests = new_middle
        .iter()
        .map(digest_value)
        .collect::<Result<Vec<_>, _>>()?;
    let operations = myers_diff_arrays(
        old_middle.len(),
        new_middle.len(),
        |old_index, new_index| {
            old_digests[old_index] == new_digests[new_index]
                && old_middle[old_index] == new_middle[new_index]
        },
    );
    let edits = array_edit_ops_to_patch_edits(&operations, new_middle, prefix_len);
    if edits.is_empty() {
        return Ok(None);
    }

    plan(
        vec![Action::ArrayPatch {
            path: path.to_vec(),
            edits,
        }],
        path_segment_pool,
    )
    .map(Some)
}

fn common_array_prefix_len(old: &[Value], new: &[Value]) -> usize {
    old.iter()
        .zip(new.iter())
        .take_while(|(old, new)| old == new)
        .count()
}

fn common_array_suffix_len(old: &[Value], new: &[Value], prefix_len: usize) -> usize {
    let max_suffix_len = old.len().min(new.len()) - prefix_len;
    old.iter()
        .rev()
        .zip(new.iter().rev())
        .take(max_suffix_len)
        .take_while(|(old, new)| old == new)
        .count()
}

fn should_run_myers_array_diff(old: &[Value], new: &[Value]) -> bool {
    let prefix_len = common_array_prefix_len(old, new);
    let suffix_len = common_array_suffix_len(old, new, prefix_len);
    let old_middle_len = old.len() - prefix_len - suffix_len;
    let new_middle_len = new.len() - prefix_len - suffix_len;
    if old_middle_len == 0 || new_middle_len == 0 {
        return false;
    }
    let Some(product) = old_middle_len.checked_mul(new_middle_len) else {
        return false;
    };
    if product > MYERS_ARRAY_MIDDLE_PRODUCT_THRESHOLD {
        return false;
    }

    let Some(max) = old_middle_len.checked_add(new_middle_len) else {
        return false;
    };
    let Some(trace_depth) = max.checked_add(1) else {
        return false;
    };
    let Some(trace_width) = max.checked_mul(2).and_then(|value| value.checked_add(3)) else {
        return false;
    };
    let Some(trace_cells) = trace_depth.checked_mul(trace_width) else {
        return false;
    };
    trace_cells <= MYERS_ARRAY_TRACE_CELL_THRESHOLD
}

#[derive(Debug, Clone, PartialEq)]
enum ArrayEditOp {
    Keep,
    Delete,
    Insert(usize),
}

fn myers_diff_arrays<F>(n: usize, m: usize, mut equal: F) -> Vec<ArrayEditOp>
where
    F: FnMut(usize, usize) -> bool,
{
    if n == 0 {
        return (0..m).map(ArrayEditOp::Insert).collect();
    }
    if m == 0 {
        return vec![ArrayEditOp::Delete; n];
    }

    let max = n + m;
    let offset = max + 1;
    let mut trace = Vec::new();
    let mut v = vec![-1isize; 2 * max + 3];
    v[offset + 1] = 0;

    for d in 0..=max {
        for k in (-(d as isize)..=(d as isize)).step_by(2) {
            let index = (offset as isize + k) as usize;
            let x = if k == -(d as isize) || (k != d as isize && v[index - 1] < v[index + 1]) {
                v[index + 1]
            } else {
                v[index - 1] + 1
            };
            let mut x = x;
            let mut y = x - k;
            while x < n as isize && y < m as isize && equal(x as usize, y as usize) {
                x += 1;
                y += 1;
            }
            v[index] = x;
            if x >= n as isize && y >= m as isize {
                trace.push(v);
                return backtrack_myers_array_diff(&trace, d, n, m, offset);
            }
        }
        trace.push(v.clone());
    }

    unreachable!("Myers diff must reach the end within n + m edits")
}

fn backtrack_myers_array_diff(
    trace: &[Vec<isize>],
    edit_distance: usize,
    n: usize,
    m: usize,
    offset: usize,
) -> Vec<ArrayEditOp> {
    let mut x = n as isize;
    let mut y = m as isize;
    let mut operations = Vec::new();

    for d in (1..=edit_distance).rev() {
        let k = x - y;
        let previous = &trace[d - 1];
        let previous_k = if k == -(d as isize)
            || (k != d as isize
                && previous[(offset as isize + k - 1) as usize]
                    < previous[(offset as isize + k + 1) as usize])
        {
            k + 1
        } else {
            k - 1
        };
        let previous_x = previous[(offset as isize + previous_k) as usize];
        let previous_y = previous_x - previous_k;

        while x > previous_x && y > previous_y {
            operations.push(ArrayEditOp::Keep);
            x -= 1;
            y -= 1;
        }

        if x == previous_x {
            operations.push(ArrayEditOp::Insert((y - 1) as usize));
            y -= 1;
        } else {
            operations.push(ArrayEditOp::Delete);
            x -= 1;
        }
    }

    while x > 0 && y > 0 {
        operations.push(ArrayEditOp::Keep);
        x -= 1;
        y -= 1;
    }

    operations.reverse();
    operations
}

fn array_edit_ops_to_patch_edits(
    operations: &[ArrayEditOp],
    new_values: &[Value],
    prefix_offset: usize,
) -> Vec<ArrayPatchEdit> {
    let mut edits = Vec::new();
    let mut old_cursor = 0usize;
    let mut hunk_start = None;
    let mut delete_count = 0usize;
    let mut values = Vec::new();

    for operation in operations {
        match operation {
            ArrayEditOp::Keep => {
                flush_array_patch_hunk(
                    &mut edits,
                    &mut hunk_start,
                    &mut delete_count,
                    &mut values,
                    prefix_offset,
                );
                old_cursor += 1;
            }
            ArrayEditOp::Delete => {
                if hunk_start.is_none() {
                    hunk_start = Some(old_cursor);
                }
                delete_count += 1;
                old_cursor += 1;
            }
            ArrayEditOp::Insert(new_index) => {
                if hunk_start.is_none() {
                    hunk_start = Some(old_cursor);
                }
                values.push(new_values[*new_index].clone());
            }
        }
    }

    flush_array_patch_hunk(
        &mut edits,
        &mut hunk_start,
        &mut delete_count,
        &mut values,
        prefix_offset,
    );
    edits.reverse();
    edits
}

fn flush_array_patch_hunk(
    edits: &mut Vec<ArrayPatchEdit>,
    hunk_start: &mut Option<usize>,
    delete_count: &mut usize,
    values: &mut Vec<Value>,
    prefix_offset: usize,
) {
    let Some(start) = hunk_start.take() else {
        return;
    };
    if *delete_count > 0 || !values.is_empty() {
        edits.push(ArrayPatchEdit {
            start: prefix_offset + start,
            delete_count: *delete_count,
            values: std::mem::take(values),
        });
    }
    *delete_count = 0;
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
            vec![Action::StringAppend {
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
            vec![Action::StringPrepend {
                path: path.to_vec(),
                text: prefix.to_string(),
            }],
            path_segment_pool,
        )?;
        if prepend.cost < best.cost {
            best = prepend;
        }
    }

    if let Some(index) = (!old.is_empty()).then(|| new.find(old)).flatten() {
        let prefix = &new[..index];
        let suffix = &new[index + old.len()..];
        let mut actions = Vec::new();
        if !prefix.is_empty() {
            actions.push(Action::StringPrepend {
                path: path.to_vec(),
                text: prefix.to_string(),
            });
        }
        if !suffix.is_empty() {
            actions.push(Action::StringAppend {
                path: path.to_vec(),
                text: suffix.to_string(),
            });
        }
        if actions.len() > 1 {
            let prepend_append = plan(actions, path_segment_pool)?;
            if prepend_append.cost < best.cost {
                best = prepend_append;
            }
        }
    }

    let old_tokens = string_tokens(old);
    let new_tokens = string_tokens(new);
    if let Some(single_patch) =
        single_string_patch_plan(&old_tokens, &new_tokens, path, path_segment_pool)?
    {
        if single_patch.cost < best.cost {
            best = single_patch;
        }
    }

    if should_run_myers_string_diff(&old_tokens, &new_tokens) {
        if let Some(myers_patch) =
            myers_string_patch_plan(&old_tokens, &new_tokens, path, path_segment_pool)?
        {
            if myers_patch.cost < best.cost {
                best = myers_patch;
            }
        }
    }

    Ok(best)
}

fn string_tokens(value: &str) -> Vec<char> {
    value.chars().collect()
}

fn tokens_to_string(tokens: &[char]) -> String {
    tokens.iter().collect()
}

fn single_string_patch_plan(
    old_tokens: &[char],
    new_tokens: &[char],
    path: &[PathSegment],
    path_segment_pool: &ProducerPathSegmentPool,
) -> Result<Option<DiffPlan>, JsyncError> {
    let prefix_len = common_string_prefix_len(old_tokens, new_tokens);
    let suffix_len = common_string_suffix_len(old_tokens, new_tokens, prefix_len);
    let old_mid_end = old_tokens.len() - suffix_len;
    let new_mid_end = new_tokens.len() - suffix_len;
    let edit = StringPatchEdit {
        start: prefix_len,
        delete_count: old_mid_end - prefix_len,
        text: tokens_to_string(&new_tokens[prefix_len..new_mid_end]),
    };
    if edit.delete_count == 0 && edit.text.is_empty() {
        return Ok(None);
    }
    plan(
        vec![Action::StringPatch {
            path: path.to_vec(),
            edits: vec![edit],
        }],
        path_segment_pool,
    )
    .map(Some)
}

fn myers_string_patch_plan(
    old_tokens: &[char],
    new_tokens: &[char],
    path: &[PathSegment],
    path_segment_pool: &ProducerPathSegmentPool,
) -> Result<Option<DiffPlan>, JsyncError> {
    let prefix_len = common_string_prefix_len(old_tokens, new_tokens);
    let suffix_len = common_string_suffix_len(old_tokens, new_tokens, prefix_len);
    let old_mid_end = old_tokens.len() - suffix_len;
    let new_mid_end = new_tokens.len() - suffix_len;
    let old_middle = &old_tokens[prefix_len..old_mid_end];
    let new_middle = &new_tokens[prefix_len..new_mid_end];
    let operations = myers_diff_strings(old_middle, new_middle);
    let edits = string_edit_ops_to_patch_edits(&operations, new_middle, prefix_len);
    if edits.is_empty() {
        return Ok(None);
    }
    plan(
        vec![Action::StringPatch {
            path: path.to_vec(),
            edits,
        }],
        path_segment_pool,
    )
    .map(Some)
}

fn common_string_prefix_len(old_tokens: &[char], new_tokens: &[char]) -> usize {
    old_tokens
        .iter()
        .zip(new_tokens.iter())
        .take_while(|(old, new)| old == new)
        .count()
}

fn common_string_suffix_len(old_tokens: &[char], new_tokens: &[char], prefix_len: usize) -> usize {
    let max_suffix_len = old_tokens.len().min(new_tokens.len()) - prefix_len;
    old_tokens
        .iter()
        .rev()
        .zip(new_tokens.iter().rev())
        .take(max_suffix_len)
        .take_while(|(old, new)| old == new)
        .count()
}

fn should_run_myers_string_diff(old_tokens: &[char], new_tokens: &[char]) -> bool {
    let prefix_len = common_string_prefix_len(old_tokens, new_tokens);
    let suffix_len = common_string_suffix_len(old_tokens, new_tokens, prefix_len);
    let old_middle_len = old_tokens.len() - prefix_len - suffix_len;
    let new_middle_len = new_tokens.len() - prefix_len - suffix_len;
    if old_middle_len == 0 || new_middle_len == 0 {
        return false;
    }
    let Some(product) = old_middle_len.checked_mul(new_middle_len) else {
        return false;
    };
    if product > MYERS_MIDDLE_PRODUCT_THRESHOLD {
        return false;
    }

    let Some(max) = old_middle_len.checked_add(new_middle_len) else {
        return false;
    };
    let Some(trace_depth) = max.checked_add(1) else {
        return false;
    };
    let Some(trace_width) = max.checked_mul(2).and_then(|value| value.checked_add(3)) else {
        return false;
    };
    let Some(trace_cells) = trace_depth.checked_mul(trace_width) else {
        return false;
    };
    trace_cells <= MYERS_TRACE_CELL_THRESHOLD
}

#[derive(Debug, Clone, PartialEq)]
enum StringEditOp {
    Keep,
    Delete,
    Insert(usize),
}

fn myers_diff_strings(old: &[char], new: &[char]) -> Vec<StringEditOp> {
    let n = old.len();
    let m = new.len();
    if n == 0 {
        return (0..m).map(StringEditOp::Insert).collect();
    }
    if m == 0 {
        return vec![StringEditOp::Delete; n];
    }

    let max = n + m;
    let offset = max + 1;
    let mut trace = Vec::new();
    let mut v = vec![-1isize; 2 * max + 3];
    v[offset + 1] = 0;

    for d in 0..=max {
        for k in (-(d as isize)..=(d as isize)).step_by(2) {
            let index = (offset as isize + k) as usize;
            let x = if k == -(d as isize) || (k != d as isize && v[index - 1] < v[index + 1]) {
                v[index + 1]
            } else {
                v[index - 1] + 1
            };
            let mut x = x;
            let mut y = x - k;
            while x < n as isize && y < m as isize && old[x as usize] == new[y as usize] {
                x += 1;
                y += 1;
            }
            v[index] = x;
            if x >= n as isize && y >= m as isize {
                trace.push(v);
                return backtrack_myers_string_diff(&trace, d, n, m, offset);
            }
        }
        trace.push(v.clone());
    }

    unreachable!("Myers diff must reach the end within n + m edits")
}

fn backtrack_myers_string_diff(
    trace: &[Vec<isize>],
    edit_distance: usize,
    n: usize,
    m: usize,
    offset: usize,
) -> Vec<StringEditOp> {
    let mut x = n as isize;
    let mut y = m as isize;
    let mut operations = Vec::new();

    for d in (1..=edit_distance).rev() {
        let k = x - y;
        let previous = &trace[d - 1];
        let previous_k = if k == -(d as isize)
            || (k != d as isize
                && previous[(offset as isize + k - 1) as usize]
                    < previous[(offset as isize + k + 1) as usize])
        {
            k + 1
        } else {
            k - 1
        };
        let previous_x = previous[(offset as isize + previous_k) as usize];
        let previous_y = previous_x - previous_k;

        while x > previous_x && y > previous_y {
            operations.push(StringEditOp::Keep);
            x -= 1;
            y -= 1;
        }

        if x == previous_x {
            operations.push(StringEditOp::Insert((y - 1) as usize));
            y -= 1;
        } else {
            operations.push(StringEditOp::Delete);
            x -= 1;
        }
    }

    while x > 0 && y > 0 {
        operations.push(StringEditOp::Keep);
        x -= 1;
        y -= 1;
    }

    operations.reverse();
    operations
}

fn string_edit_ops_to_patch_edits(
    operations: &[StringEditOp],
    new_tokens: &[char],
    prefix_offset: usize,
) -> Vec<StringPatchEdit> {
    let mut edits = Vec::new();
    let mut old_cursor = 0usize;
    let mut hunk_start = None;
    let mut delete_count = 0usize;
    let mut text = String::new();

    for operation in operations {
        match operation {
            StringEditOp::Keep => {
                flush_string_patch_hunk(
                    &mut edits,
                    &mut hunk_start,
                    &mut delete_count,
                    &mut text,
                    prefix_offset,
                );
                old_cursor += 1;
            }
            StringEditOp::Delete => {
                if hunk_start.is_none() {
                    hunk_start = Some(old_cursor);
                }
                delete_count += 1;
                old_cursor += 1;
            }
            StringEditOp::Insert(new_index) => {
                if hunk_start.is_none() {
                    hunk_start = Some(old_cursor);
                }
                text.push(new_tokens[*new_index]);
            }
        }
    }

    flush_string_patch_hunk(
        &mut edits,
        &mut hunk_start,
        &mut delete_count,
        &mut text,
        prefix_offset,
    );
    edits.reverse();
    edits
}

fn flush_string_patch_hunk(
    edits: &mut Vec<StringPatchEdit>,
    hunk_start: &mut Option<usize>,
    delete_count: &mut usize,
    text: &mut String,
    prefix_offset: usize,
) {
    let Some(start) = hunk_start.take() else {
        return;
    };
    if *delete_count > 0 || !text.is_empty() {
        edits.push(StringPatchEdit {
            start: prefix_offset + start,
            delete_count: *delete_count,
            text: std::mem::take(text),
        });
    }
    *delete_count = 0;
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

fn choose_smaller(structural: DiffPlan, replace: DiffPlan) -> DiffPlan {
    if replace.cost < structural.cost {
        replace
    } else {
        structural
    }
}
