use std::collections::HashMap;

use serde_json::{Map, Value};

use super::cost::plan;
use super::digest::{ValueDigest, digest_value};
use crate::error::JsyncError;
use crate::message::{Action, PathSegment, ProducerPathSegmentPool};

type DigestIndex = HashMap<ValueDigest, Vec<String>>;
type KeyDigestIndex = HashMap<String, ValueDigest>;

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
        if old_digest == digest_value(&new[key.as_str()])? {
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

fn choose_smaller(structural: DiffPlan, replace: DiffPlan) -> DiffPlan {
    if replace.cost < structural.cost {
        replace
    } else {
        structural
    }
}
