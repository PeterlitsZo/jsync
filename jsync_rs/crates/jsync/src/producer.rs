//! Jsync message producer.

use serde_json::{Map, Value};

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

    // Handle the key deletion.
    let mut move_actions = Vec::new();
    let mut remaining_removed = Vec::new();
    for key in removed {
        if let Some(added_index) = added
            .iter()
            .position(|added_key| old[&key] == new[added_key])
        {
            let added_key = added.remove(added_index);
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
                move_actions.push(move_action);
            } else {
                remaining_removed.push(key);
                added.push(added_key);
                added.sort();
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
    let unchanged = common
        .iter()
        .filter(|key| old[key.as_str()] == new[key.as_str()])
        .cloned()
        .collect::<Vec<_>>();

    // Handle the key addition.
    let mut remaining_added = Vec::new();
    for key in added {
        if let Some(source) = unchanged
            .iter()
            .find(|source| old[source.as_str()] == new[&key])
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

/// Generates a plan for the given actions with the path segment pool.
///
/// We will calculate the cost of the plan by checking the size of the
/// serialized message.
fn plan(
    actions: Vec<Action>,
    path_segment_pool: &ProducerPathSegmentPool,
) -> Result<DiffPlan, JsyncError> {
    let cost = if actions.is_empty() {
        0
    } else {
        let mut path_segment_pool = path_segment_pool.clone();
        let mut txn = path_segment_pool.transaction();
        let len = Message::new(actions.clone())
            .to_bytes_with_pool_txn(&mut txn)?
            .len();
        txn.commit();
        len
    };
    Ok(DiffPlan { actions, cost })
}

fn choose_smaller(structural: DiffPlan, replace: DiffPlan) -> DiffPlan {
    if replace.cost < structural.cost {
        replace
    } else {
        structural
    }
}
