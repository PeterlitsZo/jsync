//! Jsync message producer.

use serde_json::{Map, Value};

use crate::error::{JsyncError, JsyncErrorKind};
use crate::message::{Action, Message, PathSegment};

/// Produces Jsync snapshots and incremental messages for a JSON document.
#[derive(Debug, Clone)]
pub struct Producer {
    current_document: Value,
    last_emitted_document: Option<Value>,
}

impl Producer {
    /// Creates a producer with the initial JSON document.
    pub fn new(initial_document: Value) -> Self {
        Self {
            current_document: initial_document,
            last_emitted_document: None,
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
                let actions = build_diff(previous, &self.current_document, &mut path)?.actions;
                if actions.is_empty() {
                    return Err(JsyncError::new(
                        JsyncErrorKind::ApplyFailed,
                        "The Jsync producer generated an empty diff for changed documents.",
                    ));
                }
                actions
            }
        };

        let message = Message::new(actions).to_bytes()?;
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
) -> Result<DiffPlan, JsyncError> {
    if from == to {
        return Ok(DiffPlan {
            actions: Vec::new(),
            cost: 0,
        });
    }

    let replace = replace_plan(path, to)?;
    let structural = match (from, to) {
        (Value::Object(old), Value::Object(new)) => diff_objects(old, new, path),
        (Value::Array(old), Value::Array(new)) => diff_arrays(old, new, path),
        (Value::String(old), Value::String(new)) => return diff_strings(old, new, path, replace),
        _ => return Ok(replace),
    }?;

    Ok(choose_smaller(structural, replace))
}

fn diff_objects(
    old: &Map<String, Value>,
    new: &Map<String, Value>,
    path: &mut Vec<PathSegment>,
) -> Result<DiffPlan, JsyncError> {
    let mut actions = Vec::new();

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
            if plan(vec![move_action.clone()])?.cost < plan(fallback.clone())?.cost {
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
            if plan(vec![copy_action.clone()])?.cost < plan(vec![fallback.clone()])?.cost {
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
        actions.extend(build_diff(&old[key.as_str()], &new[key.as_str()], path)?.actions);
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

    Ok(plan(actions)?)
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
) -> Result<DiffPlan, JsyncError> {
    let mut actions = Vec::new();

    for index in 0..old.len().min(new.len()) {
        path.push(PathSegment::Index(index));
        actions.extend(build_diff(&old[index], &new[index], path)?.actions);
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

    Ok(plan(actions)?)
}

fn diff_strings(
    old: &str,
    new: &str,
    path: &[PathSegment],
    replace: DiffPlan,
) -> Result<DiffPlan, JsyncError> {
    let mut best = replace;

    if let Some(suffix) = new.strip_prefix(old) {
        if !suffix.is_empty() {
            let append = plan(vec![Action::Append {
                path: path.to_vec(),
                text: suffix.to_string(),
            }])?;
            if append.cost < best.cost {
                best = append;
            }
        }
    }

    if let Some(prefix) = new.strip_suffix(old) {
        if !prefix.is_empty() {
            let prepend = plan(vec![Action::Prepend {
                path: path.to_vec(),
                text: prefix.to_string(),
            }])?;
            if prepend.cost < best.cost {
                best = prepend;
            }
        }
    }

    Ok(best)
}

fn replace_plan(path: &[PathSegment], value: &Value) -> Result<DiffPlan, JsyncError> {
    plan(vec![Action::Replace {
        path: path.to_vec(),
        value: value.clone(),
    }])
}

fn plan(actions: Vec<Action>) -> Result<DiffPlan, JsyncError> {
    let cost = if actions.is_empty() {
        0
    } else {
        Message::new(actions.clone()).to_bytes()?.len()
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
