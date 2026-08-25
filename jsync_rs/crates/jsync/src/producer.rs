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
                let mut actions = Vec::new();
                let mut path = Vec::new();
                build_diff(previous, &self.current_document, &mut path, &mut actions);
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

fn build_diff(from: &Value, to: &Value, path: &mut Vec<PathSegment>, actions: &mut Vec<Action>) {
    if from == to {
        return;
    }

    match (from, to) {
        (Value::Object(old), Value::Object(new)) => diff_objects(old, new, path, actions),
        (Value::Array(old), Value::Array(new)) => diff_arrays(old, new, path, actions),
        _ => actions.push(Action::Replace {
            path: path.clone(),
            value: to.clone(),
        }),
    }
}

fn diff_objects(
    old: &Map<String, Value>,
    new: &Map<String, Value>,
    path: &mut Vec<PathSegment>,
    actions: &mut Vec<Action>,
) {
    let mut removed = old
        .keys()
        .filter(|key| !new.contains_key(*key))
        .collect::<Vec<_>>();
    removed.sort();
    for key in removed {
        let mut target = path.clone();
        target.push(PathSegment::Key(key.clone()));
        actions.push(Action::Remove { path: target });
    }

    let mut common = old
        .keys()
        .filter(|key| new.contains_key(*key))
        .collect::<Vec<_>>();
    common.sort();
    for key in common {
        path.push(PathSegment::Key(key.clone()));
        build_diff(&old[key], &new[key], path, actions);
        path.pop();
    }

    let mut added = new
        .keys()
        .filter(|key| !old.contains_key(*key))
        .collect::<Vec<_>>();
    added.sort();
    for key in added {
        let mut target = path.clone();
        target.push(PathSegment::Key(key.clone()));
        actions.push(Action::Add {
            path: target,
            value: new[key].clone(),
        });
    }
}

fn diff_arrays(
    old: &[Value],
    new: &[Value],
    path: &mut Vec<PathSegment>,
    actions: &mut Vec<Action>,
) {
    for index in 0..old.len().min(new.len()) {
        path.push(PathSegment::Index(index));
        build_diff(&old[index], &new[index], path, actions);
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
}
