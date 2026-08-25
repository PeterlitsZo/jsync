//! Jsync message producer.

use ciborium::Value as CborValue;
use serde_json::{Map, Number, Value};

use crate::error::{JsyncError, JsyncErrorKind};
use crate::value::{Action, PathSegment};

const HEADER: [u8; 3] = [0xd9, 0xff, 0x01];

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

        let message = encode_message(&actions)?;
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

fn encode_message(actions: &[Action]) -> Result<Vec<u8>, JsyncError> {
    let payload = CborValue::Array(
        actions
            .iter()
            .map(action_to_cbor)
            .collect::<Result<Vec<_>, _>>()?,
    );
    let mut bytes = HEADER.to_vec();
    ciborium::ser::into_writer(&payload, &mut bytes).map_err(|error| {
        JsyncError::new(
            JsyncErrorKind::ApplyFailed,
            "The Jsync producer could not encode a message.",
        )
        .with_source(anyhow::Error::new(error))
    })?;
    Ok(bytes)
}

fn action_to_cbor(action: &Action) -> Result<CborValue, JsyncError> {
    match action {
        Action::Snapshot { value } => Ok(CborValue::Array(vec![integer(0), json_to_cbor(value)?])),
        Action::Add { path, value } => Ok(CborValue::Array(vec![
            integer(1),
            path_to_cbor(path),
            json_to_cbor(value)?,
        ])),
        Action::Remove { path } => Ok(CborValue::Array(vec![integer(2), path_to_cbor(path)])),
        Action::Replace { path, value } => Ok(CborValue::Array(vec![
            integer(3),
            path_to_cbor(path),
            json_to_cbor(value)?,
        ])),
    }
}

fn path_to_cbor(path: &[PathSegment]) -> CborValue {
    CborValue::Array(
        path.iter()
            .map(|segment| match segment {
                PathSegment::Key(key) => CborValue::Text(key.clone()),
                PathSegment::Index(index) => integer(*index as u64),
            })
            .collect(),
    )
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
        return Ok(integer(value));
    }
    if let Some(value) = number.as_u64() {
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
        return Ok(CborValue::Float(value));
    }

    Err(JsyncError::new(
        JsyncErrorKind::InvalidJsonValue,
        "The JSON number cannot be encoded as a CBOR number.",
    ))
}

fn integer<T>(value: T) -> CborValue
where
    ciborium::value::Integer: From<T>,
{
    CborValue::Integer(value.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn builds_a_deterministic_nested_diff() {
        let from = json!({"b": 2, "remove": true, "nested": {"a": 1}});
        let to = json!({"a": 1, "b": 3, "nested": {"a": 2}});
        let mut actions = Vec::new();
        build_diff(&from, &to, &mut Vec::new(), &mut actions);

        assert_eq!(
            actions,
            vec![
                Action::Remove {
                    path: vec![PathSegment::Key("remove".into())],
                },
                Action::Replace {
                    path: vec![PathSegment::Key("b".into())],
                    value: json!(3),
                },
                Action::Replace {
                    path: vec![
                        PathSegment::Key("nested".into()),
                        PathSegment::Key("a".into()),
                    ],
                    value: json!(2),
                },
                Action::Add {
                    path: vec![PathSegment::Key("a".into())],
                    value: json!(1),
                },
            ]
        );
    }
}
