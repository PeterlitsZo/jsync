//! Jsync message producer.

mod cost;
mod diff;
mod digest;

use serde_json::Value;

use crate::error::{JsyncError, JsyncErrorKind};
use crate::message::{Action, Message, ProducerPathSegmentPool};

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
                let actions = diff::build_diff(
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
