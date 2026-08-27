mod action;
mod codec;
mod json_cbor;
mod opcode;
mod pool;

pub use action::{Action, ArrayPatchEdit, PathSegment, StringPatchEdit};
pub use pool::{
    ConsumerPathSegmentPool, ConsumerPathSegmentPoolTransaction, ProducerPathSegmentPool,
    ProducerPathSegmentPoolTransaction,
};

pub(crate) use opcode::{
    OPCODE_ADD, OPCODE_ARRAY_PATCH, OPCODE_COPY, OPCODE_MOVE, OPCODE_REMOVE, OPCODE_REPLACE,
    OPCODE_SNAPSHOT, OPCODE_STRING_APPEND, OPCODE_STRING_PATCH, OPCODE_STRING_PREPEND,
};

use crate::error::JsyncError;

/// A structured Jsync message.
#[derive(Debug, Clone, PartialEq)]
pub struct Message {
    /// Ordered actions contained in the message.
    pub actions: Vec<Action>,
}

impl Message {
    /// Creates a message from already structured actions.
    pub fn new(actions: Vec<Action>) -> Self {
        Self { actions }
    }

    /// Decodes a message using a caller-owned consumer path segment pool transaction.
    pub fn from_bytes_with_pool_txn(
        bytes: Vec<u8>,
        txn: &mut ConsumerPathSegmentPoolTransaction<'_>,
    ) -> Result<Self, JsyncError> {
        codec::from_bytes_with_pool_txn(&bytes, txn).map(|actions| Self { actions })
    }

    /// Encodes this message using a caller-owned producer path segment pool
    /// transaction.
    pub fn to_bytes_with_pool_txn(
        &self,
        txn: &mut ProducerPathSegmentPoolTransaction<'_>,
    ) -> Result<Vec<u8>, JsyncError> {
        codec::to_bytes_with_pool_txn(&self.actions, txn)
    }
}
