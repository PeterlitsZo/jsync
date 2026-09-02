use std::collections::HashMap;

use super::PathSegment;

/// Producer-side path segment pool with stable indexes and O(1) segment lookup.
#[derive(Debug, Clone, Default)]
pub struct ProducerPathSegmentPool {
    segments: Vec<PathSegment>,
    indexes: HashMap<PathSegment, usize>,
}

impl ProducerPathSegmentPool {
    /// Creates an empty producer path segment pool.
    pub fn new() -> Self {
        Self::default()
    }

    /// Starts an atomic producer pool update.
    pub fn transaction(&mut self) -> ProducerPathSegmentPoolTransaction<'_> {
        let checkpoint = self.segments.len();
        ProducerPathSegmentPoolTransaction {
            pool: self,
            checkpoint,
            committed: false,
        }
    }

    pub(super) fn index_for(&mut self, segment: &PathSegment) -> usize {
        if let Some(index) = self.indexes.get(segment) {
            return *index;
        }

        let index = self.segments.len();
        let segment = segment.clone();
        self.segments.push(segment.clone());
        self.indexes.insert(segment, index);
        index
    }

    pub(crate) fn index_of(&self, segment: &PathSegment) -> Option<usize> {
        self.indexes.get(segment).copied()
    }

    /// Returns the committed pool size so producer-side estimators can simulate
    /// future appended segment indexes without mutating the real pool.
    pub(crate) fn len(&self) -> usize {
        self.segments.len()
    }

    fn rollback_to(&mut self, len: usize) {
        if len >= self.segments.len() {
            return;
        }

        for segment in self.segments.drain(len..) {
            self.indexes.remove(&segment);
        }
    }
}

/// Producer-side path segment pool transaction.
#[derive(Debug)]
pub struct ProducerPathSegmentPoolTransaction<'a> {
    pub(super) pool: &'a mut ProducerPathSegmentPool,
    checkpoint: usize,
    committed: bool,
}

impl ProducerPathSegmentPoolTransaction<'_> {
    /// Returns the segments appended since this transaction started.
    pub fn appended_segments(&self) -> &[PathSegment] {
        &self.pool.segments[self.checkpoint..]
    }

    /// Commits this transaction.
    pub fn commit(mut self) {
        self.committed = true;
    }

    /// Aborts this transaction and rolls the pool back to its checkpoint.
    pub fn abort(self) {}
}

impl Drop for ProducerPathSegmentPoolTransaction<'_> {
    fn drop(&mut self) {
        if !self.committed {
            self.pool.rollback_to(self.checkpoint);
        }
    }
}

/// Consumer-side path segment pool with stable indexes.
#[derive(Debug, Clone, Default)]
pub struct ConsumerPathSegmentPool {
    segments: Vec<PathSegment>,
}

impl ConsumerPathSegmentPool {
    /// Creates an empty consumer path segment pool.
    pub fn new() -> Self {
        Self::default()
    }

    /// Starts an atomic consumer pool update.
    pub fn transaction(&mut self) -> ConsumerPathSegmentPoolTransaction<'_> {
        let checkpoint = self.segments.len();
        ConsumerPathSegmentPoolTransaction {
            pool: self,
            checkpoint,
            committed: false,
        }
    }

    fn rollback_to(&mut self, len: usize) {
        self.segments.truncate(len);
    }
}

/// Consumer-side path segment pool transaction.
#[derive(Debug)]
pub struct ConsumerPathSegmentPoolTransaction<'a> {
    pool: &'a mut ConsumerPathSegmentPool,
    checkpoint: usize,
    committed: bool,
}

impl ConsumerPathSegmentPoolTransaction<'_> {
    /// Appends path segments declared by message metadata.
    pub fn append_segments(&mut self, segments: Vec<PathSegment>) {
        self.pool.segments.extend(segments);
    }

    pub(super) fn path_segment_at(&self, index: usize) -> Option<PathSegment> {
        self.pool.segments.get(index).cloned()
    }

    pub(super) fn pool_len(&self) -> usize {
        self.pool.segments.len()
    }

    /// Commits this transaction.
    pub fn commit(mut self) {
        self.committed = true;
    }

    /// Aborts this transaction and rolls the pool back to its checkpoint.
    pub fn abort(self) {}
}

impl Drop for ConsumerPathSegmentPoolTransaction<'_> {
    fn drop(&mut self) {
        if !self.committed {
            self.pool.rollback_to(self.checkpoint);
        }
    }
}
