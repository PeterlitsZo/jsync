use serde_json::Value;

/// A structured Jsync action.
#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    /// Replaces the current document with the given JSON value.
    Snapshot {
        /// The snapshot value to replace the current document with.
        value: Value,
    },
    /// Inserts a JSON value at the given path.
    Add {
        /// The validated destination path.
        path: Vec<PathSegment>,
        /// The value to insert or overwrite.
        value: Value,
    },
    /// Removes the value at the given path.
    Remove {
        /// The validated path of the value to remove.
        path: Vec<PathSegment>,
    },
    /// Replaces the value at the given path.
    Replace {
        /// The validated path of the value to replace.
        path: Vec<PathSegment>,
        /// The replacement JSON value.
        value: Value,
    },
    /// Appends text to an existing string value at the given path.
    StringAppend {
        /// The validated path of the string to append to.
        path: Vec<PathSegment>,
        /// The text to append.
        text: String,
    },
    /// Prepends text to an existing string value at the given path.
    StringPrepend {
        /// The validated path of the string to prepend to.
        path: Vec<PathSegment>,
        /// The text to prepend.
        text: String,
    },
    /// Copies an existing JSON value to another path.
    Copy {
        /// The validated source path.
        from: Vec<PathSegment>,
        /// The validated destination path.
        path: Vec<PathSegment>,
    },
    /// Moves an existing JSON value to another path.
    Move {
        /// The validated source path.
        from: Vec<PathSegment>,
        /// The validated destination path.
        path: Vec<PathSegment>,
    },
}

/// One segment in a validated Jsync action path.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PathSegment {
    /// Selects an object property by key.
    Key(String),
    /// Selects an array element by non-negative index.
    Index(usize),
}
