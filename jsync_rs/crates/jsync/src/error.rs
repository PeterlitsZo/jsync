use std::collections::HashMap;
use std::error::Error;
use std::fmt;

/// Identifies the category of a failure while consuming a Jsync message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JsyncErrorKind {
    /// The three-byte Jsync header is missing or does not identify version 1.
    InvalidHeader,
    /// The message uses a Jsync version newer than the implementation supports.
    UnsupportedVersion,
    /// The payload could not be decoded as CBOR.
    CborDecode,
    /// The payload contains more than one CBOR value.
    TrailingBytes,
    /// The top-level payload is not an array.
    MessageNotArray,
    /// An action element is not an array.
    ActionNotArray,
    /// An action has the wrong number of elements.
    InvalidActionLength,
    /// An action opcode is unknown or malformed.
    UnknownAction,
    /// A CBOR value cannot be represented as a legal JSON value.
    InvalidJsonValue,
    /// A path has an invalid segment or invalid container operation.
    InvalidPath,
    /// A path refers to a missing object key.
    PathParentMissing,
    /// A path traverses a scalar instead of an object or array.
    PathParentNotContainer,
    /// An array insertion index is greater than the array length.
    ArrayIndexOutOfBounds,
    /// Applying an action failed for a general reason.
    ApplyFailed,
    /// The first successfully consumed message did not start with SNAPSHOT.
    InitialSnapshotRequired,
}

/// Describes a Jsync consumption failure with structured metadata and context.
#[derive(Debug)]
pub struct JsyncError {
    /// The machine-readable category of this error.
    pub kind: JsyncErrorKind,
    /// Key-value details associated with the error.
    pub metadata: HashMap<String, String>,
    /// Human-readable locations where the error occurred, from inner to outer.
    pub context: Vec<String>,
    /// The human-readable explanation of the error.
    pub message: String,
    /// The underlying error, when this error wraps another library or operation.
    pub source: Option<anyhow::Error>,
}

impl JsyncError {
    /// Creates an error with a kind and human-readable message.
    pub fn new(kind: JsyncErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            metadata: HashMap::new(),
            context: Vec::new(),
            message: message.into(),
            source: None,
        }
    }

    /// Adds a key-value detail and returns the error for fluent construction.
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Appends a human-readable error context and returns the error for fluent construction.
    pub fn with_context(mut self, context: impl Into<String>) -> Self {
        self.context.push(context.into());
        self
    }

    /// Attaches an underlying error and returns the error for fluent construction.
    pub fn with_source(mut self, source: anyhow::Error) -> Self {
        self.source = Some(source);
        self
    }
}

impl fmt::Display for JsyncError {
    /// Renders context, kind, metadata, and the optional source error.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for context in self.context.iter().rev() {
            write!(f, "{context}: ")?;
        }
        write!(f, "({:?}) {}", self.kind, self.message)?;

        if !self.metadata.is_empty() {
            let mut entries = self.metadata.iter().collect::<Vec<_>>();
            entries.sort_unstable_by_key(|(key, _)| *key);
            write!(f, " (")?;
            for (index, (key, value)) in entries.into_iter().enumerate() {
                if index > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{key}={value}")?;
            }
            write!(f, ")")?;
        }

        if let Some(source) = &self.source {
            write!(f, " Source: {source:#}")?;
        }
        Ok(())
    }
}

impl Error for JsyncError {
    /// Returns the wrapped source error, when one is attached.
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source.as_ref().map(|source| source.as_ref())
    }
}
