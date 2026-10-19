//! Standalone log event model, nested field-path lookup, and delivery acknowledgment finalizers.

use std::borrow::Cow;
use std::fmt;
use std::str::FromStr;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use tokio::sync::oneshot;

pub use serde_json::Value;

/// JSON object map backing a [`LogEvent`].
pub type ObjectMap = serde_json::Map<String, Value>;

/// Delivery status reported to upstream acknowledgment receivers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
#[repr(u8)]
pub enum AckStatus {
    /// Event or batch was durably flushed or delivered downstream.
    #[default]
    Delivered = 0,
    /// Event or batch encountered a transient or storage error.
    Errored = 1,
    /// Event or batch was permanently rejected downstream.
    Rejected = 2,
}

impl AckStatus {
    #[must_use]
    const fn from_u8(val: u8) -> Self {
        match val {
            0 => Self::Delivered,
            1 => Self::Errored,
            _ => Self::Rejected,
        }
    }
}

#[derive(Debug)]
struct AckInner {
    status: AtomicU8,
    tx: Mutex<Option<oneshot::Sender<AckStatus>>>,
}

impl Drop for AckInner {
    fn drop(&mut self) {
        let sender = match self.tx.lock() {
            Ok(mut guard) => guard.take(),
            Err(poisoned) => poisoned.into_inner().take(),
        };
        if let Some(tx) = sender {
            let raw = self.status.load(Ordering::Acquire);
            let _ = tx.send(AckStatus::from_u8(raw));
        }
    }
}

/// Factory for creating paired [`AckFinalizer`] handles and [`oneshot::Receiver`] channels.
#[derive(Debug, Default)]
pub struct AckNotifier;

impl AckNotifier {
    /// Creates a new [`AckFinalizer`] and a oneshot receiver that resolves when all clones drop.
    #[must_use]
    pub fn new_with_receiver() -> (AckFinalizer, oneshot::Receiver<AckStatus>) {
        let (tx, rx) = oneshot::channel();
        let inner = Arc::new(AckInner {
            status: AtomicU8::new(AckStatus::Delivered as u8),
            tx: Mutex::new(Some(tx)),
        });
        (AckFinalizer { inner }, rx)
    }
}

/// Reference-counted acknowledgment token that notifies its receiver when the last clone drops.
#[derive(Debug, Clone)]
pub struct AckFinalizer {
    inner: Arc<AckInner>,
}

impl AckFinalizer {
    /// Escalates the recorded delivery status using atomic maximum semantics (`Delivered < Errored < Rejected`).
    pub fn update_status(&self, status: AckStatus) {
        self.inner.status.fetch_max(status as u8, Ordering::AcqRel);
    }
}

impl PartialEq for AckFinalizer {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

impl Eq for AckFinalizer {}

/// Collection of [`AckFinalizer`] tokens attached to an event or micro-batch.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AckFinalizers(Vec<AckFinalizer>);

impl AckFinalizers {
    /// Creates an empty finalizer set.
    #[must_use]
    pub fn new() -> Self {
        Self(Vec::new())
    }

    /// Creates a finalizer set containing a single [`AckFinalizer`].
    #[must_use]
    pub fn from_single(finalizer: AckFinalizer) -> Self {
        Self(vec![finalizer])
    }

    /// Appends a single [`AckFinalizer`] to this collection.
    pub fn push(&mut self, finalizer: AckFinalizer) {
        self.0.push(finalizer);
    }

    /// Merges all finalizers from `other` into `self`.
    pub fn merge(&mut self, other: Self) {
        self.0.extend(other.0);
    }

    /// Updates the delivery status on all contained finalizers.
    pub fn update_status(&self, status: AckStatus) {
        for finalizer in &self.0 {
            finalizer.update_status(status);
        }
    }

    /// Returns `true` if no finalizers are attached.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// Parsed dot-separated path into a nested [`LogEvent`] map.
///
/// Supports optional leading `.`, quoted segments (`resource."service.name"`),
/// and backslash-escaped dots (`resource.service\.name`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FieldPath {
    raw: String,
    segments: Vec<String>,
}

impl FieldPath {
    /// Parses a raw field path string into segments.
    ///
    /// # Errors
    /// Returns an error description if the path is empty, has empty unquoted segments,
    /// or contains unterminated quotes or trailing escape characters.
    pub fn parse(raw: &str) -> Result<Self, String> {
        let stripped = raw.strip_prefix('.').unwrap_or(raw);
        if stripped.is_empty() {
            return Err("field path cannot be empty".to_string());
        }

        let mut segments = Vec::new();
        let mut current = String::new();
        let mut in_quotes = false;
        let mut was_quoted = false;
        let mut chars = stripped.chars();

        while let Some(ch) = chars.next() {
            if in_quotes {
                match ch {
                    '\\' => {
                        let next_ch = chars.next().ok_or_else(|| {
                            "trailing backslash inside quoted segment".to_string()
                        })?;
                        current.push(next_ch);
                    }
                    '"' => {
                        in_quotes = false;
                    }
                    other => current.push(other),
                }
            } else {
                match ch {
                    '"' => {
                        in_quotes = true;
                        was_quoted = true;
                    }
                    '\\' => {
                        let next_ch = chars
                            .next()
                            .ok_or_else(|| "trailing backslash in field path".to_string())?;
                        current.push(next_ch);
                    }
                    '.' => {
                        if current.is_empty() && !was_quoted {
                            return Err(format!("empty segment in field path '{raw}'"));
                        }
                        segments.push(std::mem::take(&mut current));
                        was_quoted = false;
                    }
                    other => current.push(other),
                }
            }
        }

        if in_quotes {
            return Err(format!("unterminated quote in field path '{raw}'"));
        }
        if current.is_empty() && !was_quoted {
            return Err(format!("empty trailing segment in field path '{raw}'"));
        }
        segments.push(current);

        Ok(Self {
            raw: raw.to_string(),
            segments,
        })
    }

    /// Parses `raw` as a [`FieldPath`], falling back to a single literal segment if invalid.
    #[must_use]
    pub fn parse_or_literal(raw: &str) -> Self {
        Self::parse(raw).unwrap_or_else(|_| Self {
            raw: raw.to_string(),
            segments: vec![raw.to_string()],
        })
    }

    /// Returns the parsed path segments.
    #[must_use]
    pub fn segments(&self) -> &[String] {
        &self.segments
    }

    /// Returns the original raw path string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.raw
    }
}

impl fmt::Display for FieldPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.raw)
    }
}

impl FromStr for FieldPath {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl Serialize for FieldPath {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.raw)
    }
}

impl<'de> Deserialize<'de> for FieldPath {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        Self::parse(&raw).map_err(serde::de::Error::custom)
    }
}

/// Structured log event backed by a JSON [`ObjectMap`] and [`AckFinalizers`].
#[derive(Debug, Clone, Default, PartialEq)]
pub struct LogEvent {
    fields: ObjectMap,
    finalizers: AckFinalizers,
}

impl LogEvent {
    /// Creates an empty [`LogEvent`].
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a [`LogEvent`] from an existing [`ObjectMap`].
    #[must_use]
    pub fn from_map(fields: ObjectMap) -> Self {
        Self {
            fields,
            finalizers: AckFinalizers::new(),
        }
    }

    /// Creates a [`LogEvent`] from a JSON [`Value`] if it is a `Value::Object`.
    #[must_use]
    pub fn from_value(val: Value) -> Option<Self> {
        match val {
            Value::Object(map) => Some(Self::from_map(map)),
            _ => None,
        }
    }

    /// Returns an immutable reference to the root field map.
    #[must_use]
    pub fn as_map(&self) -> &ObjectMap {
        &self.fields
    }

    /// Returns a mutable reference to the root field map.
    pub fn as_map_mut(&mut self) -> &mut ObjectMap {
        &mut self.fields
    }

    /// Consumes the event and returns its underlying field map and finalizers.
    #[must_use]
    pub fn into_parts(self) -> (ObjectMap, AckFinalizers) {
        (self.fields, self.finalizers)
    }

    /// Looks up a nested field value by [`FieldPath`].
    #[must_use]
    pub fn get(&self, path: &FieldPath) -> Option<&Value> {
        let (first, rest) = path.segments.split_first()?;
        let mut current = self.fields.get(first.as_str())?;
        for seg in rest {
            match current {
                Value::Object(map) => {
                    current = map.get(seg.as_str())?;
                }
                _ => return None,
            }
        }
        Some(current)
    }

    /// Looks up a mutable nested field value by [`FieldPath`].
    pub fn get_mut(&mut self, path: &FieldPath) -> Option<&mut Value> {
        let (first, rest) = path.segments.split_first()?;
        let mut current = self.fields.get_mut(first.as_str())?;
        for seg in rest {
            match current {
                Value::Object(map) => {
                    current = map.get_mut(seg.as_str())?;
                }
                _ => return None,
            }
        }
        Some(current)
    }

    /// Inserts a value at `path`, auto-vivifying intermediate [`Value::Object`] maps.
    pub fn insert(&mut self, path: &FieldPath, val: Value) -> Option<Value> {
        let (last, prefix) = path.segments.split_last()?;
        let mut current = &mut self.fields;
        for seg in prefix {
            let entry = current
                .entry(seg.clone())
                .or_insert_with(|| Value::Object(ObjectMap::new()));
            if !matches!(entry, Value::Object(_)) {
                *entry = Value::Object(ObjectMap::new());
            }
            let Value::Object(map) = entry else {
                return None;
            };
            current = map;
        }
        current.insert(last.clone(), val)
    }

    /// Removes and returns the value at `path`, if present.
    pub fn remove(&mut self, path: &FieldPath) -> Option<Value> {
        let (last, prefix) = path.segments.split_last()?;
        let mut current = &mut self.fields;
        for seg in prefix {
            match current.get_mut(seg.as_str()) {
                Some(Value::Object(map)) => current = map,
                _ => return None,
            }
        }
        current.remove(last.as_str())
    }

    /// Parses `raw_path` and looks up the corresponding value.
    #[must_use]
    pub fn get_path(&self, raw_path: &str) -> Option<&Value> {
        let path = FieldPath::parse(raw_path).ok()?;
        self.get(&path)
    }

    /// Parses `raw_path` and inserts `val` if the path is valid.
    pub fn insert_path(&mut self, raw_path: &str, val: impl Into<Value>) {
        if let Ok(path) = FieldPath::parse(raw_path) {
            let _ = self.insert(&path, val.into());
        }
    }

    /// Parses `raw_path` and removes the corresponding value.
    pub fn remove_path(&mut self, raw_path: &str) -> Option<Value> {
        let path = FieldPath::parse(raw_path).ok()?;
        self.remove(&path)
    }

    /// Takes and returns all attached [`AckFinalizers`], leaving an empty set on the event.
    pub fn take_finalizers(&mut self) -> AckFinalizers {
        std::mem::take(&mut self.finalizers)
    }

    /// Merges additional [`AckFinalizers`] into this event.
    pub fn merge_finalizers(&mut self, other: AckFinalizers) {
        self.finalizers.merge(other);
    }

    /// Returns a reference to the event's [`AckFinalizers`].
    #[must_use]
    pub fn finalizers(&self) -> &AckFinalizers {
        &self.finalizers
    }

    /// Attaches a new [`AckNotifier`] finalizer to this event and returns the event with its receiver.
    #[must_use]
    pub fn with_ack_notifier(mut self) -> (Self, oneshot::Receiver<AckStatus>) {
        let (finalizer, rx) = AckNotifier::new_with_receiver();
        self.finalizers.push(finalizer);
        (self, rx)
    }
}

impl From<ObjectMap> for LogEvent {
    fn from(fields: ObjectMap) -> Self {
        Self::from_map(fields)
    }
}

/// Converts a JSON [`Value`] into an unquoted string for [`Value::String`] or canonical JSON text for other variants.
#[must_use]
pub fn value_to_string_lossy(value: &Value) -> Cow<'_, str> {
    match value {
        Value::String(s) => Cow::Borrowed(s.as_str()),
        Value::Null => Cow::Borrowed("null"),
        Value::Bool(b) => Cow::Borrowed(if *b { "true" } else { "false" }),
        Value::Number(n) => Cow::Owned(n.to_string()),
        Value::Array(_) | Value::Object(_) => Cow::Owned(value.to_string()),
    }
}

/// Converts a JSON [`Value`] into UTF-8 [`Bytes`].
#[must_use]
pub fn value_to_bytes(value: &Value) -> Bytes {
    match value {
        Value::String(s) => Bytes::copy_from_slice(s.as_bytes()),
        _ => Bytes::from(value_to_string_lossy(value).into_owned()),
    }
}
