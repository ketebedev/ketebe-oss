use std::collections::BTreeMap;

/// JSON-like metadata value without imposing a serialization dependency on `ketebe-core`.
#[derive(Debug, Clone, PartialEq)]
pub enum MetadataValue {
    Null,
    Bool(bool),
    Number(f64),
    String(String),
    Array(Vec<MetadataValue>),
    Object(BTreeMap<String, MetadataValue>),
}

/// Metadata attached to a record.
pub type Metadata = BTreeMap<String, MetadataValue>;
