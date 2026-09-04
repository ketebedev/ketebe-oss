use crate::DomainError;
use std::fmt;

const MAX_COLLECTION_ID_LEN: usize = 128;

/// Stable identifier for a collection.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CollectionId(String);

impl CollectionId {
    /// Creates a validated collection identifier.
    ///
    /// v0 accepts lowercase ASCII letters, digits, `-`, and `_`.
    pub fn new(value: impl Into<String>) -> Result<Self, DomainError> {
        let value = value.into();
        if value.is_empty() {
            return Err(DomainError::EmptyCollectionId);
        }
        if value.len() > MAX_COLLECTION_ID_LEN {
            return Err(DomainError::CollectionIdTooLong {
                max: MAX_COLLECTION_ID_LEN,
                actual: value.len(),
            });
        }

        for (index, character) in value.char_indices() {
            let valid = character.is_ascii_lowercase()
                || character.is_ascii_digit()
                || character == '-'
                || character == '_';
            if !valid {
                return Err(DomainError::InvalidCollectionIdCharacter { character, index });
            }
        }

        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for CollectionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// User-visible record identifier. String and numeric IDs are distinct namespaces.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RecordId {
    String(String),
    Unsigned(u64),
}

impl RecordId {
    pub fn string(value: impl Into<String>) -> Result<Self, DomainError> {
        let value = value.into();
        if value.is_empty() {
            return Err(DomainError::EmptyRecordId);
        }
        Ok(Self::String(value))
    }

    #[must_use]
    pub const fn unsigned(value: u64) -> Self {
        Self::Unsigned(value)
    }
}

/// Monotonically increasing logical mutation position within a collection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SequenceNumber(u64);

impl SequenceNumber {
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collection_id_accepts_supported_characters() {
        let id = CollectionId::new("traffic_vectors-1").expect("valid collection id");
        assert_eq!(id.as_str(), "traffic_vectors-1");
    }

    #[test]
    fn collection_id_rejects_invalid_characters() {
        let error = CollectionId::new("Traffic").expect_err("uppercase must be rejected");
        assert!(matches!(
            error,
            DomainError::InvalidCollectionIdCharacter { .. }
        ));
    }

    #[test]
    fn string_and_numeric_record_ids_are_distinct() {
        let string = RecordId::string("42").expect("valid id");
        let numeric = RecordId::unsigned(42);
        assert_ne!(string, numeric);
    }

    #[test]
    fn empty_string_record_id_is_rejected() {
        assert_eq!(
            RecordId::string("").expect_err("empty id must fail"),
            DomainError::EmptyRecordId
        );
    }

    #[test]
    fn sequence_numbers_preserve_order() {
        assert!(SequenceNumber::new(2) > SequenceNumber::new(1));
    }
}
