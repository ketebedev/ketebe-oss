use std::fmt;

/// Errors produced while constructing or validating Ketebe domain values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DomainError {
    EmptyRecordId,
    EmptyCollectionId,
    InvalidCollectionIdCharacter { character: char, index: usize },
    CollectionIdTooLong { max: usize, actual: usize },
    InvalidDimension { dimension: usize },
    VectorDimensionMismatch { expected: usize, actual: usize },
    NonFiniteVectorValue { index: usize },
    InvalidIngestionConfig { message: String },
}

impl fmt::Display for DomainError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyRecordId => write!(f, "record id must not be empty"),
            Self::EmptyCollectionId => write!(f, "collection id must not be empty"),
            Self::InvalidCollectionIdCharacter { character, index } => write!(
                f,
                "collection id contains invalid character '{character}' at byte index {index}"
            ),
            Self::CollectionIdTooLong { max, actual } => {
                write!(f, "collection id exceeds maximum length {max}: {actual}")
            }
            Self::InvalidDimension { dimension } => {
                write!(f, "vector dimension must be greater than zero: {dimension}")
            }
            Self::VectorDimensionMismatch { expected, actual } => write!(
                f,
                "vector dimension mismatch: expected {expected}, got {actual}"
            ),
            Self::NonFiniteVectorValue { index } => {
                write!(f, "vector contains a non-finite value at index {index}")
            }
            Self::InvalidIngestionConfig { message } => {
                write!(f, "invalid collection ingestion config: {message}")
            }
        }
    }
}

impl std::error::Error for DomainError {}
