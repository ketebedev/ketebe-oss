use crate::{Metadata, RecordId, SequenceNumber, Vector};

/// User-visible logical record stored in a collection.
#[derive(Debug, Clone, PartialEq)]
pub struct Record {
    id: RecordId,
    vector: Vector,
    metadata: Metadata,
    sequence_number: SequenceNumber,
}

impl Record {
    #[must_use]
    pub fn new(
        id: RecordId,
        vector: Vector,
        metadata: Metadata,
        sequence_number: SequenceNumber,
    ) -> Self {
        Self {
            id,
            vector,
            metadata,
            sequence_number,
        }
    }

    #[must_use]
    pub fn id(&self) -> &RecordId {
        &self.id
    }

    #[must_use]
    pub fn vector(&self) -> &Vector {
        &self.vector
    }

    #[must_use]
    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    #[must_use]
    pub const fn sequence_number(&self) -> SequenceNumber {
        self.sequence_number
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_preserves_domain_values() {
        let id = RecordId::string("record-1").expect("valid id");
        let vector = Vector::new(vec![1.0, 2.0]).expect("valid vector");
        let sequence = SequenceNumber::new(7);
        let record = Record::new(id.clone(), vector.clone(), Default::default(), sequence);

        assert_eq!(record.id(), &id);
        assert_eq!(record.vector(), &vector);
        assert_eq!(record.sequence_number(), sequence);
        assert!(record.metadata().is_empty());
    }
}
