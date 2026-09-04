use crate::{Metadata, MetadataValue};
use std::cmp::Ordering;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FieldPath(Vec<String>);

impl FieldPath {
    pub fn new<I, S>(segments: I) -> Result<Self, PredicateError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let segments: Vec<String> = segments.into_iter().map(Into::into).collect();
        if segments.is_empty() {
            return Err(PredicateError::EmptyFieldPath);
        }
        if segments.iter().any(String::is_empty) {
            return Err(PredicateError::EmptyFieldPathSegment);
        }
        Ok(Self(segments))
    }

    #[must_use]
    pub fn segments(&self) -> &[String] {
        &self.0
    }

    fn resolve<'a>(&self, metadata: &'a Metadata) -> Option<&'a MetadataValue> {
        let mut current = metadata.get(&self.0[0])?;
        for segment in &self.0[1..] {
            match current {
                MetadataValue::Object(object) => current = object.get(segment)?,
                _ => return None,
            }
        }
        Some(current)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    Eq(FieldPath, MetadataValue),
    Ne(FieldPath, MetadataValue),
    Lt(FieldPath, MetadataValue),
    Lte(FieldPath, MetadataValue),
    Gt(FieldPath, MetadataValue),
    Gte(FieldPath, MetadataValue),
    Exists(FieldPath),
    In(FieldPath, Vec<MetadataValue>),
    Contains(FieldPath, MetadataValue),
    And(Vec<Predicate>),
    Or(Vec<Predicate>),
    Not(Box<Predicate>),
}

impl Predicate {
    pub fn evaluate(&self, metadata: &Metadata) -> Result<bool, PredicateError> {
        match self {
            Self::Eq(path, expected) => Ok(path
                .resolve(metadata)
                .is_some_and(|value| value == expected)),
            Self::Ne(path, expected) => Ok(path
                .resolve(metadata)
                .is_some_and(|value| value != expected)),
            Self::Lt(path, expected) => compare(path, metadata, expected, |order| order.is_lt()),
            Self::Lte(path, expected) => compare(path, metadata, expected, |order| order.is_le()),
            Self::Gt(path, expected) => compare(path, metadata, expected, |order| order.is_gt()),
            Self::Gte(path, expected) => compare(path, metadata, expected, |order| order.is_ge()),
            Self::Exists(path) => Ok(path.resolve(metadata).is_some()),
            Self::In(path, expected) => Ok(path
                .resolve(metadata)
                .is_some_and(|value| expected.iter().any(|candidate| candidate == value))),
            Self::Contains(path, expected) => {
                Ok(path.resolve(metadata).is_some_and(|value| match value {
                    MetadataValue::Array(values) => {
                        values.iter().any(|candidate| candidate == expected)
                    }
                    _ => false,
                }))
            }
            Self::And(predicates) => {
                for predicate in predicates {
                    if !predicate.evaluate(metadata)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            Self::Or(predicates) => {
                for predicate in predicates {
                    if predicate.evaluate(metadata)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            Self::Not(predicate) => Ok(!predicate.evaluate(metadata)?),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PredicateError {
    EmptyFieldPath,
    EmptyFieldPathSegment,
    InvalidOrderingComparison {
        actual: &'static str,
        expected: &'static str,
    },
}

impl fmt::Display for PredicateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyFieldPath => f.write_str("field path must contain at least one segment"),
            Self::EmptyFieldPathSegment => f.write_str("field path segments must not be empty"),
            Self::InvalidOrderingComparison { actual, expected } => write!(
                f,
                "metadata ordering comparison requires matching comparable types: actual={actual}, expected={expected}"
            ),
        }
    }
}

impl std::error::Error for PredicateError {}

fn compare(
    path: &FieldPath,
    metadata: &Metadata,
    expected: &MetadataValue,
    predicate: impl FnOnce(Ordering) -> bool,
) -> Result<bool, PredicateError> {
    let Some(actual) = path.resolve(metadata) else {
        return Ok(false);
    };
    let order = comparable_order(actual, expected)?;
    Ok(predicate(order))
}

fn comparable_order(
    actual: &MetadataValue,
    expected: &MetadataValue,
) -> Result<Ordering, PredicateError> {
    match (actual, expected) {
        (MetadataValue::Number(left), MetadataValue::Number(right)) => left
            .partial_cmp(right)
            .ok_or(PredicateError::InvalidOrderingComparison {
                actual: "number",
                expected: "number",
            }),
        (MetadataValue::String(left), MetadataValue::String(right)) => Ok(left.cmp(right)),
        _ => Err(PredicateError::InvalidOrderingComparison {
            actual: value_kind(actual),
            expected: value_kind(expected),
        }),
    }
}

const fn value_kind(value: &MetadataValue) -> &'static str {
    match value {
        MetadataValue::Null => "null",
        MetadataValue::Bool(_) => "bool",
        MetadataValue::Number(_) => "number",
        MetadataValue::String(_) => "string",
        MetadataValue::Array(_) => "array",
        MetadataValue::Object(_) => "object",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn path(parts: &[&str]) -> FieldPath {
        FieldPath::new(parts.iter().copied()).expect("path")
    }

    fn metadata() -> Metadata {
        let mut product = BTreeMap::new();
        product.insert("category".to_string(), MetadataValue::String("book".into()));
        let mut metadata = Metadata::new();
        metadata.insert("price".into(), MetadataValue::Number(120.0));
        metadata.insert("active".into(), MetadataValue::Bool(true));
        metadata.insert("nullable".into(), MetadataValue::Null);
        metadata.insert(
            "tags".into(),
            MetadataValue::Array(vec![MetadataValue::String("rust".into())]),
        );
        metadata.insert("product".into(), MetadataValue::Object(product));
        metadata
    }

    #[test]
    fn scalar_and_range_predicates_work() {
        let metadata = metadata();
        assert!(
            Predicate::Eq(path(&["active"]), MetadataValue::Bool(true))
                .evaluate(&metadata)
                .expect("evaluate")
        );
        assert!(
            Predicate::Lt(path(&["price"]), MetadataValue::Number(200.0))
                .evaluate(&metadata)
                .expect("evaluate")
        );
        assert!(
            Predicate::Gte(path(&["price"]), MetadataValue::Number(120.0))
                .evaluate(&metadata)
                .expect("evaluate")
        );
    }

    #[test]
    fn nested_exists_membership_and_boolean_composition_work() {
        let metadata = metadata();
        let predicate = Predicate::And(vec![
            Predicate::Eq(
                path(&["product", "category"]),
                MetadataValue::String("book".into()),
            ),
            Predicate::Contains(path(&["tags"]), MetadataValue::String("rust".into())),
            Predicate::Not(Box::new(Predicate::Exists(path(&["missing"])))),
        ]);
        assert!(predicate.evaluate(&metadata).expect("evaluate"));
        assert!(
            Predicate::In(
                path(&["product", "category"]),
                vec![
                    MetadataValue::String("book".into()),
                    MetadataValue::String("game".into())
                ]
            )
            .evaluate(&metadata)
            .expect("evaluate")
        );
    }

    #[test]
    fn missing_fields_are_false_except_negated_exists() {
        let metadata = metadata();
        assert!(
            !Predicate::Eq(path(&["missing"]), MetadataValue::Null)
                .evaluate(&metadata)
                .expect("evaluate")
        );
        assert!(
            !Predicate::Exists(path(&["missing"]))
                .evaluate(&metadata)
                .expect("evaluate")
        );
    }

    #[test]
    fn invalid_cross_type_ordering_is_typed_error() {
        let error = Predicate::Lt(path(&["price"]), MetadataValue::String("200".into()))
            .evaluate(&metadata())
            .expect_err("type mismatch");
        assert!(matches!(
            error,
            PredicateError::InvalidOrderingComparison { .. }
        ));
    }
}
