use crate::DomainError;

/// Dense vector owned by a Ketebe record.
#[derive(Debug, Clone, PartialEq)]
pub struct Vector(Vec<f32>);

impl Vector {
    pub fn new(values: Vec<f32>) -> Result<Self, DomainError> {
        for (index, value) in values.iter().enumerate() {
            if !value.is_finite() {
                return Err(DomainError::NonFiniteVectorValue { index });
            }
        }
        Ok(Self(values))
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[must_use]
    pub fn as_slice(&self) -> &[f32] {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finite_vector_is_valid() {
        let vector = Vector::new(vec![1.0, -2.5, 0.0]).expect("finite values are valid");
        assert_eq!(vector.len(), 3);
    }

    #[test]
    fn nan_is_rejected() {
        assert_eq!(
            Vector::new(vec![1.0, f32::NAN]).expect_err("NaN must fail"),
            DomainError::NonFiniteVectorValue { index: 1 }
        );
    }

    #[test]
    fn infinity_is_rejected() {
        assert_eq!(
            Vector::new(vec![f32::INFINITY]).expect_err("infinity must fail"),
            DomainError::NonFiniteVectorValue { index: 0 }
        );
    }
}
