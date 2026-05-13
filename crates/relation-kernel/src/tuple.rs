use mica_var::Value;
use std::sync::Arc;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Tuple(pub(crate) Arc<[Value]>);

impl Tuple {
    pub fn new(values: impl IntoIterator<Item = Value>) -> Self {
        Self(values.into_iter().collect::<Vec<_>>().into())
    }

    pub fn values(&self) -> &[Value] {
        &self.0
    }

    pub fn arity(&self) -> usize {
        self.0.len()
    }

    pub(crate) fn project(&self, positions: &[u16]) -> TupleKey {
        TupleKey(
            positions
                .iter()
                .map(|position| self.0[*position as usize].clone())
                .collect(),
        )
    }

    pub fn select(&self, positions: impl IntoIterator<Item = u16>) -> Self {
        Self::new(
            positions
                .into_iter()
                .map(|position| self.0[position as usize].clone()),
        )
    }

    pub fn concat(&self, other: &Tuple) -> Self {
        Self::new(self.0.iter().cloned().chain(other.0.iter().cloned()))
    }

    pub(crate) fn matches_bindings(&self, bindings: &[Option<Value>]) -> bool {
        bindings
            .iter()
            .enumerate()
            .all(|(index, binding)| binding.as_ref().is_none_or(|value| &self.0[index] == value))
    }
}

impl<const N: usize> From<[Value; N]> for Tuple {
    fn from(value: [Value; N]) -> Self {
        Self::new(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub(crate) struct TupleKey(pub(crate) Vec<Value>);
