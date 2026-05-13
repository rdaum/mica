use crate::{FactId, RelationId, Tuple};

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Fact {
    id: FactId,
    relation: RelationId,
    tuple: Tuple,
}

impl Fact {
    pub fn new(id: FactId, relation: RelationId, tuple: Tuple) -> Self {
        Self {
            id,
            relation,
            tuple,
        }
    }

    pub fn id(&self) -> FactId {
        self.id
    }

    pub fn relation(&self) -> RelationId {
        self.relation
    }

    pub fn tuple(&self) -> &Tuple {
        &self.tuple
    }

    pub fn into_tuple(self) -> Tuple {
        self.tuple
    }
}
