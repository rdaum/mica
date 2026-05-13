use crate::{ConflictPolicy, Snapshot, Tuple};
use mica_var::{Identity, Symbol, Value};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum CatalogPredicate {
    Relation,
    RelationName,
    Arity,
    ArgumentName,
    ConflictPolicy,
    FunctionalKey,
    Index,
    IndexPosition,
    IndexStorageKind,
}

impl CatalogPredicate {
    pub fn symbol(self) -> Symbol {
        match self {
            Self::Relation => Symbol::intern("Relation"),
            Self::RelationName => Symbol::intern("RelationName"),
            Self::Arity => Symbol::intern("Arity"),
            Self::ArgumentName => Symbol::intern("ArgumentName"),
            Self::ConflictPolicy => Symbol::intern("ConflictPolicy"),
            Self::FunctionalKey => Symbol::intern("FunctionalKey"),
            Self::Index => Symbol::intern("Index"),
            Self::IndexPosition => Symbol::intern("IndexPosition"),
            Self::IndexStorageKind => Symbol::intern("IndexStorageKind"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogFact {
    pub predicate: CatalogPredicate,
    pub tuple: Tuple,
}

impl Snapshot {
    pub fn catalog_facts(&self) -> Vec<CatalogFact> {
        let mut facts = Vec::new();
        for (relation_id, relation) in &self.relations {
            let metadata = relation.metadata();
            facts.push(catalog_fact(
                CatalogPredicate::Relation,
                [Value::identity(*relation_id)],
            ));
            facts.push(catalog_fact(
                CatalogPredicate::RelationName,
                [
                    Value::identity(*relation_id),
                    Value::symbol(metadata.name()),
                ],
            ));
            facts.push(catalog_fact(
                CatalogPredicate::Arity,
                [
                    Value::identity(*relation_id),
                    Value::int(metadata.arity() as i64).unwrap(),
                ],
            ));
            for position in 0..metadata.arity() {
                if let Some(name) = metadata.argument_name(position) {
                    facts.push(catalog_fact(
                        CatalogPredicate::ArgumentName,
                        [
                            Value::identity(*relation_id),
                            Value::int(position as i64).unwrap(),
                            Value::symbol(name),
                        ],
                    ));
                }
            }

            push_conflict_policy_facts(&mut facts, *relation_id, metadata.conflict_policy());
            for (ordinal, index) in metadata.indexes().iter().enumerate() {
                let index_value = Value::identity(index_identity(*relation_id, ordinal as u16));
                facts.push(catalog_fact(
                    CatalogPredicate::Index,
                    [Value::identity(*relation_id), index_value.clone()],
                ));
                facts.push(catalog_fact(
                    CatalogPredicate::IndexStorageKind,
                    [index_value.clone(), Value::symbol(Symbol::intern("btree"))],
                ));
                for (slot, position) in index.positions().iter().enumerate() {
                    facts.push(catalog_fact(
                        CatalogPredicate::IndexPosition,
                        [
                            index_value.clone(),
                            Value::int(slot as i64).unwrap(),
                            Value::int(*position as i64).unwrap(),
                        ],
                    ));
                }
            }
        }
        facts
    }
}

fn push_conflict_policy_facts(
    facts: &mut Vec<CatalogFact>,
    relation_id: Identity,
    conflict_policy: &ConflictPolicy,
) {
    let relation = Value::identity(relation_id);
    match conflict_policy {
        ConflictPolicy::Set => facts.push(catalog_fact(
            CatalogPredicate::ConflictPolicy,
            [relation, Value::symbol(Symbol::intern("set"))],
        )),
        ConflictPolicy::Functional { key_positions } => {
            facts.push(catalog_fact(
                CatalogPredicate::ConflictPolicy,
                [
                    relation.clone(),
                    Value::symbol(Symbol::intern("functional")),
                ],
            ));
            for (slot, position) in key_positions.iter().enumerate() {
                facts.push(catalog_fact(
                    CatalogPredicate::FunctionalKey,
                    [
                        relation.clone(),
                        Value::int(slot as i64).unwrap(),
                        Value::int(*position as i64).unwrap(),
                    ],
                ));
            }
        }
        ConflictPolicy::EventAppend => facts.push(catalog_fact(
            CatalogPredicate::ConflictPolicy,
            [relation, Value::symbol(Symbol::intern("event_append"))],
        )),
    }
}

fn catalog_fact<const N: usize>(predicate: CatalogPredicate, values: [Value; N]) -> CatalogFact {
    CatalogFact {
        predicate,
        tuple: Tuple::from(values),
    }
}

fn index_identity(relation_id: Identity, ordinal: u16) -> Identity {
    let raw = relation_id
        .raw()
        .wrapping_mul(65_537)
        .wrapping_add(ordinal as u64)
        & Identity::MAX;
    Identity::new(raw).unwrap()
}
