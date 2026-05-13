use crate::{RelationId, RuleEvalError, RuleSet, Transaction, Tuple};
use std::collections::BTreeMap;

pub fn materialize_rule_set(
    tx: &mut Transaction<'_>,
    rules: &RuleSet,
) -> Result<BTreeMap<RelationId, Vec<Tuple>>, RuleEvalError> {
    let derived = rules.evaluate_fixpoint(tx)?;
    for (relation, tuples) in &derived {
        tx.reconcile_relation(*relation, tuples.iter().cloned())?;
    }
    Ok(derived)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Atom, RelationKernel, RelationMetadata, Term};
    use mica_var::{Identity, Symbol, Value};

    fn rel(id: u64) -> RelationId {
        Identity::new(id).unwrap()
    }

    fn int(value: i64) -> Value {
        Value::int(value).unwrap()
    }

    fn var(name: &str) -> Term {
        Term::Var(Symbol::intern(name))
    }

    #[test]
    fn materialized_rule_reconciles_target_relation_in_transaction() {
        let kernel = RelationKernel::new();
        kernel
            .create_relation(RelationMetadata::new(
                rel(80),
                Symbol::intern("LocatedIn"),
                2,
            ))
            .unwrap();
        kernel
            .create_relation(RelationMetadata::new(
                rel(81),
                Symbol::intern("VisibleObject"),
                1,
            ))
            .unwrap();

        let visible = crate::Rule::new(
            rel(81),
            [var("obj")],
            [Atom::positive(rel(80), [var("obj"), var("room")])],
        );
        let rules = RuleSet::new([visible]);

        let mut seed = kernel.begin();
        seed.assert(rel(80), Tuple::from([int(1), int(10)]))
            .unwrap();
        seed.assert(rel(80), Tuple::from([int(2), int(10)]))
            .unwrap();
        materialize_rule_set(&mut seed, &rules).unwrap();
        seed.commit().unwrap();
        assert_eq!(
            kernel.snapshot().scan(rel(81), &[None]).unwrap(),
            vec![Tuple::from([int(1)]), Tuple::from([int(2)])]
        );

        let mut tx = kernel.begin();
        tx.retract(rel(80), Tuple::from([int(1), int(10)])).unwrap();
        materialize_rule_set(&mut tx, &rules).unwrap();
        tx.commit().unwrap();
        assert_eq!(
            kernel.snapshot().scan(rel(81), &[None]).unwrap(),
            vec![Tuple::from([int(2)])]
        );
    }

    #[test]
    fn materialized_rule_reconciles_recursive_fixpoint() {
        let kernel = RelationKernel::new();
        kernel
            .create_relation(RelationMetadata::new(rel(90), Symbol::intern("Edge"), 2))
            .unwrap();
        kernel
            .create_relation(RelationMetadata::new(
                rel(91),
                Symbol::intern("Reachable"),
                2,
            ))
            .unwrap();

        let reachable_base = crate::Rule::new(
            rel(91),
            [var("a"), var("b")],
            [Atom::positive(rel(90), [var("a"), var("b")])],
        );
        let reachable_step = crate::Rule::new(
            rel(91),
            [var("a"), var("c")],
            [
                Atom::positive(rel(90), [var("a"), var("b")]),
                Atom::positive(rel(91), [var("b"), var("c")]),
            ],
        );
        let rules = RuleSet::new([reachable_base, reachable_step]);

        let mut tx = kernel.begin();
        tx.assert(rel(90), Tuple::from([int(1), int(2)])).unwrap();
        tx.assert(rel(90), Tuple::from([int(2), int(3)])).unwrap();
        materialize_rule_set(&mut tx, &rules).unwrap();
        tx.commit().unwrap();

        assert_eq!(
            kernel.snapshot().scan(rel(91), &[None, None]).unwrap(),
            vec![
                Tuple::from([int(1), int(2)]),
                Tuple::from([int(1), int(3)]),
                Tuple::from([int(2), int(3)]),
            ]
        );
    }
}
