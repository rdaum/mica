use crate::{KernelError, QueryPlan, RelationId, RelationRead, Transaction, Tuple};
use mica_var::Value;
use std::collections::BTreeSet;

pub fn delegates_star(
    reader: &impl RelationRead,
    delegates_relation: RelationId,
) -> Result<Vec<Tuple>, KernelError> {
    let edges = QueryPlan::scan(delegates_relation, [None, None, None]).execute(reader)?;
    let children = edges
        .iter()
        .map(|edge| edge.values()[0].clone())
        .collect::<BTreeSet<_>>();

    let mut closure = BTreeSet::new();
    for child in children {
        for proto in delegates_star_from(reader, delegates_relation, &child)? {
            closure.insert(Tuple::from([child.clone(), proto]));
        }
    }
    Ok(closure.into_iter().collect())
}

pub fn delegates_star_from(
    reader: &impl RelationRead,
    delegates_relation: RelationId,
    child: &Value,
) -> Result<Vec<Value>, KernelError> {
    let mut seen = BTreeSet::new();
    let mut frontier = vec![child.clone()];

    while let Some(current) = frontier.pop() {
        for edge in
            QueryPlan::scan(delegates_relation, [Some(current), None, None]).execute(reader)?
        {
            let proto = edge.values()[1].clone();
            if seen.insert(proto.clone()) {
                frontier.push(proto);
            }
        }
    }

    Ok(seen.into_iter().collect())
}

pub fn materialize_delegates_star(
    tx: &mut Transaction<'_>,
    delegates_relation: RelationId,
    delegates_star_relation: RelationId,
) -> Result<Vec<Tuple>, KernelError> {
    let closure = delegates_star(tx, delegates_relation)?;
    tx.reconcile_relation(delegates_star_relation, closure.iter().cloned())?;
    Ok(closure)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RelationKernel, RelationMetadata};
    use mica_var::{Identity, Symbol, Value};

    fn rel(id: u64) -> RelationId {
        Identity::new(id).unwrap()
    }

    fn int(value: i64) -> Value {
        Value::int(value).unwrap()
    }

    fn kernel_with_delegates() -> RelationKernel {
        let kernel = RelationKernel::new();
        kernel
            .create_relation(
                RelationMetadata::new(rel(30), Symbol::intern("Delegates"), 3)
                    .with_index([0, 2, 1]),
            )
            .unwrap();
        kernel
            .create_relation(RelationMetadata::new(
                rel(31),
                Symbol::intern("DelegatesStar"),
                2,
            ))
            .unwrap();
        kernel
    }

    #[test]
    fn delegates_star_finds_transitive_prototypes() {
        let kernel = kernel_with_delegates();
        let mut tx = kernel.begin();
        tx.assert(rel(30), Tuple::from([int(1), int(2), int(0)]))
            .unwrap();
        tx.assert(rel(30), Tuple::from([int(2), int(3), int(0)]))
            .unwrap();
        tx.assert(rel(30), Tuple::from([int(4), int(5), int(0)]))
            .unwrap();
        tx.commit().unwrap();

        assert_eq!(
            delegates_star(&*kernel.snapshot(), rel(30)).unwrap(),
            vec![
                Tuple::from([int(1), int(2)]),
                Tuple::from([int(1), int(3)]),
                Tuple::from([int(2), int(3)]),
                Tuple::from([int(4), int(5)]),
            ]
        );
    }

    #[test]
    fn delegates_star_reads_transaction_overlay() {
        let kernel = kernel_with_delegates();
        let mut tx = kernel.begin();
        tx.assert(rel(30), Tuple::from([int(1), int(2), int(0)]))
            .unwrap();
        tx.assert(rel(30), Tuple::from([int(2), int(3), int(0)]))
            .unwrap();

        assert_eq!(
            delegates_star_from(&tx, rel(30), &int(1)).unwrap(),
            vec![int(2), int(3)]
        );
    }

    #[test]
    fn materialized_delegates_star_reconciles_output_relation() {
        let kernel = kernel_with_delegates();
        let mut seed = kernel.begin();
        seed.assert(rel(30), Tuple::from([int(1), int(2), int(0)]))
            .unwrap();
        seed.assert(rel(30), Tuple::from([int(2), int(3), int(0)]))
            .unwrap();
        materialize_delegates_star(&mut seed, rel(30), rel(31)).unwrap();
        seed.commit().unwrap();

        assert_eq!(
            kernel.snapshot().scan(rel(31), &[None, None]).unwrap(),
            vec![
                Tuple::from([int(1), int(2)]),
                Tuple::from([int(1), int(3)]),
                Tuple::from([int(2), int(3)]),
            ]
        );

        let mut tx = kernel.begin();
        tx.retract(rel(30), Tuple::from([int(2), int(3), int(0)]))
            .unwrap();
        materialize_delegates_star(&mut tx, rel(30), rel(31)).unwrap();
        tx.commit().unwrap();

        assert_eq!(
            kernel.snapshot().scan(rel(31), &[None, None]).unwrap(),
            vec![Tuple::from([int(1), int(2)])]
        );
    }
}
