// Copyright (C) 2026 Ryan Daum <ryan.daum@gmail.com> This program is free
// software: you can redistribute it and/or modify it under the terms of the GNU
// Affero General Public License as published by the Free Software Foundation,
// version 3.
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
// FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
// details.
//
// You should have received a copy of the GNU Affero General Public License along
// with this program. If not, see <https://www.gnu.org/licenses/>.

use crate::{KernelError, QueryPlan, RelationId, RelationRead, delegates_star_from};
use mica_var::{Value, primitive_prototype_for_value};
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DispatchRelations {
    pub method_selector: RelationId,
    pub param: RelationId,
    pub delegates: RelationId,
}

pub fn applicable_methods(
    reader: &impl RelationRead,
    relations: DispatchRelations,
    selector: Value,
    roles: impl IntoIterator<Item = (Value, Value)>,
) -> Result<Vec<Value>, KernelError> {
    let role_env = roles.into_iter().collect::<BTreeMap<_, _>>();
    let selector_rows =
        QueryPlan::scan(relations.method_selector, [None, Some(selector)]).execute(reader)?;
    let mut methods = Vec::new();

    for row in selector_rows {
        let method = row.values()[0].clone();
        let params = QueryPlan::scan(relations.param, [Some(method.clone()), None, None, None])
            .execute(reader)?;
        if params_match(reader, relations.delegates, &role_env, &params)? {
            methods.push(method);
        }
    }

    methods.sort();
    methods.dedup();
    Ok(methods)
}

pub fn applicable_positional_methods(
    reader: &impl RelationRead,
    relations: DispatchRelations,
    selector: Value,
    args: &[Value],
) -> Result<Vec<Value>, KernelError> {
    let selector_rows =
        QueryPlan::scan(relations.method_selector, [None, Some(selector)]).execute(reader)?;
    let mut methods = Vec::new();

    for row in selector_rows {
        let method = row.values()[0].clone();
        let params = QueryPlan::scan(relations.param, [Some(method.clone()), None, None, None])
            .execute(reader)?;
        if positional_params_match(reader, relations.delegates, args, &params)? {
            methods.push(method);
        }
    }

    methods.sort();
    methods.dedup();
    Ok(methods)
}

fn params_match(
    reader: &impl RelationRead,
    delegates_relation: RelationId,
    role_env: &BTreeMap<Value, Value>,
    params: &[crate::Tuple],
) -> Result<bool, KernelError> {
    for param in params {
        let role = &param.values()[1];
        let restriction = &param.values()[2];
        let Some(value) = role_env.get(role) else {
            return Ok(false);
        };
        if !matches_restriction(reader, delegates_relation, value, restriction)? {
            return Ok(false);
        }
    }
    Ok(true)
}

pub fn positional_method_args(
    params: &[crate::Tuple],
    args: &[Value],
) -> Option<Vec<(Value, Value)>> {
    let mut params = ordered_params(params)?;
    if params.len() != args.len() {
        return None;
    }
    Some(
        params
            .drain(..)
            .zip(args)
            .map(|(param, value)| (param.values()[1].clone(), value.clone()))
            .collect(),
    )
}

pub fn ordered_params(params: &[crate::Tuple]) -> Option<Vec<crate::Tuple>> {
    let mut params = params.to_vec();
    params.sort_by_key(|param| param_position(param).unwrap_or(u16::MAX));
    if params.iter().any(|param| param_position(param).is_none()) {
        return None;
    }
    Some(params)
}

fn positional_params_match(
    reader: &impl RelationRead,
    delegates_relation: RelationId,
    args: &[Value],
    params: &[crate::Tuple],
) -> Result<bool, KernelError> {
    let Some(params) = ordered_params(params) else {
        return Ok(false);
    };
    if params.len() != args.len() {
        return Ok(false);
    }
    for (param, value) in params.iter().zip(args) {
        if !matches_restriction(reader, delegates_relation, value, &param.values()[2])? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn param_position(param: &crate::Tuple) -> Option<u16> {
    let raw = param.values().get(3)?.as_int()?;
    u16::try_from(raw).ok()
}

fn matches_restriction(
    reader: &impl RelationRead,
    delegates_relation: RelationId,
    value: &Value,
    restriction: &Value,
) -> Result<bool, KernelError> {
    if *restriction == Value::nothing() {
        return Ok(true);
    }
    if value == restriction {
        return Ok(true);
    }

    if delegates_star_from(reader, delegates_relation, value)?
        .iter()
        .any(|proto| proto == restriction)
    {
        return Ok(true);
    }

    let prototype = Value::identity(primitive_prototype_for_value(value));
    if &prototype == restriction {
        return Ok(true);
    }
    Ok(delegates_star_from(reader, delegates_relation, &prototype)?
        .iter()
        .any(|proto| proto == restriction))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RelationKernel, RelationMetadata, Tuple};
    use mica_var::{Identity, STRING_PROTOTYPE, Symbol, Value};

    fn rel(id: u64) -> RelationId {
        Identity::new(id).unwrap()
    }

    fn int(value: i64) -> Value {
        Value::int(value).unwrap()
    }

    fn sym(name: &str) -> Value {
        Value::symbol(Symbol::intern(name))
    }

    fn kernel_with_dispatch_relations() -> RelationKernel {
        let kernel = RelationKernel::new();
        kernel
            .create_relation(
                RelationMetadata::new(rel(40), Symbol::intern("MethodSelector"), 2)
                    .with_index([1, 0]),
            )
            .unwrap();
        kernel
            .create_relation(
                RelationMetadata::new(rel(41), Symbol::intern("Param"), 4).with_index([0, 1]),
            )
            .unwrap();
        kernel
            .create_relation(
                RelationMetadata::new(rel(42), Symbol::intern("Delegates"), 3)
                    .with_index([0, 2, 1]),
            )
            .unwrap();
        kernel
    }

    fn dispatch_relations() -> DispatchRelations {
        DispatchRelations {
            method_selector: rel(40),
            param: rel(41),
            delegates: rel(42),
        }
    }

    #[test]
    fn dispatch_matches_method_params_through_delegation() {
        let kernel = kernel_with_dispatch_relations();
        let mut tx = kernel.begin();
        tx.assert(rel(40), Tuple::from([int(100), sym("take")]))
            .unwrap();
        tx.assert(
            rel(41),
            Tuple::from([int(100), sym("actor"), int(11), int(0)]),
        )
        .unwrap();
        tx.assert(
            rel(41),
            Tuple::from([int(100), sym("item"), int(2), int(1)]),
        )
        .unwrap();
        tx.assert(rel(42), Tuple::from([int(10), int(11), int(0)]))
            .unwrap();
        tx.assert(rel(42), Tuple::from([int(1), int(2), int(0)]))
            .unwrap();

        assert_eq!(
            applicable_methods(
                &tx,
                dispatch_relations(),
                sym("take"),
                [(sym("actor"), int(10)), (sym("item"), int(1))]
            )
            .unwrap(),
            vec![int(100)]
        );
    }

    #[test]
    fn dispatch_rejects_missing_roles() {
        let kernel = kernel_with_dispatch_relations();
        let mut tx = kernel.begin();
        tx.assert(rel(40), Tuple::from([int(100), sym("take")]))
            .unwrap();
        tx.assert(
            rel(41),
            Tuple::from([int(100), sym("actor"), int(11), int(0)]),
        )
        .unwrap();

        assert!(
            applicable_methods(
                &tx,
                dispatch_relations(),
                sym("take"),
                [(sym("item"), int(1))]
            )
            .unwrap()
            .is_empty()
        );
    }

    #[test]
    fn dispatch_requires_unrestricted_params_without_matching_them() {
        let kernel = kernel_with_dispatch_relations();
        let mut tx = kernel.begin();
        tx.assert(rel(40), Tuple::from([int(100), sym("say")]))
            .unwrap();
        tx.assert(
            rel(41),
            Tuple::from([int(100), sym("actor"), int(11), int(0)]),
        )
        .unwrap();
        tx.assert(
            rel(41),
            Tuple::from([int(100), sym("message"), Value::nothing(), int(1)]),
        )
        .unwrap();
        tx.assert(rel(42), Tuple::from([int(10), int(11), int(0)]))
            .unwrap();

        assert_eq!(
            applicable_methods(
                &tx,
                dispatch_relations(),
                sym("say"),
                [
                    (sym("actor"), int(10)),
                    (sym("message"), Value::string("hi"))
                ]
            )
            .unwrap(),
            vec![int(100)]
        );
        assert!(
            applicable_methods(
                &tx,
                dispatch_relations(),
                sym("say"),
                [(sym("actor"), int(10))]
            )
            .unwrap()
            .is_empty()
        );
    }

    #[test]
    fn positional_dispatch_matches_primitive_restrictions() {
        let kernel = kernel_with_dispatch_relations();
        let mut tx = kernel.begin();
        tx.assert(rel(40), Tuple::from([int(100), sym("split")]))
            .unwrap();
        tx.assert(
            rel(41),
            Tuple::from([
                int(100),
                sym("text"),
                Value::identity(STRING_PROTOTYPE),
                int(0),
            ]),
        )
        .unwrap();

        assert_eq!(
            applicable_positional_methods(
                &tx,
                dispatch_relations(),
                sym("split"),
                &[Value::string("a b")]
            )
            .unwrap(),
            vec![int(100)]
        );
        assert!(
            applicable_positional_methods(&tx, dispatch_relations(), sym("split"), &[int(1)])
                .unwrap()
                .is_empty()
        );
    }
}
