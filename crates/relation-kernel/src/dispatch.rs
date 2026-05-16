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

use crate::{KernelError, RelationId, RelationRead, ScanControl, delegates_reaches};
use mica_var::{Value, primitive_prototype_for_value};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DispatchRelations {
    pub method_selector: RelationId,
    pub param: RelationId,
    pub delegates: RelationId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicableMethod {
    pub method: Value,
    pub params: Vec<crate::Tuple>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicableMethodCall {
    pub method: Value,
    pub args: Option<Vec<Value>>,
}

pub fn applicable_methods(
    reader: &impl RelationRead,
    relations: DispatchRelations,
    selector: Value,
    roles: impl IntoIterator<Item = (Value, Value)>,
) -> Result<Vec<Value>, KernelError> {
    let roles = roles.into_iter().collect::<Vec<_>>();
    Ok(
        applicable_method_entries(reader, relations, selector, &roles)?
            .into_iter()
            .map(|entry| entry.method)
            .collect(),
    )
}

pub fn applicable_method_entries(
    reader: &impl RelationRead,
    relations: DispatchRelations,
    selector: Value,
    roles: &[(Value, Value)],
) -> Result<Vec<ApplicableMethod>, KernelError> {
    let mut methods = Vec::new();

    reader.visit_relation(
        relations.method_selector,
        &[None, Some(selector)],
        &mut |row| {
            let method = row.values()[0].clone();
            let mut params = Vec::new();
            reader.visit_relation(
                relations.param,
                &[Some(method.clone()), None, None, None],
                &mut |param| {
                    params.push(param.clone());
                    Ok(ScanControl::Continue)
                },
            )?;
            if params_match(reader, relations.delegates, roles, &params)? {
                methods.push(ApplicableMethod { method, params });
            }
            Ok(ScanControl::Continue)
        },
    )?;

    methods.sort_by(|left, right| left.method.cmp(&right.method));
    methods.dedup_by(|left, right| left.method == right.method);
    Ok(methods)
}

pub fn applicable_method_calls(
    reader: &impl RelationRead,
    relations: DispatchRelations,
    selector: Value,
    roles: &[(Value, Value)],
) -> Result<Vec<ApplicableMethodCall>, KernelError> {
    if let Some(methods) = reader.cached_applicable_method_calls(relations, &selector, roles)? {
        return Ok(methods);
    }
    applicable_method_calls_uncached(reader, relations, &selector, roles)
}

pub fn applicable_method_calls_normalized(
    reader: &impl RelationRead,
    relations: DispatchRelations,
    selector: Value,
    roles: &[(Value, Value)],
) -> Result<Vec<ApplicableMethodCall>, KernelError> {
    if let Some(methods) =
        reader.cached_applicable_method_calls_normalized(relations, &selector, roles)?
    {
        return Ok(methods);
    }
    applicable_method_calls_uncached(reader, relations, &selector, roles)
}

pub(crate) fn applicable_method_calls_uncached(
    reader: &impl RelationRead,
    relations: DispatchRelations,
    selector: &Value,
    roles: &[(Value, Value)],
) -> Result<Vec<ApplicableMethodCall>, KernelError> {
    let mut methods = Vec::new();

    reader.visit_relation(
        relations.method_selector,
        &[None, Some(selector.clone())],
        &mut |row| {
            let method = row.values()[0].clone();
            if let Some(args) = method_call_args(reader, relations, &method, roles)? {
                methods.push(ApplicableMethodCall { method, args });
            }
            Ok(ScanControl::Continue)
        },
    )?;

    methods.sort_by(|left, right| left.method.cmp(&right.method));
    methods.dedup_by(|left, right| left.method == right.method);
    Ok(methods)
}

pub fn applicable_positional_methods(
    reader: &impl RelationRead,
    relations: DispatchRelations,
    selector: Value,
    args: &[Value],
) -> Result<Vec<Value>, KernelError> {
    let mut methods = Vec::new();

    reader.visit_relation(
        relations.method_selector,
        &[None, Some(selector)],
        &mut |row| {
            let method = row.values()[0].clone();
            let mut params = Vec::new();
            reader.visit_relation(
                relations.param,
                &[Some(method.clone()), None, None, None],
                &mut |param| {
                    params.push(param.clone());
                    Ok(ScanControl::Continue)
                },
            )?;
            if positional_params_match(reader, relations.delegates, args, &params)? {
                methods.push(method);
            }
            Ok(ScanControl::Continue)
        },
    )?;

    methods.sort();
    methods.dedup();
    Ok(methods)
}

fn method_call_args(
    reader: &impl RelationRead,
    relations: DispatchRelations,
    method: &Value,
    roles: &[(Value, Value)],
) -> Result<Option<Option<Vec<Value>>>, KernelError> {
    let mut args = Vec::new();
    let mut matched = true;
    let mut invalid_position = false;

    reader.visit_relation(
        relations.param,
        &[Some(method.clone()), None, None, None],
        &mut |param| {
            let Some(position) = param_position(param) else {
                invalid_position = true;
                return Ok(ScanControl::Stop);
            };
            let role = &param.values()[1];
            let restriction = &param.values()[2];
            let Some(value) = role_value(roles, role) else {
                matched = false;
                return Ok(ScanControl::Stop);
            };
            if !matches_restriction(reader, relations.delegates, value, restriction)? {
                matched = false;
                return Ok(ScanControl::Stop);
            }
            args.push((position, value.clone()));
            Ok(ScanControl::Continue)
        },
    )?;

    if !matched {
        return Ok(None);
    }
    if invalid_position {
        return Ok(Some(None));
    }

    args.sort_by_key(|(position, _)| *position);
    Ok(Some(Some(
        args.into_iter().map(|(_, value)| value).collect(),
    )))
}

fn params_match(
    reader: &impl RelationRead,
    delegates_relation: RelationId,
    roles: &[(Value, Value)],
    params: &[crate::Tuple],
) -> Result<bool, KernelError> {
    for param in params {
        let role = &param.values()[1];
        let restriction = &param.values()[2];
        let Some(value) = role_value(roles, role) else {
            return Ok(false);
        };
        if !matches_restriction(reader, delegates_relation, value, restriction)? {
            return Ok(false);
        }
    }
    Ok(true)
}

pub fn role_value<'a>(roles: &'a [(Value, Value)], role: &Value) -> Option<&'a Value> {
    roles
        .iter()
        .find_map(|(candidate, value)| (candidate == role).then_some(value))
}

pub fn normalize_dispatch_roles(roles: &mut [(Value, Value)]) {
    roles.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
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

pub fn named_method_args(params: &[crate::Tuple], roles: &[(Value, Value)]) -> Option<Vec<Value>> {
    let mut args = Vec::with_capacity(params.len());

    if params.len() <= 1 {
        for param in params {
            param_position(param)?;
            if let Some(value) = role_value(roles, &param.values()[1]) {
                args.push(value.clone());
            }
        }
        return Some(args);
    }

    let mut ordered = params.iter().collect::<Vec<_>>();
    ordered.sort_by_key(|param| param_position(param).unwrap_or(u16::MAX));
    if ordered.iter().any(|param| param_position(param).is_none()) {
        return None;
    }

    for param in ordered {
        if let Some(value) = role_value(roles, &param.values()[1]) {
            args.push(value.clone());
        }
    }
    Some(args)
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

    if delegates_reaches(reader, delegates_relation, value, restriction)? {
        return Ok(true);
    }

    let prototype = Value::identity(primitive_prototype_for_value(value));
    if &prototype == restriction {
        return Ok(true);
    }
    delegates_reaches(reader, delegates_relation, &prototype, restriction)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ComposedTransactionRead, RelationKernel, RelationMetadata, TransientStore, Tuple};
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

    #[test]
    fn snapshot_dispatch_cache_is_scoped_to_snapshot_version() {
        let kernel = kernel_with_dispatch_relations();
        let snapshot = kernel.snapshot();
        assert!(
            applicable_method_calls(&*snapshot, dispatch_relations(), sym("look"), &[])
                .unwrap()
                .is_empty()
        );

        let mut tx = kernel.begin();
        tx.assert(rel(40), Tuple::from([int(100), sym("look")]))
            .unwrap();
        let next = tx.commit().unwrap().into_snapshot();

        assert_eq!(
            applicable_method_calls(&*next, dispatch_relations(), sym("look"), &[]).unwrap(),
            vec![ApplicableMethodCall {
                method: int(100),
                args: Some(Vec::new())
            }]
        );
    }

    #[test]
    fn transaction_dispatch_bypasses_snapshot_cache_after_local_writes() {
        let kernel = kernel_with_dispatch_relations();
        let snapshot = kernel.snapshot();
        assert!(
            applicable_method_calls(&*snapshot, dispatch_relations(), sym("look"), &[])
                .unwrap()
                .is_empty()
        );

        let mut tx = kernel.begin();
        tx.assert(rel(40), Tuple::from([int(100), sym("look")]))
            .unwrap();

        assert_eq!(
            applicable_method_calls(&tx, dispatch_relations(), sym("look"), &[]).unwrap(),
            vec![ApplicableMethodCall {
                method: int(100),
                args: Some(Vec::new())
            }]
        );
    }

    #[test]
    fn composed_dispatch_bypasses_snapshot_cache_for_transient_dispatch_facts() {
        let kernel = kernel_with_dispatch_relations();
        let snapshot = kernel.snapshot();
        assert!(
            applicable_method_calls(&*snapshot, dispatch_relations(), sym("look"), &[])
                .unwrap()
                .is_empty()
        );

        let tx = kernel.begin();
        let mut transient = TransientStore::new();
        let scope = rel(90);
        transient
            .assert(
                scope,
                RelationMetadata::new(rel(40), Symbol::intern("MethodSelector"), 2)
                    .with_index([1, 0]),
                Tuple::from([int(100), sym("look")]),
            )
            .unwrap();
        let scopes = [scope];
        let reader = ComposedTransactionRead::new(&tx, &transient, &scopes);

        assert_eq!(
            applicable_method_calls(&reader, dispatch_relations(), sym("look"), &[]).unwrap(),
            vec![ApplicableMethodCall {
                method: int(100),
                args: Some(Vec::new())
            }]
        );
    }

    #[test]
    fn named_method_args_follow_param_positions() {
        let params = vec![
            Tuple::from([int(100), sym("item"), Value::nothing(), int(1)]),
            Tuple::from([int(100), sym("actor"), Value::nothing(), int(0)]),
        ];

        assert_eq!(
            named_method_args(&params, &[(sym("actor"), int(10)), (sym("item"), int(1))]).unwrap(),
            vec![int(10), int(1)]
        );
    }
}
