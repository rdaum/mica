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

use crate::{BinaryOp, Binding, HirExpr, HirItem, HirPlace, Literal, LocalKind, UnaryOp};
use mica_var::ValueKind;

const KIND_COUNT: u32 = 16;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct KindSet(u32);

impl KindSet {
    pub(crate) const EMPTY: Self = Self(0);
    pub(crate) const ALL: Self = Self((1 << KIND_COUNT) - 1);

    pub(crate) const fn exact(kind: ValueKind) -> Self {
        Self(1 << kind_bit(kind))
    }

    pub(crate) const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    pub(crate) const fn intersection(self, other: Self) -> Self {
        Self(self.0 & other.0)
    }

    pub(crate) const fn is_empty(self) -> bool {
        self.0 == 0
    }

    pub(crate) const fn is_subset(self, other: Self) -> bool {
        self.0 & !other.0 == 0
    }

    pub(crate) const fn is_disjoint(self, other: Self) -> bool {
        self.0 & other.0 == 0
    }

    pub(crate) fn singleton(self) -> Option<ValueKind> {
        if self.0.count_ones() != 1 {
            return None;
        }
        kind_for_bit(self.0.trailing_zeros())
    }

    pub(crate) fn names(self) -> String {
        if self == Self::ALL {
            return "any value kind".to_owned();
        }
        if self.is_empty() {
            return "no normally completing value".to_owned();
        }
        let names = (0..KIND_COUNT)
            .filter(|bit| self.0 & (1 << bit) != 0)
            .filter_map(kind_for_bit)
            .map(ValueKind::name)
            .collect::<Vec<_>>();
        names.join(" or ")
    }
}

pub(crate) struct KindInference<'a> {
    bindings: &'a [Binding],
}

impl<'a> KindInference<'a> {
    pub(crate) const fn new(bindings: &'a [Binding]) -> Self {
        Self { bindings }
    }

    pub(crate) fn expr(&self, expr: &HirExpr) -> KindSet {
        match expr {
            HirExpr::Literal { value, .. } => KindSet::exact(literal_kind(value)),
            HirExpr::LocalRef { binding, .. } => self
                .binding(*binding)
                .map(|binding| {
                    binding.declared_kind.map_or_else(
                        || match binding.kind {
                            LocalKind::Catch => KindSet::exact(ValueKind::Error),
                            _ => KindSet::ALL,
                        },
                        KindSet::exact,
                    )
                })
                .unwrap_or(KindSet::ALL),
            HirExpr::Identity { .. } => KindSet::exact(ValueKind::Identity),
            HirExpr::Frob { .. } => KindSet::exact(ValueKind::Frob),
            HirExpr::Symbol { .. } => KindSet::exact(ValueKind::Symbol),
            HirExpr::List { .. } => KindSet::exact(ValueKind::List),
            HirExpr::Relation { .. } => KindSet::exact(ValueKind::Relation),
            HirExpr::Map { .. } => KindSet::exact(ValueKind::Map),
            HirExpr::Unary { op, expr, .. } => self.unary(*op, self.expr(expr)),
            HirExpr::Binary {
                op, left, right, ..
            } => self.binary(*op, self.expr(left), self.expr(right)),
            HirExpr::Assign { target, value, .. } => match target {
                HirPlace::Local { .. } | HirPlace::Dot { .. } => self.expr(value),
                HirPlace::Index { collection, .. } => self.expr(collection),
                HirPlace::Invalid { .. } => KindSet::ALL,
            },
            HirExpr::RelationAtom(atom) => {
                if atom
                    .args
                    .iter()
                    .any(|arg| matches!(arg.value, HirExpr::QueryVar { .. } | HirExpr::Hole { .. }))
                {
                    KindSet::exact(ValueKind::Relation)
                } else {
                    KindSet::exact(ValueKind::Bool)
                }
            }
            HirExpr::FactChange { .. } | HirExpr::For { .. } | HirExpr::While { .. } => {
                KindSet::exact(ValueKind::Relation)
            }
            HirExpr::Require { .. } => KindSet::exact(ValueKind::Bool),
            HirExpr::Binding { value, .. } => value
                .as_deref()
                .map_or(KindSet::exact(ValueKind::Relation), |value| {
                    self.expr(value)
                }),
            HirExpr::If {
                then_items,
                elseif,
                else_items,
                ..
            } => {
                let mut result = self.items(then_items);
                for (_, items) in elseif {
                    result = result.union(self.items(items));
                }
                result.union(self.items(else_items))
            }
            HirExpr::Block { items, .. } => self.items(items),
            HirExpr::Recover { expr, catches, .. } => {
                catches.iter().fold(self.expr(expr), |kinds, catch| {
                    kinds.union(self.expr(&catch.value))
                })
            }
            HirExpr::Function { name: None, .. } => KindSet::exact(ValueKind::Function),
            HirExpr::Return { .. }
            | HirExpr::Raise { .. }
            | HirExpr::Break { .. }
            | HirExpr::Continue { .. }
            | HirExpr::Error { .. } => KindSet::EMPTY,
            HirExpr::ExternalRef { .. }
            | HirExpr::QueryVar { .. }
            | HirExpr::Hole { .. }
            | HirExpr::Call { .. }
            | HirExpr::RoleDispatch { .. }
            | HirExpr::ReceiverDispatch { .. }
            | HirExpr::Spawn { .. }
            | HirExpr::Index { .. }
            | HirExpr::Field { .. }
            | HirExpr::One { .. }
            | HirExpr::Try { .. }
            | HirExpr::Function { name: Some(_), .. } => KindSet::ALL,
        }
    }

    fn items(&self, items: &[HirItem]) -> KindSet {
        let mut result = KindSet::exact(ValueKind::Relation);
        for item in items {
            let HirItem::Expr { expr, .. } = item else {
                return KindSet::ALL;
            };
            result = self.expr(expr);
            if result.is_empty() {
                return result;
            }
        }
        result
    }

    fn unary(&self, op: UnaryOp, operand: KindSet) -> KindSet {
        match op {
            UnaryOp::Not => KindSet::exact(ValueKind::Bool),
            UnaryOp::Neg => operand.intersection(numeric_kinds()),
        }
    }

    fn binary(&self, op: BinaryOp, left: KindSet, right: KindSet) -> KindSet {
        match op {
            BinaryOp::Eq
            | BinaryOp::Ne
            | BinaryOp::Lt
            | BinaryOp::Le
            | BinaryOp::Gt
            | BinaryOp::Ge => KindSet::exact(ValueKind::Bool),
            BinaryOp::Range => KindSet::exact(ValueKind::Range),
            BinaryOp::And | BinaryOp::Or if left.is_empty() => KindSet::EMPTY,
            BinaryOp::And | BinaryOp::Or => KindSet::exact(ValueKind::Bool).union(right),
            BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Rem => {
                arithmetic_kinds(op, left, right)
            }
        }
    }

    fn binding(&self, id: crate::BindingId) -> Option<&Binding> {
        self.bindings.get(id.as_u32() as usize)
    }
}

const fn literal_kind(literal: &Literal) -> ValueKind {
    match literal {
        Literal::Int(_) => ValueKind::Int,
        Literal::Float(_) => ValueKind::Float,
        Literal::String(_) => ValueKind::String,
        Literal::Bytes(_) => ValueKind::Bytes,
        Literal::Bool(_) => ValueKind::Bool,
        Literal::ErrorCode(_) => ValueKind::ErrorCode,
        Literal::Nothing => ValueKind::Relation,
    }
}

const fn numeric_kinds() -> KindSet {
    KindSet::exact(ValueKind::Int).union(KindSet::exact(ValueKind::Float))
}

fn arithmetic_kinds(op: BinaryOp, left: KindSet, right: KindSet) -> KindSet {
    let left = left.intersection(numeric_kinds());
    let right = right.intersection(numeric_kinds());
    if left.is_empty() || right.is_empty() {
        return KindSet::EMPTY;
    }

    let integers = KindSet::exact(ValueKind::Int);
    let floats = KindSet::exact(ValueKind::Float);
    let mut result = KindSet::EMPTY;
    if !left.is_disjoint(integers) && !right.is_disjoint(integers) {
        result = result.union(integers);
        if op == BinaryOp::Div {
            result = result.union(floats);
        }
    }
    if (!left.is_disjoint(floats) && !right.is_disjoint(numeric_kinds()))
        || (!right.is_disjoint(floats) && !left.is_disjoint(numeric_kinds()))
    {
        result = result.union(floats);
    }
    result
}

const fn kind_bit(kind: ValueKind) -> u32 {
    match kind {
        ValueKind::Bool => 0,
        ValueKind::Int => 1,
        ValueKind::Float => 2,
        ValueKind::Identity => 3,
        ValueKind::String => 4,
        ValueKind::Bytes => 5,
        ValueKind::Symbol => 6,
        ValueKind::ErrorCode => 7,
        ValueKind::Error => 8,
        ValueKind::Capability => 9,
        ValueKind::Frob => 10,
        ValueKind::Function => 11,
        ValueKind::List => 12,
        ValueKind::Map => 13,
        ValueKind::Range => 14,
        ValueKind::Relation => 15,
    }
}

const fn kind_for_bit(bit: u32) -> Option<ValueKind> {
    match bit {
        0 => Some(ValueKind::Bool),
        1 => Some(ValueKind::Int),
        2 => Some(ValueKind::Float),
        3 => Some(ValueKind::Identity),
        4 => Some(ValueKind::String),
        5 => Some(ValueKind::Bytes),
        6 => Some(ValueKind::Symbol),
        7 => Some(ValueKind::ErrorCode),
        8 => Some(ValueKind::Error),
        9 => Some(ValueKind::Capability),
        10 => Some(ValueKind::Frob),
        11 => Some(ValueKind::Function),
        12 => Some(ValueKind::List),
        13 => Some(ValueKind::Map),
        14 => Some(ValueKind::Range),
        15 => Some(ValueKind::Relation),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kind_sets_keep_compiler_bits_independent_and_composable() {
        let int = KindSet::exact(ValueKind::Int);
        let float = KindSet::exact(ValueKind::Float);
        let numeric = int.union(float);

        assert!(int.is_subset(numeric));
        assert!(int.is_disjoint(float));
        assert_eq!(int.singleton(), Some(ValueKind::Int));
        assert_eq!(numeric.singleton(), None);
        assert_eq!(numeric.names(), "int or float");
        assert_eq!(KindSet::EMPTY.intersection(KindSet::ALL), KindSet::EMPTY);
    }

    #[test]
    fn arithmetic_result_sets_follow_runtime_numeric_rules() {
        let int = KindSet::exact(ValueKind::Int);
        let float = KindSet::exact(ValueKind::Float);

        assert_eq!(arithmetic_kinds(BinaryOp::Add, int, int), int);
        assert_eq!(arithmetic_kinds(BinaryOp::Add, int, float), float);
        assert_eq!(arithmetic_kinds(BinaryOp::Div, int, int), int.union(float));
        assert_eq!(
            arithmetic_kinds(BinaryOp::Add, KindSet::ALL, int),
            int.union(float)
        );
    }
}
