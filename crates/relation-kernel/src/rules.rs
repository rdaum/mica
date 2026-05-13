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

use crate::{KernelError, RelationId, RelationRead, Tuple};
use mica_var::{Identity, Symbol, Value};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Term {
    Var(Symbol),
    Value(Value),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Atom {
    relation: RelationId,
    terms: Vec<Term>,
    negated: bool,
}

impl Atom {
    pub fn positive(relation: RelationId, terms: impl IntoIterator<Item = Term>) -> Self {
        Self {
            relation,
            terms: terms.into_iter().collect(),
            negated: false,
        }
    }

    pub fn negated(relation: RelationId, terms: impl IntoIterator<Item = Term>) -> Self {
        Self {
            relation,
            terms: terms.into_iter().collect(),
            negated: true,
        }
    }

    pub fn relation(&self) -> RelationId {
        self.relation
    }

    pub fn terms(&self) -> &[Term] {
        &self.terms
    }

    pub fn is_negated(&self) -> bool {
        self.negated
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Rule {
    head_relation: RelationId,
    head_terms: Vec<Term>,
    body: Vec<Atom>,
}

impl Rule {
    pub fn new(
        head_relation: RelationId,
        head_terms: impl IntoIterator<Item = Term>,
        body: impl IntoIterator<Item = Atom>,
    ) -> Self {
        Self {
            head_relation,
            head_terms: head_terms.into_iter().collect(),
            body: body.into_iter().collect(),
        }
    }

    pub fn head_relation(&self) -> RelationId {
        self.head_relation
    }

    pub fn head_terms(&self) -> &[Term] {
        &self.head_terms
    }

    pub fn body(&self) -> &[Atom] {
        &self.body
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuleDefinition {
    id: Identity,
    rule: Rule,
    source: String,
    active: bool,
}

impl RuleDefinition {
    pub fn new(id: Identity, rule: Rule, source: impl Into<String>) -> Self {
        Self {
            id,
            rule,
            source: source.into(),
            active: true,
        }
    }

    pub fn id(&self) -> Identity {
        self.id
    }

    pub fn rule(&self) -> &Rule {
        &self.rule
    }

    pub fn source(&self) -> &str {
        &self.source
    }

    pub fn active(&self) -> bool {
        self.active
    }

    pub fn deactivate(&mut self) {
        self.active = false;
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RuleSet {
    rules: Vec<Rule>,
}

impl RuleSet {
    pub fn new(rules: impl IntoIterator<Item = Rule>) -> Self {
        Self {
            rules: rules.into_iter().collect(),
        }
    }

    pub fn validate_stratified(&self) -> Result<(), RuleError> {
        self.stratified_rules()?;
        Ok(())
    }

    pub fn evaluate(
        &self,
        reader: &impl RelationRead,
    ) -> Result<BTreeMap<RelationId, Vec<Tuple>>, RuleEvalError> {
        let strata = self.stratified_rules()?;
        let mut derived: BTreeMap<RelationId, BTreeSet<Tuple>> = BTreeMap::new();

        for rules in strata {
            let overlay = DerivedReader {
                base: reader,
                derived: &derived,
            };
            let mut stratum_out: BTreeMap<RelationId, BTreeSet<Tuple>> = BTreeMap::new();
            evaluate_rules_once(&overlay, &rules, &mut stratum_out)?;
            for (relation, tuples) in stratum_out {
                derived.entry(relation).or_default().extend(tuples);
            }
        }

        Ok(derived
            .into_iter()
            .map(|(relation, tuples)| (relation, tuples.into_iter().collect()))
            .collect())
    }

    pub fn evaluate_fixpoint(
        &self,
        reader: &impl RelationRead,
    ) -> Result<BTreeMap<RelationId, Vec<Tuple>>, RuleEvalError> {
        let strata = self.stratified_rules()?;
        let mut derived: BTreeMap<RelationId, BTreeSet<Tuple>> = BTreeMap::new();

        for rules in strata {
            loop {
                let overlay = DerivedReader {
                    base: reader,
                    derived: &derived,
                };
                let mut round = BTreeMap::new();
                evaluate_rules_once(&overlay, &rules, &mut round)?;
                let mut changed = false;
                for (relation, tuples) in round {
                    let relation_tuples = derived.entry(relation).or_default();
                    for tuple in tuples {
                        changed |= relation_tuples.insert(tuple);
                    }
                }
                if !changed {
                    break;
                }
            }
        }

        Ok(derived
            .into_iter()
            .map(|(relation, tuples)| (relation, tuples.into_iter().collect()))
            .collect())
    }

    pub fn iter(&self) -> impl Iterator<Item = &Rule> {
        self.rules.iter()
    }

    fn stratified_rules(&self) -> Result<Vec<Vec<&Rule>>, RuleError> {
        let relation_strata = self.relation_strata()?;
        let mut strata = BTreeMap::<usize, Vec<&Rule>>::new();
        for rule in &self.rules {
            strata
                .entry(relation_strata[&rule.head_relation])
                .or_default()
                .push(rule);
        }
        Ok(strata.into_values().collect())
    }

    fn relation_strata(&self) -> Result<BTreeMap<RelationId, usize>, RuleError> {
        let derived = self
            .rules
            .iter()
            .map(|rule| rule.head_relation)
            .collect::<BTreeSet<_>>();
        let mut strata = derived
            .iter()
            .copied()
            .map(|relation| (relation, 0))
            .collect::<BTreeMap<_, _>>();

        for _ in 0..=derived.len() {
            let mut changed = false;
            for rule in &self.rules {
                let mut head_stratum = strata[&rule.head_relation];
                for atom in &rule.body {
                    if !derived.contains(&atom.relation) {
                        continue;
                    }
                    let dependency_stratum = strata[&atom.relation];
                    let required = dependency_stratum + usize::from(atom.negated);
                    if head_stratum < required {
                        head_stratum = required;
                    }
                }
                if strata[&rule.head_relation] != head_stratum {
                    strata.insert(rule.head_relation, head_stratum);
                    changed = true;
                }
            }
            if !changed {
                return Ok(strata);
            }
        }

        Err(RuleError::UnstratifiedNegation)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuleError {
    UnstratifiedNegation,
    UnsafeNegation { relation: RelationId },
    UnboundHeadVariable { variable: Symbol },
}

#[derive(Debug, Eq, PartialEq)]
pub enum RuleEvalError {
    Rule(RuleError),
    Kernel(KernelError),
}

impl From<RuleError> for RuleEvalError {
    fn from(value: RuleError) -> Self {
        Self::Rule(value)
    }
}

impl From<KernelError> for RuleEvalError {
    fn from(value: KernelError) -> Self {
        Self::Kernel(value)
    }
}

type Binding = BTreeMap<Symbol, Value>;

struct DerivedReader<'a, R> {
    base: &'a R,
    derived: &'a BTreeMap<RelationId, BTreeSet<Tuple>>,
}

impl<R: RelationRead> RelationRead for DerivedReader<'_, R> {
    fn scan_relation(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
    ) -> Result<Vec<Tuple>, KernelError> {
        let mut rows = match self.base.scan_relation(relation, bindings) {
            Ok(rows) => rows.into_iter().collect::<BTreeSet<_>>(),
            Err(KernelError::UnknownRelation(unknown)) if unknown == relation => BTreeSet::new(),
            Err(error) => return Err(error),
        };
        if let Some(derived) = self.derived.get(&relation) {
            rows.extend(
                derived
                    .iter()
                    .filter(|tuple| tuple.matches_bindings(bindings))
                    .cloned(),
            );
        }
        Ok(rows.into_iter().collect())
    }
}

fn evaluate_rules_once(
    reader: &impl RelationRead,
    rules: &[&Rule],
    out: &mut BTreeMap<RelationId, BTreeSet<Tuple>>,
) -> Result<(), RuleEvalError> {
    for rule in rules {
        for binding in evaluate_body(reader, &rule.body)? {
            out.entry(rule.head_relation)
                .or_default()
                .insert(instantiate_head(rule, &binding)?);
        }
    }
    Ok(())
}

fn evaluate_body(reader: &impl RelationRead, body: &[Atom]) -> Result<Vec<Binding>, RuleEvalError> {
    let mut bindings = vec![Binding::new()];
    for atom in body {
        bindings = if atom.negated {
            apply_negated_atom(reader, atom, bindings)?
        } else {
            apply_positive_atom(reader, atom, bindings)?
        };
    }
    Ok(bindings)
}

fn apply_positive_atom(
    reader: &impl RelationRead,
    atom: &Atom,
    bindings: Vec<Binding>,
) -> Result<Vec<Binding>, RuleEvalError> {
    let mut out = Vec::new();
    for binding in bindings {
        let scan_bindings = scan_bindings(atom, &binding)?;
        for tuple in reader.scan_relation(atom.relation, &scan_bindings)? {
            if let Some(next) = unify_tuple(atom, &binding, &tuple) {
                out.push(next);
            }
        }
    }
    Ok(out)
}

fn apply_negated_atom(
    reader: &impl RelationRead,
    atom: &Atom,
    bindings: Vec<Binding>,
) -> Result<Vec<Binding>, RuleEvalError> {
    let mut out = Vec::new();
    for binding in bindings {
        ensure_negation_safe(atom, &binding)?;
        let scan_bindings = scan_bindings(atom, &binding)?;
        if reader
            .scan_relation(atom.relation, &scan_bindings)?
            .is_empty()
        {
            out.push(binding);
        }
    }
    Ok(out)
}

fn scan_bindings(atom: &Atom, binding: &Binding) -> Result<Vec<Option<Value>>, RuleEvalError> {
    let mut out = Vec::with_capacity(atom.terms.len());
    for term in &atom.terms {
        out.push(match term {
            Term::Value(value) => Some(value.clone()),
            Term::Var(variable) => binding.get(variable).cloned(),
        });
    }
    Ok(out)
}

fn unify_tuple(atom: &Atom, binding: &Binding, tuple: &Tuple) -> Option<Binding> {
    let mut next = binding.clone();
    for (term, value) in atom.terms.iter().zip(tuple.values()) {
        match term {
            Term::Value(expected) if expected != value => return None,
            Term::Value(_) => {}
            Term::Var(variable) => {
                if let Some(bound) = next.get(variable) {
                    if bound != value {
                        return None;
                    }
                } else {
                    next.insert(*variable, value.clone());
                }
            }
        }
    }
    Some(next)
}

fn ensure_negation_safe(atom: &Atom, binding: &Binding) -> Result<(), RuleEvalError> {
    if atom.terms.iter().all(|term| match term {
        Term::Value(_) => true,
        Term::Var(variable) => binding.contains_key(variable),
    }) {
        return Ok(());
    }

    Err(RuleError::UnsafeNegation {
        relation: atom.relation,
    }
    .into())
}

fn instantiate_head(rule: &Rule, binding: &Binding) -> Result<Tuple, RuleEvalError> {
    let mut values = Vec::with_capacity(rule.head_terms.len());
    for term in &rule.head_terms {
        values.push(match term {
            Term::Value(value) => value.clone(),
            Term::Var(variable) => {
                binding
                    .get(variable)
                    .cloned()
                    .ok_or(RuleError::UnboundHeadVariable {
                        variable: *variable,
                    })?
            }
        });
    }
    Ok(Tuple::new(values))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RelationKernel, RelationMetadata};
    use mica_var::Identity;

    fn rel(id: u64) -> RelationId {
        Identity::new(id).unwrap()
    }

    fn int(value: i64) -> Value {
        Value::int(value).unwrap()
    }

    fn var(name: &str) -> Term {
        Term::Var(Symbol::intern(name))
    }

    fn val(value: Value) -> Term {
        Term::Value(value)
    }

    fn kernel_with_visibility_relations() -> RelationKernel {
        let kernel = RelationKernel::new();
        kernel
            .create_relation(RelationMetadata::new(
                rel(50),
                Symbol::intern("LocatedIn"),
                2,
            ))
            .unwrap();
        kernel
            .create_relation(RelationMetadata::new(
                rel(51),
                Symbol::intern("CanSeeRoom"),
                2,
            ))
            .unwrap();
        kernel
            .create_relation(RelationMetadata::new(
                rel(52),
                Symbol::intern("HiddenFrom"),
                2,
            ))
            .unwrap();
        kernel
    }

    #[test]
    fn nonrecursive_rule_evaluation_supports_safe_negation() {
        let kernel = kernel_with_visibility_relations();
        let mut tx = kernel.begin();
        tx.assert(rel(50), Tuple::from([int(10), int(1)])).unwrap();
        tx.assert(rel(50), Tuple::from([int(11), int(1)])).unwrap();
        tx.assert(rel(51), Tuple::from([int(99), int(1)])).unwrap();
        tx.assert(rel(52), Tuple::from([int(11), int(99)])).unwrap();

        let visible = Rule::new(
            rel(53),
            [var("actor"), var("obj")],
            [
                Atom::positive(rel(50), [var("obj"), var("room")]),
                Atom::positive(rel(51), [var("actor"), var("room")]),
                Atom::negated(rel(52), [var("obj"), var("actor")]),
            ],
        );

        assert_eq!(
            RuleSet::new([visible]).evaluate(&tx).unwrap()[&rel(53)],
            vec![Tuple::from([int(99), int(10)])]
        );
    }

    #[test]
    fn rule_set_rejects_unstratified_negation() {
        let rules = RuleSet::new([Rule::new(
            rel(60),
            [var("x")],
            [Atom::negated(rel(60), [var("x")])],
        )]);

        assert_eq!(
            rules.validate_stratified(),
            Err(RuleError::UnstratifiedNegation)
        );
    }

    #[test]
    fn rule_set_allows_negation_of_lower_derived_strata() {
        let kernel = kernel_with_visibility_relations();
        let mut tx = kernel.begin();
        tx.assert(rel(50), Tuple::from([int(10), int(1)])).unwrap();
        tx.assert(rel(50), Tuple::from([int(11), int(1)])).unwrap();
        tx.assert(rel(51), Tuple::from([int(99), int(1)])).unwrap();
        tx.assert(rel(52), Tuple::from([int(11), int(99)])).unwrap();

        let hidden = Rule::new(
            rel(62),
            [var("actor"), var("obj")],
            [Atom::positive(rel(52), [var("obj"), var("actor")])],
        );
        let visible = Rule::new(
            rel(63),
            [var("actor"), var("obj")],
            [
                Atom::positive(rel(50), [var("obj"), var("room")]),
                Atom::positive(rel(51), [var("actor"), var("room")]),
                Atom::negated(rel(62), [var("actor"), var("obj")]),
            ],
        );

        assert_eq!(
            RuleSet::new([visible, hidden]).evaluate(&tx).unwrap()[&rel(63)],
            vec![Tuple::from([int(99), int(10)])]
        );
    }

    #[test]
    fn rule_evaluation_rejects_unsafe_negation() {
        let kernel = kernel_with_visibility_relations();
        let rules = RuleSet::new([Rule::new(
            rel(61),
            [val(int(1))],
            [Atom::negated(rel(52), [var("obj"), val(int(99))])],
        )]);

        assert_eq!(
            rules.evaluate(&*kernel.snapshot()),
            Err(RuleEvalError::Rule(RuleError::UnsafeNegation {
                relation: rel(52)
            }))
        );
    }

    #[test]
    fn fixpoint_evaluation_supports_positive_recursion() {
        let kernel = RelationKernel::new();
        kernel
            .create_relation(RelationMetadata::new(rel(70), Symbol::intern("Edge"), 2))
            .unwrap();
        let mut tx = kernel.begin();
        tx.assert(rel(70), Tuple::from([int(1), int(2)])).unwrap();
        tx.assert(rel(70), Tuple::from([int(2), int(3)])).unwrap();
        tx.assert(rel(70), Tuple::from([int(3), int(4)])).unwrap();

        let reachable_base = Rule::new(
            rel(71),
            [var("a"), var("b")],
            [Atom::positive(rel(70), [var("a"), var("b")])],
        );
        let reachable_step = Rule::new(
            rel(71),
            [var("a"), var("c")],
            [
                Atom::positive(rel(70), [var("a"), var("b")]),
                Atom::positive(rel(71), [var("b"), var("c")]),
            ],
        );

        assert_eq!(
            RuleSet::new([reachable_base, reachable_step])
                .evaluate_fixpoint(&tx)
                .unwrap()[&rel(71)],
            vec![
                Tuple::from([int(1), int(2)]),
                Tuple::from([int(1), int(3)]),
                Tuple::from([int(1), int(4)]),
                Tuple::from([int(2), int(3)]),
                Tuple::from([int(2), int(4)]),
                Tuple::from([int(3), int(4)]),
            ]
        );
    }
}
