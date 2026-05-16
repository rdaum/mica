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

use crate::{KernelError, RelationId, RelationRead, ScanControl, Tuple};
use mica_var::{Identity, Symbol, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap};

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
            let rules = compile_rules(&rules);
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
            let rules = compile_rules(&rules);
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

type Binding = Vec<Option<Value>>;

#[derive(Clone, Debug, Eq, PartialEq)]
struct CompiledRule {
    head_relation: RelationId,
    head_terms: Vec<CompiledTerm>,
    body: Vec<CompiledAtom>,
    slot_count: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CompiledAtom {
    relation: RelationId,
    terms: Vec<CompiledTerm>,
    negated: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum CompiledTerm {
    Var { symbol: Symbol, slot: usize },
    Value(Value),
}

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

    fn visit_relation(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
        visitor: &mut dyn FnMut(&Tuple) -> Result<ScanControl, KernelError>,
    ) -> Result<(), KernelError> {
        if !self.derived.contains_key(&relation) {
            return match self.base.visit_relation(relation, bindings, visitor) {
                Ok(()) => Ok(()),
                Err(KernelError::UnknownRelation(unknown)) if unknown == relation => Ok(()),
                Err(error) => Err(error),
            };
        }

        for tuple in self.scan_relation(relation, bindings)? {
            if visitor(&tuple)? == ScanControl::Stop {
                break;
            }
        }
        Ok(())
    }

    fn estimate_relation_scan(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
    ) -> Result<Option<usize>, KernelError> {
        let base_estimate = match self.base.estimate_relation_scan(relation, bindings) {
            Ok(estimate) => estimate,
            Err(KernelError::UnknownRelation(unknown)) if unknown == relation => Some(0),
            Err(error) => return Err(error),
        };
        let derived_estimate = self
            .derived
            .get(&relation)
            .map(|tuples| {
                tuples
                    .iter()
                    .filter(|tuple| tuple.matches_bindings(bindings))
                    .count()
            })
            .unwrap_or(0);
        Ok(base_estimate.map(|estimate| estimate + derived_estimate))
    }
}

fn evaluate_rules_once(
    reader: &impl RelationRead,
    rules: &[CompiledRule],
    out: &mut BTreeMap<RelationId, BTreeSet<Tuple>>,
) -> Result<(), RuleEvalError> {
    for rule in rules {
        for binding in evaluate_body(reader, rule)? {
            out.entry(rule.head_relation)
                .or_default()
                .insert(instantiate_head(rule, &binding)?);
        }
    }
    Ok(())
}

fn compile_rules(rules: &[&Rule]) -> Vec<CompiledRule> {
    rules.iter().map(|rule| compile_rule(rule)).collect()
}

fn compile_rule(rule: &Rule) -> CompiledRule {
    let mut variables = HashMap::new();
    let head_terms = rule
        .head_terms
        .iter()
        .map(|term| compile_term(term, &mut variables))
        .collect();
    let body = rule
        .body
        .iter()
        .map(|atom| CompiledAtom {
            relation: atom.relation,
            terms: atom
                .terms
                .iter()
                .map(|term| compile_term(term, &mut variables))
                .collect(),
            negated: atom.negated,
        })
        .collect();
    CompiledRule {
        head_relation: rule.head_relation,
        head_terms,
        body,
        slot_count: variables.len(),
    }
}

fn compile_term(term: &Term, variables: &mut HashMap<Symbol, usize>) -> CompiledTerm {
    match term {
        Term::Value(value) => CompiledTerm::Value(value.clone()),
        Term::Var(symbol) => {
            let next_slot = variables.len();
            let slot = *variables.entry(*symbol).or_insert(next_slot);
            CompiledTerm::Var {
                symbol: *symbol,
                slot,
            }
        }
    }
}

fn evaluate_body(
    reader: &impl RelationRead,
    rule: &CompiledRule,
) -> Result<Vec<Binding>, RuleEvalError> {
    let mut bindings = vec![vec![None; rule.slot_count]];
    let mut remaining = rule.body.iter().collect::<Vec<_>>();
    while !remaining.is_empty() {
        let next = select_next_atom(reader, &bindings, &remaining)?;
        let atom = remaining.remove(next);
        bindings = if atom.negated {
            apply_negated_atom(reader, atom, bindings)?
        } else {
            apply_positive_atom(reader, atom, bindings)?
        };
    }
    Ok(bindings)
}

fn select_next_atom(
    reader: &impl RelationRead,
    bindings: &[Binding],
    atoms: &[&CompiledAtom],
) -> Result<usize, RuleEvalError> {
    let mut best = None;
    for (index, atom) in atoms.iter().enumerate() {
        if atom.negated
            && !bindings
                .iter()
                .all(|binding| negated_atom_is_safe(atom, binding))
        {
            continue;
        }
        let estimate = atom_estimate(reader, atom, bindings)?;
        let bound_terms = bindings
            .iter()
            .map(|binding| bound_term_count(atom, binding))
            .max()
            .unwrap_or(0);
        let rank = (
            estimate,
            usize::from(atom.negated),
            usize::MAX - bound_terms,
            index,
        );
        if best.is_none_or(|(_, best_rank)| rank < best_rank) {
            best = Some((index, rank));
        }
    }
    best.map(|(index, _)| index).ok_or_else(|| {
        RuleError::UnsafeNegation {
            relation: atoms[0].relation,
        }
        .into()
    })
}

fn atom_estimate(
    reader: &impl RelationRead,
    atom: &CompiledAtom,
    bindings: &[Binding],
) -> Result<usize, RuleEvalError> {
    let mut total = 0usize;
    for binding in bindings {
        let scan_bindings = scan_bindings(atom, binding)?;
        total = total.saturating_add(
            reader
                .estimate_relation_scan(atom.relation, &scan_bindings)?
                .unwrap_or(usize::MAX / 4),
        );
    }
    Ok(total)
}

fn bound_term_count(atom: &CompiledAtom, binding: &Binding) -> usize {
    atom.terms
        .iter()
        .filter(|term| match term {
            CompiledTerm::Value(_) => true,
            CompiledTerm::Var { slot, .. } => binding[*slot].is_some(),
        })
        .count()
}

fn negated_atom_is_safe(atom: &CompiledAtom, binding: &Binding) -> bool {
    atom.terms.iter().all(|term| match term {
        CompiledTerm::Value(_) => true,
        CompiledTerm::Var { slot, .. } => binding[*slot].is_some(),
    })
}

fn apply_positive_atom(
    reader: &impl RelationRead,
    atom: &CompiledAtom,
    bindings: Vec<Binding>,
) -> Result<Vec<Binding>, RuleEvalError> {
    let mut out = Vec::new();
    for binding in bindings {
        let scan_bindings = scan_bindings(atom, &binding)?;
        reader.visit_relation(atom.relation, &scan_bindings, &mut |tuple| {
            if let Some(next) = unify_tuple(atom, &binding, tuple) {
                out.push(next);
            }
            Ok(ScanControl::Continue)
        })?;
    }
    Ok(out)
}

fn apply_negated_atom(
    reader: &impl RelationRead,
    atom: &CompiledAtom,
    bindings: Vec<Binding>,
) -> Result<Vec<Binding>, RuleEvalError> {
    let mut out = Vec::new();
    for binding in bindings {
        ensure_negation_safe(atom, &binding)?;
        let scan_bindings = scan_bindings(atom, &binding)?;
        let mut found = false;
        reader.visit_relation(atom.relation, &scan_bindings, &mut |_| {
            found = true;
            Ok(ScanControl::Stop)
        })?;
        if !found {
            out.push(binding);
        }
    }
    Ok(out)
}

fn scan_bindings(
    atom: &CompiledAtom,
    binding: &Binding,
) -> Result<Vec<Option<Value>>, RuleEvalError> {
    let mut out = Vec::with_capacity(atom.terms.len());
    for term in &atom.terms {
        out.push(match term {
            CompiledTerm::Value(value) => Some(value.clone()),
            CompiledTerm::Var { slot, .. } => binding[*slot].clone(),
        });
    }
    Ok(out)
}

fn unify_tuple(atom: &CompiledAtom, binding: &Binding, tuple: &Tuple) -> Option<Binding> {
    let mut next = binding.clone();
    for (term, value) in atom.terms.iter().zip(tuple.values()) {
        match term {
            CompiledTerm::Value(expected) if expected != value => return None,
            CompiledTerm::Value(_) => {}
            CompiledTerm::Var { slot, .. } => {
                if let Some(bound) = &next[*slot] {
                    if bound != value {
                        return None;
                    }
                } else {
                    next[*slot] = Some(value.clone());
                }
            }
        }
    }
    Some(next)
}

fn ensure_negation_safe(atom: &CompiledAtom, binding: &Binding) -> Result<(), RuleEvalError> {
    if negated_atom_is_safe(atom, binding) {
        return Ok(());
    }

    Err(RuleError::UnsafeNegation {
        relation: atom.relation,
    }
    .into())
}

fn instantiate_head(rule: &CompiledRule, binding: &Binding) -> Result<Tuple, RuleEvalError> {
    let mut values = Vec::with_capacity(rule.head_terms.len());
    for term in &rule.head_terms {
        values.push(match term {
            CompiledTerm::Value(value) => value.clone(),
            CompiledTerm::Var { symbol, slot } => binding[*slot]
                .clone()
                .ok_or(RuleError::UnboundHeadVariable { variable: *symbol })?,
        });
    }
    Ok(Tuple::new(values))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RelationKernel, RelationMetadata};
    use mica_var::Identity;
    use std::cell::RefCell;

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

    struct PlanningReader {
        scanned: RefCell<Vec<RelationId>>,
    }

    impl RelationRead for PlanningReader {
        fn scan_relation(
            &self,
            relation: RelationId,
            bindings: &[Option<Value>],
        ) -> Result<Vec<Tuple>, KernelError> {
            self.scanned.borrow_mut().push(relation);
            match relation {
                relation if relation == rel(80) => {
                    assert_eq!(bindings, &[None]);
                    Ok(vec![Tuple::from([int(1)])])
                }
                relation if relation == rel(81) => {
                    assert_eq!(bindings, &[Some(int(1)), None]);
                    Ok(vec![Tuple::from([int(1), int(2)])])
                }
                _ => panic!("unexpected relation scan: {relation:?}"),
            }
        }

        fn estimate_relation_scan(
            &self,
            relation: RelationId,
            bindings: &[Option<Value>],
        ) -> Result<Option<usize>, KernelError> {
            let estimate = match relation {
                relation if relation == rel(80) => 1,
                relation if relation == rel(81) && bindings == [None, None] => 1_000,
                relation if relation == rel(81) => 1,
                _ => 1_000,
            };
            Ok(Some(estimate))
        }
    }

    #[test]
    fn rule_body_planner_starts_with_selective_atom_not_source_order() {
        let reader = PlanningReader {
            scanned: RefCell::new(Vec::new()),
        };
        let rule = compile_rule(&Rule::new(
            rel(82),
            [var("y")],
            [
                Atom::positive(rel(81), [var("x"), var("y")]),
                Atom::positive(rel(80), [var("x")]),
            ],
        ));
        let bindings = evaluate_body(&reader, &rule).unwrap();

        assert_eq!(reader.scanned.borrow().as_slice(), &[rel(80), rel(81)]);
        assert_eq!(
            instantiate_head(&rule, &bindings[0]).unwrap(),
            Tuple::from([int(2)])
        );
    }

    #[test]
    fn rule_evaluation_enforces_repeated_variables_in_one_atom() {
        let kernel = RelationKernel::new();
        kernel
            .create_relation(RelationMetadata::new(rel(90), Symbol::intern("Pair"), 2))
            .unwrap();
        let mut tx = kernel.begin();
        tx.assert(rel(90), Tuple::from([int(1), int(1)])).unwrap();
        tx.assert(rel(90), Tuple::from([int(1), int(2)])).unwrap();

        let same = Rule::new(
            rel(91),
            [var("x")],
            [Atom::positive(rel(90), [var("x"), var("x")])],
        );

        assert_eq!(
            RuleSet::new([same]).evaluate(&tx).unwrap()[&rel(91)],
            vec![Tuple::from([int(1)])]
        );
    }

    #[test]
    fn rule_evaluation_rejects_unbound_head_variable() {
        let kernel = RelationKernel::new();
        kernel
            .create_relation(RelationMetadata::new(rel(92), Symbol::intern("Source"), 1))
            .unwrap();
        let mut tx = kernel.begin();
        tx.assert(rel(92), Tuple::from([int(1)])).unwrap();

        let invalid = Rule::new(
            rel(93),
            [var("missing")],
            [Atom::positive(rel(92), [var("present")])],
        );

        assert_eq!(
            RuleSet::new([invalid]).evaluate(&tx),
            Err(RuleEvalError::Rule(RuleError::UnboundHeadVariable {
                variable: Symbol::intern("missing")
            }))
        );
    }

    struct VisitOnlyReader;

    impl RelationRead for VisitOnlyReader {
        fn scan_relation(
            &self,
            _relation: RelationId,
            _bindings: &[Option<Value>],
        ) -> Result<Vec<Tuple>, KernelError> {
            panic!("rule atom application should use visit_relation")
        }

        fn visit_relation(
            &self,
            relation: RelationId,
            bindings: &[Option<Value>],
            visitor: &mut dyn FnMut(&Tuple) -> Result<ScanControl, KernelError>,
        ) -> Result<(), KernelError> {
            assert_eq!(relation, rel(94));
            assert_eq!(bindings, &[None]);
            visitor(&Tuple::from([int(1)]))?;
            Ok(())
        }
    }

    #[test]
    fn rule_atom_application_streams_relation_visits() {
        let rule = Rule::new(rel(95), [var("x")], [Atom::positive(rel(94), [var("x")])]);

        assert_eq!(
            RuleSet::new([rule]).evaluate(&VisitOnlyReader).unwrap()[&rel(95)],
            vec![Tuple::from([int(1)])]
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
