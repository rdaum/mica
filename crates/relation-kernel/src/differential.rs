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

use crate::index::RelationState;
use crate::rules::{
    CompiledAtom, CompiledBodyItem, CompiledGuard, CompiledRule, CompiledTerm, compare_values,
};
use crate::snapshot::active_rules;
use crate::{
    FactChange, FactChangeKind, KernelError, RelationId, RuleSet, Snapshot, Tuple, Version,
};
use mica_var::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

pub(crate) type Diff = i64;
type WeightedRows = BTreeMap<Tuple, Diff>;
type Binding = Vec<Option<Value>>;
type WeightedBindings = BTreeMap<Binding, Diff>;
const TRACE_COMPACTION_BATCHES: usize = 8;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct MaintenanceWork {
    pub(crate) input_changes: usize,
    pub(crate) affected_components: usize,
    pub(crate) candidate_changes: usize,
    pub(crate) consolidated_changes: usize,
    pub(crate) visible_changes: usize,
    pub(crate) arrangement_lookups: usize,
    pub(crate) rows_visited: usize,
    pub(crate) trace_batches: usize,
    pub(crate) trace_bytes: usize,
    pub(crate) compaction_rows: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct ArrangementSpec {
    relation: RelationId,
    positions: Vec<u16>,
}

#[derive(Clone, Debug)]
struct Arrangement {
    spec: ArrangementSpec,
    rows_by_key: BTreeMap<Vec<Value>, Arc<[Tuple]>>,
}

impl Arrangement {
    fn build(spec: ArrangementSpec, rows: &BTreeSet<Tuple>) -> Self {
        let mut grouped = BTreeMap::<Vec<Value>, Vec<Tuple>>::new();
        for tuple in rows {
            grouped
                .entry(arrangement_tuple_key(tuple, &spec.positions))
                .or_default()
                .push(tuple.clone());
        }
        Self {
            spec,
            rows_by_key: grouped
                .into_iter()
                .map(|(key, rows)| (key, Arc::from(rows)))
                .collect(),
        }
    }

    fn apply(&self, changes: &WeightedRows) -> Self {
        let mut rows_by_key = self.rows_by_key.clone();
        for (tuple, difference) in changes {
            let key = arrangement_tuple_key(tuple, &self.spec.positions);
            let mut rows = rows_by_key
                .get(&key)
                .map(|rows| rows.to_vec())
                .unwrap_or_default();
            match difference.cmp(&0) {
                std::cmp::Ordering::Greater => {
                    if let Err(position) = rows.binary_search(tuple) {
                        rows.insert(position, tuple.clone());
                    }
                }
                std::cmp::Ordering::Less => {
                    if let Ok(position) = rows.binary_search(tuple) {
                        rows.remove(position);
                    }
                }
                std::cmp::Ordering::Equal => {}
            }
            if rows.is_empty() {
                rows_by_key.remove(&key);
            } else {
                rows_by_key.insert(key, Arc::from(rows));
            }
        }
        Self {
            spec: self.spec.clone(),
            rows_by_key,
        }
    }

    fn lookup(&self, key: &[Value]) -> &[Tuple] {
        self.rows_by_key.get(key).map_or(&[], AsRef::as_ref)
    }
}

#[derive(Clone, Debug)]
struct TraceBatch {
    epoch: Version,
    rows: Arc<[Tuple]>,
    differences: Arc<[Diff]>,
    retained_bytes: usize,
}

impl TraceBatch {
    fn from_rows(epoch: Version, rows: impl IntoIterator<Item = (Tuple, Diff)>) -> Self {
        let (rows, differences): (Vec<_>, Vec<_>) = rows
            .into_iter()
            .filter(|(_, difference)| *difference != 0)
            .unzip();
        let retained_bytes = rows
            .iter()
            .map(|tuple| {
                std::mem::size_of::<Tuple>()
                    + tuple.arity() * std::mem::size_of::<Value>()
                    + std::mem::size_of::<Diff>()
            })
            .sum();
        Self {
            epoch,
            rows: rows.into(),
            differences: differences.into(),
            retained_bytes,
        }
    }

    fn from_set(epoch: Version, rows: &BTreeSet<Tuple>) -> Self {
        Self::from_rows(epoch, rows.iter().cloned().map(|tuple| (tuple, 1)))
    }
}

#[derive(Clone, Debug)]
struct Trace {
    base: Arc<TraceBatch>,
    batches: Arc<[Arc<TraceBatch>]>,
}

impl Trace {
    fn initialize(epoch: Version, rows: &BTreeSet<Tuple>) -> Self {
        Self {
            base: Arc::new(TraceBatch::from_set(epoch, rows)),
            batches: Arc::from([]),
        }
    }

    fn append(
        &self,
        epoch: Version,
        changes: &WeightedRows,
        current: &BTreeSet<Tuple>,
    ) -> (Self, usize) {
        debug_assert!(epoch > self.base.epoch);
        let batch = Arc::new(TraceBatch::from_rows(
            epoch,
            changes
                .iter()
                .map(|(tuple, difference)| (tuple.clone(), *difference)),
        ));
        let mut batches = self.batches.to_vec();
        batches.push(batch);
        let delta_bytes = batches
            .iter()
            .map(|batch| batch.retained_bytes)
            .sum::<usize>();
        let should_compact = batches.len() >= TRACE_COMPACTION_BATCHES
            || delta_bytes.saturating_mul(4) >= self.base.retained_bytes.max(1);
        if should_compact {
            return (Self::initialize(epoch, current), current.len());
        }
        (
            Self {
                base: Arc::clone(&self.base),
                batches: batches.into(),
            },
            0,
        )
    }

    fn batch_count(&self) -> usize {
        1 + self.batches.len()
    }

    fn retained_bytes(&self) -> usize {
        self.base.retained_bytes
            + self
                .batches
                .iter()
                .map(|batch| {
                    debug_assert_eq!(batch.rows.len(), batch.differences.len());
                    batch.retained_bytes
                })
                .sum::<usize>()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct MaintainedState {
    version: Version,
    program: Arc<MaintainedProgram>,
    requested_targets: BTreeSet<RelationId>,
    collections: BTreeMap<RelationId, BTreeSet<Tuple>>,
    derived_support: BTreeMap<RelationId, WeightedRows>,
    arrangements: BTreeMap<ArrangementSpec, Arc<Arrangement>>,
    traces: BTreeMap<RelationId, Trace>,
    #[cfg(test)]
    visible_changes: Arc<[FactChange]>,
    work: MaintenanceWork,
}

impl MaintainedState {
    pub(crate) fn initialize(
        snapshot: &Snapshot,
        complete: &BTreeMap<RelationId, RelationState>,
        requested_targets: BTreeSet<RelationId>,
    ) -> Result<Option<Arc<Self>>, KernelError> {
        let Some(program) = MaintainedProgram::compile(snapshot, &requested_targets)? else {
            return Ok(None);
        };
        let program = Arc::new(program);
        let mut collections = BTreeMap::new();
        for relation in &program.relations {
            collections.insert(*relation, extensional_rows(snapshot, *relation)?);
        }
        let mut derived_support = BTreeMap::new();
        for component in &program.components {
            let mut support = WeightedRows::new();
            for rule in &component.rules {
                let contribution = evaluate_rule_full(
                    rule,
                    &collections,
                    snapshot.version(),
                    &mut MaintenanceWork::default(),
                )?;
                accumulate_rows(
                    &mut support,
                    contribution,
                    component.target,
                    "rule union",
                    snapshot.version(),
                )?;
            }
            ensure_non_negative(&support, component.target, snapshot.version())?;
            let derived = positive_rows(&support);
            let extensional = collections.remove(&component.target).unwrap_or_default();
            collections.insert(
                component.target,
                extensional.union(&derived).cloned().collect(),
            );
            derived_support.insert(component.target, support);
        }

        if !matches_complete_output(&program, &derived_support, complete) {
            return Ok(None);
        }
        let arrangements = build_arrangements(&program.arrangement_specs, &collections);
        let traces = collections
            .iter()
            .map(|(relation, rows)| (*relation, Trace::initialize(snapshot.version(), rows)))
            .collect();
        Ok(Some(Arc::new(Self {
            version: snapshot.version(),
            program,
            requested_targets,
            collections,
            derived_support,
            arrangements,
            traces,
            #[cfg(test)]
            visible_changes: Arc::from([]),
            work: MaintenanceWork::default(),
        })))
    }

    pub(crate) fn advance(
        &self,
        current: &Snapshot,
        next: &Snapshot,
        fact_changes: &[FactChange],
    ) -> Result<Arc<Self>, KernelError> {
        debug_assert_eq!(self.version, current.version());
        let version = next.version();
        let mut work = MaintenanceWork {
            input_changes: fact_changes.len(),
            ..MaintenanceWork::default()
        };
        let mut collections = self.collections.clone();
        let mut derived_support = self.derived_support.clone();
        let mut arrangements = self.arrangements.clone();
        let mut relation_deltas = BTreeMap::<RelationId, WeightedRows>::new();
        let changed_by_relation = group_fact_changes(fact_changes);

        for (relation, changes) in &changed_by_relation {
            if !self.program.relations.contains(relation) {
                continue;
            }
            let old_support = self.derived_support.get(relation);
            let collection = collections.entry(*relation).or_default();
            let deltas = relation_deltas.entry(*relation).or_default();
            for change in changes {
                let old_visible = collection.contains(&change.tuple);
                let new_visible = extensional_contains(next, *relation, &change.tuple)?
                    || support_is_positive(old_support, &change.tuple);
                set_presence_delta(
                    collection,
                    deltas,
                    change.tuple.clone(),
                    old_visible,
                    new_visible,
                );
            }
            update_arrangements(&mut arrangements, *relation, deltas);
        }

        for component in &self.program.components {
            let body_changed = component.rules.iter().any(|rule| {
                rule_atoms(rule).any(|atom| {
                    relation_deltas
                        .get(&atom.relation)
                        .is_some_and(|changes| !changes.is_empty())
                })
            });
            let target_changed = changed_by_relation.contains_key(&component.target);
            if !body_changed && !target_changed {
                continue;
            }
            work.affected_components += 1;

            let mut support_delta = WeightedRows::new();
            if body_changed {
                let evaluation = DeltaEvaluation {
                    old_collections: &self.collections,
                    new_collections: &collections,
                    relation_deltas: &relation_deltas,
                    old_arrangements: &self.arrangements,
                    new_arrangements: &arrangements,
                    target: component.target,
                    version,
                };
                for rule in &component.rules {
                    let contribution = evaluate_rule_delta(rule, &evaluation, &mut work)?;
                    accumulate_rows(
                        &mut support_delta,
                        contribution,
                        component.target,
                        "rule union",
                        version,
                    )?;
                }
            }
            work.consolidated_changes += support_delta.len();

            let support = derived_support.entry(component.target).or_default();
            for (tuple, difference) in &support_delta {
                checked_accumulate(
                    support,
                    tuple.clone(),
                    *difference,
                    component.target,
                    "head contribution",
                    version,
                )?;
            }
            ensure_non_negative(support, component.target, version)?;

            let mut touched = support_delta.keys().cloned().collect::<BTreeSet<_>>();
            if let Some(changes) = changed_by_relation.get(&component.target) {
                touched.extend(changes.iter().map(|change| change.tuple.clone()));
            }
            let collection = collections.entry(component.target).or_default();
            let deltas = relation_deltas.entry(component.target).or_default();
            for tuple in touched {
                let old_visible = self
                    .collections
                    .get(&component.target)
                    .is_some_and(|rows| rows.contains(&tuple));
                let new_visible = extensional_contains(next, component.target, &tuple)?
                    || support_is_positive(Some(support), &tuple);
                set_presence_delta(collection, deltas, tuple, old_visible, new_visible);
            }
            update_arrangements(&mut arrangements, component.target, deltas);
        }

        let mut traces = self.traces.clone();
        for (relation, changes) in &relation_deltas {
            if changes.is_empty() {
                continue;
            }
            let trace = traces
                .get(relation)
                .expect("maintained relation should have a trace");
            let (next_trace, compacted_rows) = trace.append(
                version,
                changes,
                collections
                    .get(relation)
                    .expect("maintained relation should have current rows"),
            );
            work.compaction_rows += compacted_rows;
            traces.insert(*relation, next_trace);
        }
        work.trace_batches = traces.values().map(Trace::batch_count).sum();
        work.trace_bytes = traces.values().map(Trace::retained_bytes).sum();

        let visible_changes = relation_deltas
            .into_iter()
            .flat_map(|(relation, changes)| {
                changes
                    .into_iter()
                    .map(move |(tuple, difference)| FactChange {
                        relation,
                        tuple,
                        kind: if difference > 0 {
                            FactChangeKind::Assert
                        } else {
                            FactChangeKind::Retract
                        },
                    })
            })
            .collect::<Vec<_>>();
        work.visible_changes = visible_changes.len();

        Ok(Arc::new(Self {
            version,
            program: Arc::clone(&self.program),
            requested_targets: self.requested_targets.clone(),
            collections,
            derived_support,
            arrangements,
            traces,
            #[cfg(test)]
            visible_changes: visible_changes.into(),
            work,
        }))
    }

    pub(crate) fn build_derived_relations(
        &self,
        snapshot: &Snapshot,
    ) -> Result<BTreeMap<RelationId, RelationState>, KernelError> {
        let derived = self
            .derived_support
            .iter()
            .map(|(relation, support)| (*relation, positive_rows(support).into_iter().collect()))
            .collect();
        crate::snapshot::build_derived_relations(&snapshot.relations, derived)
    }

    #[cfg(test)]
    pub(crate) fn version(&self) -> Version {
        self.version
    }

    #[cfg(test)]
    pub(crate) fn visible_changes(&self) -> &[FactChange] {
        &self.visible_changes
    }

    pub(crate) fn work(&self) -> &MaintenanceWork {
        &self.work
    }

    pub(crate) fn serves(&self, relation: RelationId) -> bool {
        self.program.targets.contains(&relation)
    }

    pub(crate) fn requested_targets(&self) -> &BTreeSet<RelationId> {
        &self.requested_targets
    }

    #[cfg(test)]
    pub(crate) fn trace_batch_count(&self, relation: RelationId) -> Option<usize> {
        self.traces.get(&relation).map(Trace::batch_count)
    }

    #[cfg(test)]
    pub(crate) fn arrangement_count(&self) -> usize {
        self.arrangements.len()
    }
}

#[derive(Clone, Debug)]
struct MaintainedProgram {
    components: Vec<MaintainedComponent>,
    relations: BTreeSet<RelationId>,
    targets: BTreeSet<RelationId>,
    arrangement_specs: BTreeSet<ArrangementSpec>,
}

impl MaintainedProgram {
    fn compile(
        snapshot: &Snapshot,
        requested_targets: &BTreeSet<RelationId>,
    ) -> Result<Option<Self>, KernelError> {
        let rules = active_rules(snapshot.rules());
        if rules.is_empty() {
            return Ok(None);
        }
        let rules = RuleSet::new(rules);
        let compiled = rules.compile().map_err(KernelError::Rule)?;
        let all_targets = compiled
            .strata
            .iter()
            .flat_map(|stratum| &stratum.components)
            .flat_map(|component| component.target_relations.iter().copied())
            .collect::<BTreeSet<_>>();
        let mut required_targets = requested_targets
            .intersection(&all_targets)
            .copied()
            .collect::<BTreeSet<_>>();
        if required_targets.is_empty() {
            return Ok(None);
        }
        loop {
            let before = required_targets.len();
            for stratum in &compiled.strata {
                for component in &stratum.components {
                    if component.target_relations.is_disjoint(&required_targets) {
                        continue;
                    }
                    for rule_index in &component.rule_indices {
                        required_targets.extend(
                            rule_atoms(&stratum.rules[*rule_index])
                                .map(|atom| atom.relation)
                                .filter(|relation| all_targets.contains(relation)),
                        );
                    }
                }
            }
            if required_targets.len() == before {
                break;
            }
        }
        let mut relations = BTreeSet::new();
        let mut components = Vec::new();
        for stratum in &compiled.strata {
            for component in &stratum.components {
                if component.target_relations.is_disjoint(&required_targets) {
                    continue;
                }
                if !component.recursive_variants.is_empty() || component.target_relations.len() != 1
                {
                    return Ok(None);
                }
                let target = *component.target_relations.first().unwrap();
                let rules = component
                    .rule_indices
                    .iter()
                    .map(|index| stratum.rules[*index].clone())
                    .collect::<Vec<_>>();
                if rules
                    .iter()
                    .flat_map(|rule| &rule.body)
                    .any(|item| matches!(item, CompiledBodyItem::Atom(atom) if atom.negated))
                {
                    return Ok(None);
                }
                relations.insert(target);
                relations.extend(rules.iter().flat_map(rule_atoms).map(|atom| atom.relation));
                components.push(MaintainedComponent { target, rules });
            }
        }
        for relation in &relations {
            let state = snapshot
                .relations
                .get(relation)
                .ok_or(KernelError::UnknownRelation(*relation))?;
            if snapshot
                .computed_relations
                .is_computed_relation(state.metadata())
            {
                return Ok(None);
            }
        }
        let arrangement_specs = components
            .iter()
            .flat_map(|component| &component.rules)
            .flat_map(required_arrangements)
            .collect();

        Ok(Some(Self {
            components,
            relations,
            targets: required_targets,
            arrangement_specs,
        }))
    }
}

#[derive(Clone, Debug)]
struct MaintainedComponent {
    target: RelationId,
    rules: Vec<CompiledRule>,
}

fn required_arrangements(rule: &CompiledRule) -> BTreeSet<ArrangementSpec> {
    let mut arrangements = BTreeSet::new();
    let atoms = rule_atoms(rule).collect::<Vec<_>>();
    for first in 0..atoms.len().max(1) {
        let mut bound_slots = BTreeSet::new();
        let order = std::iter::once(first).chain((0..atoms.len()).filter(|index| *index != first));
        for index in order {
            let Some(atom) = atoms.get(index) else {
                continue;
            };
            let positions = atom
                .terms
                .iter()
                .enumerate()
                .filter_map(|(position, term)| match term {
                    CompiledTerm::Value(_) => Some(position as u16),
                    CompiledTerm::Var { slot, .. } if bound_slots.contains(slot) => {
                        Some(position as u16)
                    }
                    CompiledTerm::Var { .. } => None,
                })
                .collect::<Vec<_>>();
            if !positions.is_empty() {
                arrangements.insert(ArrangementSpec {
                    relation: atom.relation,
                    positions,
                });
            }
            bound_slots.extend(atom.terms.iter().filter_map(|term| match term {
                CompiledTerm::Var { slot, .. } => Some(*slot),
                CompiledTerm::Value(_) => None,
            }));
        }
    }
    arrangements
}

fn build_arrangements(
    specs: &BTreeSet<ArrangementSpec>,
    collections: &BTreeMap<RelationId, BTreeSet<Tuple>>,
) -> BTreeMap<ArrangementSpec, Arc<Arrangement>> {
    specs
        .iter()
        .cloned()
        .map(|spec| {
            let rows = collections
                .get(&spec.relation)
                .expect("arranged relation should have a maintained collection");
            let arrangement = Arc::new(Arrangement::build(spec.clone(), rows));
            (spec, arrangement)
        })
        .collect()
}

fn update_arrangements(
    arrangements: &mut BTreeMap<ArrangementSpec, Arc<Arrangement>>,
    relation: RelationId,
    changes: &WeightedRows,
) {
    if changes.is_empty() {
        return;
    }
    let specs = arrangements
        .keys()
        .filter(|spec| spec.relation == relation)
        .cloned()
        .collect::<Vec<_>>();
    for spec in specs {
        let next = arrangements[&spec].apply(changes);
        arrangements.insert(spec, Arc::new(next));
    }
}

fn arrangement_tuple_key(tuple: &Tuple, positions: &[u16]) -> Vec<Value> {
    positions
        .iter()
        .map(|position| tuple.values()[*position as usize].clone())
        .collect()
}

fn rule_atoms(rule: &CompiledRule) -> impl Iterator<Item = &CompiledAtom> {
    rule.body.iter().filter_map(|item| match item {
        CompiledBodyItem::Atom(atom) => Some(atom),
        CompiledBodyItem::Guard(_) => None,
    })
}

fn evaluate_rule_full(
    rule: &CompiledRule,
    collections: &BTreeMap<RelationId, BTreeSet<Tuple>>,
    version: Version,
    work: &mut MaintenanceWork,
) -> Result<WeightedRows, KernelError> {
    let mut bindings = unit_binding(rule.slot_count);
    for atom in rule_atoms(rule) {
        let rows = collections
            .get(&atom.relation)
            .expect("compiled relation should have a maintained collection");
        bindings = join_full(bindings, atom, rows, rule.head_relation, version)?;
    }
    finish_rule(rule, bindings, version, work)
}

struct DeltaEvaluation<'a> {
    old_collections: &'a BTreeMap<RelationId, BTreeSet<Tuple>>,
    new_collections: &'a BTreeMap<RelationId, BTreeSet<Tuple>>,
    relation_deltas: &'a BTreeMap<RelationId, WeightedRows>,
    old_arrangements: &'a BTreeMap<ArrangementSpec, Arc<Arrangement>>,
    new_arrangements: &'a BTreeMap<ArrangementSpec, Arc<Arrangement>>,
    target: RelationId,
    version: Version,
}

fn evaluate_rule_delta(
    rule: &CompiledRule,
    evaluation: &DeltaEvaluation<'_>,
    work: &mut MaintenanceWork,
) -> Result<WeightedRows, KernelError> {
    let mut output = WeightedRows::new();
    let mut atoms = rule_atoms(rule).collect::<Vec<_>>();
    if let Some((first, _)) = atoms.iter().enumerate().min_by_key(|(_, atom)| {
        evaluation
            .relation_deltas
            .get(&atom.relation)
            .map_or(usize::MAX, BTreeMap::len)
    }) && evaluation
        .relation_deltas
        .get(&atoms[first].relation)
        .is_some_and(|delta| !delta.is_empty())
    {
        let first = atoms.remove(first);
        atoms.insert(0, first);
    }
    // Expanding one changed atom at a time as NEW * DELTA * OLD accounts for every term in
    // the product exactly once, including commits that change more than one input relation.
    for pivot in 0..atoms.len() {
        let delta = evaluation.relation_deltas.get(&atoms[pivot].relation);
        if delta.is_none_or(BTreeMap::is_empty) {
            continue;
        }
        let mut bindings = unit_binding(rule.slot_count);
        for (index, atom) in atoms.iter().enumerate() {
            bindings = match index.cmp(&pivot) {
                std::cmp::Ordering::Less => join_arranged(
                    bindings,
                    atom,
                    evaluation
                        .new_collections
                        .get(&atom.relation)
                        .expect("compiled relation should have a maintained collection"),
                    evaluation.new_arrangements,
                    evaluation.target,
                    evaluation.version,
                    work,
                )?,
                std::cmp::Ordering::Equal => join_weighted(
                    bindings,
                    atom,
                    delta.unwrap(),
                    evaluation.target,
                    evaluation.version,
                    work,
                )?,
                std::cmp::Ordering::Greater => join_arranged(
                    bindings,
                    atom,
                    evaluation
                        .old_collections
                        .get(&atom.relation)
                        .expect("compiled relation should have a maintained collection"),
                    evaluation.old_arrangements,
                    evaluation.target,
                    evaluation.version,
                    work,
                )?,
            };
            if bindings.is_empty() {
                break;
            }
        }
        let contribution = finish_rule(rule, bindings, evaluation.version, work)?;
        accumulate_rows(
            &mut output,
            contribution,
            evaluation.target,
            "delta union",
            evaluation.version,
        )?;
    }
    Ok(output)
}

fn unit_binding(slot_count: usize) -> WeightedBindings {
    BTreeMap::from([(vec![None; slot_count], 1)])
}

fn join_full(
    bindings: WeightedBindings,
    atom: &CompiledAtom,
    rows: &BTreeSet<Tuple>,
    target: RelationId,
    version: Version,
) -> Result<WeightedBindings, KernelError> {
    let weighted = rows.iter().map(|tuple| (tuple, 1));
    join_rows(bindings, atom, weighted, target, version)
}

fn join_weighted(
    bindings: WeightedBindings,
    atom: &CompiledAtom,
    rows: &WeightedRows,
    target: RelationId,
    version: Version,
    work: &mut MaintenanceWork,
) -> Result<WeightedBindings, KernelError> {
    work.rows_visited = work
        .rows_visited
        .saturating_add(bindings.len().saturating_mul(rows.len()));
    join_rows(
        bindings,
        atom,
        rows.iter().map(|(tuple, difference)| (tuple, *difference)),
        target,
        version,
    )
}

fn join_arranged(
    bindings: WeightedBindings,
    atom: &CompiledAtom,
    rows: &BTreeSet<Tuple>,
    arrangements: &BTreeMap<ArrangementSpec, Arc<Arrangement>>,
    target: RelationId,
    version: Version,
    work: &mut MaintenanceWork,
) -> Result<WeightedBindings, KernelError> {
    let mut output = WeightedBindings::new();
    for (binding, binding_difference) in bindings {
        let key = arrangement_binding_key(atom, &binding);
        let arranged = key.as_ref().and_then(|(positions, key)| {
            let spec = ArrangementSpec {
                relation: atom.relation,
                positions: positions.clone(),
            };
            arrangements
                .get(&spec)
                .map(|arrangement| arrangement.lookup(key))
        });
        if let Some(candidates) = arranged {
            work.arrangement_lookups += 1;
            work.rows_visited = work.rows_visited.saturating_add(candidates.len());
            join_candidates(
                &mut output,
                atom,
                &binding,
                binding_difference,
                candidates.iter(),
                target,
                version,
            )?;
        } else {
            work.rows_visited = work.rows_visited.saturating_add(rows.len());
            join_candidates(
                &mut output,
                atom,
                &binding,
                binding_difference,
                rows.iter(),
                target,
                version,
            )?;
        }
    }
    Ok(output)
}

fn join_candidates<'a>(
    output: &mut WeightedBindings,
    atom: &CompiledAtom,
    binding: &Binding,
    binding_difference: Diff,
    candidates: impl IntoIterator<Item = &'a Tuple>,
    target: RelationId,
    version: Version,
) -> Result<(), KernelError> {
    for tuple in candidates {
        let Some(next) = unify(atom, binding, tuple) else {
            continue;
        };
        checked_accumulate(
            output,
            next,
            binding_difference,
            target,
            "arranged join",
            version,
        )?;
    }
    Ok(())
}

fn arrangement_binding_key(
    atom: &CompiledAtom,
    binding: &Binding,
) -> Option<(Vec<u16>, Vec<Value>)> {
    let mut positions = Vec::new();
    let mut key = Vec::new();
    for (position, term) in atom.terms.iter().enumerate() {
        match term {
            CompiledTerm::Value(value) => {
                positions.push(position as u16);
                key.push(value.clone());
            }
            CompiledTerm::Var { slot, .. } if let Some(value) = &binding[*slot] => {
                positions.push(position as u16);
                key.push(value.clone());
            }
            CompiledTerm::Var { .. } => {}
        }
    }
    (!positions.is_empty()).then_some((positions, key))
}

fn join_rows<'a>(
    bindings: WeightedBindings,
    atom: &CompiledAtom,
    rows: impl IntoIterator<Item = (&'a Tuple, Diff)> + Clone,
    target: RelationId,
    version: Version,
) -> Result<WeightedBindings, KernelError> {
    let mut output = WeightedBindings::new();
    for (binding, binding_difference) in bindings {
        for (tuple, tuple_difference) in rows.clone() {
            let Some(next) = unify(atom, &binding, tuple) else {
                continue;
            };
            let difference = checked_multiply(
                binding_difference,
                tuple_difference,
                target,
                "join",
                version,
            )?;
            checked_accumulate(&mut output, next, difference, target, "join", version)?;
        }
    }
    Ok(output)
}

fn unify(atom: &CompiledAtom, binding: &Binding, tuple: &Tuple) -> Option<Binding> {
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

fn finish_rule(
    rule: &CompiledRule,
    bindings: WeightedBindings,
    version: Version,
    work: &mut MaintenanceWork,
) -> Result<WeightedRows, KernelError> {
    let mut output = WeightedRows::new();
    for (binding, difference) in bindings {
        let guards_match = rule.body.iter().all(|item| match item {
            CompiledBodyItem::Atom(_) => true,
            CompiledBodyItem::Guard(guard) => guard_matches(guard, &binding),
        });
        if !guards_match {
            continue;
        }
        let tuple = instantiate_head(rule, &binding)?;
        work.candidate_changes += 1;
        checked_accumulate(
            &mut output,
            tuple,
            difference,
            rule.head_relation,
            "projection",
            version,
        )?;
    }
    Ok(output)
}

fn guard_matches(guard: &CompiledGuard, binding: &Binding) -> bool {
    let Some(left) = term_value(&guard.left, binding) else {
        return false;
    };
    let Some(right) = term_value(&guard.right, binding) else {
        return false;
    };
    compare_values(guard.op, left, right)
}

fn term_value<'a>(term: &'a CompiledTerm, binding: &'a Binding) -> Option<&'a Value> {
    match term {
        CompiledTerm::Value(value) => Some(value),
        CompiledTerm::Var { slot, .. } => binding[*slot].as_ref(),
    }
}

fn instantiate_head(rule: &CompiledRule, binding: &Binding) -> Result<Tuple, KernelError> {
    let mut values = Vec::with_capacity(rule.head_terms.len());
    for term in &rule.head_terms {
        match term {
            CompiledTerm::Value(value) => values.push(value.clone()),
            CompiledTerm::Var { symbol, slot } => {
                let value = binding[*slot].clone().ok_or(KernelError::Rule(
                    crate::RuleError::UnboundHeadVariable { variable: *symbol },
                ))?;
                values.push(value);
            }
        }
    }
    Ok(Tuple::new(values))
}

fn checked_multiply(
    left: Diff,
    right: Diff,
    relation: RelationId,
    operation: &'static str,
    version: Version,
) -> Result<Diff, KernelError> {
    left.checked_mul(right)
        .ok_or(KernelError::DifferentialWeightOverflow {
            relation,
            operation,
            version,
            left,
            right,
        })
}

fn checked_accumulate<K: Ord>(
    values: &mut BTreeMap<K, Diff>,
    key: K,
    difference: Diff,
    relation: RelationId,
    operation: &'static str,
    version: Version,
) -> Result<(), KernelError> {
    if difference == 0 {
        return Ok(());
    }
    match values.entry(key) {
        std::collections::btree_map::Entry::Vacant(entry) => {
            entry.insert(difference);
        }
        std::collections::btree_map::Entry::Occupied(mut entry) => {
            let next = entry.get().checked_add(difference).ok_or(
                KernelError::DifferentialWeightOverflow {
                    relation,
                    operation,
                    version,
                    left: *entry.get(),
                    right: difference,
                },
            )?;
            if next == 0 {
                entry.remove();
            } else {
                *entry.get_mut() = next;
            }
        }
    }
    Ok(())
}

fn accumulate_rows(
    target: &mut WeightedRows,
    rows: WeightedRows,
    relation: RelationId,
    operation: &'static str,
    version: Version,
) -> Result<(), KernelError> {
    for (tuple, difference) in rows {
        checked_accumulate(target, tuple, difference, relation, operation, version)?;
    }
    Ok(())
}

fn ensure_non_negative(
    support: &WeightedRows,
    relation: RelationId,
    version: Version,
) -> Result<(), KernelError> {
    if let Some((tuple, difference)) = support.iter().find(|(_, difference)| **difference < 0) {
        return Err(KernelError::NegativeDifferentialSupport {
            relation,
            tuple: tuple.clone(),
            version,
            support: *difference,
        });
    }
    Ok(())
}

fn positive_rows(support: &WeightedRows) -> BTreeSet<Tuple> {
    support
        .iter()
        .filter(|(_, difference)| **difference > 0)
        .map(|(tuple, _)| tuple.clone())
        .collect()
}

fn support_is_positive(support: Option<&WeightedRows>, tuple: &Tuple) -> bool {
    support
        .and_then(|support| support.get(tuple))
        .is_some_and(|difference| *difference > 0)
}

fn group_fact_changes(changes: &[FactChange]) -> BTreeMap<RelationId, Vec<&FactChange>> {
    let mut grouped = BTreeMap::<RelationId, Vec<&FactChange>>::new();
    for change in changes {
        grouped.entry(change.relation).or_default().push(change);
    }
    grouped
}

fn set_presence_delta(
    collection: &mut BTreeSet<Tuple>,
    deltas: &mut WeightedRows,
    tuple: Tuple,
    old_visible: bool,
    new_visible: bool,
) {
    match (old_visible, new_visible) {
        (false, true) => {
            collection.insert(tuple.clone());
            deltas.insert(tuple, 1);
        }
        (true, false) => {
            collection.remove(&tuple);
            deltas.insert(tuple, -1);
        }
        _ => {
            deltas.remove(&tuple);
        }
    }
}

fn extensional_rows(
    snapshot: &Snapshot,
    relation: RelationId,
) -> Result<BTreeSet<Tuple>, KernelError> {
    let state = snapshot
        .relations
        .get(&relation)
        .ok_or(KernelError::UnknownRelation(relation))?;
    Ok(state
        .scan(&vec![None; state.metadata().arity() as usize])?
        .into_iter()
        .collect())
}

fn extensional_contains(
    snapshot: &Snapshot,
    relation: RelationId,
    tuple: &Tuple,
) -> Result<bool, KernelError> {
    Ok(snapshot
        .relations
        .get(&relation)
        .ok_or(KernelError::UnknownRelation(relation))?
        .contains_tuple(tuple))
}

fn matches_complete_output(
    program: &MaintainedProgram,
    support: &BTreeMap<RelationId, WeightedRows>,
    complete: &BTreeMap<RelationId, RelationState>,
) -> bool {
    program.components.iter().all(|component| {
        let incremental = support
            .get(&component.target)
            .map(positive_rows)
            .unwrap_or_default()
            .into_iter()
            .collect::<Vec<_>>();
        let complete = complete
            .get(&component.target)
            .map(|state| {
                state
                    .scan(&vec![None; state.metadata().arity() as usize])
                    .unwrap_or_default()
            })
            .unwrap_or_default();
        incremental == complete
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use mica_var::Identity;

    fn relation() -> RelationId {
        Identity::new(900).unwrap()
    }

    #[test]
    fn differential_arithmetic_reports_multiplication_overflow() {
        assert_eq!(
            checked_multiply(i64::MAX, 2, relation(), "join", 17).unwrap_err(),
            KernelError::DifferentialWeightOverflow {
                relation: relation(),
                operation: "join",
                version: 17,
                left: i64::MAX,
                right: 2,
            }
        );
    }

    #[test]
    fn differential_arithmetic_reports_consolidation_overflow() {
        let mut values = BTreeMap::from([(Tuple::from([Value::int(1).unwrap()]), i64::MAX)]);
        assert_eq!(
            checked_accumulate(
                &mut values,
                Tuple::from([Value::int(1).unwrap()]),
                1,
                relation(),
                "projection",
                18,
            )
            .unwrap_err(),
            KernelError::DifferentialWeightOverflow {
                relation: relation(),
                operation: "projection",
                version: 18,
                left: i64::MAX,
                right: 1,
            }
        );
    }

    #[test]
    fn settled_support_cannot_be_negative() {
        let tuple = Tuple::from([Value::int(1).unwrap()]);
        assert_eq!(
            ensure_non_negative(&BTreeMap::from([(tuple.clone(), -1)]), relation(), 19)
                .unwrap_err(),
            KernelError::NegativeDifferentialSupport {
                relation: relation(),
                tuple,
                version: 19,
                support: -1,
            }
        );
    }
}
