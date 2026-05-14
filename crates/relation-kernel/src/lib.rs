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

//! Live MVCC relation storage for Mica.
//!
//! This crate is the first relation-kernel slice: cataloged n-ary set
//! relations, transaction-local assert/retract overlays, snapshot reads,
//! commit-time conflict validation, catalog commits, rule evaluation, and a
//! pluggable commit provider boundary. It intentionally follows mooR's live
//! transaction shape while keeping physical index storage narrow and replaceable.

mod catalog;
mod closure;
mod commit_bloom;
mod dispatch;
mod error;
mod fact;
mod index;
mod kernel;
mod materialized;
mod metadata;
mod neighborhood;
mod provider;
mod query;
mod rules;
mod snapshot;
mod transaction;
mod transient;
mod tuple;

#[cfg(test)]
mod tests;

use mica_var::Identity;

pub use catalog::{CatalogFact, CatalogPredicate};
pub use closure::{delegates_star, delegates_star_from, materialize_delegates_star};
pub use dispatch::{DispatchRelations, applicable_methods};
pub use error::{Conflict, ConflictKind, KernelError};
pub use fact::Fact;
pub use kernel::RelationKernel;
pub use materialized::materialize_rule_set;
pub use metadata::{ConflictPolicy, RelationMetadata, RelationSchema, TupleIndexSpec};
pub use neighborhood::{MentionedFact, SubjectFact};
pub use provider::{
    CommitProvider, FjallDurabilityMode, FjallFormatStatus, FjallStateProvider,
    InMemoryCommitProvider, PersistedKernelState,
};
pub use query::{QueryPlan, RelationRead};
pub use rules::{Atom, Rule, RuleDefinition, RuleError, RuleEvalError, RuleSet, Term};
pub use snapshot::{CatalogChange, Commit, CommitResult, FactChange, FactChangeKind, Snapshot};
pub use transaction::Transaction;
pub use transient::{ComposedRelationRead, ComposedTransactionRead, TransientStore};
pub use tuple::Tuple;

pub type RelationId = Identity;
pub type FactId = Identity;
pub type Version = u64;
