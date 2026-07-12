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

use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;

/// Admission control for relation operators that want additional CPU workers.
///
/// The executor implements this interface because it owns global worker
/// capacity. Relation operators reserve capacity through [`ExecutionContext`]
/// instead of inspecting executor-specific state.
pub trait ExecutionBudget: Send + Sync {
    fn try_reserve(&self, additional_workers: NonZeroUsize) -> bool;

    fn release(&self, additional_workers: NonZeroUsize);
}

#[derive(Clone)]
pub struct ExecutionContext {
    budget: Option<Arc<dyn ExecutionBudget>>,
}

impl ExecutionContext {
    pub fn serial() -> Self {
        Self { budget: None }
    }

    pub fn with_budget(budget: Arc<dyn ExecutionBudget>) -> Self {
        Self {
            budget: Some(budget),
        }
    }

    pub fn try_acquire(&self, additional_workers: NonZeroUsize) -> Option<ExecutionPermit> {
        let budget = self.budget.as_ref()?;
        if !budget.try_reserve(additional_workers) {
            return None;
        }
        Some(ExecutionPermit {
            budget: Arc::clone(budget),
            additional_workers,
        })
    }
}

impl fmt::Debug for ExecutionContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExecutionContext")
            .field("has_budget", &self.budget.is_some())
            .finish()
    }
}

pub struct ExecutionPermit {
    budget: Arc<dyn ExecutionBudget>,
    additional_workers: NonZeroUsize,
}

impl ExecutionPermit {
    pub fn additional_workers(&self) -> NonZeroUsize {
        self.additional_workers
    }
}

impl fmt::Debug for ExecutionPermit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExecutionPermit")
            .field("additional_workers", &self.additional_workers)
            .finish_non_exhaustive()
    }
}

impl Drop for ExecutionPermit {
    fn drop(&mut self) {
        self.budget.release(self.additional_workers);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct FixedBudget {
        available: AtomicUsize,
    }

    impl ExecutionBudget for FixedBudget {
        fn try_reserve(&self, additional_workers: NonZeroUsize) -> bool {
            self.available
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |available| {
                    available.checked_sub(additional_workers.get())
                })
                .is_ok()
        }

        fn release(&self, additional_workers: NonZeroUsize) {
            self.available
                .fetch_add(additional_workers.get(), Ordering::Release);
        }
    }

    #[test]
    fn serial_context_declines_parallel_work() {
        assert!(
            ExecutionContext::serial()
                .try_acquire(NonZeroUsize::new(1).unwrap())
                .is_none()
        );
    }

    #[test]
    fn permit_reserves_and_releases_capacity() {
        let budget = Arc::new(FixedBudget {
            available: AtomicUsize::new(2),
        });
        let context = ExecutionContext::with_budget(budget.clone());
        let permit = context.try_acquire(NonZeroUsize::new(2).unwrap()).unwrap();

        assert!(context.try_acquire(NonZeroUsize::new(1).unwrap()).is_none());
        drop(permit);
        assert_eq!(budget.available.load(Ordering::Acquire), 2);
    }

    #[test]
    fn kernel_copies_execution_context_into_transactions() {
        let budget = Arc::new(FixedBudget {
            available: AtomicUsize::new(1),
        });
        let kernel = crate::RelationKernel::new().with_execution_budget(budget.clone());
        let transaction = kernel.begin();
        let permit = transaction
            .execution_context()
            .try_acquire(NonZeroUsize::new(1).unwrap())
            .unwrap();

        assert_eq!(budget.available.load(Ordering::Acquire), 0);
        drop(permit);
        assert_eq!(budget.available.load(Ordering::Acquire), 1);
    }
}
