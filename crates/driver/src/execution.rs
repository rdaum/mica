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

use mica_runtime::ExecutionBudget;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

pub(crate) struct DispatcherExecutionBudget {
    worker_count: NonZeroUsize,
    claimed_workers: AtomicUsize,
}

impl DispatcherExecutionBudget {
    pub(crate) fn new(worker_count: NonZeroUsize) -> Self {
        Self {
            worker_count,
            claimed_workers: AtomicUsize::new(0),
        }
    }

    pub(crate) fn enter_dispatch(self: &Arc<Self>) -> DispatcherPermit {
        self.claimed_workers.fetch_add(1, Ordering::AcqRel);
        DispatcherPermit {
            budget: Arc::clone(self),
        }
    }

    fn release_workers(&self, workers: NonZeroUsize) {
        let previous = self
            .claimed_workers
            .fetch_sub(workers.get(), Ordering::AcqRel);
        debug_assert!(previous >= workers.get());
    }

    #[cfg(test)]
    fn claimed_workers(&self) -> usize {
        self.claimed_workers.load(Ordering::Acquire)
    }
}

impl ExecutionBudget for DispatcherExecutionBudget {
    fn try_reserve(&self, additional_workers: NonZeroUsize) -> bool {
        self.claimed_workers
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |claimed| {
                claimed
                    .checked_add(additional_workers.get())
                    .filter(|next| *next <= self.worker_count.get())
            })
            .is_ok()
    }

    fn release(&self, additional_workers: NonZeroUsize) {
        self.release_workers(additional_workers);
    }
}

pub(crate) struct DispatcherPermit {
    budget: Arc<DispatcherExecutionBudget>,
}

impl Drop for DispatcherPermit {
    fn drop(&mut self) {
        self.budget.release_workers(NonZeroUsize::new(1).unwrap());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dispatched_and_parallel_work_share_one_capacity_limit() {
        let budget = Arc::new(DispatcherExecutionBudget::new(
            NonZeroUsize::new(4).unwrap(),
        ));
        let first_dispatch = budget.enter_dispatch();
        let second_dispatch = budget.enter_dispatch();
        let parallel_workers = NonZeroUsize::new(2).unwrap();
        assert!(budget.try_reserve(parallel_workers));

        assert_eq!(budget.claimed_workers(), 4);
        assert!(!budget.try_reserve(NonZeroUsize::new(1).unwrap()));

        budget.release(parallel_workers);
        drop(first_dispatch);
        assert_eq!(budget.claimed_workers(), 1);
        let remaining_workers = NonZeroUsize::new(3).unwrap();
        assert!(budget.try_reserve(remaining_workers));
        budget.release(remaining_workers);
        drop(second_dispatch);
    }

    #[test]
    fn queued_dispatch_work_prevents_parallel_admission() {
        let budget = Arc::new(DispatcherExecutionBudget::new(
            NonZeroUsize::new(2).unwrap(),
        ));
        let first_dispatch = budget.enter_dispatch();
        let second_dispatch = budget.enter_dispatch();
        let parallel_worker = NonZeroUsize::new(1).unwrap();
        assert!(!budget.try_reserve(parallel_worker));

        drop(first_dispatch);
        assert!(budget.try_reserve(parallel_worker));
        budget.release(parallel_worker);
        drop(second_dispatch);
    }
}
