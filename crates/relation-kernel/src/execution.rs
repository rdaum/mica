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

use mica_var::Value;
use rayon::{ThreadPool, ThreadPoolBuilder};
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::{Arc, OnceLock};

/// Admission control shared by task execution and parallel relation operators.
///
/// Implementations own the global CPU capacity policy. Relation operators never
/// wait for capacity: a declined reservation means they execute serially.
pub trait ExecutionAdmission: Send + Sync {
    fn capacity(&self) -> NonZeroUsize;

    fn try_reserve_parallel(&self, additional_workers: NonZeroUsize) -> bool;

    fn release_parallel(&self, additional_workers: NonZeroUsize);
}

/// Immutable columns participating in one equality-membership selection.
///
/// Implementations return indexes into `left` in ascending order. `right` is
/// not necessarily ordered by the selected relation column.
pub struct MembershipSelection<'a> {
    pub left: &'a Arc<[Value]>,
    pub right: &'a Arc<[Value]>,
    pub keep_matches: bool,
}

/// Immutable key columns participating in one equality join.
///
/// Each side contains one or two columns with equal row counts. Accelerators
/// return matching row pairs ordered by `left_row` and then `right_row`.
pub struct EqualityJoin<'a> {
    pub left: &'a [Arc<[Value]>],
    pub right: &'a [Arc<[Value]>],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EqualityJoinMatch {
    pub left_row: usize,
    pub right_row: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AccelerationDecline {
    Busy,
    UnsupportedInput,
    UnsupportedDomain,
    Unavailable,
    Failed,
}

#[derive(Debug, Eq, PartialEq)]
pub enum AccelerationOutcome<T> {
    Completed(T),
    Declined(AccelerationDecline),
}

/// Optional executor for large packed relation operators.
pub trait RelationAccelerator: Send + Sync {
    fn select_membership(
        &self,
        selection: MembershipSelection<'_>,
    ) -> AccelerationOutcome<Vec<usize>>;

    fn join_equality(&self, join: EqualityJoin<'_>) -> AccelerationOutcome<Vec<EqualityJoinMatch>>;
}

#[derive(Clone)]
pub struct ExecutionContext {
    parallel: Option<Arc<ParallelExecution>>,
    accelerator: Option<Arc<dyn RelationAccelerator>>,
}

impl ExecutionContext {
    pub fn serial() -> Self {
        Self {
            parallel: None,
            accelerator: None,
        }
    }

    pub fn parallel(admission: Arc<dyn ExecutionAdmission>) -> Self {
        let rayon_workers = admission.capacity().get().saturating_sub(1);
        if rayon_workers == 0 {
            return Self::serial();
        }
        Self {
            parallel: Some(Arc::new(ParallelExecution {
                admission,
                rayon_workers,
                pool: OnceLock::new(),
            })),
            accelerator: None,
        }
    }

    pub fn with_accelerator(mut self, accelerator: Arc<dyn RelationAccelerator>) -> Self {
        self.accelerator = Some(accelerator);
        self
    }

    pub(crate) fn has_accelerator(&self) -> bool {
        self.accelerator.is_some()
    }

    pub(crate) fn select_membership(
        &self,
        selection: MembershipSelection<'_>,
    ) -> AccelerationOutcome<Vec<usize>> {
        let Some(accelerator) = &self.accelerator else {
            return AccelerationOutcome::Declined(AccelerationDecline::Unavailable);
        };
        accelerator.select_membership(selection)
    }

    pub(crate) fn join_equality(
        &self,
        join: EqualityJoin<'_>,
    ) -> AccelerationOutcome<Vec<EqualityJoinMatch>> {
        let Some(accelerator) = &self.accelerator else {
            return AccelerationOutcome::Declined(AccelerationDecline::Unavailable);
        };
        accelerator.join_equality(join)
    }

    pub(crate) fn try_join<A, B, RA, RB>(
        &self,
        additional_workers: NonZeroUsize,
        left: A,
        right: B,
    ) -> Result<(RA, RB), ParallelUnavailable>
    where
        A: FnOnce() -> RA + Send,
        B: FnOnce() -> RB + Send,
        RA: Send,
        RB: Send,
    {
        let parallel = self
            .parallel
            .as_ref()
            .ok_or(ParallelUnavailable::NoExecutor)?;
        let Some(_reservation) =
            ParallelReservation::try_new(Arc::clone(&parallel.admission), additional_workers)
        else {
            return Err(ParallelUnavailable::Capacity);
        };
        let pool = parallel
            .pool
            .get_or_init(|| build_parallel_pool(parallel.rayon_workers))
            .as_ref()
            .map_err(|_| ParallelUnavailable::NoExecutor)?;
        Ok(pool.join(left, right))
    }
}

impl fmt::Debug for ExecutionContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExecutionContext")
            .field(
                "parallel_workers",
                &self
                    .parallel
                    .as_ref()
                    .map(|parallel| parallel.rayon_workers),
            )
            .field(
                "pool_started",
                &self
                    .parallel
                    .as_ref()
                    .is_some_and(|parallel| parallel.pool.get().is_some()),
            )
            .field("accelerator", &self.accelerator.is_some())
            .finish()
    }
}

struct ParallelExecution {
    admission: Arc<dyn ExecutionAdmission>,
    rayon_workers: usize,
    pool: OnceLock<Result<ThreadPool, String>>,
}

fn build_parallel_pool(worker_count: usize) -> Result<ThreadPool, String> {
    ThreadPoolBuilder::new()
        .num_threads(worker_count)
        .thread_name(|index| format!("mica-relation-pool-{index}"))
        .build()
        .map_err(|error| format!("failed to start relation worker pool: {error}"))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ParallelUnavailable {
    NoExecutor,
    Capacity,
}

struct ParallelReservation {
    admission: Arc<dyn ExecutionAdmission>,
    additional_workers: NonZeroUsize,
}

impl ParallelReservation {
    fn try_new(
        admission: Arc<dyn ExecutionAdmission>,
        additional_workers: NonZeroUsize,
    ) -> Option<Self> {
        admission
            .try_reserve_parallel(additional_workers)
            .then(|| Self {
                admission,
                additional_workers,
            })
    }
}

impl Drop for ParallelReservation {
    fn drop(&mut self) {
        self.admission.release_parallel(self.additional_workers);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct FixedAdmission {
        capacity: NonZeroUsize,
        available: Mutex<usize>,
    }

    impl ExecutionAdmission for FixedAdmission {
        fn capacity(&self) -> NonZeroUsize {
            self.capacity
        }

        fn try_reserve_parallel(&self, additional_workers: NonZeroUsize) -> bool {
            let mut available = self.available.lock().unwrap();
            let Some(remaining) = available.checked_sub(additional_workers.get()) else {
                return false;
            };
            *available = remaining;
            true
        }

        fn release_parallel(&self, additional_workers: NonZeroUsize) {
            *self.available.lock().unwrap() += additional_workers.get();
        }
    }

    fn admission(capacity: usize, available: usize) -> Arc<FixedAdmission> {
        Arc::new(FixedAdmission {
            capacity: NonZeroUsize::new(capacity).unwrap(),
            available: Mutex::new(available),
        })
    }

    #[test]
    fn serial_context_declines_parallel_work() {
        assert_eq!(
            ExecutionContext::serial().try_join(NonZeroUsize::new(2).unwrap(), || 1, || 2,),
            Err(ParallelUnavailable::NoExecutor)
        );
    }

    #[test]
    fn parallel_context_reserves_executes_and_releases_capacity() {
        let admission = admission(3, 2);
        let context = ExecutionContext::parallel(admission.clone());
        let result = context
            .try_join(NonZeroUsize::new(2).unwrap(), || 1, || 2)
            .unwrap();

        assert_eq!(result, (1, 2));
        assert_eq!(*admission.available.lock().unwrap(), 2);
    }

    #[test]
    fn parallel_context_declines_when_capacity_is_reserved() {
        let admission = admission(3, 1);
        let context = ExecutionContext::parallel(admission.clone());

        assert_eq!(
            context.try_join(NonZeroUsize::new(2).unwrap(), || 1, || 2),
            Err(ParallelUnavailable::Capacity)
        );
        assert_eq!(*admission.available.lock().unwrap(), 1);
    }

    #[test]
    fn kernel_copies_execution_context_into_transactions() {
        let admission = admission(3, 2);
        let execution_context = ExecutionContext::parallel(admission.clone());
        let kernel = crate::RelationKernel::new().with_execution_context(execution_context);
        let transaction = kernel.begin();

        assert_eq!(
            transaction
                .execution_context()
                .try_join(NonZeroUsize::new(2).unwrap(), || 1, || 2)
                .unwrap(),
            (1, 2)
        );
        assert_eq!(*admission.available.lock().unwrap(), 2);
    }
}
