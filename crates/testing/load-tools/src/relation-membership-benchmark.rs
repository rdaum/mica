// Copyright (C) 2026 Ryan Daum <ryan.daum@gmail.com>
//
// This program is free software: you can redistribute it and/or modify it under
// the terms of the GNU Affero General Public License as published by the Free
// Software Foundation, version 3.

//! Measures membership selection through the production packed-query path.

use clap::{Parser, ValueEnum};
use mica_relation_kernel::metrics::{self, MembershipAccelerationPlacement};
use mica_relation_kernel::{
    ExecutionAdmission, ExecutionContext, KernelError, PackedRelation, PreparedQuery, QueryPlan,
    RelationCapabilities, RelationId, RelationRead, RelationSource, Tuple, ValueDomain,
};
use mica_relation_wgpu::{WgpuAccelerator, WgpuAcceleratorOptions};
use mica_var::{Identity, Value};
use std::hint::black_box;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

const LEFT_RELATION: u64 = 40_001;
const RIGHT_RELATION: u64 = 40_002;

#[derive(Clone, Debug, Parser)]
#[command(
    name = "relation-membership-benchmark",
    about = "Measure CPU and wgpu membership through PackedRelation execution"
)]
struct Args {
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "16384,65536,131072,262144,524288,1048576,4194304"
    )]
    rows: Vec<usize>,

    #[arg(long, default_value_t = 7)]
    iterations: usize,

    #[arg(long, default_value_t = 2)]
    warmup_iterations: usize,

    #[arg(long, value_enum, default_value_t = ProbeDomain::Int)]
    value_domain: ProbeDomain,

    #[arg(long, value_enum, default_value_t = MatchRate::Half)]
    match_rate: MatchRate,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum ProbeDomain {
    Int,
    Identity,
    Float,
}

impl ProbeDomain {
    fn label(self) -> &'static str {
        match self {
            Self::Int => "int",
            Self::Identity => "identity",
            Self::Float => "float",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum MatchRate {
    None,
    Half,
    All,
}

impl MatchRate {
    fn label(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Half => "half",
            Self::All => "all",
        }
    }
}

struct PackedReader {
    left: Arc<PackedRelation>,
    right: Arc<PackedRelation>,
}

impl PackedReader {
    fn relation(&self, relation: RelationId) -> Result<&Arc<PackedRelation>, KernelError> {
        match relation.raw() {
            LEFT_RELATION => Ok(&self.left),
            RIGHT_RELATION => Ok(&self.right),
            _ => Err(KernelError::UnknownRelation(relation)),
        }
    }
}

impl RelationRead for PackedReader {
    fn scan_relation(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
    ) -> Result<Vec<Tuple>, KernelError> {
        debug_assert!(bindings.iter().all(Option::is_none));
        Ok(self.relation(relation)?.rows().to_vec())
    }

    fn relation_capabilities(
        &self,
        relation: RelationId,
    ) -> Result<RelationCapabilities, KernelError> {
        let batch = self.relation(relation)?;
        Ok(RelationCapabilities {
            source: RelationSource::Snapshot,
            cardinality: Some(batch.row_count()),
            exact_indexes: Vec::new(),
            value_domains: vec![ValueDomain::Immediate; batch.columns().len()],
            supports_streaming: true,
            supports_batch_export: true,
        })
    }

    fn export_relation_batch(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
    ) -> Result<Option<Arc<PackedRelation>>, KernelError> {
        debug_assert!(bindings.iter().all(Option::is_none));
        Ok(Some(Arc::clone(self.relation(relation)?)))
    }
}

struct BenchmarkAdmission {
    capacity: NonZeroUsize,
}

impl ExecutionAdmission for BenchmarkAdmission {
    fn capacity(&self) -> NonZeroUsize {
        self.capacity
    }

    fn try_reserve_parallel(&self, _additional_workers: NonZeroUsize) -> bool {
        true
    }

    fn release_parallel(&self, _additional_workers: NonZeroUsize) {}
}

fn main() -> Result<(), String> {
    let args = Args::parse();
    if args.rows.is_empty() || args.rows.contains(&0) {
        return Err("--rows must contain only non-zero sizes".to_owned());
    }
    if args.iterations == 0 {
        return Err("--iterations must be non-zero".to_owned());
    }
    let accelerator = Arc::new(
        WgpuAccelerator::new(WgpuAcceleratorOptions::default())
            .map_err(|error| error.to_string())?,
    );
    println!(
        "adapter={:?} shared_mappable={}",
        accelerator.adapter_name(),
        accelerator.uses_shared_mappable_buffers(),
    );
    println!("domain,match_rate,rows,hits,backend,residency,host_us,speedup_vs_serial");

    for &rows in &args.rows {
        run_size(&args, rows, Arc::clone(&accelerator))?;
    }
    Ok(())
}

fn run_size(args: &Args, rows: usize, accelerator: Arc<WgpuAccelerator>) -> Result<(), String> {
    let (reader, query) = make_workload(rows, args.value_domain, args.match_rate)?;
    let serial_context = ExecutionContext::serial();
    let expected = query
        .execute(&reader, &serial_context)
        .map_err(|error| format!("{error:?}"))?;
    let (serial, _) = benchmark(args.iterations, &query, &reader, &serial_context, &expected)?;

    let capacity = std::thread::available_parallelism()
        .unwrap_or(NonZeroUsize::new(1).unwrap())
        .max(NonZeroUsize::new(2).unwrap());
    let parallel_context = ExecutionContext::parallel(Arc::new(BenchmarkAdmission { capacity }));
    let (parallel, _) = benchmark(
        args.iterations,
        &query,
        &reader,
        &parallel_context,
        &expected,
    )?;

    let accelerated_context = parallel_context.with_accelerator(accelerator);
    let accelerated_before = accelerated_count();
    let cold_started = Instant::now();
    let cold_rows = black_box(
        query
            .execute(&reader, &accelerated_context)
            .map_err(|error| format!("{error:?}"))?,
    );
    let cold = cold_started.elapsed();
    let cold_accelerated = accelerated_count() > accelerated_before;
    verify_rows(&cold_rows, &expected)?;
    for _ in 0..args.warmup_iterations {
        let rows = query
            .execute(&reader, &accelerated_context)
            .map_err(|error| format!("{error:?}"))?;
        verify_rows(&rows, &expected)?;
    }
    let (warm, warm_accelerated) = benchmark(
        args.iterations,
        &query,
        &reader,
        &accelerated_context,
        &expected,
    )?;

    let shape = Shape {
        domain: args.value_domain,
        match_rate: args.match_rate,
        rows,
        hits: expected.len(),
    };
    print_result(shape, "cpu_serial", "resident", serial, serial);
    print_result(shape, "cpu_parallel", "resident", parallel, serial);
    print_result(
        shape,
        if cold_accelerated {
            "wgpu"
        } else {
            "cpu_fallback"
        },
        "cold",
        cold,
        serial,
    );
    print_result(
        shape,
        if warm_accelerated {
            "wgpu"
        } else {
            "cpu_fallback"
        },
        "warm",
        warm,
        serial,
    );
    Ok(())
}

fn make_workload(
    rows: usize,
    domain: ProbeDomain,
    match_rate: MatchRate,
) -> Result<(PackedReader, PreparedQuery), String> {
    let left = (0..rows)
        .map(|row| {
            let mixed = splitmix64(row as u64) % rows as u64;
            let probe = match match_rate {
                MatchRate::None => mixed * 4 + 2,
                MatchRate::Half => splitmix64(row as u64) % ((rows as u64) * 2) * 2,
                MatchRate::All => mixed * 4,
            };
            Tuple::from([
                Value::int(row as i64).expect("row id should fit in a Mica integer"),
                probe_value(domain, probe),
            ])
        })
        .collect();
    let right = (0..rows)
        .map(|row| Tuple::from([probe_value(domain, (row as u64) * 4)]))
        .collect();
    let left = PackedRelation::from_canonical_tuples(left, 2)
        .ok_or_else(|| "left input could not be packed".to_owned())?;
    let right = PackedRelation::from_canonical_tuples(right, 1)
        .ok_or_else(|| "right input could not be packed".to_owned())?;
    let query = QueryPlan::semi_join(
        QueryPlan::scan(relation(LEFT_RELATION), [None, None]),
        QueryPlan::scan(relation(RIGHT_RELATION), [None]),
        [1],
        [0],
    )
    .prepare();
    Ok((
        PackedReader {
            left: Arc::new(left),
            right: Arc::new(right),
        },
        query,
    ))
}

fn benchmark(
    iterations: usize,
    query: &PreparedQuery,
    reader: &PackedReader,
    execution_context: &ExecutionContext,
    expected: &[Tuple],
) -> Result<(Duration, bool), String> {
    let mut samples = Vec::with_capacity(iterations);
    let accelerated_before = accelerated_count();
    for _ in 0..iterations {
        let started = Instant::now();
        let rows = black_box(
            query
                .execute(reader, execution_context)
                .map_err(|error| format!("{error:?}"))?,
        );
        samples.push(started.elapsed());
        verify_rows(&rows, expected)?;
    }
    samples.sort_unstable();
    Ok((
        samples[samples.len() / 2],
        accelerated_count() > accelerated_before,
    ))
}

fn accelerated_count() -> isize {
    metrics::metrics()
        .membership_acceleration_placements
        .get(MembershipAccelerationPlacement::Accelerated)
}

fn verify_rows(actual: &[Tuple], expected: &[Tuple]) -> Result<(), String> {
    if actual == expected {
        return Ok(());
    }
    Err(format!(
        "membership result mismatch: expected {} rows, received {}",
        expected.len(),
        actual.len()
    ))
}

#[derive(Clone, Copy)]
struct Shape {
    domain: ProbeDomain,
    match_rate: MatchRate,
    rows: usize,
    hits: usize,
}

fn print_result(
    shape: Shape,
    backend: &str,
    residency: &str,
    duration: Duration,
    serial: Duration,
) {
    println!(
        "{},{},{},{},{backend},{residency},{:.3},{:.3}",
        shape.domain.label(),
        shape.match_rate.label(),
        shape.rows,
        shape.hits,
        duration.as_secs_f64() * 1_000_000.0,
        serial.as_secs_f64() / duration.as_secs_f64(),
    );
}

fn relation(raw: u64) -> RelationId {
    Identity::new(raw).unwrap()
}

fn probe_value(domain: ProbeDomain, value: u64) -> Value {
    match domain {
        ProbeDomain::Int => Value::int(value as i64).expect("probe should fit in a Mica integer"),
        ProbeDomain::Identity => {
            Value::identity(Identity::new(value).expect("probe should fit in a Mica identity"))
        }
        ProbeDomain::Float => Value::float(value as f64),
    }
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packed_workload_has_requested_match_rates() {
        for (match_rate, expected) in [
            (MatchRate::None, 0),
            (MatchRate::Half, 512),
            (MatchRate::All, 1_024),
        ] {
            let (reader, query) = make_workload(1_024, ProbeDomain::Identity, match_rate).unwrap();
            let rows = query.execute(&reader, &ExecutionContext::serial()).unwrap();
            if match_rate == MatchRate::Half {
                assert!(rows.len().abs_diff(expected) < 64);
            } else {
                assert_eq!(rows.len(), expected);
            }
        }
    }
}
