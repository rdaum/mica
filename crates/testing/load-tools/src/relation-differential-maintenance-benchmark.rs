// Copyright (C) 2026 Ryan Daum <ryan.daum@gmail.com>
//
// This program is free software: you can redistribute it and/or modify it under
// the terms of the GNU Affero General Public License as published by the Free
// Software Foundation, version 3.

//! Measures native and `wgpu` differential equality joins through committed maintenance.

use clap::{Parser, ValueEnum};
use mica_relation_kernel::metrics::{self, EqualityJoinAccelerationPlacement};
use mica_relation_kernel::{
    Atom, ExecutionContext, RelationId, RelationKernel, RelationMetadata, Rule, Term, Tuple,
};
use mica_relation_wgpu::{WgpuAccelerator, WgpuAcceleratorOptions};
use mica_var::{Identity, Symbol, Value};
use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

const LEFT_RELATION: u64 = 42_001;
const RIGHT_RELATION: u64 = 42_002;
const JOINED_RELATION: u64 = 42_003;

#[derive(Clone, Debug, Parser)]
#[command(
    name = "relation-differential-maintenance-benchmark",
    about = "Measure native and wgpu equality joins through committed differential maintenance"
)]
struct Args {
    #[arg(long, value_delimiter = ',', default_value = "258048")]
    full_rows: Vec<usize>,

    #[arg(long, value_delimiter = ',', default_value = "4096")]
    delta_rows: Vec<usize>,

    #[arg(long, default_value_t = 5)]
    iterations: usize,

    #[arg(long, default_value_t = 1)]
    warmup_iterations: usize,

    #[arg(long, value_enum, default_value_t = MatchRate::All)]
    match_rate: MatchRate,

    #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u16).range(1..=2))]
    key_columns: u16,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum MatchRate {
    None,
    All,
}

impl MatchRate {
    fn label(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::All => "all",
        }
    }

    fn output_rows(self, delta_rows: usize) -> usize {
        match self {
            Self::None => 0,
            Self::All => delta_rows,
        }
    }
}

fn main() -> Result<(), String> {
    let args = Args::parse();
    validate_args(&args)?;
    let accelerator = Arc::new(
        WgpuAccelerator::new(WgpuAcceleratorOptions::default())
            .map_err(|error| error.to_string())?,
    );
    println!(
        "adapter={:?} shared_mappable={}",
        accelerator.adapter_name(),
        accelerator.uses_shared_mappable_buffers(),
    );
    println!(
        "full_rows,delta_rows,key_columns,match_rate,operation,backend,residency,host_us,speedup_vs_native,accelerated,gpu_operator_mean_us,right_cache_hits,right_cache_misses"
    );

    for &full_rows in &args.full_rows {
        for &delta_rows in &args.delta_rows {
            if delta_rows > full_rows {
                return Err(format!(
                    "delta rows ({delta_rows}) must not exceed full rows ({full_rows})"
                ));
            }
            run_shape(&args, full_rows, delta_rows, Arc::clone(&accelerator))?;
        }
    }
    Ok(())
}

fn validate_args(args: &Args) -> Result<(), String> {
    if args.full_rows.is_empty()
        || args.delta_rows.is_empty()
        || args.full_rows.contains(&0)
        || args.delta_rows.contains(&0)
    {
        return Err("--full-rows and --delta-rows must contain non-zero sizes".to_owned());
    }
    if args.iterations == 0 {
        return Err("--iterations must be non-zero".to_owned());
    }
    Ok(())
}

fn run_shape(
    args: &Args,
    full_rows: usize,
    delta_rows: usize,
    accelerator: Arc<WgpuAccelerator>,
) -> Result<(), String> {
    let native = benchmark_backend(
        full_rows,
        delta_rows,
        args.iterations,
        args.warmup_iterations,
        args.match_rate,
        args.key_columns,
        ExecutionContext::serial(),
    )?;
    let accelerated = benchmark_backend(
        full_rows,
        delta_rows,
        args.iterations,
        args.warmup_iterations,
        args.match_rate,
        args.key_columns,
        ExecutionContext::serial()
            .with_accelerator(accelerator)
            .with_weighted_join_acceleration(),
    )?;

    print_result(
        full_rows,
        delta_rows,
        args.key_columns,
        args.match_rate,
        "assert",
        "native",
        "resident",
        native.assert_median,
        native.assert_median,
        false,
        GpuEvidence::default(),
    );
    print_result(
        full_rows,
        delta_rows,
        args.key_columns,
        args.match_rate,
        "retract",
        "native",
        "resident",
        native.retract_median,
        native.retract_median,
        false,
        GpuEvidence::default(),
    );
    print_result(
        full_rows,
        delta_rows,
        args.key_columns,
        args.match_rate,
        "assert",
        if accelerated.cold_accelerated {
            "wgpu"
        } else {
            "native_fallback"
        },
        "cold",
        accelerated.cold_assert,
        native.assert_median,
        accelerated.cold_accelerated,
        accelerated.cold_evidence,
    );
    print_result(
        full_rows,
        delta_rows,
        args.key_columns,
        args.match_rate,
        "assert",
        if accelerated.warm_accelerated {
            "wgpu"
        } else {
            "native_fallback"
        },
        "warm",
        accelerated.assert_median,
        native.assert_median,
        accelerated.warm_accelerated,
        accelerated.warm_evidence,
    );
    print_result(
        full_rows,
        delta_rows,
        args.key_columns,
        args.match_rate,
        "retract",
        if accelerated.warm_accelerated {
            "wgpu"
        } else {
            "native_fallback"
        },
        "warm",
        accelerated.retract_median,
        native.retract_median,
        accelerated.warm_accelerated,
        accelerated.warm_evidence,
    );
    Ok(())
}

struct BackendSamples {
    cold_assert: Duration,
    assert_median: Duration,
    retract_median: Duration,
    cold_accelerated: bool,
    warm_accelerated: bool,
    cold_evidence: GpuEvidence,
    warm_evidence: GpuEvidence,
}

#[derive(Clone, Copy, Default)]
struct GpuEvidence {
    operator_duration_us: u64,
    operator_count: u64,
    right_cache_hits: isize,
    right_cache_misses: isize,
}

impl GpuEvidence {
    fn since(self, earlier: Self) -> Self {
        Self {
            operator_duration_us: self
                .operator_duration_us
                .saturating_sub(earlier.operator_duration_us),
            operator_count: self.operator_count.saturating_sub(earlier.operator_count),
            right_cache_hits: self
                .right_cache_hits
                .saturating_sub(earlier.right_cache_hits),
            right_cache_misses: self
                .right_cache_misses
                .saturating_sub(earlier.right_cache_misses),
        }
    }

    fn operator_mean_us(self) -> f64 {
        if self.operator_count == 0 {
            return 0.0;
        }
        self.operator_duration_us as f64 / self.operator_count as f64
    }
}

fn benchmark_backend(
    full_rows: usize,
    delta_rows: usize,
    iterations: usize,
    warmup_iterations: usize,
    match_rate: MatchRate,
    key_columns: u16,
    execution_context: ExecutionContext,
) -> Result<BackendSamples, String> {
    let kernel = make_kernel(full_rows, key_columns, execution_context)?;
    let accelerated_before = accelerated_count();
    let cold_evidence_before = gpu_evidence();
    let cold_assert = apply_delta(
        &kernel,
        full_rows,
        delta_rows,
        match_rate,
        key_columns,
        true,
    )?;
    let cold_evidence = gpu_evidence().since(cold_evidence_before);
    let cold_accelerated = accelerated_count() > accelerated_before;
    verify_joined_rows(&kernel, match_rate.output_rows(delta_rows))?;
    apply_delta(
        &kernel,
        full_rows,
        delta_rows,
        match_rate,
        key_columns,
        false,
    )?;
    verify_joined_rows(&kernel, 0)?;

    for _ in 0..warmup_iterations {
        apply_delta(
            &kernel,
            full_rows,
            delta_rows,
            match_rate,
            key_columns,
            true,
        )?;
        verify_joined_rows(&kernel, match_rate.output_rows(delta_rows))?;
        apply_delta(
            &kernel,
            full_rows,
            delta_rows,
            match_rate,
            key_columns,
            false,
        )?;
        verify_joined_rows(&kernel, 0)?;
    }

    let accelerated_before = accelerated_count();
    let warm_evidence_before = gpu_evidence();
    let mut assertions = Vec::with_capacity(iterations);
    let mut retractions = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        assertions.push(apply_delta(
            &kernel,
            full_rows,
            delta_rows,
            match_rate,
            key_columns,
            true,
        )?);
        verify_joined_rows(&kernel, match_rate.output_rows(delta_rows))?;
        retractions.push(apply_delta(
            &kernel,
            full_rows,
            delta_rows,
            match_rate,
            key_columns,
            false,
        )?);
        verify_joined_rows(&kernel, 0)?;
    }
    let warm_accelerated = accelerated_count() > accelerated_before;
    let warm_evidence = gpu_evidence().since(warm_evidence_before);
    Ok(BackendSamples {
        cold_assert,
        assert_median: median(assertions),
        retract_median: median(retractions),
        cold_accelerated,
        warm_accelerated,
        cold_evidence,
        warm_evidence,
    })
}

fn make_kernel(
    full_rows: usize,
    key_columns: u16,
    execution_context: ExecutionContext,
) -> Result<RelationKernel, String> {
    let kernel = RelationKernel::new().with_execution_context(execution_context);
    for (relation_id, name) in [
        (LEFT_RELATION, "DifferentialLeft"),
        (RIGHT_RELATION, "DifferentialRight"),
        (JOINED_RELATION, "DifferentialJoined"),
    ] {
        kernel
            .create_relation(RelationMetadata::new(
                relation(relation_id),
                Symbol::intern(name),
                if relation_id == JOINED_RELATION {
                    2
                } else {
                    key_columns + 1
                },
            ))
            .map_err(|error| format!("{error:?}"))?;
    }
    let mut left_terms = vec![variable("from"), variable("key_first")];
    let mut right_terms = vec![variable("key_first")];
    if key_columns == 2 {
        left_terms.push(variable("key_second"));
        right_terms.push(variable("key_second"));
    }
    right_terms.push(variable("to"));
    kernel
        .install_rule(
            Rule::new(
                relation(JOINED_RELATION),
                [variable("from"), variable("to")],
                [
                    Atom::positive(relation(LEFT_RELATION), left_terms),
                    Atom::positive(relation(RIGHT_RELATION), right_terms),
                ],
            ),
            "DifferentialJoined(from, to) :- DifferentialLeft(from, key), DifferentialRight(key, to)",
        )
        .map_err(|error| format!("{error:?}"))?;

    let mut seed = kernel.begin();
    for row in 0..full_rows {
        let value = int_value(row)?;
        let mut values = vec![value.clone(), value];
        if key_columns == 2 {
            values.push(int_value(secondary_key(row))?);
        }
        seed.assert(relation(LEFT_RELATION), Tuple::new(values))
            .map_err(|error| format!("{error:?}"))?;
    }
    seed.commit().map_err(|error| format!("{error:?}"))?;
    verify_joined_rows(&kernel, 0)?;
    Ok(kernel)
}

fn apply_delta(
    kernel: &RelationKernel,
    full_rows: usize,
    delta_rows: usize,
    match_rate: MatchRate,
    key_columns: u16,
    asserting: bool,
) -> Result<Duration, String> {
    let mut tx = kernel.begin();
    for row in 0..delta_rows {
        let key = match match_rate {
            MatchRate::None => full_rows.checked_add(row).ok_or("row value overflow")?,
            MatchRate::All => row,
        };
        let mut values = vec![int_value(key)?];
        if key_columns == 2 {
            values.push(int_value(secondary_key(row))?);
        }
        values.push(int_value(
            row.checked_add(1_000_000).ok_or("row value overflow")?,
        )?);
        let tuple = Tuple::new(values);
        if asserting {
            tx.assert(relation(RIGHT_RELATION), tuple)
        } else {
            tx.retract(relation(RIGHT_RELATION), tuple)
        }
        .map_err(|error| format!("{error:?}"))?;
    }
    let started = Instant::now();
    black_box(tx.commit().map_err(|error| format!("{error:?}"))?);
    Ok(started.elapsed())
}

fn verify_joined_rows(kernel: &RelationKernel, expected: usize) -> Result<(), String> {
    let rows = kernel
        .snapshot()
        .scan(relation(JOINED_RELATION), &[None, None])
        .map_err(|error| format!("{error:?}"))?;
    if rows.len() == expected {
        return Ok(());
    }
    Err(format!(
        "differential join result mismatch: expected {expected} rows, received {}",
        rows.len()
    ))
}

fn median(mut samples: Vec<Duration>) -> Duration {
    samples.sort_unstable();
    samples[samples.len() / 2]
}

fn accelerated_count() -> isize {
    metrics::metrics()
        .equality_join_acceleration_placements
        .get(EqualityJoinAccelerationPlacement::Accelerated)
}

fn gpu_evidence() -> GpuEvidence {
    let metrics = mica_relation_wgpu::metrics();
    GpuEvidence {
        operator_duration_us: metrics.equality_join_duration_us.sum(),
        operator_count: metrics.equality_join_duration_us.count(),
        right_cache_hits: metrics.equality_join_right_cache_hits.sum(),
        right_cache_misses: metrics.equality_join_right_cache_misses.sum(),
    }
}

#[allow(clippy::too_many_arguments)]
fn print_result(
    full_rows: usize,
    delta_rows: usize,
    key_columns: u16,
    match_rate: MatchRate,
    operation: &str,
    backend: &str,
    residency: &str,
    duration: Duration,
    native: Duration,
    accelerated: bool,
    evidence: GpuEvidence,
) {
    println!(
        "{full_rows},{delta_rows},{key_columns},{},{operation},{backend},{residency},{:.3},{:.3},{accelerated},{:.3},{},{}",
        match_rate.label(),
        duration.as_secs_f64() * 1_000_000.0,
        native.as_secs_f64() / duration.as_secs_f64(),
        evidence.operator_mean_us(),
        evidence.right_cache_hits,
        evidence.right_cache_misses,
    );
}

fn relation(raw: u64) -> RelationId {
    Identity::new(raw).unwrap()
}

fn variable(name: &str) -> Term {
    Term::Var(Symbol::intern(name))
}

fn int_value(value: usize) -> Result<Value, String> {
    let value = i64::try_from(value).map_err(|_| "row value exceeds i64".to_owned())?;
    Value::int(value).map_err(|_| "row value exceeds the Mica integer range".to_owned())
}

fn secondary_key(row: usize) -> usize {
    row.rotate_left(7) ^ row.wrapping_mul(17)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn small_committed_workload_toggles_derived_rows() {
        for key_columns in [1, 2] {
            let kernel = make_kernel(32, key_columns, ExecutionContext::serial()).unwrap();
            apply_delta(&kernel, 32, 8, MatchRate::All, key_columns, true).unwrap();
            verify_joined_rows(&kernel, 8).unwrap();
            apply_delta(&kernel, 32, 8, MatchRate::All, key_columns, false).unwrap();
            verify_joined_rows(&kernel, 0).unwrap();

            apply_delta(&kernel, 32, 8, MatchRate::None, key_columns, true).unwrap();
            verify_joined_rows(&kernel, 0).unwrap();
            apply_delta(&kernel, 32, 8, MatchRate::None, key_columns, false).unwrap();
            verify_joined_rows(&kernel, 0).unwrap();
        }
    }
}
