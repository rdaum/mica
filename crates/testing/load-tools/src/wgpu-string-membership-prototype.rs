// Copyright (C) 2026 Ryan Daum <ryan.daum@gmail.com>
//
// This program is free software: you can redistribute it and/or modify it under
// the terms of the GNU Affero General Public License as published by the Free
// Software Foundation, version 3.

//! Measures pair-compatible dictionary encoding for string membership.
//!
//! Heap-backed columns are not eligible for production packed execution. The
//! reported amortization count is the gate for revisiting that boundary: it is
//! the number of executions over the same immutable column pair required to
//! recover dictionary construction cost relative to Rayon.

use clap::Parser;
use mica_relation_kernel::{AccelerationOutcome, MembershipSelection, RelationAccelerator};
use mica_relation_wgpu::{WgpuAccelerator, WgpuAcceleratorOptions};
use mica_var::Value;
use rayon::prelude::*;
use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Clone, Debug, Parser)]
#[command(
    name = "wgpu-string-membership-prototype",
    about = "Measure pair-compatible string dictionaries on CPU and wgpu"
)]
struct Args {
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "16384,131072,262144,1048576"
    )]
    rows: Vec<usize>,

    #[arg(long, default_value_t = 4_096)]
    cardinality: usize,

    #[arg(long, default_value_t = 5)]
    iterations: usize,

    #[arg(long, default_value_t = 2)]
    warmup_iterations: usize,
}

fn main() -> Result<(), String> {
    let args = Args::parse();
    if args.rows.is_empty() || args.rows.contains(&0) {
        return Err("--rows must contain only non-zero sizes".to_owned());
    }
    if args.cardinality == 0 {
        return Err("--cardinality must be non-zero".to_owned());
    }
    if args.iterations == 0 {
        return Err("--iterations must be non-zero".to_owned());
    }
    let accelerator = WgpuAccelerator::new(WgpuAcceleratorOptions::default())
        .map_err(|error| error.to_string())?;
    println!(
        "adapter={:?} shared_mappable={}",
        accelerator.adapter_name(),
        accelerator.uses_shared_mappable_buffers(),
    );
    println!(
        "rows,cardinality,hits,backend,residency,dictionary_us,host_us,speedup_vs_serial,amortization_runs_vs_rayon"
    );
    for &rows in &args.rows {
        run_size(&args, rows, &accelerator)?;
    }
    Ok(())
}

fn run_size(args: &Args, rows: usize, accelerator: &WgpuAccelerator) -> Result<(), String> {
    let cardinality = args.cardinality.min(rows);
    let (left, right) = make_inputs(rows, cardinality);
    let expected = membership_serial(&left, &right);
    let serial = benchmark_cpu(
        args.iterations,
        || membership_serial(&left, &right),
        &expected,
    );
    let parallel = benchmark_cpu(
        args.iterations,
        || membership_rayon(&left, &right),
        &expected,
    );

    let dictionary_before = mica_relation_wgpu::metrics()
        .encoded_column_duration_us
        .sum();
    let cold_started = Instant::now();
    let cold = execute_wgpu(accelerator, &left, &right)?;
    let cold_duration = cold_started.elapsed();
    let dictionary_us = mica_relation_wgpu::metrics()
        .encoded_column_duration_us
        .sum()
        .saturating_sub(dictionary_before);
    verify(&cold, &expected)?;
    for _ in 0..args.warmup_iterations {
        verify(&execute_wgpu(accelerator, &left, &right)?, &expected)?;
    }
    let mut warm_samples = Vec::with_capacity(args.iterations);
    for _ in 0..args.iterations {
        let started = Instant::now();
        let actual = execute_wgpu(accelerator, &left, &right)?;
        warm_samples.push(started.elapsed());
        verify(&actual, &expected)?;
    }
    let warm = median(&mut warm_samples);
    let amortization_runs = amortization_runs(cold_duration, warm, parallel);
    let shape = Shape {
        rows,
        cardinality,
        hits: expected.len(),
    };

    print_result(shape, "cpu_serial", "resident", serial, serial, None, None);
    print_result(shape, "cpu_rayon", "resident", parallel, serial, None, None);
    print_result(
        shape,
        "wgpu_dictionary",
        "cold",
        cold_duration,
        serial,
        amortization_runs,
        Some(dictionary_us),
    );
    print_result(
        shape,
        "wgpu_dictionary",
        "warm",
        warm,
        serial,
        amortization_runs,
        None,
    );
    Ok(())
}

fn make_inputs(rows: usize, cardinality: usize) -> (Arc<[Value]>, Arc<[Value]>) {
    let left = (0..rows)
        .map(|row| {
            if row % 2 == 0 {
                Value::string(format!(
                    "key-{:08}",
                    splitmix64(row as u64) % cardinality as u64
                ))
            } else {
                Value::string(format!(
                    "miss-{:08}",
                    splitmix64(row as u64) % cardinality as u64
                ))
            }
        })
        .collect::<Vec<_>>()
        .into();
    let mut right = (0..rows)
        .map(|row| Value::string(format!("key-{:08}", row % cardinality)))
        .collect::<Vec<_>>();
    right.sort_unstable();
    (left, right.into())
}

fn membership_serial(left: &[Value], right: &[Value]) -> Vec<usize> {
    left.iter()
        .enumerate()
        .filter_map(|(row, value)| right.binary_search(value).is_ok().then_some(row))
        .collect()
}

fn membership_rayon(left: &[Value], right: &[Value]) -> Vec<usize> {
    left.par_iter()
        .enumerate()
        .filter_map(|(row, value)| right.binary_search(value).is_ok().then_some(row))
        .collect()
}

fn benchmark_cpu(
    iterations: usize,
    execute: impl Fn() -> Vec<usize>,
    expected: &[usize],
) -> Duration {
    let mut samples = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let started = Instant::now();
        let actual = black_box(execute());
        samples.push(started.elapsed());
        assert_eq!(actual, expected);
    }
    median(&mut samples)
}

fn execute_wgpu(
    accelerator: &WgpuAccelerator,
    left: &Arc<[Value]>,
    right: &Arc<[Value]>,
) -> Result<Vec<usize>, String> {
    match accelerator.select_membership(MembershipSelection {
        left,
        right,
        keep_matches: true,
    }) {
        AccelerationOutcome::Selected(rows) => Ok(rows),
        AccelerationOutcome::Declined(reason) => {
            Err(format!("wgpu string membership was declined: {reason:?}"))
        }
    }
}

fn verify(actual: &[usize], expected: &[usize]) -> Result<(), String> {
    if actual == expected {
        return Ok(());
    }
    Err(format!(
        "string membership mismatch: expected {} rows, received {}",
        expected.len(),
        actual.len()
    ))
}

fn median(samples: &mut [Duration]) -> Duration {
    samples.sort_unstable();
    samples[samples.len() / 2]
}

fn amortization_runs(cold: Duration, warm: Duration, rayon: Duration) -> Option<u64> {
    if cold <= rayon {
        return Some(1);
    }
    if warm >= rayon {
        return None;
    }
    let cold = cold.as_secs_f64();
    let warm = warm.as_secs_f64();
    let rayon = rayon.as_secs_f64();
    Some(((cold - warm) / (rayon - warm)).floor() as u64 + 1)
}

#[derive(Clone, Copy)]
struct Shape {
    rows: usize,
    cardinality: usize,
    hits: usize,
}

fn print_result(
    shape: Shape,
    backend: &str,
    residency: &str,
    duration: Duration,
    serial: Duration,
    amortization_runs: Option<u64>,
    dictionary_us: Option<u64>,
) {
    println!(
        "{},{},{},{backend},{residency},{},{:.3},{:.3},{}",
        shape.rows,
        shape.cardinality,
        shape.hits,
        dictionary_us.map_or_else(String::new, |duration| duration.to_string()),
        duration.as_secs_f64() * 1_000_000.0,
        serial.as_secs_f64() / duration.as_secs_f64(),
        amortization_runs.map_or_else(String::new, |runs| runs.to_string()),
    );
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
    fn generated_inputs_have_half_matches() {
        let (left, right) = make_inputs(1_024, 64);
        assert_eq!(membership_serial(&left, &right).len(), 512);
        assert_eq!(membership_rayon(&left, &right).len(), 512);
    }

    #[test]
    fn amortization_requires_cold_cost_to_be_recovered() {
        assert_eq!(
            amortization_runs(
                Duration::from_millis(176),
                Duration::from_millis(2),
                Duration::from_millis(34),
            ),
            Some(6)
        );
        assert_eq!(
            amortization_runs(
                Duration::from_millis(20),
                Duration::from_millis(2),
                Duration::from_millis(34),
            ),
            Some(1)
        );
    }
}
