// Copyright (C) 2026 Ryan Daum <ryan.daum@gmail.com> This program is free
// software: you can redistribute it and/or modify it under the terms of the GNU
// Affero General Public License as published by the Free Software Foundation,
// version 3.

use mica_var::Value;
use mica_var::abi::VALUE_ABI_VERSION;
use mica_var::language_cmp;
use mica_vm_cranelift::{CompiledFloatLoop, FloatLoopOutcome};
use std::sync::Arc;

fn float(value: f32) -> Value {
    Value::float(value).unwrap()
}

fn interpreted_float_loop(start: f32, step: f32, limit: f32) -> (Value, u64) {
    let mut current = float(start);
    let step = float(step);
    let limit = float(limit);
    let mut iterations = 0;
    loop {
        current = current.checked_add(&step).unwrap();
        iterations += 1;
        if !language_cmp::numeric_cmp(&current, &limit).is_lt() {
            return (current, iterations);
        }
    }
}

#[test]
fn generated_float_loop_matches_binary32_value_arithmetic_and_comparison() {
    let compiled = CompiledFloatLoop::compile().unwrap();
    for start in [-10.0f32, -1.5, 0.0, 1.5, 10.0] {
        for step in [0.25f32, 0.5, 1.5] {
            for distance in [0.5f32, 1.0, 4.0, 16.0] {
                let limit = start + distance;
                let (expected, iterations) = interpreted_float_loop(start, step, limit);
                assert_eq!(
                    compiled.run(&float(start), &float(step), &float(limit), iterations + 1),
                    FloatLoopOutcome::Complete {
                        current: expected,
                        condition: Value::bool(false),
                        iterations,
                    },
                );
            }
        }
    }
}

#[test]
fn generated_float_loop_reports_budget_exhaustion_at_a_branch_boundary() {
    let compiled = CompiledFloatLoop::compile().unwrap();
    assert_eq!(
        compiled.run(&float(0.0), &float(0.5), &float(100.0), 17),
        FloatLoopOutcome::BudgetExhausted {
            current: float(8.5),
            condition: Value::bool(true),
            iterations: 17,
        },
    );
}

#[test]
fn generated_float_loop_side_exits_for_non_floats_and_non_finite_results() {
    let compiled = CompiledFloatLoop::compile().unwrap();
    assert_eq!(
        compiled.run(&Value::int(1).unwrap(), &float(1.0), &float(10.0), 10),
        FloatLoopOutcome::SideExit,
    );
    assert_eq!(
        compiled.run(&float(f32::MAX), &float(f32::MAX), &float(f32::MAX), 1),
        FloatLoopOutcome::SideExit,
    );
}

#[test]
fn generated_float_loop_records_abi_and_codegen_properties() {
    let compiled = CompiledFloatLoop::compile().unwrap();
    assert_eq!(compiled.value_abi_version(), VALUE_ABI_VERSION);
    assert!(compiled.code_size() > 0);
    assert_eq!(compiled.imported_helper_count(), 0);
}

#[test]
fn generated_float_loop_can_execute_concurrently() {
    let compiled = Arc::new(CompiledFloatLoop::compile().unwrap());
    let mut threads = Vec::new();
    for worker in 0..4 {
        let compiled = Arc::clone(&compiled);
        threads.push(std::thread::spawn(move || {
            let start = float(worker as f32 * 10_000.0);
            let step = float(0.5);
            let limit = float(worker as f32 * 10_000.0 + 512.0);
            for _ in 0..1_000 {
                assert_eq!(
                    compiled.run(&start, &step, &limit, 1_024),
                    FloatLoopOutcome::Complete {
                        current: limit.clone(),
                        condition: Value::bool(false),
                        iterations: 1_024,
                    },
                );
            }
        }));
    }
    for thread in threads {
        thread.join().unwrap();
    }
}
