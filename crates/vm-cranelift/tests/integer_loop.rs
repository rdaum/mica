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
use mica_var::abi::{VALUE_ABI_VERSION, VALUE_INT_MAX, VALUE_INT_MIN};
use mica_vm_cranelift::{CompiledIntegerLoop, IntegerLoopOutcome};
use std::sync::Arc;

fn interpreted_integer_loop(start: i64, step: i64, limit: i64) -> (Value, u64) {
    let mut current = Value::int(start).unwrap();
    let step = Value::int(step).unwrap();
    let limit = Value::int(limit).unwrap();
    let mut iterations = 0;
    loop {
        current = current.checked_add(&step).unwrap();
        iterations += 1;
        if current >= limit {
            return (current, iterations);
        }
    }
}

#[test]
fn generated_integer_loop_matches_value_arithmetic_and_comparison() {
    let compiled = CompiledIntegerLoop::compile().unwrap();
    for start in [-10_000, -1, 0, 1, 10_000] {
        for step in [1, 2, 31] {
            for distance in [1, 2, 17, 1_024] {
                let limit = start + distance;
                let (expected, iterations) = interpreted_integer_loop(start, step, limit);
                assert_eq!(
                    compiled.run(
                        &Value::int(start).unwrap(),
                        &Value::int(step).unwrap(),
                        &Value::int(limit).unwrap(),
                        iterations + 1,
                    ),
                    IntegerLoopOutcome::Complete {
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
fn generated_integer_loop_reports_budget_exhaustion_at_a_branch_boundary() {
    let compiled = CompiledIntegerLoop::compile().unwrap();
    assert_eq!(
        compiled.run(
            &Value::int(0).unwrap(),
            &Value::int(1).unwrap(),
            &Value::int(100).unwrap(),
            17,
        ),
        IntegerLoopOutcome::BudgetExhausted {
            current: Value::int(17).unwrap(),
            condition: Value::bool(true),
            iterations: 17,
        },
    );
}

#[test]
fn generated_integer_loop_side_exits_for_non_integers_and_overflow() {
    let compiled = CompiledIntegerLoop::compile().unwrap();
    assert_eq!(
        compiled.run(
            &Value::float(1.0).unwrap(),
            &Value::int(1).unwrap(),
            &Value::int(10).unwrap(),
            10,
        ),
        IntegerLoopOutcome::SideExit,
    );
    assert_eq!(
        compiled.run(
            &Value::int(1).unwrap(),
            &Value::string("step"),
            &Value::int(10).unwrap(),
            10,
        ),
        IntegerLoopOutcome::SideExit,
    );
    assert_eq!(
        compiled.run(
            &Value::int(VALUE_INT_MAX - 1).unwrap(),
            &Value::int(2).unwrap(),
            &Value::int(VALUE_INT_MAX).unwrap(),
            1,
        ),
        IntegerLoopOutcome::SideExit,
    );
    assert_eq!(
        compiled.run(
            &Value::int(VALUE_INT_MIN).unwrap(),
            &Value::int(-1).unwrap(),
            &Value::int(VALUE_INT_MIN + 1).unwrap(),
            1,
        ),
        IntegerLoopOutcome::SideExit,
    );
}

#[test]
fn generated_integer_loop_records_abi_and_code_properties() {
    let compiled = CompiledIntegerLoop::compile().unwrap();
    assert_eq!(compiled.value_abi_version(), VALUE_ABI_VERSION);
    assert!(compiled.code_size() > 0);
    assert_eq!(compiled.imported_helper_count(), 0);
}

#[test]
fn generated_integer_loop_can_execute_concurrently() {
    let compiled = Arc::new(CompiledIntegerLoop::compile().unwrap());
    let mut threads = Vec::new();
    for worker in 0..4 {
        let compiled = Arc::clone(&compiled);
        threads.push(std::thread::spawn(move || {
            let start = Value::int(worker * 10_000).unwrap();
            let step = Value::int(3).unwrap();
            let limit = Value::int(worker * 10_000 + 3 * 1_024).unwrap();
            for _ in 0..1_000 {
                assert_eq!(
                    compiled.run(&start, &step, &limit, 1_024),
                    IntegerLoopOutcome::Complete {
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
