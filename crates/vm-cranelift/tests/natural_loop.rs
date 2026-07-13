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
use mica_var::abi::{borrowed_value_bits, from_owned_value_bits};
use mica_vm_cranelift::{
    CompiledNaturalLoop, NaturalLoopInstruction, NaturalLoopOutcome, NaturalLoopPlan,
    NaturalLoopRangeView, ScalarComparison,
};
use std::sync::{Arc, Barrier};

const CURRENT: u16 = 0;
const TOTAL: u16 = 1;
const LIMIT: u16 = 2;
const CONDITION: u16 = 3;
const STEP: u16 = 4;
const NEXT: u16 = 5;
const NEXT_TOTAL: u16 = 6;

fn bits(value: Value) -> u64 {
    borrowed_value_bits(&value)
}

fn int_bits(value: i64) -> u64 {
    bits(Value::int(value).unwrap())
}

fn plan(limit: i64) -> NaturalLoopPlan {
    NaturalLoopPlan::new(
        7,
        0,
        3,
        [
            NaturalLoopInstruction::Load {
                dst: LIMIT,
                value: int_bits(limit),
            },
            NaturalLoopInstruction::Compare {
                dst: CONDITION,
                comparison: ScalarComparison::LessThan,
                left: CURRENT,
                right: LIMIT,
            },
            NaturalLoopInstruction::Branch {
                condition: CONDITION,
                if_true: 3,
                if_false: 9,
            },
            NaturalLoopInstruction::Load {
                dst: STEP,
                value: int_bits(1),
            },
            NaturalLoopInstruction::Add {
                dst: NEXT,
                left: CURRENT,
                right: STEP,
            },
            NaturalLoopInstruction::Move {
                dst: CURRENT,
                src: NEXT,
            },
            NaturalLoopInstruction::Add {
                dst: NEXT_TOTAL,
                left: TOTAL,
                right: CURRENT,
            },
            NaturalLoopInstruction::Move {
                dst: TOTAL,
                src: NEXT_TOTAL,
            },
            NaturalLoopInstruction::Jump { target: 0 },
        ],
    )
    .unwrap()
}

fn scratch(limit: i64) -> [u64; 7] {
    [
        int_bits(0),
        int_bits(0),
        int_bits(limit),
        bits(Value::bool(true)),
        bits(Value::nothing()),
        bits(Value::nothing()),
        bits(Value::nothing()),
    ]
}

fn value(bits: u64) -> Value {
    unsafe { from_owned_value_bits(bits) }
}

fn range_value_plan() -> NaturalLoopPlan {
    NaturalLoopPlan::new(
        2,
        1,
        0,
        [NaturalLoopInstruction::RangeValueAt {
            dst: 0,
            view: 0,
            index: 1,
        }],
    )
    .unwrap()
}

#[test]
fn generated_range_value_at_emits_checked_integer_values_without_helpers() {
    let compiled = CompiledNaturalLoop::compile(&range_value_plan()).unwrap();
    let range = [NaturalLoopRangeView::new(-5, 5).unwrap()];
    let mut scratch = [bits(Value::nothing()), int_bits(7)];

    assert_eq!(
        compiled.run(&mut scratch, &range, 1),
        NaturalLoopOutcome::Complete {
            instructions: 1,
            modified_slots: 1,
        },
    );
    assert_eq!(value(scratch[0]).as_int(), Some(2));
    assert_eq!(compiled.imported_helper_count(), 0);
}

#[test]
fn generated_range_value_at_side_exits_on_invalid_indices_and_bounds() {
    let compiled = CompiledNaturalLoop::compile(&range_value_plan()).unwrap();
    let cases = [
        (NaturalLoopRangeView::new(5, 4).unwrap(), int_bits(0)),
        (NaturalLoopRangeView::new(0, 5).unwrap(), int_bits(-1)),
        (NaturalLoopRangeView::new(0, 5).unwrap(), int_bits(6)),
        (
            NaturalLoopRangeView::new(0, 5).unwrap(),
            bits(Value::float(1.0).unwrap()),
        ),
    ];

    for (range, index) in cases {
        let mut scratch = [bits(Value::nothing()), index];
        assert_eq!(
            compiled.run(&mut scratch, &[range], 1),
            NaturalLoopOutcome::SideExit,
        );
    }
}

#[test]
fn generated_natural_loop_completes_compiler_shaped_accumulation() {
    let compiled = CompiledNaturalLoop::compile(&plan(16_384)).unwrap();
    let mut scratch = scratch(16_384);
    assert_eq!(
        compiled.run(&mut scratch, &[], 16_384 * 9),
        NaturalLoopOutcome::Complete {
            instructions: 16_384 * 9,
            modified_slots: 0x7f,
        },
    );
    assert_eq!(value(scratch[CURRENT as usize]).as_int(), Some(16_384));
    assert_eq!(value(scratch[TOTAL as usize]).as_int(), Some(134_225_920),);
    assert_eq!(value(scratch[CONDITION as usize]).as_bool(), Some(false),);
    assert_eq!(compiled.imported_helper_count(), 0);
    assert!(compiled.code_size() > 0);
}

#[test]
fn generated_natural_loop_stops_at_an_exact_instruction_budget() {
    let compiled = CompiledNaturalLoop::compile(&plan(16_384)).unwrap();
    let mut scratch = scratch(16_384);
    assert_eq!(
        compiled.run(&mut scratch, &[], 90),
        NaturalLoopOutcome::BudgetExhausted {
            instructions: 90,
            resume: 3,
            modified_slots: 0x7f,
        },
    );
    assert_eq!(value(scratch[CURRENT as usize]).as_int(), Some(10));
    assert_eq!(value(scratch[TOTAL as usize]).as_int(), Some(55));
    assert_eq!(value(scratch[CONDITION as usize]).as_bool(), Some(true),);
}

#[test]
fn generated_natural_loop_side_exits_on_mixed_arithmetic() {
    let compiled = CompiledNaturalLoop::compile(&plan(16_384)).unwrap();
    let mut scratch = scratch(16_384);
    let mixed = Value::string("not an integer");
    scratch[TOTAL as usize] = borrowed_value_bits(&mixed);
    assert_eq!(
        compiled.run(&mut scratch, &[], 16_384 * 9),
        NaturalLoopOutcome::SideExit,
    );
}

#[test]
fn generated_natural_loop_executes_concurrently() {
    let compiled = Arc::new(CompiledNaturalLoop::compile(&plan(16_384)).unwrap());
    let barrier = Arc::new(Barrier::new(4));
    let mut threads = Vec::new();
    for _ in 0..4 {
        let compiled = Arc::clone(&compiled);
        let barrier = Arc::clone(&barrier);
        threads.push(std::thread::spawn(move || {
            let mut scratch = scratch(16_384);
            barrier.wait();
            let outcome = compiled.run(&mut scratch, &[], 16_384 * 9);
            (outcome, value(scratch[TOTAL as usize]).as_int())
        }));
    }
    for thread in threads {
        assert_eq!(
            thread.join().unwrap(),
            (
                NaturalLoopOutcome::Complete {
                    instructions: 16_384 * 9,
                    modified_slots: 0x7f,
                },
                Some(134_225_920),
            ),
        );
    }
}
