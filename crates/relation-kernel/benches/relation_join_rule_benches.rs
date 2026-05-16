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

use mica_relation_kernel::{
    Atom, QueryPlan, RelationId, RelationKernel, RelationMetadata, Rule, RuleSet, Snapshot, Term,
    Tuple,
};
use mica_var::{Identity, Symbol, Value};
use micromeasure::{BenchContext, BenchmarkMainOptions, Throughput, benchmark_main, black_box};
use std::sync::Arc;
use std::time::Duration;

const ROOMS: usize = 96;
const ITEMS_PER_ROOM: usize = 64;
const ACTORS: usize = 24;
const ROOMS_PER_ACTOR: usize = 8;
const HIDDEN_EVERY: usize = 17;

struct JoinRuleContext {
    snapshot: Arc<Snapshot>,
    query: QueryPlan,
    rules: RuleSet,
}

impl BenchContext for JoinRuleContext {
    fn prepare(_num_chunks: usize) -> Self {
        let kernel = build_kernel();
        let mut tx = kernel.begin();
        seed_world(&mut tx);
        tx.commit().unwrap();
        Self {
            snapshot: kernel.snapshot(),
            query: visible_items_query(actor(0)),
            rules: visible_items_rules(),
        }
    }

    fn chunk_size() -> Option<usize> {
        Some(1)
    }
}

fn query_visible_items(ctx: &mut JoinRuleContext, chunk_size: usize, _chunk_num: usize) {
    for _ in 0..chunk_size {
        black_box(ctx.query.execute(&*ctx.snapshot).unwrap());
    }
}

fn rule_visible_items(ctx: &mut JoinRuleContext, chunk_size: usize, _chunk_num: usize) {
    for _ in 0..chunk_size {
        black_box(ctx.rules.evaluate(&*ctx.snapshot).unwrap());
    }
}

fn build_kernel() -> RelationKernel {
    let kernel = RelationKernel::new();
    kernel
        .create_relation(
            RelationMetadata::new(located_in(), Symbol::intern("LocatedIn"), 2).with_index([1, 0]),
        )
        .unwrap();
    kernel
        .create_relation(
            RelationMetadata::new(can_see_room(), Symbol::intern("CanSeeRoom"), 2)
                .with_index([1, 0]),
        )
        .unwrap();
    kernel
        .create_relation(RelationMetadata::new(
            portable(),
            Symbol::intern("Portable"),
            1,
        ))
        .unwrap();
    kernel
        .create_relation(
            RelationMetadata::new(hidden_from(), Symbol::intern("HiddenFrom"), 2)
                .with_index([1, 0]),
        )
        .unwrap();
    kernel
}

fn seed_world(tx: &mut mica_relation_kernel::Transaction<'_>) {
    for room in 0..ROOMS {
        for item in 0..ITEMS_PER_ROOM {
            let item_id = item_identity(room, item);
            tx.assert(
                located_in(),
                Tuple::from([
                    Value::identity(item_id),
                    Value::identity(room_identity(room)),
                ]),
            )
            .unwrap();
            tx.assert(portable(), Tuple::from([Value::identity(item_id)]))
                .unwrap();
            if item % HIDDEN_EVERY == 0 {
                tx.assert(
                    hidden_from(),
                    Tuple::from([
                        Value::identity(item_id),
                        Value::identity(actor(room % ACTORS)),
                    ]),
                )
                .unwrap();
            }
        }
    }

    for actor_index in 0..ACTORS {
        for offset in 0..ROOMS_PER_ACTOR {
            let room = (actor_index * ROOMS_PER_ACTOR + offset) % ROOMS;
            tx.assert(
                can_see_room(),
                Tuple::from([
                    Value::identity(actor(actor_index)),
                    Value::identity(room_identity(room)),
                ]),
            )
            .unwrap();
        }
    }
}

fn visible_items_query(actor_id: Identity) -> QueryPlan {
    let rooms_seen = QueryPlan::scan(can_see_room(), [Some(Value::identity(actor_id)), None]);
    let items_in_seen_rooms = QueryPlan::join_eq(
        rooms_seen,
        QueryPlan::scan(located_in(), [None, None]),
        [1],
        [1],
    );
    let portable_items = QueryPlan::join_eq(
        items_in_seen_rooms,
        QueryPlan::scan(portable(), [None]),
        [2],
        [0],
    );
    QueryPlan::anti_join(
        portable_items,
        QueryPlan::scan(hidden_from(), [None, Some(Value::identity(actor_id))]),
        [2],
        [0],
    )
    .project([2])
}

fn visible_items_rules() -> RuleSet {
    RuleSet::new([Rule::new(
        visible(),
        [var("actor"), var("item")],
        [
            Atom::positive(located_in(), [var("item"), var("room")]),
            Atom::positive(can_see_room(), [var("actor"), var("room")]),
            Atom::positive(portable(), [var("item")]),
            Atom::negated(hidden_from(), [var("item"), var("actor")]),
        ],
    )])
}

fn var(name: &str) -> Term {
    Term::Var(Symbol::intern(name))
}

fn located_in() -> RelationId {
    relation(90)
}

fn can_see_room() -> RelationId {
    relation(91)
}

fn portable() -> RelationId {
    relation(92)
}

fn hidden_from() -> RelationId {
    relation(93)
}

fn visible() -> RelationId {
    relation(94)
}

fn relation(raw: u64) -> Identity {
    Identity::new(raw).unwrap()
}

fn actor(index: usize) -> Identity {
    relation(1_000 + index as u64)
}

fn room_identity(index: usize) -> Identity {
    relation(10_000 + index as u64)
}

fn item_identity(room: usize, item: usize) -> Identity {
    relation(100_000 + (room * ITEMS_PER_ROOM + item) as u64)
}

benchmark_main!(
    BenchmarkMainOptions {
        filter_help: Some("all, query, rule, or any benchmark name substring".to_string()),
        runtime: micromeasure::BenchmarkRuntimeOptions {
            warm_up_duration: Duration::from_millis(100),
            benchmark_duration: Duration::from_secs(1),
            min_samples: 5,
            max_samples: 10,
        },
        ..Default::default()
    },
    |runner| {
        runner.group::<JoinRuleContext>("query", |g| {
            g.throughput(Throughput::per_operation(1, "query"))
                .bench("visible_items_query_3way_anti", query_visible_items);
        });

        runner.group::<JoinRuleContext>("rule", |g| {
            g.throughput(Throughput::per_operation(1, "rule_eval"))
                .bench("visible_items_rule_join_order", rule_visible_items);
        });
    }
);
