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
    ComputedRelation, ComputedRelationRead, KernelError, RelationId, RelationMetadata,
    RelationRead, Tuple, system_computed_relations,
};
use mica_var::Value;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::Arc;

const REQUIRED_BOUND_POSITIONS: &[u16] = &[0, 1, 2];

pub(crate) fn default_computed_relations() -> Vec<Arc<dyn ComputedRelation>> {
    let mut relations = system_computed_relations();
    relations.push(Arc::new(ExactEmbeddingSearchRelation));
    relations
}

struct ExactEmbeddingSearchRelation;

impl ComputedRelation for ExactEmbeddingSearchRelation {
    fn name(&self) -> &'static str {
        "exact-embedding-search"
    }

    fn matches(&self, metadata: &RelationMetadata) -> bool {
        metadata.name().name() == Some("NearestEmbedding") && metadata.arity() == 6
    }

    fn required_bound_positions(&self, _metadata: &RelationMetadata) -> &[u16] {
        REQUIRED_BOUND_POSITIONS
    }

    fn scan(
        &self,
        reader: &dyn ComputedRelationRead,
        metadata: &RelationMetadata,
        bindings: &[Option<Value>],
    ) -> Result<Vec<Tuple>, KernelError> {
        let index = bindings[0]
            .clone()
            .ok_or_else(|| invalid_relation(metadata.id(), "expected bound index in position 0"))?;
        let query = parse_vector(
            metadata.id(),
            &bindings[1].clone().ok_or_else(|| {
                invalid_relation(
                    metadata.id(),
                    "expected bound query embedding in position 1",
                )
            })?,
        )?;
        let limit = parse_limit(
            metadata.id(),
            &bindings[2].clone().ok_or_else(|| {
                invalid_relation(metadata.id(), "expected bound limit in position 2")
            })?,
        )?;

        let vector_index_contains =
            relation_id(reader, "VectorIndexContains", 2).ok_or_else(|| {
                invalid_relation(metadata.id(), "missing relation VectorIndexContains/2")
            })?;
        let embedding_of = relation_id(reader, "EmbeddingOf", 2)
            .ok_or_else(|| invalid_relation(metadata.id(), "missing relation EmbeddingOf/2"))?;
        let embedding_vector = relation_id(reader, "EmbeddingVector", 2)
            .ok_or_else(|| invalid_relation(metadata.id(), "missing relation EmbeddingVector/2"))?;

        let mut best_by_subject = BTreeMap::<Value, f64>::new();
        for membership in
            reader.scan_relation(vector_index_contains, &[Some(index.clone()), None])?
        {
            let Some(embedding) = membership.values().get(1).cloned() else {
                continue;
            };
            let vector_row = one_value(
                reader,
                embedding_vector,
                &[Some(embedding.clone()), None],
                metadata.id(),
                "expected EmbeddingVector(embedding, payload)",
            )?;
            let subject = one_value(
                reader,
                embedding_of,
                &[Some(embedding.clone()), None],
                metadata.id(),
                "expected EmbeddingOf(embedding, subject)",
            )?;
            let candidate = parse_vector(metadata.id(), &vector_row)?;
            let score = cosine_similarity(metadata.id(), &query, &candidate)?;
            best_by_subject
                .entry(subject)
                .and_modify(|current| {
                    if score > *current {
                        *current = score;
                    }
                })
                .or_insert(score);
        }

        let version = Value::int(reader.version() as i64)
            .map_err(|_| invalid_relation(metadata.id(), "snapshot version exceeds i64"))?;
        let mut rows = best_by_subject
            .into_iter()
            .map(|(subject, score)| {
                Ok((
                    subject.clone(),
                    score,
                    Tuple::from([
                        index.clone(),
                        bindings[1]
                            .clone()
                            .expect("validated query embedding should exist"),
                        bindings[2].clone().expect("validated limit should exist"),
                        subject,
                        Value::float(score),
                        version.clone(),
                    ]),
                ))
            })
            .collect::<Result<Vec<_>, KernelError>>()?;
        rows.sort_by(|left, right| {
            right
                .1
                .partial_cmp(&left.1)
                .unwrap_or(Ordering::Equal)
                .then_with(|| left.0.cmp(&right.0))
        });
        Ok(rows
            .into_iter()
            .take(limit)
            .map(|(_, _, tuple)| tuple)
            .collect())
    }
}

fn invalid_relation(relation: RelationId, message: impl Into<String>) -> KernelError {
    KernelError::InvalidComputedRelation {
        relation,
        message: message.into(),
    }
}

fn relation_id(reader: &dyn ComputedRelationRead, name: &str, arity: u16) -> Option<RelationId> {
    reader
        .relation_metadata_vec()
        .into_iter()
        .find(|metadata| metadata.name().name() == Some(name) && metadata.arity() == arity)
        .map(|metadata| metadata.id())
}

fn parse_limit(relation: RelationId, value: &Value) -> Result<usize, KernelError> {
    let Some(limit) = value.as_int() else {
        return Err(invalid_relation(relation, "limit must be an integer"));
    };
    if limit < 0 {
        return Err(invalid_relation(relation, "limit must be non-negative"));
    }
    Ok(limit as usize)
}

fn parse_vector(relation: RelationId, value: &Value) -> Result<Vec<f64>, KernelError> {
    value
        .with_list(|values| {
            values
                .iter()
                .map(|value| {
                    value
                        .as_float()
                        .or_else(|| value.as_int().map(|value| value as f64))
                        .ok_or_else(|| {
                            invalid_relation(
                                relation,
                                "embedding payloads must be lists of ints or floats",
                            )
                        })
                })
                .collect::<Result<Vec<_>, KernelError>>()
        })
        .ok_or_else(|| invalid_relation(relation, "embedding payload must be a list"))?
}

fn one_value(
    reader: &dyn RelationRead,
    relation: RelationId,
    bindings: &[Option<Value>],
    computed_relation: RelationId,
    message: &str,
) -> Result<Value, KernelError> {
    let rows = reader.scan_relation(relation, bindings)?;
    rows.first()
        .and_then(|row| row.values().get(1))
        .cloned()
        .ok_or_else(|| invalid_relation(computed_relation, message))
}

fn cosine_similarity(
    relation: RelationId,
    left: &[f64],
    right: &[f64],
) -> Result<f64, KernelError> {
    if left.is_empty() || right.is_empty() {
        return Err(invalid_relation(
            relation,
            "embedding vectors must be non-empty",
        ));
    }
    if left.len() != right.len() {
        return Err(invalid_relation(
            relation,
            "query and candidate embeddings must have the same dimension",
        ));
    }

    let mut dot = 0.0;
    let mut left_norm = 0.0;
    let mut right_norm = 0.0;
    for (left_value, right_value) in left.iter().zip(right.iter()) {
        dot += left_value * right_value;
        left_norm += left_value * left_value;
        right_norm += right_value * right_value;
    }
    if left_norm == 0.0 || right_norm == 0.0 {
        return Err(invalid_relation(
            relation,
            "embedding vectors must not have zero magnitude",
        ));
    }
    Ok(dot / (left_norm.sqrt() * right_norm.sqrt()))
}

#[cfg(test)]
mod tests {
    use crate::{SourceRunner, TaskOutcome};
    use mica_var::{Symbol, Value};

    #[test]
    fn runner_queries_exact_nearest_embedding_relation() {
        let mut runner = SourceRunner::new_empty();
        runner
            .run_filein(include_str!("../../../apps/shared/retrieval.mica"))
            .unwrap();
        runner
            .run_source(
                "make_identity(:main_index)\n\
                 make_identity(:doc_one)\n\
                 make_identity(:doc_two)\n\
                 make_identity(:emb_one)\n\
                 make_identity(:emb_two)\n\
                 assert VectorIndex(#main_index)\n\
                 assert VectorIndexContains(#main_index, #emb_one)\n\
                 assert VectorIndexContains(#main_index, #emb_two)\n\
                 assert Embedding(#emb_one)\n\
                 assert Embedding(#emb_two)\n\
                 assert EmbeddingOf(#emb_one, #doc_one)\n\
                 assert EmbeddingOf(#emb_two, #doc_two)\n\
                 assert EmbeddingVector(#emb_one, [1.0, 0.0])\n\
                 assert EmbeddingVector(#emb_two, [0.0, 1.0])",
            )
            .unwrap();

        let report = runner
            .run_source(
                "let rows = NearestEmbedding(#main_index, [1.0, 0.0], 2, ?subject, ?score, ?version)\n\
                 return [rows[0][:subject], rows[1][:subject]]",
            )
            .unwrap();

        let TaskOutcome::Complete { value, .. } = report.outcome else {
            panic!("expected complete outcome");
        };
        value
            .with_list(|values| {
                assert_eq!(values.len(), 2);
                assert_eq!(
                    values[0],
                    Value::identity(runner.named_identity(Symbol::intern("doc_one")).unwrap())
                );
                assert_eq!(
                    values[1],
                    Value::identity(runner.named_identity(Symbol::intern("doc_two")).unwrap())
                );
            })
            .expect("expected list result");
    }

    #[test]
    fn runner_rejects_writes_to_nearest_embedding_relation() {
        let mut runner = SourceRunner::new_empty();
        runner
            .run_filein(include_str!("../../../apps/shared/retrieval.mica"))
            .unwrap();
        runner.run_source("make_identity(:main_index)").unwrap();

        let error = runner
            .run_source("assert NearestEmbedding(#main_index, [1.0], 1, #main_index, 1.0, 0)")
            .unwrap_err();
        assert!(format!("{error:?}").contains("ReadOnlyRelation"));
    }

    #[test]
    fn runner_indexes_text_units_with_host_embedding_builtin() {
        let mut runner = SourceRunner::new_empty();
        runner
            .run_filein(include_str!("../../../apps/shared/retrieval.mica"))
            .unwrap();
        runner
            .run_source(
                "make_identity(:main_index)\n\
                 make_identity(:unit_one)\n\
                 assert TextUnit(#unit_one)\n\
                 assert TextUnitText(#unit_one, \"red brass lamp\")\n\
                 return index_text_unit(nothing, #main_index, #unit_one, \"host-test\")",
            )
            .unwrap();

        let query = runner
            .run_source(
                "let embedding = one VectorIndexContains(#main_index, ?embedding)\n\
                 let subject = one EmbeddingOf(embedding, ?subject)\n\
                 let model = one EmbeddingModel(embedding, ?model)\n\
                 return [embedding, subject, model]",
            )
            .unwrap();

        let TaskOutcome::Complete { value, .. } = query.outcome else {
            panic!("expected complete outcome");
        };
        value
            .with_list(|values| {
                assert_eq!(values.len(), 3);
                assert_eq!(
                    values[1],
                    Value::identity(runner.named_identity(Symbol::intern("unit_one")).unwrap())
                );
                assert_eq!(values[2], Value::string("host-test"));
            })
            .expect("expected list result");
    }

    #[test]
    fn answer_question_records_retrieval_artifacts_and_citations() {
        let mut runner = SourceRunner::new_empty();
        runner
            .run_filein(include_str!("../../../apps/shared/retrieval.mica"))
            .unwrap();
        runner
            .run_source(
                "make_identity(:main_index)\n\
                 make_identity(:unit_one)\n\
                 make_identity(:unit_two)\n\
                 assert TextUnit(#unit_one)\n\
                 assert TextUnit(#unit_two)\n\
                 assert TextUnitText(#unit_one, \"lamp oil brass light\")\n\
                 assert TextUnitText(#unit_two, \"apple orchard green fruit\")\n\
                 index_text_unit(nothing, #main_index, #unit_one, \"host-test\")\n\
                 index_text_unit(nothing, #main_index, #unit_two, \"host-test\")\n\
                 return answer_question(#main_index, #main_index, \"brass lamp\", 1)",
            )
            .unwrap();

        let report = runner
            .run_source(
                "let answer = one Answer(?answer)\n\
                 let question = one Question(?question)\n\
                 let plan = one RetrievalPlan(?plan)\n\
                 let context = one RetrievedContext(?context)\n\
                 let question_text = one QuestionText(question, ?text)\n\
                 let plan_kind = one PlanKind(plan, ?kind)\n\
                 let context_reason = one ContextReason(context, ?reason)\n\
                 let has_citation = one AnswerCitation(answer, ?subject) != nothing\n\
                 return [question_text, plan_kind, context_reason, has_citation]",
            )
            .unwrap();

        let TaskOutcome::Complete { value, .. } = report.outcome else {
            panic!("expected complete outcome");
        };
        value
            .with_list(|values| {
                assert_eq!(values[0], Value::string("brass lamp"));
                assert_eq!(values[1], Value::string("nearest_embedding"));
                assert_eq!(values[2], Value::string("nearest_embedding"));
                assert_eq!(values[3], Value::bool(true));
            })
            .expect("expected list result");
    }
}
