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

use crate::commit_bloom::CommitBloom;
use crate::{
    Atom, CatalogChange, Commit, ConflictPolicy, FactChange, FactChangeKind, RelationId,
    RelationMetadata, Rule, RuleDefinition, Term, Tuple,
};
use mica_var::{Identity, Symbol, Value, ValueKind};

pub(super) fn fact_key(relation: RelationId, tuple: &Tuple) -> Result<Vec<u8>, String> {
    let mut key = relation.raw().to_be_bytes().to_vec();
    encode_tuple(tuple, &mut key)?;
    Ok(key)
}

pub(super) fn encode_relation_metadata_record(
    metadata: &RelationMetadata,
) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    encode_relation_metadata(metadata, &mut out)?;
    Ok(out)
}

pub(super) fn decode_relation_metadata(bytes: &[u8]) -> Result<RelationMetadata, String> {
    let mut reader = Reader::new(bytes);
    let metadata = reader.read_relation_metadata()?;
    reader.expect_end()?;
    Ok(metadata)
}

pub(super) fn encode_rule_definition_record(rule: &RuleDefinition) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    encode_rule_definition(rule, &mut out)?;
    Ok(out)
}

pub(super) fn decode_rule_definition(bytes: &[u8]) -> Result<RuleDefinition, String> {
    let mut reader = Reader::new(bytes);
    let rule = reader.read_rule_definition()?;
    reader.expect_end()?;
    Ok(rule)
}

pub(super) fn encode_tuple_record(tuple: &Tuple) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    encode_tuple(tuple, &mut out)?;
    Ok(out)
}

pub(super) fn decode_tuple(bytes: &[u8]) -> Result<Tuple, String> {
    let mut reader = Reader::new(bytes);
    let tuple = reader.read_tuple()?;
    reader.expect_end()?;
    Ok(tuple)
}

pub(super) fn encode_commit(commit: &Commit) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    write_magic(&mut out);
    write_u64(&mut out, commit.version());
    write_u32(&mut out, commit.catalog_changes().len())?;
    for change in commit.catalog_changes() {
        encode_catalog_change(change, &mut out)?;
    }
    write_u32(&mut out, commit.changes().len())?;
    for change in commit.changes() {
        encode_fact_change(change, &mut out)?;
    }
    Ok(out)
}

pub(super) fn decode_commit(bytes: &[u8]) -> Result<Commit, String> {
    let mut reader = Reader::new(bytes);
    reader.expect_magic()?;
    let version = reader.read_u64()?;
    let catalog_count = reader.read_len()?;
    let mut catalog_changes = Vec::with_capacity(catalog_count);
    for _ in 0..catalog_count {
        catalog_changes.push(reader.read_catalog_change()?);
    }
    let fact_count = reader.read_len()?;
    let mut changes = Vec::with_capacity(fact_count);
    for _ in 0..fact_count {
        changes.push(reader.read_fact_change()?);
    }
    reader.expect_end()?;
    Ok(Commit {
        version,
        catalog_changes: catalog_changes.into(),
        changes: changes.into(),
        bloom: CommitBloom::new(),
    })
}

fn write_magic(out: &mut Vec<u8>) {
    out.extend_from_slice(b"MICACMT1");
}

fn encode_catalog_change(change: &CatalogChange, out: &mut Vec<u8>) -> Result<(), String> {
    match change {
        CatalogChange::RelationCreated(metadata) => {
            out.push(0);
            encode_relation_metadata(metadata, out)
        }
        CatalogChange::RuleInstalled(rule) => {
            out.push(1);
            encode_rule_definition(rule, out)
        }
        CatalogChange::RuleDisabled(rule_id) => {
            out.push(2);
            write_identity(out, *rule_id);
            Ok(())
        }
    }
}

fn encode_relation_metadata(metadata: &RelationMetadata, out: &mut Vec<u8>) -> Result<(), String> {
    write_identity(out, metadata.id());
    write_symbol(out, metadata.name())?;
    write_u16(out, metadata.arity());
    for position in 0..metadata.arity() {
        write_optional_symbol(out, metadata.argument_name(position))?;
    }
    write_u32(out, metadata.indexes().len())?;
    for index in metadata.indexes() {
        write_u32(out, index.positions().len())?;
        for position in index.positions() {
            write_u16(out, *position);
        }
    }
    match metadata.conflict_policy() {
        ConflictPolicy::Set => out.push(0),
        ConflictPolicy::Functional { key_positions } => {
            out.push(1);
            write_u32(out, key_positions.len())?;
            for position in key_positions {
                write_u16(out, *position);
            }
        }
        ConflictPolicy::EventAppend => out.push(2),
    }
    Ok(())
}

fn encode_rule_definition(rule: &RuleDefinition, out: &mut Vec<u8>) -> Result<(), String> {
    write_identity(out, rule.id());
    out.push(rule.active() as u8);
    write_string(out, rule.source())?;
    encode_rule(rule.rule(), out)
}

fn encode_rule(rule: &Rule, out: &mut Vec<u8>) -> Result<(), String> {
    write_identity(out, rule.head_relation());
    encode_terms(rule.head_terms(), out)?;
    write_u32(out, rule.body().len())?;
    for atom in rule.body() {
        out.push(atom.is_negated() as u8);
        write_identity(out, atom.relation());
        encode_terms(atom.terms(), out)?;
    }
    Ok(())
}

fn encode_terms(terms: &[Term], out: &mut Vec<u8>) -> Result<(), String> {
    write_u32(out, terms.len())?;
    for term in terms {
        match term {
            Term::Var(symbol) => {
                out.push(0);
                write_symbol(out, *symbol)?;
            }
            Term::Value(value) => {
                out.push(1);
                encode_value(value, out)?;
            }
        }
    }
    Ok(())
}

fn encode_fact_change(change: &FactChange, out: &mut Vec<u8>) -> Result<(), String> {
    write_identity(out, change.relation);
    encode_tuple(&change.tuple, out)?;
    out.push(match change.kind {
        FactChangeKind::Assert => 0,
        FactChangeKind::Retract => 1,
    });
    Ok(())
}

fn encode_tuple(tuple: &Tuple, out: &mut Vec<u8>) -> Result<(), String> {
    write_u32(out, tuple.arity())?;
    for value in tuple.values() {
        encode_value(value, out)?;
    }
    Ok(())
}

fn encode_value(value: &Value, out: &mut Vec<u8>) -> Result<(), String> {
    out.push(value.kind() as u8);
    match value.kind() {
        ValueKind::Nothing => {}
        ValueKind::Bool => out.push(value.as_bool().unwrap() as u8),
        ValueKind::Int => write_i64(out, value.as_int().unwrap()),
        ValueKind::Float => write_u64(out, value.as_float().unwrap().to_bits()),
        ValueKind::Identity => write_identity(out, value.as_identity().unwrap()),
        ValueKind::Symbol => write_symbol(out, value.as_symbol().unwrap())?,
        ValueKind::ErrorCode => write_symbol(out, value.as_error_code().unwrap())?,
        ValueKind::String => value
            .with_str(|string| write_string(out, string))
            .ok_or_else(|| "string value missing heap payload".to_owned())??,
        ValueKind::Bytes => value
            .with_bytes(|bytes| write_bytes(out, bytes))
            .ok_or_else(|| "bytes value missing heap payload".to_owned())??,
        ValueKind::List => value
            .with_list(|values| {
                write_u32(out, values.len())?;
                for value in values {
                    encode_value(value, out)?;
                }
                Ok::<(), String>(())
            })
            .ok_or_else(|| "list value missing heap payload".to_owned())??,
        ValueKind::Map => value
            .with_map(|entries| {
                write_u32(out, entries.len())?;
                for (key, value) in entries {
                    encode_value(key, out)?;
                    encode_value(value, out)?;
                }
                Ok::<(), String>(())
            })
            .ok_or_else(|| "map value missing heap payload".to_owned())??,
        ValueKind::Range => value
            .with_range(|start, end| {
                encode_value(start, out)?;
                match end {
                    Some(end) => {
                        out.push(1);
                        encode_value(end, out)
                    }
                    None => {
                        out.push(0);
                        Ok(())
                    }
                }
            })
            .ok_or_else(|| "range value missing heap payload".to_owned())??,
        ValueKind::Error => value
            .with_error(|error| {
                write_symbol(out, error.code())?;
                write_optional_string(out, error.message())?;
                match error.value() {
                    Some(value) => {
                        out.push(1);
                        encode_value(value, out)
                    }
                    None => {
                        out.push(0);
                        Ok(())
                    }
                }
            })
            .ok_or_else(|| "error value missing heap payload".to_owned())??,
        ValueKind::Capability => {
            return Err("capability values cannot be persisted".to_owned());
        }
    }
    Ok(())
}

fn write_identity(out: &mut Vec<u8>, identity: Identity) {
    write_u64(out, identity.raw());
}

fn write_symbol(out: &mut Vec<u8>, symbol: Symbol) -> Result<(), String> {
    let name = symbol
        .name()
        .ok_or_else(|| format!("cannot persist unnamed symbol id {}", symbol.id()))?;
    write_string(out, name)
}

fn write_optional_symbol(out: &mut Vec<u8>, symbol: Option<Symbol>) -> Result<(), String> {
    match symbol {
        Some(symbol) => {
            out.push(1);
            write_symbol(out, symbol)
        }
        None => {
            out.push(0);
            Ok(())
        }
    }
}

fn write_optional_string(out: &mut Vec<u8>, value: Option<&str>) -> Result<(), String> {
    match value {
        Some(value) => {
            out.push(1);
            write_string(out, value)
        }
        None => {
            out.push(0);
            Ok(())
        }
    }
}

fn write_string(out: &mut Vec<u8>, value: &str) -> Result<(), String> {
    write_bytes(out, value.as_bytes())
}

fn write_bytes(out: &mut Vec<u8>, value: &[u8]) -> Result<(), String> {
    write_u32(out, value.len())?;
    out.extend_from_slice(value);
    Ok(())
}

fn write_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn write_u32(out: &mut Vec<u8>, value: usize) -> Result<(), String> {
    let value = u32::try_from(value).map_err(|_| format!("length {value} exceeds u32"))?;
    out.extend_from_slice(&value.to_be_bytes());
    Ok(())
}

fn write_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn write_i64(out: &mut Vec<u8>, value: i64) {
    out.extend_from_slice(&value.to_be_bytes());
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn expect_magic(&mut self) -> Result<(), String> {
        let magic = self.read_exact(8)?;
        if magic == b"MICACMT1" {
            Ok(())
        } else {
            Err("invalid mica commit record magic".to_owned())
        }
    }

    fn expect_end(&self) -> Result<(), String> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(format!(
                "trailing bytes in mica commit record: {}",
                self.bytes.len() - self.offset
            ))
        }
    }

    fn read_catalog_change(&mut self) -> Result<CatalogChange, String> {
        Ok(match self.read_u8()? {
            0 => CatalogChange::RelationCreated(self.read_relation_metadata()?),
            1 => CatalogChange::RuleInstalled(self.read_rule_definition()?),
            2 => CatalogChange::RuleDisabled(self.read_identity()?),
            tag => return Err(format!("unknown catalog change tag {tag}")),
        })
    }

    fn read_relation_metadata(&mut self) -> Result<RelationMetadata, String> {
        let id = self.read_identity()?;
        let name = self.read_symbol()?;
        let arity = self.read_u16()?;
        let mut metadata = RelationMetadata::new(id, name, arity);
        for position in 0..arity {
            if let Some(name) = self.read_optional_symbol()? {
                metadata = metadata.with_argument_name(position, name);
            }
        }
        let index_count = self.read_len()?;
        for index in 0..index_count {
            let position_count = self.read_len()?;
            let mut positions = Vec::with_capacity(position_count);
            for _ in 0..position_count {
                positions.push(self.read_u16()?);
            }
            if index == 0 {
                let expected = (0..arity).collect::<Vec<_>>();
                if positions != expected {
                    return Err("first persisted relation index is not all positions".to_owned());
                }
            } else {
                metadata = metadata.with_index(positions);
            }
        }
        metadata = match self.read_u8()? {
            0 => metadata.with_conflict_policy(ConflictPolicy::Set),
            1 => {
                let key_count = self.read_len()?;
                let mut key_positions = Vec::with_capacity(key_count);
                for _ in 0..key_count {
                    key_positions.push(self.read_u16()?);
                }
                metadata.with_conflict_policy(ConflictPolicy::Functional { key_positions })
            }
            2 => metadata.with_conflict_policy(ConflictPolicy::EventAppend),
            tag => return Err(format!("unknown conflict policy tag {tag}")),
        };
        Ok(metadata)
    }

    fn read_rule_definition(&mut self) -> Result<RuleDefinition, String> {
        let id = self.read_identity()?;
        let active = self.read_bool()?;
        let source = self.read_string()?;
        let mut definition = RuleDefinition::new(id, self.read_rule()?, source);
        if !active {
            definition.deactivate();
        }
        Ok(definition)
    }

    fn read_rule(&mut self) -> Result<Rule, String> {
        let head_relation = self.read_identity()?;
        let head_terms = self.read_terms()?;
        let body_count = self.read_len()?;
        let mut body = Vec::with_capacity(body_count);
        for _ in 0..body_count {
            let negated = self.read_bool()?;
            let relation = self.read_identity()?;
            let terms = self.read_terms()?;
            body.push(if negated {
                Atom::negated(relation, terms)
            } else {
                Atom::positive(relation, terms)
            });
        }
        Ok(Rule::new(head_relation, head_terms, body))
    }

    fn read_terms(&mut self) -> Result<Vec<Term>, String> {
        let count = self.read_len()?;
        let mut terms = Vec::with_capacity(count);
        for _ in 0..count {
            terms.push(match self.read_u8()? {
                0 => Term::Var(self.read_symbol()?),
                1 => Term::Value(self.read_value()?),
                tag => return Err(format!("unknown term tag {tag}")),
            });
        }
        Ok(terms)
    }

    fn read_fact_change(&mut self) -> Result<FactChange, String> {
        let relation = self.read_identity()?;
        let tuple = self.read_tuple()?;
        let kind = match self.read_u8()? {
            0 => FactChangeKind::Assert,
            1 => FactChangeKind::Retract,
            tag => return Err(format!("unknown fact change tag {tag}")),
        };
        Ok(FactChange {
            relation,
            tuple,
            kind,
        })
    }

    fn read_tuple(&mut self) -> Result<Tuple, String> {
        let arity = self.read_len()?;
        let mut values = Vec::with_capacity(arity);
        for _ in 0..arity {
            values.push(self.read_value()?);
        }
        Ok(Tuple::new(values))
    }

    fn read_value(&mut self) -> Result<Value, String> {
        let kind = self.read_u8()?;
        Ok(match kind {
            0 => Value::nothing(),
            1 => Value::bool(self.read_bool()?),
            2 => Value::int(self.read_i64()?).map_err(|error| format!("{error:?}"))?,
            3 => Value::float(f64::from_bits(self.read_u64()?)),
            4 => Value::identity(self.read_identity()?),
            5 => Value::symbol(self.read_symbol()?),
            6 => Value::error_code(self.read_symbol()?),
            7 => Value::string(self.read_string()?),
            8 => Value::bytes(self.read_bytes()?),
            9 => {
                let count = self.read_len()?;
                let mut values = Vec::with_capacity(count);
                for _ in 0..count {
                    values.push(self.read_value()?);
                }
                Value::list(values)
            }
            10 => {
                let count = self.read_len()?;
                let mut entries = Vec::with_capacity(count);
                for _ in 0..count {
                    entries.push((self.read_value()?, self.read_value()?));
                }
                Value::map(entries)
            }
            11 => {
                let start = self.read_value()?;
                let end = if self.read_bool()? {
                    Some(self.read_value()?)
                } else {
                    None
                };
                Value::range(start, end)
            }
            12 => {
                let code = self.read_symbol()?;
                let message = self.read_optional_string()?;
                let value = if self.read_bool()? {
                    Some(self.read_value()?)
                } else {
                    None
                };
                Value::error(code, message, value)
            }
            tag => return Err(format!("unknown value kind tag {tag}")),
        })
    }

    fn read_identity(&mut self) -> Result<Identity, String> {
        let raw = self.read_u64()?;
        Identity::new(raw).ok_or_else(|| format!("identity {raw} is out of range"))
    }

    fn read_symbol(&mut self) -> Result<Symbol, String> {
        Ok(Symbol::intern(&self.read_string()?))
    }

    fn read_optional_symbol(&mut self) -> Result<Option<Symbol>, String> {
        if self.read_bool()? {
            Ok(Some(self.read_symbol()?))
        } else {
            Ok(None)
        }
    }

    fn read_optional_string(&mut self) -> Result<Option<String>, String> {
        if self.read_bool()? {
            Ok(Some(self.read_string()?))
        } else {
            Ok(None)
        }
    }

    fn read_string(&mut self) -> Result<String, String> {
        String::from_utf8(self.read_bytes()?).map_err(|error| format!("invalid utf-8: {error}"))
    }

    fn read_bytes(&mut self) -> Result<Vec<u8>, String> {
        let len = self.read_len()?;
        Ok(self.read_exact(len)?.to_vec())
    }

    fn read_bool(&mut self) -> Result<bool, String> {
        match self.read_u8()? {
            0 => Ok(false),
            1 => Ok(true),
            value => Err(format!("invalid boolean byte {value}")),
        }
    }

    fn read_len(&mut self) -> Result<usize, String> {
        Ok(self.read_u32()? as usize)
    }

    fn read_u8(&mut self) -> Result<u8, String> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_u16(&mut self) -> Result<u16, String> {
        let bytes = self.read_exact(2)?;
        Ok(u16::from_be_bytes(bytes.try_into().unwrap()))
    }

    fn read_u32(&mut self) -> Result<u32, String> {
        let bytes = self.read_exact(4)?;
        Ok(u32::from_be_bytes(bytes.try_into().unwrap()))
    }

    fn read_u64(&mut self) -> Result<u64, String> {
        let bytes = self.read_exact(8)?;
        Ok(u64::from_be_bytes(bytes.try_into().unwrap()))
    }

    fn read_i64(&mut self) -> Result<i64, String> {
        let bytes = self.read_exact(8)?;
        Ok(i64::from_be_bytes(bytes.try_into().unwrap()))
    }

    fn read_exact(&mut self, len: usize) -> Result<&'a [u8], String> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| "commit record offset overflow".to_owned())?;
        if end > self.bytes.len() {
            return Err(format!(
                "commit record ended early: need {len} bytes at offset {}, len {}",
                self.offset,
                self.bytes.len()
            ));
        }
        let bytes = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(bytes)
    }
}
