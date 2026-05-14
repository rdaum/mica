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

mod codec;

use self::codec::{
    decode_commit, decode_relation_metadata, decode_rule_definition, decode_tuple, encode_commit,
    encode_relation_metadata_record, encode_rule_definition_record, encode_tuple_record, fact_key,
};
use super::{CommitProvider, PersistedKernelState};
use crate::{CatalogChange, Commit, FactChangeKind};
use fjall::{Database, Keyspace, KeyspaceCreateOptions};
use mica_var::Identity;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread::{self, JoinHandle};

const FJALL_FORMAT_VERSION: &str = "mica-relation-kernel-state-1.0.0";
const FJALL_SHAPE: &str = "relations:v1;rules:v1;facts:v1;commits:v1;encoding:mica-binary-v1";
const FJALL_METADATA_KEYSPACE: &str = "metadata";
const FJALL_RELATIONS_KEYSPACE: &str = "relations";
const FJALL_RULES_KEYSPACE: &str = "rules";
const FJALL_FACTS_KEYSPACE: &str = "facts";
const FJALL_COMMITS_KEYSPACE: &str = "commits";
const FORMAT_VERSION_KEY: &[u8] = b"format_version";
const SHAPE_KEY: &[u8] = b"shape";
const STATE_VERSION_KEY: &[u8] = b"state_version";

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FjallFormatStatus {
    Fresh,
    Uninitialized,
    Current,
    MigrationRequired {
        stored_version: Option<String>,
        stored_shape: Option<String>,
        current_version: &'static str,
        current_shape: &'static str,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FjallDurabilityMode {
    Relaxed,
    Strict,
}

#[derive(Clone)]
struct FjallKeyspaces {
    metadata: Keyspace,
    relations: Keyspace,
    rules: Keyspace,
    facts: Keyspace,
    commits: Keyspace,
}

pub struct FjallStateProvider {
    keyspaces: FjallKeyspaces,
    durability: FjallDurabilityMode,
    sender: mpsc::SyncSender<WriterMessage>,
    queued_version: AtomicU64,
    completed_version: Arc<AtomicU64>,
    write_error: Arc<Mutex<Option<String>>>,
    writer: Mutex<Option<JoinHandle<()>>>,
}

impl FjallStateProvider {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, String> {
        Self::open_with_durability(path, FjallDurabilityMode::Relaxed)
    }

    pub fn open_strict(path: impl AsRef<Path>) -> Result<Self, String> {
        Self::open_with_durability(path, FjallDurabilityMode::Strict)
    }

    pub fn open_with_durability(
        path: impl AsRef<Path>,
        durability: FjallDurabilityMode,
    ) -> Result<Self, String> {
        let path = path.as_ref();
        match Self::check_format(path)? {
            FjallFormatStatus::Fresh
            | FjallFormatStatus::Uninitialized
            | FjallFormatStatus::Current => {}
            FjallFormatStatus::MigrationRequired {
                stored_version,
                stored_shape,
                current_version,
                current_shape,
            } => {
                return Err(format!(
                    "fjall relation-kernel store needs migration: version {:?}, shape {:?}; current version {current_version}, shape {current_shape}",
                    stored_version, stored_shape
                ));
            }
        }

        let database = Database::builder(path)
            .open()
            .map_err(|error| format!("failed to open fjall database: {error}"))?;
        let metadata = open_keyspace(&database, FJALL_METADATA_KEYSPACE)?;
        let relations = open_keyspace(&database, FJALL_RELATIONS_KEYSPACE)?;
        let rules = open_keyspace(&database, FJALL_RULES_KEYSPACE)?;
        let facts = open_keyspace(&database, FJALL_FACTS_KEYSPACE)?;
        let commits = open_keyspace(&database, FJALL_COMMITS_KEYSPACE)?;
        write_format_markers(&metadata)?;

        let keyspaces = FjallKeyspaces {
            metadata,
            relations,
            rules,
            facts,
            commits,
        };
        let (sender, receiver) = mpsc::sync_channel(1024);
        let writer_database = database.clone();
        let writer_keyspaces = keyspaces.clone();
        let completed_version = Arc::new(AtomicU64::new(
            load_state_version(&keyspaces.metadata)?
                .unwrap_or(load_last_commit_version(&keyspaces.commits)?),
        ));
        let write_error = Arc::new(Mutex::new(None));
        let writer_completed_version = completed_version.clone();
        let writer_error = write_error.clone();
        let writer = thread::Builder::new()
            .name("mica-fjall-commit-writer".to_owned())
            .spawn(move || {
                writer_loop(
                    writer_database,
                    writer_keyspaces,
                    receiver,
                    writer_completed_version,
                    writer_error,
                )
            })
            .map_err(|error| format!("failed to spawn fjall commit writer: {error}"))?;

        let initial_version = completed_version.load(Ordering::Acquire);
        Ok(Self {
            keyspaces,
            durability,
            sender,
            queued_version: AtomicU64::new(initial_version),
            completed_version,
            write_error,
            writer: Mutex::new(Some(writer)),
        })
    }

    pub fn check_format(path: impl AsRef<Path>) -> Result<FjallFormatStatus, String> {
        let path = path.as_ref();
        if !path.exists() {
            return Ok(FjallFormatStatus::Fresh);
        }

        let database = Database::builder(path)
            .open()
            .map_err(|error| format!("failed to open fjall database for format check: {error}"))?;
        let metadata = open_keyspace(&database, FJALL_METADATA_KEYSPACE)?;
        let relations = open_keyspace(&database, FJALL_RELATIONS_KEYSPACE)?;
        let rules = open_keyspace(&database, FJALL_RULES_KEYSPACE)?;
        let facts = open_keyspace(&database, FJALL_FACTS_KEYSPACE)?;
        let commits = open_keyspace(&database, FJALL_COMMITS_KEYSPACE)?;
        let stored_version = read_marker(&metadata, FORMAT_VERSION_KEY)?;
        let stored_shape = read_marker(&metadata, SHAPE_KEY)?;

        match (&stored_version, &stored_shape) {
            (None, None)
                if !has_entries(&relations)
                    && !has_entries(&rules)
                    && !has_entries(&facts)
                    && !has_entries(&commits) =>
            {
                Ok(FjallFormatStatus::Uninitialized)
            }
            (Some(version), Some(shape))
                if version == FJALL_FORMAT_VERSION && shape == FJALL_SHAPE =>
            {
                Ok(FjallFormatStatus::Current)
            }
            _ => Ok(FjallFormatStatus::MigrationRequired {
                stored_version,
                stored_shape,
                current_version: FJALL_FORMAT_VERSION,
                current_shape: FJALL_SHAPE,
            }),
        }
    }

    pub fn load_commits(&self) -> Result<Vec<Commit>, String> {
        load_commits(&self.keyspaces.commits)
    }

    pub fn load_state(&self) -> Result<PersistedKernelState, String> {
        load_state(&self.keyspaces)
    }

    pub fn completed_version(&self) -> u64 {
        self.completed_version.load(Ordering::Acquire)
    }

    pub fn queued_version(&self) -> u64 {
        self.queued_version.load(Ordering::Acquire)
    }

    pub fn durability(&self) -> FjallDurabilityMode {
        self.durability
    }

    pub fn last_write_error(&self) -> Option<String> {
        self.write_error.lock().unwrap().clone()
    }

    fn check_writer_error(&self) -> Result<(), String> {
        match self.last_write_error() {
            Some(error) => Err(format!("fjall commit writer failed: {error}")),
            None => Ok(()),
        }
    }
}

impl CommitProvider for FjallStateProvider {
    fn persist_commit(&self, commit: &Commit) -> Result<(), String> {
        self.check_writer_error()?;
        match self.durability {
            FjallDurabilityMode::Relaxed => {
                self.sender
                    .send(WriterMessage::Persist {
                        commit: commit.clone(),
                        reply: None,
                    })
                    .map_err(|error| format!("fjall commit writer is stopped: {error}"))?;
                self.queued_version
                    .fetch_max(commit.version(), Ordering::AcqRel);
                Ok(())
            }
            FjallDurabilityMode::Strict => {
                let (reply_tx, reply_rx) = mpsc::channel();
                self.sender
                    .send(WriterMessage::Persist {
                        commit: commit.clone(),
                        reply: Some(reply_tx),
                    })
                    .map_err(|error| format!("fjall commit writer is stopped: {error}"))?;
                self.queued_version
                    .fetch_max(commit.version(), Ordering::AcqRel);
                reply_rx
                    .recv()
                    .map_err(|error| format!("fjall commit writer dropped reply: {error}"))?
            }
        }
    }
}

impl Drop for FjallStateProvider {
    fn drop(&mut self) {
        let (reply_tx, reply_rx) = mpsc::channel();
        let _ = self
            .sender
            .send(WriterMessage::Shutdown { reply: reply_tx });
        let _ = reply_rx.recv();
        if let Some(writer) = self.writer.lock().unwrap().take() {
            let _ = writer.join();
        }
    }
}

enum WriterMessage {
    Persist {
        commit: Commit,
        reply: Option<mpsc::Sender<Result<(), String>>>,
    },
    Shutdown {
        reply: mpsc::Sender<()>,
    },
}

fn writer_loop(
    database: Database,
    keyspaces: FjallKeyspaces,
    receiver: mpsc::Receiver<WriterMessage>,
    completed_version: Arc<AtomicU64>,
    write_error: Arc<Mutex<Option<String>>>,
) {
    while let Ok(message) = receiver.recv() {
        match message {
            WriterMessage::Persist { commit, reply } => {
                let result = write_commit(&database, &keyspaces, &commit);
                match &result {
                    Ok(()) => {
                        completed_version.fetch_max(commit.version(), Ordering::AcqRel);
                    }
                    Err(error) => {
                        *write_error.lock().unwrap() = Some(error.clone());
                    }
                }
                if let Some(reply) = reply {
                    let _ = reply.send(result);
                }
            }
            WriterMessage::Shutdown { reply } => {
                let _ = reply.send(());
                break;
            }
        }
    }
}

fn open_keyspace(database: &Database, name: &str) -> Result<Keyspace, String> {
    database
        .keyspace(name, KeyspaceCreateOptions::default)
        .map_err(|error| format!("failed to open fjall keyspace `{name}`: {error}"))
}

fn read_marker(metadata: &Keyspace, key: &[u8]) -> Result<Option<String>, String> {
    metadata
        .get(key)
        .map_err(|error| format!("failed to read fjall metadata marker: {error}"))?
        .map(|bytes| {
            String::from_utf8(bytes.to_vec())
                .map_err(|error| format!("invalid utf-8 in fjall metadata marker: {error}"))
        })
        .transpose()
}

fn write_format_markers(metadata: &Keyspace) -> Result<(), String> {
    metadata
        .insert(FORMAT_VERSION_KEY, FJALL_FORMAT_VERSION.as_bytes())
        .map_err(|error| format!("failed to write fjall format marker: {error}"))?;
    metadata
        .insert(SHAPE_KEY, FJALL_SHAPE.as_bytes())
        .map_err(|error| format!("failed to write fjall shape marker: {error}"))?;
    Ok(())
}

fn has_entries(keyspace: &Keyspace) -> bool {
    keyspace.iter().next().is_some()
}

fn load_state_version(metadata: &Keyspace) -> Result<Option<u64>, String> {
    metadata
        .get(STATE_VERSION_KEY)
        .map_err(|error| format!("failed to read fjall state version: {error}"))?
        .map(|bytes| {
            let bytes = bytes.as_ref();
            if bytes.len() != 8 {
                return Err(format!(
                    "invalid fjall state version length {}",
                    bytes.len()
                ));
            }
            Ok(u64::from_be_bytes(bytes.try_into().unwrap()))
        })
        .transpose()
}

fn load_last_commit_version(commits: &Keyspace) -> Result<u64, String> {
    let mut last = 0;
    for entry in commits.iter() {
        let (key, _) = entry
            .into_inner()
            .map_err(|error| format!("failed to read fjall commit key: {error}"))?;
        let key = key.as_ref();
        if key.len() != 8 {
            return Err(format!("invalid fjall commit key length {}", key.len()));
        }
        last = u64::from_be_bytes(key.try_into().unwrap());
    }
    Ok(last)
}

fn load_commits(commits: &Keyspace) -> Result<Vec<Commit>, String> {
    let mut out = Vec::new();
    for entry in commits.iter() {
        let (_, value) = entry
            .into_inner()
            .map_err(|error| format!("failed to read fjall commit entry: {error}"))?;
        out.push(decode_commit(value.as_ref())?);
    }
    Ok(out)
}

fn load_state(keyspaces: &FjallKeyspaces) -> Result<PersistedKernelState, String> {
    let version = load_state_version(&keyspaces.metadata)?
        .unwrap_or(load_last_commit_version(&keyspaces.commits)?);
    let mut relations = Vec::new();
    for entry in keyspaces.relations.iter() {
        let (_, value) = entry
            .into_inner()
            .map_err(|error| format!("failed to read fjall relation entry: {error}"))?;
        relations.push(decode_relation_metadata(value.as_ref())?);
    }

    let mut rules = Vec::new();
    for entry in keyspaces.rules.iter() {
        let (_, value) = entry
            .into_inner()
            .map_err(|error| format!("failed to read fjall rule entry: {error}"))?;
        rules.push(decode_rule_definition(value.as_ref())?);
    }

    let mut facts = Vec::new();
    for entry in keyspaces.facts.iter() {
        let (key, value) = entry
            .into_inner()
            .map_err(|error| format!("failed to read fjall fact entry: {error}"))?;
        let key = key.as_ref();
        if key.len() < 8 {
            return Err(format!("invalid fjall fact key length {}", key.len()));
        }
        let relation = Identity::new(u64::from_be_bytes(key[..8].try_into().unwrap()))
            .ok_or_else(|| "invalid relation identity in fjall fact key".to_owned())?;
        facts.push((relation, decode_tuple(value.as_ref())?));
    }

    Ok(PersistedKernelState {
        version,
        relations,
        rules,
        facts,
    })
}

fn write_commit(
    database: &Database,
    keyspaces: &FjallKeyspaces,
    commit: &Commit,
) -> Result<(), String> {
    let mut batch = database.batch();
    batch.insert(
        &keyspaces.commits,
        commit.version().to_be_bytes(),
        &encode_commit(commit)?,
    );
    for change in commit.catalog_changes() {
        match change {
            CatalogChange::RelationCreated(metadata) => {
                batch.insert(
                    &keyspaces.relations,
                    metadata.id().raw().to_be_bytes(),
                    &encode_relation_metadata_record(metadata)?,
                );
            }
            CatalogChange::RuleInstalled(rule) => {
                batch.insert(
                    &keyspaces.rules,
                    rule.id().raw().to_be_bytes(),
                    &encode_rule_definition_record(rule)?,
                );
            }
            CatalogChange::RuleDisabled(rule_id) => {
                let key = rule_id.raw().to_be_bytes();
                let value = keyspaces
                    .rules
                    .get(key)
                    .map_err(|error| format!("failed to read fjall rule for disable: {error}"))?
                    .ok_or_else(|| format!("cannot disable missing persisted rule {rule_id:?}"))?;
                let mut rule = decode_rule_definition(value.as_ref())?;
                rule.deactivate();
                batch.insert(
                    &keyspaces.rules,
                    key,
                    &encode_rule_definition_record(&rule)?,
                );
            }
        }
    }
    for change in commit.changes() {
        let key = fact_key(change.relation, &change.tuple)?;
        match change.kind {
            FactChangeKind::Assert => {
                batch.insert(&keyspaces.facts, key, &encode_tuple_record(&change.tuple)?);
            }
            FactChangeKind::Retract => {
                batch.remove(&keyspaces.facts, key);
            }
        }
    }
    batch.insert(
        &keyspaces.metadata,
        STATE_VERSION_KEY,
        commit.version().to_be_bytes(),
    );
    batch.commit().map_err(|error| {
        format!(
            "failed to persist fjall commit {}: {error}",
            commit.version()
        )
    })
}
