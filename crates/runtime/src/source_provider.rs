use mica_relation_kernel::{
    ComputedRelation, ComputedRelationRead, KernelError, RelationId, RelationMetadata,
    RelationRead, Tuple,
};
use mica_var::Value;
use std::env;
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

const REPOSITORY_ENTRY_BOUND: &[u16] = &[0, 1, 2];
const FILE_TEXT_BOUND: &[u16] = &[0, 1, 2];
const FILE_LINES_BOUND: &[u16] = &[0, 1, 2, 3, 4];
const FILE_CONTENT_HASH_BOUND: &[u16] = &[0, 1, 2];

pub(crate) fn default_computed_relations() -> Vec<Arc<dyn ComputedRelation>> {
    let provider = Arc::new(LocalSourceProvider::from_env());
    vec![
        Arc::new(RepositoryEntryRelation {
            provider: provider.clone(),
        }),
        Arc::new(FileTextRelation {
            provider: provider.clone(),
        }),
        Arc::new(FileLinesRelation {
            provider: provider.clone(),
        }),
        Arc::new(FileContentHashRelation { provider }),
    ]
}

#[derive(Debug)]
struct LocalSourceProvider {
    allowed_roots: Vec<PathBuf>,
}

impl LocalSourceProvider {
    fn from_env() -> Self {
        let configured_roots = env::var_os("MICA_SOURCE_ROOTS")
            .map(|roots| env::split_paths(&roots).collect::<Vec<_>>())
            .or_else(|| env::var_os("MICA_SOURCE_ROOT").map(|root| vec![PathBuf::from(root)]))
            .unwrap_or_else(|| vec![env::current_dir().unwrap_or_else(|_| PathBuf::from("."))]);
        let allowed_roots = configured_roots
            .into_iter()
            .filter_map(|root| root.canonicalize().ok())
            .collect();
        Self { allowed_roots }
    }

    fn repository_root(
        &self,
        reader: &dyn ComputedRelationRead,
        relation: RelationId,
        repository: &Value,
        revision: &Value,
    ) -> Result<PathBuf, KernelError> {
        let root_relation = relation_id(reader, "source/RepositoryRoot", 2).ok_or_else(|| {
            invalid_relation(relation, "missing relation source/RepositoryRoot/2")
        })?;
        let revision_of = relation_id(reader, "source/RevisionOf", 2)
            .ok_or_else(|| invalid_relation(relation, "missing relation source/RevisionOf/2"))?;

        if reader
            .scan_relation(
                revision_of,
                &[Some(revision.clone()), Some(repository.clone())],
            )?
            .is_empty()
        {
            return Err(invalid_relation(
                relation,
                "revision does not belong to repository",
            ));
        }

        let root = one_value(
            reader,
            root_relation,
            &[Some(repository.clone()), None],
            relation,
            "expected source/RepositoryRoot(repository, root)",
        )?
        .with_str(str::to_owned)
        .ok_or_else(|| invalid_relation(relation, "repository root must be a string"))?;
        let root = PathBuf::from(root).canonicalize().map_err(|error| {
            invalid_relation(relation, format!("invalid repository root: {error}"))
        })?;
        if self
            .allowed_roots
            .iter()
            .any(|allowed| root.starts_with(allowed))
        {
            Ok(root)
        } else {
            Err(invalid_relation(
                relation,
                format!(
                    "repository root {} is not under an allowed source root",
                    root.display()
                ),
            ))
        }
    }

    fn resolve_path(
        &self,
        reader: &dyn ComputedRelationRead,
        relation: RelationId,
        repository: &Value,
        revision: &Value,
        relative_path: &str,
    ) -> Result<(PathBuf, PathBuf), KernelError> {
        validate_relative_path(relation, relative_path)?;
        let root = self.repository_root(reader, relation, repository, revision)?;
        let absolute = root.join(relative_path).canonicalize().map_err(|error| {
            invalid_relation(relation, format!("failed to resolve path: {error}"))
        })?;
        if !absolute.starts_with(&root) {
            return Err(invalid_relation(
                relation,
                "source path escapes repository root",
            ));
        }
        Ok((root, absolute))
    }
}

struct RepositoryEntryRelation {
    provider: Arc<LocalSourceProvider>,
}

impl ComputedRelation for RepositoryEntryRelation {
    fn name(&self) -> &'static str {
        "local-source-repository-entry"
    }

    fn matches(&self, metadata: &RelationMetadata) -> bool {
        metadata.name().name() == Some("source/RepositoryEntry") && metadata.arity() == 6
    }

    fn required_bound_positions(&self, _metadata: &RelationMetadata) -> &[u16] {
        REPOSITORY_ENTRY_BOUND
    }

    fn scan(
        &self,
        reader: &dyn ComputedRelationRead,
        metadata: &RelationMetadata,
        bindings: &[Option<Value>],
    ) -> Result<Vec<Tuple>, KernelError> {
        let repository = bound_value(metadata.id(), bindings, 0, "repository")?;
        let revision = bound_value(metadata.id(), bindings, 1, "revision")?;
        let parent = bound_string(metadata.id(), bindings, 2, "parent path")?;
        let (root, directory) =
            self.provider
                .resolve_path(reader, metadata.id(), &repository, &revision, &parent)?;
        if !directory.is_dir() {
            return Err(invalid_relation(
                metadata.id(),
                "repository entry parent path must be a directory",
            ));
        }

        let mut rows = fs::read_dir(&directory)
            .map_err(|error| {
                invalid_relation(metadata.id(), format!("failed to list directory: {error}"))
            })?
            .map(|entry| {
                let entry = entry.map_err(|error| {
                    invalid_relation(
                        metadata.id(),
                        format!("failed to read directory entry: {error}"),
                    )
                })?;
                let path = entry.path();
                let kind = if path.is_dir() { "directory" } else { "file" };
                if kind == "file" && fs::read_to_string(&path).is_err() {
                    return Ok(None);
                }
                let name = entry.file_name().to_string_lossy().into_owned();
                let relative = path.strip_prefix(&root).map_err(|_| {
                    invalid_relation(metadata.id(), "directory entry escaped repository root")
                })?;
                let relative = path_to_mica_string(metadata.id(), relative)?;
                Ok(Some(Tuple::from([
                    repository.clone(),
                    revision.clone(),
                    Value::string(&parent),
                    Value::string(relative),
                    Value::string(kind),
                    Value::string(name),
                ])))
            })
            .collect::<Result<Vec<_>, KernelError>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| left.values().cmp(right.values()));
        Ok(filter_bound_rows(rows, bindings))
    }
}

struct FileTextRelation {
    provider: Arc<LocalSourceProvider>,
}

impl ComputedRelation for FileTextRelation {
    fn name(&self) -> &'static str {
        "local-source-file-text"
    }

    fn matches(&self, metadata: &RelationMetadata) -> bool {
        metadata.name().name() == Some("source/FileText") && metadata.arity() == 5
    }

    fn required_bound_positions(&self, _metadata: &RelationMetadata) -> &[u16] {
        FILE_TEXT_BOUND
    }

    fn scan(
        &self,
        reader: &dyn ComputedRelationRead,
        metadata: &RelationMetadata,
        bindings: &[Option<Value>],
    ) -> Result<Vec<Tuple>, KernelError> {
        let repository = bound_value(metadata.id(), bindings, 0, "repository")?;
        let revision = bound_value(metadata.id(), bindings, 1, "revision")?;
        let path = bound_string(metadata.id(), bindings, 2, "path")?;
        let (_root, file) =
            self.provider
                .resolve_path(reader, metadata.id(), &repository, &revision, &path)?;
        let bytes = read_file_bytes(metadata.id(), &file)?;
        let text = String::from_utf8(bytes).map_err(|error| {
            invalid_relation(metadata.id(), format!("source file is not utf-8: {error}"))
        })?;
        let hash = content_hash(text.as_bytes());
        Ok(filter_bound_rows(
            vec![Tuple::from([
                repository,
                revision,
                Value::string(path),
                Value::string(text),
                Value::string(hash),
            ])],
            bindings,
        ))
    }
}

struct FileLinesRelation {
    provider: Arc<LocalSourceProvider>,
}

impl ComputedRelation for FileLinesRelation {
    fn name(&self) -> &'static str {
        "local-source-file-lines"
    }

    fn matches(&self, metadata: &RelationMetadata) -> bool {
        metadata.name().name() == Some("source/FileLines") && metadata.arity() == 7
    }

    fn required_bound_positions(&self, _metadata: &RelationMetadata) -> &[u16] {
        FILE_LINES_BOUND
    }

    fn scan(
        &self,
        reader: &dyn ComputedRelationRead,
        metadata: &RelationMetadata,
        bindings: &[Option<Value>],
    ) -> Result<Vec<Tuple>, KernelError> {
        let repository = bound_value(metadata.id(), bindings, 0, "repository")?;
        let revision = bound_value(metadata.id(), bindings, 1, "revision")?;
        let path = bound_string(metadata.id(), bindings, 2, "path")?;
        let start_line = bound_positive_int(metadata.id(), bindings, 3, "start line")?;
        let line_count = bound_non_negative_int(metadata.id(), bindings, 4, "line count")?;
        let (_root, file) =
            self.provider
                .resolve_path(reader, metadata.id(), &repository, &revision, &path)?;
        let bytes = read_file_bytes(metadata.id(), &file)?;
        let hash = content_hash(&bytes);
        let text = String::from_utf8(bytes).map_err(|error| {
            invalid_relation(metadata.id(), format!("source file is not utf-8: {error}"))
        })?;
        let lines = text
            .lines()
            .skip(start_line.saturating_sub(1))
            .take(line_count)
            .map(Value::string)
            .collect::<Vec<_>>();
        Ok(filter_bound_rows(
            vec![Tuple::from([
                repository,
                revision,
                Value::string(path),
                int_value(metadata.id(), start_line as i64)?,
                int_value(metadata.id(), line_count as i64)?,
                Value::list(lines),
                Value::string(hash),
            ])],
            bindings,
        ))
    }
}

struct FileContentHashRelation {
    provider: Arc<LocalSourceProvider>,
}

impl ComputedRelation for FileContentHashRelation {
    fn name(&self) -> &'static str {
        "local-source-file-content-hash"
    }

    fn matches(&self, metadata: &RelationMetadata) -> bool {
        metadata.name().name() == Some("source/FileContentHash") && metadata.arity() == 4
    }

    fn required_bound_positions(&self, _metadata: &RelationMetadata) -> &[u16] {
        FILE_CONTENT_HASH_BOUND
    }

    fn scan(
        &self,
        reader: &dyn ComputedRelationRead,
        metadata: &RelationMetadata,
        bindings: &[Option<Value>],
    ) -> Result<Vec<Tuple>, KernelError> {
        let repository = bound_value(metadata.id(), bindings, 0, "repository")?;
        let revision = bound_value(metadata.id(), bindings, 1, "revision")?;
        let path = bound_string(metadata.id(), bindings, 2, "path")?;
        let (_root, file) =
            self.provider
                .resolve_path(reader, metadata.id(), &repository, &revision, &path)?;
        let bytes = read_file_bytes(metadata.id(), &file)?;
        Ok(filter_bound_rows(
            vec![Tuple::from([
                repository,
                revision,
                Value::string(path),
                Value::string(content_hash(&bytes)),
            ])],
            bindings,
        ))
    }
}

fn relation_id(reader: &dyn ComputedRelationRead, name: &str, arity: u16) -> Option<RelationId> {
    reader
        .relation_metadata_vec()
        .into_iter()
        .find(|metadata| metadata.name().name() == Some(name) && metadata.arity() == arity)
        .map(|metadata| metadata.id())
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

fn bound_value(
    relation: RelationId,
    bindings: &[Option<Value>],
    position: usize,
    label: &str,
) -> Result<Value, KernelError> {
    bindings
        .get(position)
        .and_then(Clone::clone)
        .ok_or_else(|| invalid_relation(relation, format!("expected bound {label}")))
}

fn bound_string(
    relation: RelationId,
    bindings: &[Option<Value>],
    position: usize,
    label: &str,
) -> Result<String, KernelError> {
    bound_value(relation, bindings, position, label)?
        .with_str(str::to_owned)
        .ok_or_else(|| invalid_relation(relation, format!("{label} must be a string")))
}

fn bound_positive_int(
    relation: RelationId,
    bindings: &[Option<Value>],
    position: usize,
    label: &str,
) -> Result<usize, KernelError> {
    let value = bound_value(relation, bindings, position, label)?
        .as_int()
        .ok_or_else(|| invalid_relation(relation, format!("{label} must be an integer")))?;
    if value < 1 {
        return Err(invalid_relation(
            relation,
            format!("{label} must be greater than zero"),
        ));
    }
    Ok(value as usize)
}

fn bound_non_negative_int(
    relation: RelationId,
    bindings: &[Option<Value>],
    position: usize,
    label: &str,
) -> Result<usize, KernelError> {
    let value = bound_value(relation, bindings, position, label)?
        .as_int()
        .ok_or_else(|| invalid_relation(relation, format!("{label} must be an integer")))?;
    if value < 0 {
        return Err(invalid_relation(
            relation,
            format!("{label} must be non-negative"),
        ));
    }
    Ok(value as usize)
}

fn validate_relative_path(relation: RelationId, path: &str) -> Result<(), KernelError> {
    let path = Path::new(path);
    if path.is_absolute() {
        return Err(invalid_relation(
            relation,
            "source path must be relative to repository root",
        ));
    }
    for component in path.components() {
        match component {
            Component::Normal(_) | Component::CurDir => {}
            Component::ParentDir => {
                return Err(invalid_relation(
                    relation,
                    "source path must not contain parent components",
                ));
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(invalid_relation(
                    relation,
                    "source path must be relative to repository root",
                ));
            }
        }
    }
    Ok(())
}

fn read_file_bytes(relation: RelationId, file: &Path) -> Result<Vec<u8>, KernelError> {
    if !file.is_file() {
        return Err(invalid_relation(relation, "source path must be a file"));
    }
    fs::read(file)
        .map_err(|error| invalid_relation(relation, format!("failed to read file: {error}")))
}

fn path_to_mica_string(relation: RelationId, path: &Path) -> Result<String, KernelError> {
    path.to_str()
        .map(|path| path.replace('\\', "/"))
        .ok_or_else(|| invalid_relation(relation, "source path is not valid utf-8"))
}

fn content_hash(bytes: &[u8]) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("fnv1a64:{hash:016x}")
}

fn int_value(relation: RelationId, value: i64) -> Result<Value, KernelError> {
    Value::int(value).map_err(|error| invalid_relation(relation, format!("{error:?}")))
}

fn filter_bound_rows(rows: Vec<Tuple>, bindings: &[Option<Value>]) -> Vec<Tuple> {
    rows.into_iter()
        .filter(|row| {
            row.values()
                .iter()
                .zip(bindings.iter())
                .all(|(value, binding)| binding.as_ref().is_none_or(|binding| binding == value))
        })
        .collect()
}

fn invalid_relation(relation: RelationId, message: impl Into<String>) -> KernelError {
    KernelError::InvalidComputedRelation {
        relation,
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SourceRunner, TaskOutcome};
    use mica_var::Symbol;

    fn load_source_relations(runner: &mut SourceRunner) {
        let root = env::current_dir().unwrap().display().to_string();
        runner
            .run_source(&format!(
                "make_identity(:repo)\n\
                 make_identity(:rev)\n\
                 make_relation(:source/RepositoryRoot, 2)\n\
                 make_relation(:source/RevisionOf, 2)\n\
                 make_relation(:source/RepositoryEntry, 6)\n\
                 make_relation(:source/FileText, 5)\n\
                 make_relation(:source/FileLines, 7)\n\
                 make_relation(:source/FileContentHash, 4)\n\
                 assert source/RepositoryRoot(#repo, {root:?})\n\
                 assert source/RevisionOf(#rev, #repo)"
            ))
            .unwrap();
    }

    #[test]
    fn source_provider_reads_file_text_from_allowed_root() {
        let mut runner = SourceRunner::new_empty();
        load_source_relations(&mut runner);

        let report = runner
            .run_source(
                "let text = one source/FileText(#repo, #rev, \"Cargo.toml\", ?text, ?hash)\n\
                 return string_contains(text[:text], \"[package]\")",
            )
            .unwrap();
        assert!(matches!(
            report.outcome,
            TaskOutcome::Complete { value, .. } if value == Value::bool(true)
        ));
    }

    #[test]
    fn source_provider_reads_line_windows() {
        let mut runner = SourceRunner::new_empty();
        load_source_relations(&mut runner);

        let report = runner
            .run_source(
                "let row = one source/FileLines(#repo, #rev, \"Cargo.toml\", 1, 2, ?lines, ?hash)\n\
                 return row[:lines]",
            )
            .unwrap();
        let TaskOutcome::Complete { value, .. } = report.outcome else {
            panic!("expected complete outcome, got {:?}", report.outcome);
        };
        value
            .with_list(|values| {
                assert_eq!(values.len(), 2);
                assert_eq!(values[0], Value::string("[package]"));
            })
            .expect("expected line list");
    }

    #[test]
    fn source_provider_lists_repository_entries() {
        let mut runner = SourceRunner::new_empty();
        load_source_relations(&mut runner);

        let report = runner
            .run_source(
                "for entry in source/RepositoryEntry(#repo, #rev, \"\", ?path, ?kind, ?name)\n\
                   if entry[:path] == \"Cargo.toml\"\n\
                     return entry[:kind]\n\
                   end\n\
                 end\n\
                 return nothing",
            )
            .unwrap();
        assert!(matches!(
            report.outcome,
            TaskOutcome::Complete { value, .. } if value == Value::string("file")
        ));
    }

    #[test]
    fn source_provider_rejects_escaping_paths() {
        let mut runner = SourceRunner::new_empty();
        load_source_relations(&mut runner);

        let error = runner
            .run_source("return source/FileContentHash(#repo, #rev, \"../Cargo.toml\", ?hash)")
            .unwrap_err();
        assert!(format!("{error:?}").contains("parent components"));
    }

    #[test]
    fn source_provider_requires_bound_path() {
        let mut runner = SourceRunner::new_empty();
        load_source_relations(&mut runner);

        let error = runner
            .run_source("return source/FileText(#repo, #rev, ?path, ?text, ?hash)")
            .unwrap_err();
        assert!(format!("{error:?}").contains("MissingRequiredBindings"));
    }

    #[test]
    fn source_provider_relations_are_read_only() {
        let mut runner = SourceRunner::new_empty();
        load_source_relations(&mut runner);

        let repo = runner.named_identity(Symbol::intern("repo")).unwrap();
        let error = runner
            .run_source("assert source/FileContentHash(#repo, #rev, \"Cargo.toml\", \"x\")")
            .unwrap_err();
        assert!(format!("{error:?}").contains("ReadOnlyRelation"));
        assert!(
            format!("{error:?}").contains(&format!("{repo:?}"))
                || format!("{error:?}").contains("ReadOnlyRelation")
        );
    }
}
