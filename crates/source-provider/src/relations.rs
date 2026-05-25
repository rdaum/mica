use crate::index::PersistentSemanticIndex;
use crate::navigation::{
    SemanticSymbol, SemanticSymbolProvider, byte_offset_to_lsp_position, semantic_location,
    semantic_symbol,
};
use crate::rust_analyzer::RustAnalyzerProvider;
use crate::syntax::{SourceLanguage, SyntaxDocument, syntax_lines};
use crate::util::*;
use mica_relation_kernel::{
    ComputedRelation, ComputedRelationRead, KernelError, RelationId, RelationMetadata, Tuple,
};
use mica_var::Value;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

const REPOSITORY_ENTRY_BOUND: &[u16] = &[0, 1, 2];
const FILE_TEXT_BOUND: &[u16] = &[0, 1, 2];
const FILE_LINES_BOUND: &[u16] = &[0, 1, 2, 3, 4];
const FILE_LINE_COUNT_BOUND: &[u16] = &[0, 1, 2];
const FILE_CONTENT_HASH_BOUND: &[u16] = &[0, 1, 2];
const SYNTAX_LINE_BOUND: &[u16] = &[0, 1, 2, 3, 4];
const SYNTAX_OUTLINE_BOUND: &[u16] = &[0, 1, 2];
const SYNTAX_NODE_AT_BOUND: &[u16] = &[0, 1, 2, 3];
const DEFINITION_AT_BOUND: &[u16] = &[0, 1, 2, 3];
const REFERENCES_OF_BOUND: &[u16] = &[0, 1, 2];
const SYMBOL_SEARCH_BOUND: &[u16] = &[0, 1, 2, 3];
const INDEX_VALUE_BOUND: &[u16] = &[];
const SEMANTIC_INDEX_REFRESH_INTERVAL: Duration = Duration::from_secs(1);

pub fn default_computed_relations() -> Vec<Arc<dyn ComputedRelation>> {
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
        Arc::new(FileLineCountRelation {
            provider: provider.clone(),
        }),
        Arc::new(FileContentHashRelation {
            provider: provider.clone(),
        }),
        Arc::new(SyntaxLineRelation {
            provider: provider.clone(),
        }),
        Arc::new(SyntaxOutlineRelation {
            provider: provider.clone(),
        }),
        Arc::new(SyntaxNodeAtRelation {
            provider: provider.clone(),
        }),
        Arc::new(DefinitionAtRelation {
            provider: provider.clone(),
        }),
        Arc::new(ReferencesOfRelation {
            provider: provider.clone(),
        }),
        Arc::new(SymbolSearchRelation {
            provider: provider.clone(),
        }),
        Arc::new(SourceIndexRelation {
            provider: provider.clone(),
        }),
        Arc::new(IndexRepositoryRelation {
            provider: provider.clone(),
        }),
        Arc::new(IndexRevisionRelation {
            provider: provider.clone(),
        }),
        Arc::new(IndexProviderRelation {
            provider: provider.clone(),
        }),
        Arc::new(IndexStatusRelation {
            provider: provider.clone(),
        }),
        Arc::new(IndexVersionRelation {
            provider: provider.clone(),
        }),
        Arc::new(IndexBuildErrorRelation { provider }),
    ]
}

#[derive(Debug)]
struct LocalSourceProvider {
    allowed_roots: Vec<PathBuf>,
    semantic_index_path: PathBuf,
    semantic_index_cache: Mutex<Option<CachedSemanticIndex>>,
    rust_analyzer: RustAnalyzerProvider,
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
        let semantic_index_path = env::var_os("MICA_SOURCE_INDEX")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(".cache/source-index/mica-worktree.json"));
        Self {
            allowed_roots,
            semantic_index_path,
            semantic_index_cache: Mutex::new(None),
            rust_analyzer: RustAnalyzerProvider::from_env(),
        }
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

    fn semantic_index(
        &self,
        relation: RelationId,
    ) -> Result<Arc<PersistentSemanticIndex>, KernelError> {
        let mut cache = self.semantic_index_cache.lock().unwrap();
        if let Some(cached) = cache.as_ref()
            && cached.last_checked.elapsed() < SEMANTIC_INDEX_REFRESH_INTERVAL
        {
            return Ok(cached.index.clone());
        }

        let key = semantic_index_key(relation, &self.semantic_index_path)?;
        if let Some(cached) = cache.as_ref()
            && cached.key == key
        {
            let index = cached.index.clone();
            *cache = Some(CachedSemanticIndex {
                key,
                last_checked: Instant::now(),
                index: index.clone(),
            });
            return Ok(index);
        }
        let index = Arc::new(PersistentSemanticIndex::load(
            relation,
            &self.semantic_index_path,
        )?);
        *cache = Some(CachedSemanticIndex {
            key,
            last_checked: Instant::now(),
            index: index.clone(),
        });
        Ok(index)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SemanticIndexKey {
    len: u64,
    modified: Option<SystemTime>,
}

#[derive(Clone, Debug)]
struct CachedSemanticIndex {
    key: Option<SemanticIndexKey>,
    last_checked: Instant,
    index: Arc<PersistentSemanticIndex>,
}

fn semantic_index_key(
    relation: RelationId,
    path: &Path,
) -> Result<Option<SemanticIndexKey>, KernelError> {
    match fs::metadata(path) {
        Ok(metadata) => Ok(Some(SemanticIndexKey {
            len: metadata.len(),
            modified: metadata.modified().ok(),
        })),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(invalid_relation(
            relation,
            format!("failed to stat semantic index {}: {error}", path.display()),
        )),
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

struct FileLineCountRelation {
    provider: Arc<LocalSourceProvider>,
}

impl ComputedRelation for FileLineCountRelation {
    fn name(&self) -> &'static str {
        "local-source-file-line-count"
    }

    fn matches(&self, metadata: &RelationMetadata) -> bool {
        metadata.name().name() == Some("source/FileLineCount") && metadata.arity() == 4
    }

    fn required_bound_positions(&self, _metadata: &RelationMetadata) -> &[u16] {
        FILE_LINE_COUNT_BOUND
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
        let text = read_utf8_file(metadata.id(), &file)?;
        let count = text.lines().count().max(1);
        Ok(filter_bound_rows(
            vec![Tuple::from([
                repository,
                revision,
                Value::string(path),
                int_value(metadata.id(), count as i64)?,
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

struct SyntaxLineRelation {
    provider: Arc<LocalSourceProvider>,
}

impl ComputedRelation for SyntaxLineRelation {
    fn name(&self) -> &'static str {
        "local-source-syntax-line"
    }

    fn matches(&self, metadata: &RelationMetadata) -> bool {
        metadata.name().name() == Some("source/SyntaxLine") && metadata.arity() == 8
    }

    fn required_bound_positions(&self, _metadata: &RelationMetadata) -> &[u16] {
        SYNTAX_LINE_BOUND
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
        let syntax = SyntaxDocument::parse(&path, &text);
        let rows = syntax_lines(metadata.id(), &syntax, start_line, line_count)?
            .into_iter()
            .map(|line| {
                Ok(Tuple::from([
                    repository.clone(),
                    revision.clone(),
                    Value::string(&path),
                    int_value(metadata.id(), start_line as i64)?,
                    int_value(metadata.id(), line_count as i64)?,
                    int_value(metadata.id(), line.number as i64)?,
                    Value::list(line.segments),
                    Value::string(&hash),
                ]))
            })
            .collect::<Result<Vec<_>, KernelError>>()?;
        Ok(filter_bound_rows(rows, bindings))
    }
}

struct SyntaxOutlineRelation {
    provider: Arc<LocalSourceProvider>,
}

impl ComputedRelation for SyntaxOutlineRelation {
    fn name(&self) -> &'static str {
        "local-source-syntax-outline"
    }

    fn matches(&self, metadata: &RelationMetadata) -> bool {
        metadata.name().name() == Some("source/SyntaxOutline") && metadata.arity() == 10
    }

    fn required_bound_positions(&self, _metadata: &RelationMetadata) -> &[u16] {
        SYNTAX_OUTLINE_BOUND
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
        let syntax = SyntaxDocument::parse(&path, &text);
        let rows = syntax
            .outline
            .into_iter()
            .map(|item| {
                Ok(Tuple::from([
                    repository.clone(),
                    revision.clone(),
                    Value::string(&path),
                    Value::string(item.node),
                    Value::string(item.kind),
                    Value::string(item.name),
                    int_value(metadata.id(), item.start_line as i64)?,
                    int_value(metadata.id(), item.end_line as i64)?,
                    int_value(metadata.id(), item.start_byte as i64)?,
                    int_value(metadata.id(), item.end_byte as i64)?,
                ]))
            })
            .collect::<Result<Vec<_>, KernelError>>()?;
        Ok(filter_bound_rows(rows, bindings))
    }
}

struct SyntaxNodeAtRelation {
    provider: Arc<LocalSourceProvider>,
}

impl ComputedRelation for SyntaxNodeAtRelation {
    fn name(&self) -> &'static str {
        "local-source-syntax-node-at"
    }

    fn matches(&self, metadata: &RelationMetadata) -> bool {
        metadata.name().name() == Some("source/SyntaxNodeAt") && metadata.arity() == 11
    }

    fn required_bound_positions(&self, _metadata: &RelationMetadata) -> &[u16] {
        SYNTAX_NODE_AT_BOUND
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
        let byte_offset = bound_non_negative_int(metadata.id(), bindings, 3, "byte offset")?;
        let (_root, file) =
            self.provider
                .resolve_path(reader, metadata.id(), &repository, &revision, &path)?;
        let bytes = read_file_bytes(metadata.id(), &file)?;
        let text = String::from_utf8(bytes).map_err(|error| {
            invalid_relation(metadata.id(), format!("source file is not utf-8: {error}"))
        })?;
        if byte_offset > text.len() {
            return Err(invalid_relation(
                metadata.id(),
                "byte offset is beyond source file length",
            ));
        }
        let syntax = SyntaxDocument::parse(&path, &text);
        let item = syntax.node_at(byte_offset);
        let rows = if let Some(item) = item {
            vec![Tuple::from([
                repository,
                revision,
                Value::string(path),
                int_value(metadata.id(), byte_offset as i64)?,
                Value::string(item.node),
                Value::string(item.kind),
                Value::string(item.name),
                int_value(metadata.id(), item.start_line as i64)?,
                int_value(metadata.id(), item.end_line as i64)?,
                int_value(metadata.id(), item.start_byte as i64)?,
                int_value(metadata.id(), item.end_byte as i64)?,
            ])]
        } else {
            Vec::new()
        };
        Ok(filter_bound_rows(rows, bindings))
    }
}

struct DefinitionAtRelation {
    provider: Arc<LocalSourceProvider>,
}

impl ComputedRelation for DefinitionAtRelation {
    fn name(&self) -> &'static str {
        "rust-analyzer-definition-at"
    }

    fn matches(&self, metadata: &RelationMetadata) -> bool {
        metadata.name().name() == Some("source/DefinitionAt") && metadata.arity() == 13
    }

    fn required_bound_positions(&self, _metadata: &RelationMetadata) -> &[u16] {
        DEFINITION_AT_BOUND
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
        let byte_offset = bound_non_negative_int(metadata.id(), bindings, 3, "byte offset")?;
        let (root, file) =
            self.provider
                .resolve_path(reader, metadata.id(), &repository, &revision, &path)?;
        let text = read_utf8_file(metadata.id(), &file)?;
        if byte_offset > text.len() {
            return Err(invalid_relation(
                metadata.id(),
                "byte offset is beyond source file length",
            ));
        }
        let index = self.provider.semantic_index(metadata.id())?;
        let indexed_rows = index
            .definition_at(&path, byte_offset)
            .into_iter()
            .map(|symbol| {
                Ok(Tuple::from([
                    repository.clone(),
                    revision.clone(),
                    Value::string(&path),
                    int_value(metadata.id(), byte_offset as i64)?,
                    Value::string(symbol.symbol),
                    Value::string(symbol.name),
                    Value::string(symbol.kind),
                    Value::string(symbol.path),
                    int_value(metadata.id(), symbol.start_line as i64)?,
                    int_value(metadata.id(), symbol.end_line as i64)?,
                    int_value(metadata.id(), symbol.start_byte as i64)?,
                    int_value(metadata.id(), symbol.end_byte as i64)?,
                    Value::string(format!("{} {}", index.provider, index.version)),
                ]))
            })
            .collect::<Result<Vec<_>, KernelError>>()?;
        if !indexed_rows.is_empty() {
            return Ok(filter_bound_rows(indexed_rows, bindings));
        }
        if SourceLanguage::from_path(&path) != SourceLanguage::Rust {
            return Ok(Vec::new());
        }
        let mut locations = self
            .provider
            .rust_analyzer
            .definition(
                &rust_workspace_root(&root),
                &file,
                &text,
                byte_offset_to_lsp_position(metadata.id(), &text, byte_offset)?,
            )
            .unwrap_or_default();
        if locations.is_empty()
            && let Some(ch) = text.get(byte_offset..).and_then(|text| text.chars().next())
        {
            let inner_offset = byte_offset + ch.len_utf8();
            if inner_offset <= text.len() {
                locations = self
                    .provider
                    .rust_analyzer
                    .definition(
                        &rust_workspace_root(&root),
                        &file,
                        &text,
                        byte_offset_to_lsp_position(metadata.id(), &text, inner_offset)?,
                    )
                    .unwrap_or_default();
            }
        }
        let mut rows = Vec::new();
        for location in locations {
            let Some(location) = semantic_location(metadata.id(), &root, location)? else {
                continue;
            };
            let symbol = semantic_symbol(&location);
            rows.push(Tuple::from([
                repository.clone(),
                revision.clone(),
                Value::string(&path),
                int_value(metadata.id(), byte_offset as i64)?,
                Value::string(symbol),
                Value::string(location.name),
                Value::string(location.kind),
                Value::string(location.path),
                int_value(metadata.id(), location.start_line as i64)?,
                int_value(metadata.id(), location.end_line as i64)?,
                int_value(metadata.id(), location.start_byte as i64)?,
                int_value(metadata.id(), location.end_byte as i64)?,
                Value::string(location.provider),
            ]));
        }
        Ok(filter_bound_rows(rows, bindings))
    }
}

struct ReferencesOfRelation {
    provider: Arc<LocalSourceProvider>,
}

impl ComputedRelation for ReferencesOfRelation {
    fn name(&self) -> &'static str {
        "rust-analyzer-references-of"
    }

    fn matches(&self, metadata: &RelationMetadata) -> bool {
        metadata.name().name() == Some("source/ReferencesOf") && metadata.arity() == 10
    }

    fn required_bound_positions(&self, _metadata: &RelationMetadata) -> &[u16] {
        REFERENCES_OF_BOUND
    }

    fn scan(
        &self,
        reader: &dyn ComputedRelationRead,
        metadata: &RelationMetadata,
        bindings: &[Option<Value>],
    ) -> Result<Vec<Tuple>, KernelError> {
        let repository = bound_value(metadata.id(), bindings, 0, "repository")?;
        let revision = bound_value(metadata.id(), bindings, 1, "revision")?;
        let symbol = bound_string(metadata.id(), bindings, 2, "symbol")?;
        let Some(request) = SemanticSymbol::parse(&symbol) else {
            return Ok(Vec::new());
        };
        let index = self.provider.semantic_index(metadata.id())?;
        if request.provider == SemanticSymbolProvider::Index {
            let rows = index
                .references_of(&request)
                .into_iter()
                .map(|reference| {
                    Ok(Tuple::from([
                        repository.clone(),
                        revision.clone(),
                        Value::string(&symbol),
                        Value::string(reference.path),
                        int_value(metadata.id(), reference.start_line as i64)?,
                        int_value(metadata.id(), reference.end_line as i64)?,
                        int_value(metadata.id(), reference.start_byte as i64)?,
                        int_value(metadata.id(), reference.end_byte as i64)?,
                        Value::string(format!("{} {}", index.provider, index.version)),
                        Value::string(reference.name),
                    ]))
                })
                .collect::<Result<Vec<_>, KernelError>>()?;
            return Ok(filter_bound_rows(rows, bindings));
        }
        let root = self
            .provider
            .repository_root(reader, metadata.id(), &repository, &revision)?;
        validate_relative_path(metadata.id(), &request.path)?;
        let file = root.join(&request.path).canonicalize().map_err(|error| {
            invalid_relation(
                metadata.id(),
                format!("failed to resolve symbol path: {error}"),
            )
        })?;
        if !file.starts_with(&root) {
            return Err(invalid_relation(
                metadata.id(),
                "symbol path escapes repository root",
            ));
        }
        let text = read_utf8_file(metadata.id(), &file)?;
        let position = byte_offset_to_lsp_position(metadata.id(), &text, request.start_byte)?;
        let locations = self
            .provider
            .rust_analyzer
            .references(&rust_workspace_root(&root), &file, &text, position)
            .unwrap_or_default();
        let mut rows = Vec::new();
        for location in locations {
            let Some(location) = semantic_location(metadata.id(), &root, location)? else {
                continue;
            };
            rows.push(Tuple::from([
                repository.clone(),
                revision.clone(),
                Value::string(&symbol),
                Value::string(location.path),
                int_value(metadata.id(), location.start_line as i64)?,
                int_value(metadata.id(), location.end_line as i64)?,
                int_value(metadata.id(), location.start_byte as i64)?,
                int_value(metadata.id(), location.end_byte as i64)?,
                Value::string(location.provider),
                Value::string(location.name),
            ]));
        }
        Ok(filter_bound_rows(rows, bindings))
    }
}

struct SymbolSearchRelation {
    provider: Arc<LocalSourceProvider>,
}

impl ComputedRelation for SymbolSearchRelation {
    fn name(&self) -> &'static str {
        "persistent-source-symbol-search"
    }

    fn matches(&self, metadata: &RelationMetadata) -> bool {
        metadata.name().name() == Some("source/SymbolSearch") && metadata.arity() == 11
    }

    fn required_bound_positions(&self, _metadata: &RelationMetadata) -> &[u16] {
        SYMBOL_SEARCH_BOUND
    }

    fn scan(
        &self,
        _reader: &dyn ComputedRelationRead,
        metadata: &RelationMetadata,
        bindings: &[Option<Value>],
    ) -> Result<Vec<Tuple>, KernelError> {
        let repository = bound_value(metadata.id(), bindings, 0, "repository")?;
        let revision = bound_value(metadata.id(), bindings, 1, "revision")?;
        let query = bound_string(metadata.id(), bindings, 2, "query")?;
        let limit = bound_non_negative_int(metadata.id(), bindings, 3, "limit")?;
        let index = self.provider.semantic_index(metadata.id())?;
        let rows = index
            .search(&query, limit)
            .into_iter()
            .map(|symbol| {
                Ok(Tuple::from([
                    repository.clone(),
                    revision.clone(),
                    Value::string(&query),
                    int_value(metadata.id(), limit as i64)?,
                    Value::string(symbol.symbol),
                    Value::string(symbol.name),
                    Value::string(symbol.kind),
                    Value::string(symbol.path),
                    int_value(metadata.id(), symbol.start_line as i64)?,
                    int_value(metadata.id(), symbol.end_line as i64)?,
                    Value::string(format!("{} {}", index.provider, index.version)),
                ]))
            })
            .collect::<Result<Vec<_>, KernelError>>()?;
        Ok(filter_bound_rows(rows, bindings))
    }
}

macro_rules! index_value_relation {
    ($name:ident, $relation:literal, $field:ident) => {
        struct $name {
            provider: Arc<LocalSourceProvider>,
        }

        impl ComputedRelation for $name {
            fn name(&self) -> &'static str {
                concat!("persistent-source-", $relation)
            }

            fn matches(&self, metadata: &RelationMetadata) -> bool {
                metadata.name().name() == Some(concat!("source/", $relation))
                    && metadata.arity() == 2
            }

            fn required_bound_positions(&self, _metadata: &RelationMetadata) -> &[u16] {
                INDEX_VALUE_BOUND
            }

            fn scan(
                &self,
                _reader: &dyn ComputedRelationRead,
                metadata: &RelationMetadata,
                bindings: &[Option<Value>],
            ) -> Result<Vec<Tuple>, KernelError> {
                let index = self.provider.semantic_index(metadata.id())?;
                Ok(filter_bound_rows(
                    vec![Tuple::from([
                        Value::string(&index.id),
                        Value::string(&index.$field),
                    ])],
                    bindings,
                ))
            }
        }
    };
}

struct SourceIndexRelation {
    provider: Arc<LocalSourceProvider>,
}

impl ComputedRelation for SourceIndexRelation {
    fn name(&self) -> &'static str {
        "persistent-source-index"
    }

    fn matches(&self, metadata: &RelationMetadata) -> bool {
        metadata.name().name() == Some("source/SourceIndex") && metadata.arity() == 1
    }

    fn required_bound_positions(&self, _metadata: &RelationMetadata) -> &[u16] {
        &[]
    }

    fn scan(
        &self,
        _reader: &dyn ComputedRelationRead,
        metadata: &RelationMetadata,
        bindings: &[Option<Value>],
    ) -> Result<Vec<Tuple>, KernelError> {
        let index = self.provider.semantic_index(metadata.id())?;
        Ok(filter_bound_rows(
            vec![Tuple::from([Value::string(&index.id)])],
            bindings,
        ))
    }
}

struct IndexRepositoryRelation {
    provider: Arc<LocalSourceProvider>,
}

impl ComputedRelation for IndexRepositoryRelation {
    fn name(&self) -> &'static str {
        "persistent-source-index-repository"
    }

    fn matches(&self, metadata: &RelationMetadata) -> bool {
        metadata.name().name() == Some("source/IndexRepository") && metadata.arity() == 2
    }

    fn required_bound_positions(&self, _metadata: &RelationMetadata) -> &[u16] {
        INDEX_VALUE_BOUND
    }

    fn scan(
        &self,
        reader: &dyn ComputedRelationRead,
        metadata: &RelationMetadata,
        bindings: &[Option<Value>],
    ) -> Result<Vec<Tuple>, KernelError> {
        let index = self.provider.semantic_index(metadata.id())?;
        let repository_relation = relation_id(reader, "source/Repository", 1).ok_or_else(|| {
            invalid_relation(metadata.id(), "missing relation source/Repository/1")
        })?;
        let rows = reader
            .scan_relation(repository_relation, &[None])?
            .into_iter()
            .map(|row| Tuple::from([Value::string(&index.id), row.values()[0].clone()]))
            .collect::<Vec<_>>();
        Ok(filter_bound_rows(rows, bindings))
    }
}

struct IndexRevisionRelation {
    provider: Arc<LocalSourceProvider>,
}

impl ComputedRelation for IndexRevisionRelation {
    fn name(&self) -> &'static str {
        "persistent-source-index-revision"
    }

    fn matches(&self, metadata: &RelationMetadata) -> bool {
        metadata.name().name() == Some("source/IndexRevision") && metadata.arity() == 2
    }

    fn required_bound_positions(&self, _metadata: &RelationMetadata) -> &[u16] {
        INDEX_VALUE_BOUND
    }

    fn scan(
        &self,
        reader: &dyn ComputedRelationRead,
        metadata: &RelationMetadata,
        bindings: &[Option<Value>],
    ) -> Result<Vec<Tuple>, KernelError> {
        let index = self.provider.semantic_index(metadata.id())?;
        let revision_relation = relation_id(reader, "source/Revision", 1)
            .ok_or_else(|| invalid_relation(metadata.id(), "missing relation source/Revision/1"))?;
        let rows = reader
            .scan_relation(revision_relation, &[None])?
            .into_iter()
            .map(|row| Tuple::from([Value::string(&index.id), row.values()[0].clone()]))
            .collect::<Vec<_>>();
        Ok(filter_bound_rows(rows, bindings))
    }
}

index_value_relation!(IndexProviderRelation, "IndexProvider", provider);
index_value_relation!(IndexStatusRelation, "IndexStatus", status);
index_value_relation!(IndexVersionRelation, "IndexVersion", version);
index_value_relation!(IndexBuildErrorRelation, "IndexBuildError", error);
