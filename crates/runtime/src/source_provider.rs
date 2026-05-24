use mica_compiler::{SyntaxKind, lex};
use mica_relation_kernel::{
    ComputedRelation, ComputedRelationRead, KernelError, RelationId, RelationMetadata,
    RelationRead, Tuple,
};
use mica_var::{Symbol, Value};
use serde_json::{Value as JsonValue, json};
use std::env;
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use tree_sitter::{Language, Node, Parser};

mod rust_analyzer;
use rust_analyzer::{LspLocation, RustAnalyzerProvider};

const REPOSITORY_ENTRY_BOUND: &[u16] = &[0, 1, 2];
const FILE_TEXT_BOUND: &[u16] = &[0, 1, 2];
const FILE_LINES_BOUND: &[u16] = &[0, 1, 2, 3, 4];
const FILE_CONTENT_HASH_BOUND: &[u16] = &[0, 1, 2];
const SYNTAX_LINE_BOUND: &[u16] = &[0, 1, 2, 3, 4];
const SYNTAX_OUTLINE_BOUND: &[u16] = &[0, 1, 2];
const SYNTAX_NODE_AT_BOUND: &[u16] = &[0, 1, 2, 3];
const DEFINITION_AT_BOUND: &[u16] = &[0, 1, 2, 3];
const REFERENCES_OF_BOUND: &[u16] = &[0, 1, 2];
const SYMBOL_SEARCH_BOUND: &[u16] = &[0, 1, 2, 3];
const INDEX_VALUE_BOUND: &[u16] = &[];
const SOURCE_INDEX_ID: &str = "source-index:mica-worktree";
const SOURCE_INDEX_SCHEMA: &str = "mica-source-index-v1";
const SOURCE_INDEX_PROVIDER: &str = "mica-source-index/static-analysis";
const SOURCE_INDEX_VERSION: &str = "2";

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
        let key = semantic_index_key(relation, &self.semantic_index_path)?;
        let mut cache = self.semantic_index_cache.lock().unwrap();
        if let Some(cached) = cache.as_ref()
            && cached.key == key
        {
            return Ok(cached.index.clone());
        }
        let index = Arc::new(PersistentSemanticIndex::load(
            relation,
            &self.semantic_index_path,
        )?);
        *cache = Some(CachedSemanticIndex {
            key,
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

#[derive(Clone, Debug)]
struct SyntaxDocument {
    text: String,
    line_starts: Vec<usize>,
    highlights: Vec<HighlightSpan>,
    outline: Vec<OutlineItem>,
}

impl SyntaxDocument {
    fn parse(path: &str, text: &str) -> Self {
        let language = SourceLanguage::from_path(path);
        let line_starts = line_starts(text);
        let mut highlights = Vec::new();
        let mut outline = Vec::new();

        match language {
            SourceLanguage::Rust | SourceLanguage::JavaScript | SourceLanguage::Markdown => {
                if let Some((tree_language, tree)) = parse_tree(language, text) {
                    collect_tree_highlights(tree.root_node(), text, &mut highlights);
                    collect_tree_outline(tree.root_node(), text, language, &mut outline);
                    let _ = tree_language;
                }
            }
            SourceLanguage::Mica => {
                highlights.extend(mica_highlights(text));
                outline.extend(mica_outline(text, &line_starts));
            }
            SourceLanguage::Plain => {}
        }

        if !matches!(language, SourceLanguage::Mica) {
            highlights.extend(fallback_line_highlights(text, &line_starts));
        }
        highlights.sort_by_key(|span| (span.start, span.end));
        highlights = dedupe_highlights(highlights);
        outline.sort_by_key(|item| (item.start_byte, item.end_byte, item.name.clone()));

        Self {
            text: text.to_owned(),
            line_starts,
            highlights,
            outline,
        }
    }

    fn node_at(&self, byte_offset: usize) -> Option<OutlineItem> {
        self.outline
            .iter()
            .filter(|item| item.start_byte <= byte_offset && byte_offset <= item.end_byte)
            .min_by_key(|item| item.end_byte.saturating_sub(item.start_byte))
            .cloned()
            .or_else(|| {
                self.outline
                    .iter()
                    .filter(|item| item.start_byte <= byte_offset)
                    .max_by_key(|item| item.start_byte)
                    .cloned()
            })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SourceLanguage {
    Rust,
    Mica,
    Markdown,
    JavaScript,
    Plain,
}

impl SourceLanguage {
    fn from_path(path: &str) -> Self {
        match Path::new(path)
            .extension()
            .and_then(|extension| extension.to_str())
        {
            Some("rs") => Self::Rust,
            Some("mica") => Self::Mica,
            Some("md") | Some("markdown") => Self::Markdown,
            Some("js") | Some("mjs") | Some("cjs") => Self::JavaScript,
            _ => Self::Plain,
        }
    }
}

#[derive(Clone, Debug)]
struct HighlightSpan {
    start: usize,
    end: usize,
    kind: &'static str,
}

#[derive(Clone, Debug)]
struct OutlineItem {
    node: String,
    kind: String,
    name: String,
    start_line: usize,
    end_line: usize,
    start_byte: usize,
    end_byte: usize,
}

struct SyntaxLine {
    number: usize,
    segments: Vec<Value>,
}

#[derive(Clone, Debug)]
struct SemanticLocation {
    path: String,
    name: String,
    kind: String,
    start_line: usize,
    end_line: usize,
    start_byte: usize,
    end_byte: usize,
    provider: String,
}

#[derive(Clone, Debug)]
struct SemanticSymbol {
    id: String,
    provider: SemanticSymbolProvider,
    path: String,
    start_byte: usize,
    name: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SemanticSymbolProvider {
    Index,
    RustAnalyzer,
}

impl SemanticSymbol {
    fn parse(symbol: &str) -> Option<Self> {
        let mut parts = symbol.splitn(5, ':');
        let provider = match parts.next()? {
            "idx" => SemanticSymbolProvider::Index,
            "ra" => SemanticSymbolProvider::RustAnalyzer,
            _ => return None,
        };
        let path = parts.next()?.to_owned();
        let start_byte = parts.next()?.parse().ok()?;
        let _end_byte = parts.next()?.parse::<usize>().ok()?;
        let name = parts.next()?.to_owned();
        Some(Self {
            id: symbol.to_owned(),
            provider,
            path,
            start_byte,
            name,
        })
    }
}

#[derive(Clone, Debug)]
struct PersistentSemanticIndex {
    id: String,
    provider: String,
    version: String,
    status: String,
    error: String,
    symbols: Vec<IndexedSymbol>,
    references: Vec<IndexedReference>,
}

#[derive(Clone, Debug)]
struct IndexedSymbol {
    symbol: String,
    name: String,
    kind: String,
    path: String,
    start_line: usize,
    end_line: usize,
    start_byte: usize,
    end_byte: usize,
}

#[derive(Clone, Debug)]
struct IndexedReference {
    symbol: String,
    name: String,
    path: String,
    start_line: usize,
    end_line: usize,
    start_byte: usize,
    end_byte: usize,
}

impl PersistentSemanticIndex {
    fn missing(path: &Path) -> Self {
        Self {
            id: SOURCE_INDEX_ID.to_owned(),
            provider: SOURCE_INDEX_PROVIDER.to_owned(),
            version: SOURCE_INDEX_VERSION.to_owned(),
            status: "missing".to_owned(),
            error: format!("semantic index not found at {}", path.display()),
            symbols: Vec::new(),
            references: Vec::new(),
        }
    }

    fn load(relation: RelationId, path: &Path) -> Result<Self, KernelError> {
        let bytes = match fs::read(path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Self::missing(path));
            }
            Err(error) => {
                return Err(invalid_relation(
                    relation,
                    format!("failed to read semantic index {}: {error}", path.display()),
                ));
            }
        };
        let json = serde_json::from_slice::<JsonValue>(&bytes).map_err(|error| {
            invalid_relation(
                relation,
                format!("failed to parse semantic index {}: {error}", path.display()),
            )
        })?;
        if json.get("schema").and_then(JsonValue::as_str) != Some(SOURCE_INDEX_SCHEMA) {
            return Err(invalid_relation(
                relation,
                format!("semantic index {} has unsupported schema", path.display()),
            ));
        }
        let id = json
            .get("id")
            .and_then(JsonValue::as_str)
            .unwrap_or(SOURCE_INDEX_ID)
            .to_owned();
        let provider = json
            .get("provider")
            .and_then(JsonValue::as_str)
            .unwrap_or(SOURCE_INDEX_PROVIDER)
            .to_owned();
        let version = json
            .get("version")
            .and_then(JsonValue::as_str)
            .unwrap_or(SOURCE_INDEX_VERSION)
            .to_owned();
        let status = json
            .get("status")
            .and_then(JsonValue::as_str)
            .unwrap_or("failed")
            .to_owned();
        let error = json
            .get("error")
            .and_then(JsonValue::as_str)
            .unwrap_or("")
            .to_owned();
        let symbols = json
            .get("symbols")
            .and_then(JsonValue::as_array)
            .map(|items| {
                items
                    .iter()
                    .map(indexed_symbol_from_json)
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()
            .map_err(|error| invalid_relation(relation, error))?
            .unwrap_or_default();
        let references = json
            .get("references")
            .and_then(JsonValue::as_array)
            .map(|items| {
                items
                    .iter()
                    .map(indexed_reference_from_json)
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()
            .map_err(|error| invalid_relation(relation, error))?
            .unwrap_or_default();
        Ok(Self {
            id,
            provider,
            version,
            status,
            error,
            symbols,
            references,
        })
    }

    fn is_complete(&self) -> bool {
        self.status == "complete"
    }

    fn definition_at(&self, path: &str, byte_offset: usize) -> Vec<IndexedSymbol> {
        if !self.is_complete() {
            return Vec::new();
        }
        let Some(reference) = self
            .references
            .iter()
            .find(|reference| {
                reference.path == path
                    && reference.start_byte <= byte_offset
                    && byte_offset <= reference.end_byte
            })
            .or_else(|| {
                self.references.iter().find(|reference| {
                    reference.path == path
                        && reference.start_byte <= byte_offset.saturating_add(1)
                        && byte_offset <= reference.end_byte
                })
            })
        else {
            return Vec::new();
        };
        let mut symbols = self
            .symbols
            .iter()
            .filter(|symbol| symbol.name == reference.name)
            .cloned()
            .collect::<Vec<_>>();
        symbols.sort_by_key(|symbol| {
            (
                symbol.path != reference.path,
                symbol.start_byte.abs_diff(reference.start_byte),
                symbol.path.clone(),
                symbol.start_byte,
            )
        });
        symbols
    }

    fn references_of(&self, symbol: &SemanticSymbol) -> Vec<IndexedReference> {
        if !self.is_complete() {
            return Vec::new();
        }
        self.references
            .iter()
            .filter(|reference| reference.symbol == symbol.id || reference.name == symbol.name)
            .cloned()
            .collect()
    }

    fn search(&self, query: &str, limit: usize) -> Vec<IndexedSymbol> {
        if !self.is_complete() {
            return Vec::new();
        }
        let needle = query.to_ascii_lowercase();
        let mut symbols = self
            .symbols
            .iter()
            .filter(|symbol| symbol.name.to_ascii_lowercase().contains(&needle))
            .cloned()
            .collect::<Vec<_>>();
        symbols.sort_by_key(|symbol| {
            (
                !symbol.name.eq_ignore_ascii_case(query),
                !symbol.name.to_ascii_lowercase().starts_with(&needle),
                symbol.name.clone(),
                symbol.path.clone(),
                symbol.start_byte,
            )
        });
        symbols.truncate(limit);
        symbols
    }
}

fn indexed_symbol_from_json(value: &JsonValue) -> Result<IndexedSymbol, String> {
    Ok(IndexedSymbol {
        symbol: json_string(value, "symbol")?,
        name: json_string(value, "name")?,
        kind: json_string(value, "kind")?,
        path: json_string(value, "path")?,
        start_line: json_usize(value, "start_line")?,
        end_line: json_usize(value, "end_line")?,
        start_byte: json_usize(value, "start_byte")?,
        end_byte: json_usize(value, "end_byte")?,
    })
}

fn indexed_reference_from_json(value: &JsonValue) -> Result<IndexedReference, String> {
    Ok(IndexedReference {
        symbol: json_string(value, "symbol")?,
        name: json_string(value, "name")?,
        path: json_string(value, "path")?,
        start_line: json_usize(value, "start_line")?,
        end_line: json_usize(value, "end_line")?,
        start_byte: json_usize(value, "start_byte")?,
        end_byte: json_usize(value, "end_byte")?,
    })
}

fn json_string(value: &JsonValue, field: &str) -> Result<String, String> {
    value
        .get(field)
        .and_then(JsonValue::as_str)
        .map(str::to_owned)
        .ok_or_else(|| format!("semantic index field {field} must be a string"))
}

fn json_usize(value: &JsonValue, field: &str) -> Result<usize, String> {
    value
        .get(field)
        .and_then(JsonValue::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .ok_or_else(|| format!("semantic index field {field} must be a non-negative integer"))
}

pub fn build_source_index_file(root: &Path, output: &Path) -> Result<(), String> {
    let root = root
        .canonicalize()
        .map_err(|error| format!("invalid source index root {}: {error}", root.display()))?;
    let index = build_source_index_json(&root)?;
    if let Some(parent) = output.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)
            .map_err(|error| format!("failed to create {}: {error}", parent.display()))?;
    }
    let bytes = serde_json::to_vec_pretty(&index)
        .map_err(|error| format!("failed to encode source index: {error}"))?;
    fs::write(output, bytes)
        .map_err(|error| format!("failed to write source index {}: {error}", output.display()))
}

pub fn write_failed_source_index_file(
    root: &Path,
    output: &Path,
    error: &str,
) -> Result<(), String> {
    if let Some(parent) = output.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)
            .map_err(|error| format!("failed to create {}: {error}", parent.display()))?;
    }
    let root = root
        .canonicalize()
        .unwrap_or_else(|_| root.to_path_buf())
        .display()
        .to_string();
    let index = json!({
        "schema": SOURCE_INDEX_SCHEMA,
        "id": SOURCE_INDEX_ID,
        "provider": SOURCE_INDEX_PROVIDER,
        "version": SOURCE_INDEX_VERSION,
        "status": "failed",
        "root": root,
        "error": error,
        "symbols": [],
        "references": [],
    });
    let bytes = serde_json::to_vec_pretty(&index)
        .map_err(|error| format!("failed to encode failed source index: {error}"))?;
    fs::write(output, bytes).map_err(|error| {
        format!(
            "failed to write failed source index {}: {error}",
            output.display()
        )
    })
}

fn build_source_index_json(root: &Path) -> Result<JsonValue, String> {
    let mut files = indexed_source_files(root)?;
    files.sort();
    let mut symbols = Vec::new();
    let mut references = Vec::new();
    for file in files {
        let relative = file
            .strip_prefix(root)
            .map_err(|_| format!("indexed file escaped root: {}", file.display()))?;
        let path = relative_path_string(relative)?;
        let text = fs::read_to_string(&file)
            .map_err(|error| format!("failed to read {}: {error}", file.display()))?;
        let syntax = SyntaxDocument::parse(&path, &text);
        for item in &syntax.outline {
            if !is_index_identifier(&item.name) {
                continue;
            }
            let symbol = indexed_symbol_id(&path, item.start_byte, item.end_byte, &item.name);
            symbols.push(json!({
                "symbol": symbol,
                "name": item.name,
                "kind": item.kind,
                "path": path,
                "start_line": item.start_line,
                "end_line": item.end_line,
                "start_byte": item.start_byte,
                "end_byte": item.end_byte,
            }));
        }
        for span in &syntax.highlights {
            if !matches!(span.kind, "function" | "type" | "property" | "identifier") {
                continue;
            }
            let Some(name) = text.get(span.start..span.end) else {
                continue;
            };
            if !is_index_identifier(name) {
                continue;
            }
            let start_line = byte_line(&syntax.line_starts, span.start);
            let end_line = byte_line(&syntax.line_starts, span.end);
            references.push(json!({
                "symbol": "",
                "name": name,
                "path": path,
                "start_line": start_line,
                "end_line": end_line,
                "start_byte": span.start,
                "end_byte": span.end,
            }));
        }
    }
    symbols.sort_by_key(|value| {
        (
            value
                .get("name")
                .and_then(JsonValue::as_str)
                .unwrap_or("")
                .to_owned(),
            value
                .get("path")
                .and_then(JsonValue::as_str)
                .unwrap_or("")
                .to_owned(),
            value
                .get("start_byte")
                .and_then(JsonValue::as_u64)
                .unwrap_or(0),
        )
    });
    references.sort_by_key(|value| {
        (
            value
                .get("name")
                .and_then(JsonValue::as_str)
                .unwrap_or("")
                .to_owned(),
            value
                .get("path")
                .and_then(JsonValue::as_str)
                .unwrap_or("")
                .to_owned(),
            value
                .get("start_byte")
                .and_then(JsonValue::as_u64)
                .unwrap_or(0),
        )
    });
    symbols.dedup();
    references.dedup();
    let symbol_by_name = symbols
        .iter()
        .filter_map(|symbol| {
            Some((
                symbol.get("name")?.as_str()?.to_owned(),
                symbol.get("symbol")?.as_str()?.to_owned(),
            ))
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    for reference in &mut references {
        if let Some(name) = reference.get("name").and_then(JsonValue::as_str)
            && let Some(symbol) = symbol_by_name.get(name)
            && let Some(object) = reference.as_object_mut()
        {
            object.insert("symbol".to_owned(), JsonValue::String(symbol.clone()));
        }
    }
    Ok(json!({
        "schema": SOURCE_INDEX_SCHEMA,
        "id": SOURCE_INDEX_ID,
        "provider": SOURCE_INDEX_PROVIDER,
        "version": SOURCE_INDEX_VERSION,
        "status": "complete",
        "root": root.display().to_string(),
        "error": "",
        "symbols": symbols,
        "references": references,
    }))
}

fn indexed_source_files(root: &Path) -> Result<Vec<PathBuf>, String> {
    let mut files = Vec::new();
    collect_indexed_source_files(root, &mut files)?;
    Ok(files)
}

fn collect_indexed_source_files(directory: &Path, files: &mut Vec<PathBuf>) -> Result<(), String> {
    let entries = fs::read_dir(directory)
        .map_err(|error| format!("failed to list {}: {error}", directory.display()))?;
    for entry in entries {
        let entry = entry
            .map_err(|error| format!("failed to read entry in {}: {error}", directory.display()))?;
        let path = entry.path();
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if path.is_dir() {
            if matches!(
                name.as_ref(),
                ".git" | ".cache" | "target" | "node_modules" | ".playwright-mcp"
            ) {
                continue;
            }
            collect_indexed_source_files(&path, files)?;
        } else if is_indexed_source_path(&path) {
            files.push(path);
        }
    }
    Ok(())
}

fn is_indexed_source_path(path: &Path) -> bool {
    matches!(
        path.extension().and_then(|extension| extension.to_str()),
        Some("rs" | "mica")
    )
}

fn relative_path_string(path: &Path) -> Result<String, String> {
    let mut parts = Vec::new();
    for component in path.components() {
        let Component::Normal(part) = component else {
            return Err(format!(
                "path contains unsupported component: {}",
                path.display()
            ));
        };
        parts.push(part.to_string_lossy().into_owned());
    }
    Ok(parts.join("/"))
}

fn indexed_symbol_id(path: &str, start_byte: usize, end_byte: usize, name: &str) -> String {
    format!("idx:{path}:{start_byte}:{end_byte}:{name}")
}

fn byte_line(line_starts: &[usize], byte_offset: usize) -> usize {
    line_starts.partition_point(|start| *start <= byte_offset)
}

fn is_index_identifier(name: &str) -> bool {
    name.split('/').all(is_index_identifier_segment)
}

fn is_index_identifier_segment(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first == '_' || first.is_ascii_alphabetic())
        && chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

fn parse_tree(language: SourceLanguage, text: &str) -> Option<(Language, tree_sitter::Tree)> {
    let tree_language = match language {
        SourceLanguage::Rust => Language::new(tree_sitter_rust::LANGUAGE),
        SourceLanguage::JavaScript => Language::new(tree_sitter_javascript::LANGUAGE),
        SourceLanguage::Markdown => Language::new(tree_sitter_md::LANGUAGE),
        SourceLanguage::Mica | SourceLanguage::Plain => return None,
    };
    let mut parser = Parser::new();
    parser.set_language(&tree_language).ok()?;
    let tree = parser.parse(text, None)?;
    Some((tree_language, tree))
}

fn collect_tree_highlights(node: Node<'_>, text: &str, spans: &mut Vec<HighlightSpan>) {
    collect_tree_highlights_with_parent(node, None, text, spans);
}

fn collect_tree_highlights_with_parent(
    node: Node<'_>,
    parent_kind: Option<&str>,
    text: &str,
    spans: &mut Vec<HighlightSpan>,
) {
    let kind = node.kind();
    if node.child_count() == 0 {
        if let Some(highlight) = tree_highlight_kind(kind, parent_kind) {
            spans.push(HighlightSpan {
                start: node.start_byte(),
                end: node.end_byte(),
                kind: highlight,
            });
        }
        return;
    }

    if let Some(highlight) = tree_highlight_kind(kind, parent_kind) {
        spans.push(HighlightSpan {
            start: node.start_byte(),
            end: node.end_byte(),
            kind: highlight,
        });
    }

    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if child.start_byte() < text.len() || child.end_byte() <= text.len() {
            collect_tree_highlights_with_parent(child, Some(kind), text, spans);
        }
    }
}

fn tree_highlight_kind(kind: &str, parent_kind: Option<&str>) -> Option<&'static str> {
    if kind.contains("comment") {
        return Some("comment");
    }
    if kind.contains("string") || kind == "char_literal" || kind == "template_string" {
        return Some("string");
    }
    if kind.contains("integer") || kind.contains("float") || kind == "number" {
        return Some("number");
    }
    if matches!(
        kind,
        "type_identifier" | "primitive_type" | "scoped_type_identifier"
    ) {
        return Some("type");
    }
    if matches!(
        kind,
        "fn" | "let"
            | "mut"
            | "pub"
            | "struct"
            | "enum"
            | "trait"
            | "impl"
            | "mod"
            | "use"
            | "where"
            | "for"
            | "while"
            | "loop"
            | "if"
            | "else"
            | "match"
            | "return"
            | "async"
            | "await"
            | "const"
            | "static"
            | "type"
            | "class"
            | "function"
            | "import"
            | "export"
            | "from"
            | "new"
            | "yield"
            | "try"
            | "catch"
            | "finally"
            | "throw"
            | "=>"
    ) {
        return Some("keyword");
    }
    if kind == "identifier"
        && parent_kind.is_some_and(|parent| {
            matches!(
                parent,
                "function_item"
                    | "function_declaration"
                    | "method_definition"
                    | "call_expression"
                    | "macro_invocation"
            )
        })
    {
        return Some("function");
    }
    if kind == "identifier" {
        return Some("identifier");
    }
    if kind == "field_identifier" || kind == "property_identifier" {
        return Some("property");
    }
    if kind.starts_with("atx_h") || kind == "atx_heading_marker" {
        return Some("heading");
    }
    if kind == "code_fence_content" || kind == "code_span" {
        return Some("string");
    }
    None
}

fn collect_tree_outline(
    node: Node<'_>,
    text: &str,
    language: SourceLanguage,
    outline: &mut Vec<OutlineItem>,
) {
    if let Some(item) = tree_outline_item(node, text, language) {
        outline.push(item);
    }

    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        collect_tree_outline(child, text, language, outline);
    }
}

fn tree_outline_item(node: Node<'_>, text: &str, language: SourceLanguage) -> Option<OutlineItem> {
    let kind = node.kind();
    let (outline_kind, name) = match language {
        SourceLanguage::Rust => rust_outline_name(node, text, kind)?,
        SourceLanguage::JavaScript => javascript_outline_name(node, text, kind)?,
        SourceLanguage::Markdown => markdown_outline_name(node, text, kind)?,
        SourceLanguage::Mica | SourceLanguage::Plain => return None,
    };
    Some(outline_item(
        outline_kind,
        name,
        node.start_byte(),
        node.end_byte(),
        node.start_position().row + 1,
        node.end_position().row + 1,
    ))
}

fn rust_outline_name(node: Node<'_>, text: &str, kind: &str) -> Option<(&'static str, String)> {
    let outline_kind = match kind {
        "function_item" => "function",
        "struct_item" => "struct",
        "enum_item" => "enum",
        "trait_item" => "trait",
        "impl_item" => "impl",
        "mod_item" => "module",
        "const_item" => "const",
        "static_item" => "static",
        "type_item" => "type",
        "macro_definition" => "macro",
        _ => return None,
    };
    let name = node
        .child_by_field_name("name")
        .and_then(|child| node_text(child, text))
        .or_else(|| {
            if kind == "impl_item" {
                first_named_child_text(node, text)
            } else {
                None
            }
        })
        .unwrap_or_else(|| outline_kind.to_owned());
    Some((outline_kind, name))
}

fn javascript_outline_name(
    node: Node<'_>,
    text: &str,
    kind: &str,
) -> Option<(&'static str, String)> {
    let outline_kind = match kind {
        "function_declaration" => "function",
        "class_declaration" => "class",
        "method_definition" => "method",
        "lexical_declaration" => "binding",
        _ => return None,
    };
    let name = node
        .child_by_field_name("name")
        .and_then(|child| node_text(child, text))
        .or_else(|| first_named_child_text(node, text))
        .unwrap_or_else(|| outline_kind.to_owned());
    Some((outline_kind, name))
}

fn markdown_outline_name(node: Node<'_>, text: &str, kind: &str) -> Option<(&'static str, String)> {
    if !matches!(kind, "atx_heading" | "setext_heading") {
        return None;
    }
    let raw = node_text(node, text)?;
    let name = raw
        .lines()
        .next()
        .unwrap_or("")
        .trim()
        .trim_start_matches('#')
        .trim()
        .to_owned();
    if name.is_empty() {
        return None;
    }
    Some(("heading", name))
}

fn first_named_child_text(node: Node<'_>, text: &str) -> Option<String> {
    let mut cursor = node.walk();
    node.named_children(&mut cursor)
        .find_map(|child| node_text(child, text))
}

fn node_text(node: Node<'_>, text: &str) -> Option<String> {
    text.get(node.start_byte()..node.end_byte())
        .map(str::to_owned)
}

fn mica_highlights(text: &str) -> Vec<HighlightSpan> {
    let tokens = lex(text);
    let mut highlights = Vec::new();
    let mut index = 0;
    while let Some(token) = tokens.get(index) {
        if token.kind == SyntaxKind::Ident {
            let mut end = token.span.end;
            let mut next_index = index + 1;
            while let (Some(slash), Some(next)) =
                (tokens.get(next_index), tokens.get(next_index + 1))
            {
                if slash.kind != SyntaxKind::Slash
                    || next.kind != SyntaxKind::Ident
                    || slash.span.start != end
                    || slash.span.end != next.span.start
                {
                    break;
                }
                end = next.span.end;
                next_index += 2;
            }
            highlights.push(HighlightSpan {
                start: token.span.start,
                end,
                kind: "identifier",
            });
            index = next_index;
            continue;
        }

        let kind = match token.kind {
            SyntaxKind::LineComment => "comment",
            SyntaxKind::String => "string",
            SyntaxKind::Int | SyntaxKind::Float => "number",
            SyntaxKind::LetKw
            | SyntaxKind::ConstKw
            | SyntaxKind::IfKw
            | SyntaxKind::ElseIfKw
            | SyntaxKind::ElseKw
            | SyntaxKind::EndKw
            | SyntaxKind::BeginKw
            | SyntaxKind::ForKw
            | SyntaxKind::InKw
            | SyntaxKind::WhileKw
            | SyntaxKind::ReturnKw
            | SyntaxKind::RaiseKw
            | SyntaxKind::RecoverKw
            | SyntaxKind::OneKw
            | SyntaxKind::SpawnKw
            | SyntaxKind::AfterKw
            | SyntaxKind::NotKw
            | SyntaxKind::BreakKw
            | SyntaxKind::ContinueKw
            | SyntaxKind::TryKw
            | SyntaxKind::CatchKw
            | SyntaxKind::AsKw
            | SyntaxKind::FinallyKw
            | SyntaxKind::FnKw
            | SyntaxKind::MethodKw
            | SyntaxKind::VerbKw
            | SyntaxKind::DoKw
            | SyntaxKind::AssertKw
            | SyntaxKind::RetractKw
            | SyntaxKind::RequireKw
            | SyntaxKind::TrueKw
            | SyntaxKind::FalseKw
            | SyntaxKind::NothingKw => "keyword",
            SyntaxKind::ErrorCode => "error",
            _ => {
                index += 1;
                continue;
            }
        };
        highlights.push(HighlightSpan {
            start: token.span.start,
            end: token.span.end,
            kind,
        });
        index += 1;
    }
    highlights
}

fn mica_outline(text: &str, line_starts: &[usize]) -> Vec<OutlineItem> {
    text.lines()
        .enumerate()
        .filter_map(|(index, line)| {
            let trimmed = line.trim_start();
            let start_byte = *line_starts.get(index)?;
            let indent = line.len().saturating_sub(trimmed.len());
            let item_start = start_byte + indent;
            let (kind, name) = if let Some(rest) = trimmed.strip_prefix("verb ") {
                ("verb", rest.split('(').next().unwrap_or("verb").trim())
            } else if let Some(rest) = trimmed.strip_prefix("method ") {
                ("method", rest.split_whitespace().next().unwrap_or("method"))
            } else if let Some(rest) = trimmed.strip_prefix("make_relation(:") {
                ("relation", rest.split(',').next().unwrap_or("relation"))
            } else if let Some(rest) = trimmed.strip_prefix("make_functional_relation(:") {
                ("relation", rest.split(',').next().unwrap_or("relation"))
            } else if trimmed.contains(":-") {
                ("rule", trimmed.split(":-").next().unwrap_or("rule").trim())
            } else {
                return None;
            };
            let name = name.trim().trim_end_matches(')').to_owned();
            Some(outline_item(
                kind,
                name,
                item_start,
                start_byte + line.len(),
                index + 1,
                index + 1,
            ))
        })
        .collect()
}

fn fallback_line_highlights(text: &str, line_starts: &[usize]) -> Vec<HighlightSpan> {
    let mut spans = Vec::new();
    for (index, line) in text.lines().enumerate() {
        let Some(line_start) = line_starts.get(index).copied() else {
            continue;
        };
        if let Some(comment_start) = line.find("//") {
            spans.push(HighlightSpan {
                start: line_start + comment_start,
                end: line_start + line.len(),
                kind: "comment",
            });
        }
        if line.trim_start().starts_with('#') {
            spans.push(HighlightSpan {
                start: line_start,
                end: line_start + line.len(),
                kind: "heading",
            });
        }
    }
    spans
}

fn dedupe_highlights(spans: Vec<HighlightSpan>) -> Vec<HighlightSpan> {
    let mut out: Vec<HighlightSpan> = Vec::new();
    for span in spans {
        if span.start >= span.end {
            continue;
        }
        if out
            .last()
            .is_some_and(|last| last.start == span.start && last.end == span.end)
        {
            continue;
        }
        out.push(span);
    }
    out
}

fn syntax_lines(
    relation: RelationId,
    syntax: &SyntaxDocument,
    start_line: usize,
    line_count: usize,
) -> Result<Vec<SyntaxLine>, KernelError> {
    let mut rows = Vec::new();
    for line_index in start_line.saturating_sub(1)..start_line.saturating_sub(1) + line_count {
        let Some(line_start) = syntax.line_starts.get(line_index).copied() else {
            break;
        };
        let line_end = line_end_without_newline(&syntax.text, &syntax.line_starts, line_index);
        let line_text = syntax
            .text
            .get(line_start..line_end)
            .ok_or_else(|| invalid_relation(relation, "line boundaries are not utf-8"))?;
        let segments = line_segments(relation, syntax, line_start, line_end, line_text)?;
        rows.push(SyntaxLine {
            number: line_index + 1,
            segments,
        });
    }
    Ok(rows)
}

fn line_segments(
    relation: RelationId,
    syntax: &SyntaxDocument,
    line_start: usize,
    line_end: usize,
    line_text: &str,
) -> Result<Vec<Value>, KernelError> {
    let mut cursor = line_start;
    let mut segments = Vec::new();
    for span in syntax
        .highlights
        .iter()
        .filter(|span| span.end > line_start && span.start < line_end)
    {
        let start = span.start.max(line_start);
        let end = span.end.min(line_end);
        if start < cursor || start >= end {
            continue;
        }
        if cursor < start {
            let text = syntax
                .text
                .get(cursor..start)
                .ok_or_else(|| invalid_relation(relation, "highlight boundary is not utf-8"))?;
            segments.push(syntax_segment(
                relation, line_start, cursor, start, "plain", text,
            )?);
        }
        let text = syntax
            .text
            .get(start..end)
            .ok_or_else(|| invalid_relation(relation, "highlight boundary is not utf-8"))?;
        segments.push(syntax_segment(
            relation, line_start, start, end, span.kind, text,
        )?);
        cursor = end;
    }
    if cursor < line_end {
        let text = syntax
            .text
            .get(cursor..line_end)
            .ok_or_else(|| invalid_relation(relation, "highlight boundary is not utf-8"))?;
        segments.push(syntax_segment(
            relation, line_start, cursor, line_end, "plain", text,
        )?);
    }
    if segments.is_empty() {
        segments.push(syntax_segment(
            relation, line_start, line_start, line_end, "plain", line_text,
        )?);
    }
    Ok(segments)
}

fn syntax_segment(
    relation: RelationId,
    line_start: usize,
    start: usize,
    end: usize,
    kind: &str,
    text: &str,
) -> Result<Value, KernelError> {
    Ok(Value::map([
        (
            Value::symbol(Symbol::intern("start_col")),
            int_value(relation, byte_column(line_start, start) as i64)?,
        ),
        (
            Value::symbol(Symbol::intern("end_col")),
            int_value(relation, byte_column(line_start, end) as i64)?,
        ),
        (
            Value::symbol(Symbol::intern("start_byte")),
            int_value(relation, start as i64)?,
        ),
        (
            Value::symbol(Symbol::intern("end_byte")),
            int_value(relation, end as i64)?,
        ),
        (Value::symbol(Symbol::intern("kind")), Value::string(kind)),
        (Value::symbol(Symbol::intern("text")), Value::string(text)),
    ]))
}

fn byte_column(line_start: usize, offset: usize) -> usize {
    offset.saturating_sub(line_start)
}

fn line_starts(text: &str) -> Vec<usize> {
    let mut starts = vec![0];
    for (index, byte) in text.bytes().enumerate() {
        if byte == b'\n' && index + 1 < text.len() {
            starts.push(index + 1);
        }
    }
    starts
}

fn line_end_without_newline(text: &str, line_starts: &[usize], line_index: usize) -> usize {
    let next_start = line_starts
        .get(line_index + 1)
        .copied()
        .unwrap_or(text.len());
    let mut end = next_start;
    if end > 0 && text.as_bytes().get(end - 1) == Some(&b'\n') {
        end -= 1;
    }
    if end > 0 && text.as_bytes().get(end - 1) == Some(&b'\r') {
        end -= 1;
    }
    end
}

fn outline_item(
    kind: impl Into<String>,
    name: String,
    start_byte: usize,
    end_byte: usize,
    start_line: usize,
    end_line: usize,
) -> OutlineItem {
    let kind = kind.into();
    let node = format!("{start_byte:012}:{kind}:{end_byte:012}:{name}");
    OutlineItem {
        node,
        kind,
        name,
        start_line,
        end_line,
        start_byte,
        end_byte,
    }
}

fn semantic_location(
    relation: RelationId,
    root: &Path,
    location: LspLocation,
) -> Result<Option<SemanticLocation>, KernelError> {
    let Some(file) = location.path else {
        return Ok(None);
    };
    let file = file.canonicalize().map_err(|error| {
        invalid_relation(
            relation,
            format!("failed to resolve rust-analyzer location: {error}"),
        )
    })?;
    if !file.starts_with(root) {
        return Ok(None);
    }
    let relative = file.strip_prefix(root).map_err(|_| {
        invalid_relation(relation, "rust-analyzer location escaped repository root")
    })?;
    let path = path_to_mica_string(relation, relative)?;
    let text = read_utf8_file(relation, &file)?;
    let start = lsp_position_to_byte_offset(
        relation,
        &text,
        location.start_line,
        location.start_character,
    );
    let end =
        lsp_position_to_byte_offset(relation, &text, location.end_line, location.end_character);
    let (start_byte, end_byte, start_line, end_line) = match (start, end) {
        (Ok(start_byte), Ok(end_byte)) => (
            start_byte,
            end_byte,
            location.start_line + 1,
            location.end_line + 1,
        ),
        _ => (0, 0, 1, 1),
    };
    let syntax = SyntaxDocument::parse(&path, &text);
    let node = syntax.node_at(start_byte);
    let name = node
        .as_ref()
        .map(|node| node.name.clone())
        .or_else(|| text.get(start_byte..end_byte).map(str::to_owned))
        .filter(|name| !name.is_empty())
        .or_else(|| file.file_stem()?.to_str().map(str::to_owned))
        .unwrap_or_else(|| "symbol".to_owned());
    let kind = node
        .as_ref()
        .map(|node| node.kind.clone())
        .unwrap_or_else(|| "rust".to_owned());
    Ok(Some(SemanticLocation {
        path,
        name,
        kind,
        start_line,
        end_line,
        start_byte,
        end_byte,
        provider: location.provider,
    }))
}

fn semantic_symbol(location: &SemanticLocation) -> String {
    format!(
        "ra:{}:{}:{}:{}",
        location.path, location.start_byte, location.end_byte, location.name
    )
}

fn byte_offset_to_lsp_position(
    relation: RelationId,
    text: &str,
    byte_offset: usize,
) -> Result<rust_analyzer::LspPosition, KernelError> {
    if byte_offset > text.len() || !text.is_char_boundary(byte_offset) {
        return Err(invalid_relation(
            relation,
            "byte offset is not on a utf-8 character boundary",
        ));
    }
    let starts = line_starts(text);
    let line_index = starts
        .partition_point(|start| *start <= byte_offset)
        .saturating_sub(1);
    let line_start = starts.get(line_index).copied().unwrap_or(0);
    let prefix = text
        .get(line_start..byte_offset)
        .ok_or_else(|| invalid_relation(relation, "line boundary is not utf-8"))?;
    let character = prefix.encode_utf16().count();
    Ok(rust_analyzer::LspPosition {
        line: line_index,
        character,
    })
}

fn lsp_position_to_byte_offset(
    relation: RelationId,
    text: &str,
    line: usize,
    character: usize,
) -> Result<usize, KernelError> {
    let starts = line_starts(text);
    let line_start = starts
        .get(line)
        .copied()
        .ok_or_else(|| invalid_relation(relation, "rust-analyzer line is outside file"))?;
    let line_end = line_end_without_newline(text, &starts, line);
    let line_text = text
        .get(line_start..line_end)
        .ok_or_else(|| invalid_relation(relation, "line boundary is not utf-8"))?;
    let mut utf16 = 0;
    for (byte, ch) in line_text.char_indices() {
        if utf16 == character {
            return Ok(line_start + byte);
        }
        utf16 += ch.len_utf16();
        if utf16 > character {
            return Err(invalid_relation(
                relation,
                "rust-analyzer character offset splits a unicode scalar",
            ));
        }
    }
    if utf16 == character {
        Ok(line_end)
    } else {
        Err(invalid_relation(
            relation,
            "rust-analyzer character offset is outside line",
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

fn read_utf8_file(relation: RelationId, file: &Path) -> Result<String, KernelError> {
    let bytes = read_file_bytes(relation, file)?;
    String::from_utf8(bytes)
        .map_err(|error| invalid_relation(relation, format!("source file is not utf-8: {error}")))
}

fn rust_workspace_root(repository_root: &Path) -> PathBuf {
    for ancestor in repository_root.ancestors() {
        let manifest = ancestor.join("Cargo.toml");
        if fs::read_to_string(&manifest)
            .is_ok_and(|source| source.lines().any(|line| line.trim() == "[workspace]"))
        {
            return ancestor.to_owned();
        }
    }
    repository_root.to_owned()
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
    use std::sync::{Mutex, OnceLock};

    fn load_source_relations(runner: &mut SourceRunner) {
        let root = env::current_dir().unwrap().display().to_string();
        load_source_relations_at(runner, &root);
    }

    fn load_source_relations_at(runner: &mut SourceRunner, root: &str) {
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
                 make_relation(:source/SyntaxLine, 8)\n\
                 make_relation(:source/SyntaxOutline, 10)\n\
                 make_relation(:source/SyntaxNodeAt, 11)\n\
                 make_relation(:source/DefinitionAt, 13)\n\
                 make_relation(:source/ReferencesOf, 10)\n\
                 make_relation(:source/SymbolSearch, 11)\n\
                 make_relation(:source/SourceIndex, 1)\n\
                 make_relation(:source/IndexRepository, 2)\n\
                 make_relation(:source/IndexRevision, 2)\n\
                 make_relation(:source/IndexProvider, 2)\n\
                 make_relation(:source/IndexStatus, 2)\n\
                 make_relation(:source/IndexVersion, 2)\n\
                 make_relation(:source/IndexBuildError, 2)\n\
                 make_relation(:source/Repository, 1)\n\
                 make_relation(:source/Revision, 1)\n\
                 assert source/Repository(#repo)\n\
                 assert source/Revision(#rev)\n\
                 assert source/RepositoryRoot(#repo, {root:?})\n\
                 assert source/RevisionOf(#rev, #repo)"
            ))
            .unwrap();
    }

    fn with_source_provider_env<T>(f: impl FnOnce() -> T) -> T {
        static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        let _guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        f()
    }

    fn with_source_index_env<T>(
        index_path: &Path,
        rust_analyzer: Option<&Path>,
        f: impl FnOnce() -> T,
    ) -> T {
        with_source_provider_env(|| {
            let old_index = env::var_os("MICA_SOURCE_INDEX");
            let old_rust_analyzer = env::var_os("MICA_RUST_ANALYZER");
            unsafe {
                env::set_var("MICA_SOURCE_INDEX", index_path);
                if let Some(rust_analyzer) = rust_analyzer {
                    env::set_var("MICA_RUST_ANALYZER", rust_analyzer);
                }
            }
            let result = f();
            unsafe {
                if let Some(old_index) = old_index {
                    env::set_var("MICA_SOURCE_INDEX", old_index);
                } else {
                    env::remove_var("MICA_SOURCE_INDEX");
                }
                if let Some(old_rust_analyzer) = old_rust_analyzer {
                    env::set_var("MICA_RUST_ANALYZER", old_rust_analyzer);
                } else {
                    env::remove_var("MICA_RUST_ANALYZER");
                }
            }
            result
        })
    }

    fn temp_index_path(name: &str) -> PathBuf {
        env::temp_dir().join(format!(
            "mica-source-index-{name}-{}-{}.json",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ))
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
    fn syntax_provider_requires_constrained_queries() {
        let mut runner = SourceRunner::new_empty();
        load_source_relations(&mut runner);

        let error = runner
            .run_source(
                "return source/SyntaxOutline(#repo, #rev, ?path, ?node, ?kind, ?name, ?start_line, ?end_line, ?start_byte, ?end_byte)",
            )
            .unwrap_err();
        assert!(format!("{error:?}").contains("MissingRequiredBindings"));

        let error = runner
            .run_source(
                "return source/SyntaxNodeAt(#repo, #rev, \"src/lib.rs\", ?offset, ?node, ?kind, ?name, ?start_line, ?end_line, ?start_byte, ?end_byte)",
            )
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

    #[test]
    fn source_provider_returns_rust_syntax_outline() {
        let mut runner = SourceRunner::new_empty();
        load_source_relations(&mut runner);

        let report = runner
            .run_source(
                "for item in source/SyntaxOutline(#repo, #rev, \"src/lib.rs\", ?node, ?kind, ?name, ?start_line, ?end_line, ?start_byte, ?end_byte)\n\
                   return item[:kind] != nothing\n\
                 end\n\
                 return false",
            )
            .unwrap();
        assert!(matches!(
            report.outcome,
            TaskOutcome::Complete { value, .. } if value == Value::bool(true)
        ));
    }

    #[test]
    fn source_provider_returns_line_level_syntax_segments() {
        let mut runner = SourceRunner::new_empty();
        load_source_relations(&mut runner);

        let report = runner
            .run_source(
                "let row = one source/SyntaxLine(#repo, #rev, \"src/lib.rs\", 1, 8, 1, ?segments, ?hash)\n\
                 return row[:segments]",
            )
            .unwrap();
        let TaskOutcome::Complete { value, .. } = report.outcome else {
            panic!("expected complete outcome, got {:?}", report.outcome);
        };
        value
            .with_list(|segments| {
                assert!(!segments.is_empty());
                assert!(segments.iter().any(|segment| {
                    segment
                        .with_map(|entries| {
                            entries
                                .iter()
                                .any(|(key, _)| key == &Value::symbol(Symbol::intern("kind")))
                        })
                        .unwrap_or(false)
                }));
            })
            .expect("expected syntax segment list");
    }

    #[test]
    fn source_provider_reports_nearest_syntax_node() {
        let mut runner = SourceRunner::new_empty();
        load_source_relations(&mut runner);

        let report = runner
            .run_source(
                "let item = one source/SyntaxNodeAt(#repo, #rev, \"src/lib.rs\", 2500, ?node, ?kind, ?name, ?start_line, ?end_line, ?start_byte, ?end_byte)\n\
                 return item[:node] != nothing",
            )
            .unwrap();
        assert!(matches!(
            report.outcome,
            TaskOutcome::Complete { value, .. } if value == Value::bool(true)
        ));
    }

    #[test]
    fn syntax_document_supports_phase_three_languages() {
        let rust = SyntaxDocument::parse("lib.rs", "pub fn run() -> i32 { 1 }\n");
        assert!(
            rust.outline
                .iter()
                .any(|item| item.kind == "function" && item.name == "run")
        );
        assert!(rust.highlights.iter().any(|span| span.kind == "keyword"));

        let javascript = SyntaxDocument::parse("bootstrap.js", "export function boot() {}\n");
        assert!(
            javascript
                .outline
                .iter()
                .any(|item| item.kind == "function" && item.name == "boot")
        );
        assert!(
            javascript
                .highlights
                .iter()
                .any(|span| span.kind == "keyword")
        );

        let markdown = SyntaxDocument::parse("README.md", "# Source Viewer\n\nbody\n");
        assert!(
            markdown
                .outline
                .iter()
                .any(|item| item.kind == "heading" && item.name == "Source Viewer")
        );
        assert!(
            markdown
                .highlights
                .iter()
                .any(|span| span.kind == "heading")
        );

        let mica = SyntaxDocument::parse(
            "ui.mica",
            "verb source/app_node(view)\n  return true\nend\n",
        );
        assert!(
            mica.outline
                .iter()
                .any(|item| item.kind == "verb" && item.name == "source/app_node")
        );
        assert!(mica.highlights.iter().any(|span| span.kind == "keyword"));
        assert!(mica.highlights.iter().any(|span| {
            span.kind == "identifier"
                && mica
                    .text
                    .get(span.start..span.end)
                    .is_some_and(|text| text == "source/app_node")
        }));
    }

    #[test]
    fn rust_analyzer_provider_returns_definition_and_references() {
        if std::process::Command::new("rust-analyzer")
            .arg("--version")
            .output()
            .is_err()
        {
            return;
        }

        with_source_provider_env(|| {
            let mut runner = SourceRunner::new_empty();
            load_source_relations(&mut runner);
            let source = fs::read_to_string("src/source_provider.rs").unwrap();
            let offset = source.find("read_utf8_file(metadata").unwrap() + 5;

            let report = runner
            .run_source(&format!(
                "for def in source/DefinitionAt(#repo, #rev, \"src/source_provider.rs\", {offset}, ?symbol, ?name, ?kind, ?target_path, ?start_line, ?end_line, ?start_byte, ?end_byte, ?provider)\n\
                   if def[:target_path] == \"src/source_provider.rs\"\n\
                     return def\n\
                   end\n\
                 end\n\
                 return nothing"
            ))
            .unwrap();
            let TaskOutcome::Complete { value, .. } = report.outcome else {
                panic!("expected complete outcome, got {:?}", report.outcome);
            };
            let symbol = value
                .with_map(|entries| {
                    entries
                        .iter()
                        .find(|(key, _)| key == &Value::symbol(Symbol::intern("symbol")))
                        .map(|(_, value)| value.clone())
                })
                .flatten()
                .and_then(|value| value.with_str(str::to_owned))
                .expect("expected definition symbol");

            let report = runner
            .run_source(&format!(
                "for reference in source/ReferencesOf(#repo, #rev, {symbol:?}, ?path, ?start_line, ?end_line, ?start_byte, ?end_byte, ?provider, ?name)\n\
                   if reference[:path] == \"src/source_provider.rs\"\n\
                     return reference[:provider]\n\
                   end\n\
                 end\n\
                 return nothing"
            ))
            .unwrap();
            assert!(matches!(
                report.outcome,
                TaskOutcome::Complete { value, .. } if value.with_str(|provider| provider.contains("rust-analyzer")).unwrap_or(false)
            ));
        });
    }

    #[test]
    fn persistent_source_index_answers_navigation_without_rust_analyzer() {
        let index_path = temp_index_path("navigation");
        build_source_index_file(Path::new("."), &index_path).unwrap();
        with_source_index_env(&index_path, Some(Path::new("/bin/false")), || {
            let mut runner = SourceRunner::new_empty();
            load_source_relations(&mut runner);
            let source = fs::read_to_string("src/source_provider.rs").unwrap();
            let offset = source.find("LocalSourceProvider::from_env").unwrap();

            let report = runner
                .run_source(&format!(
                    "for def in source/DefinitionAt(#repo, #rev, \"src/source_provider.rs\", {offset}, ?symbol, ?name, ?kind, ?target_path, ?start_line, ?end_line, ?start_byte, ?end_byte, ?provider)\n\
                       if def[:name] == \"LocalSourceProvider\"\n\
                         return [def[:symbol], def[:target_path], def[:start_line], def[:provider]]\n\
                       end\n\
                     end\n\
                     return nothing"
                ))
                .unwrap();
            let TaskOutcome::Complete { value, .. } = report.outcome else {
                panic!("expected complete outcome, got {:?}", report.outcome);
            };
            let symbol =
                value
                    .with_list(|values| {
                        assert!(values[0]
                        .with_str(|symbol| symbol.starts_with("idx:src/source_provider.rs:"))
                        .unwrap_or(false));
                        assert_eq!(values[1], Value::string("src/source_provider.rs"));
                        assert!(values[2].as_int().is_some_and(|line| line > 0));
                        assert!(
                            values[3]
                                .with_str(|provider| provider.contains("mica-source-index"))
                                .unwrap_or(false)
                        );
                        values[0].clone()
                    })
                    .expect("expected indexed definition tuple");

            let report = runner
                .run_source(&format!(
                    "let symbol = {symbol:?}\n\
                     let count = 0\n\
                     for reference in source/ReferencesOf(#repo, #rev, symbol, ?path, ?start_line, ?end_line, ?start_byte, ?end_byte, ?provider, ?name)\n\
                       if reference[:name] == \"LocalSourceProvider\" && reference[:provider] == \"mica-source-index/static-analysis 2\"\n\
                         count = count + 1\n\
                       end\n\
                     end\n\
                     return count"
                ))
                .unwrap();
            assert!(matches!(
                report.outcome,
                TaskOutcome::Complete { value, .. } if value.as_int().is_some_and(|count| count > 1)
            ));

            let report = runner
                .run_source(
                    "for result in source/SymbolSearch(#repo, #rev, \"LocalSource\", 5, ?symbol, ?name, ?kind, ?path, ?start_line, ?end_line, ?provider)\n\
                       if result[:name] == \"LocalSourceProvider\"\n\
                         return result[:provider]\n\
                       end\n\
                     end\n\
                     return nothing",
                )
                .unwrap();
            assert!(matches!(
                report.outcome,
                TaskOutcome::Complete { value, .. } if value.with_str(|provider| provider.contains("mica-source-index")).unwrap_or(false)
            ));
        });
        let _ = fs::remove_file(index_path);
    }

    #[test]
    fn persistent_source_index_keeps_mica_namespace_symbols_whole() {
        let root_path = env::current_dir().unwrap().join("target").join(format!(
            "source-index-mica-symbol-fixture-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        fs::create_dir_all(&root_path).unwrap();
        let source_path = "session.mica";
        let source = "make_relation(:session/CanAssumeActor, 2)\n\
                      assert session/CanAssumeActor(#web, #alice)\n";
        fs::write(root_path.join(source_path), source).unwrap();

        let index_path = temp_index_path("mica-symbol");
        build_source_index_file(&root_path, &index_path).unwrap();
        with_source_index_env(&index_path, Some(Path::new("/bin/false")), || {
            let mut runner = SourceRunner::new_empty();
            load_source_relations_at(&mut runner, &root_path.display().to_string());
            let offset =
                source.find("session/CanAssumeActor(#web").unwrap() + "session/CanAssume".len();

            let report = runner
                .run_source(&format!(
                    "for def in source/DefinitionAt(#repo, #rev, {source_path:?}, {offset}, ?symbol, ?name, ?kind, ?target_path, ?start_line, ?end_line, ?start_byte, ?end_byte, ?provider)\n\
                       return [def[:name], def[:kind], def[:target_path], def[:start_line], def[:provider]]\n\
                     end\n\
                     return nothing",
                    source_path = source_path,
                ))
                .unwrap();
            let TaskOutcome::Complete { value, .. } = report.outcome else {
                panic!("expected complete outcome, got {:?}", report.outcome);
            };
            value
                .with_list(|values| {
                    assert_eq!(values[0], Value::string("session/CanAssumeActor"));
                    assert_eq!(values[1], Value::string("relation"));
                    assert_eq!(values[2], Value::string(source_path));
                    assert_eq!(values[3].as_int(), Some(1));
                    assert_eq!(
                        values[4],
                        Value::string("mica-source-index/static-analysis 2")
                    );
                })
                .expect("expected Mica definition tuple");
        });
        let _ = fs::remove_file(index_path);
        let _ = fs::remove_dir_all(root_path);
    }

    #[test]
    fn persistent_source_index_status_reports_build_failures() {
        let index_path = temp_index_path("failed");
        write_failed_source_index_file(Path::new("."), &index_path, "synthetic failure").unwrap();
        with_source_index_env(&index_path, None, || {
            let mut runner = SourceRunner::new_empty();
            load_source_relations(&mut runner);
            let report = runner
                .run_source(
                    "for index in source/SourceIndex(?index)\n\
                       let status = one source/IndexStatus(index[:index], ?status)\n\
                       let error = one source/IndexBuildError(index[:index], ?error)\n\
                       return [status, error]\n\
                     end\n\
                     return nothing",
                )
                .unwrap();
            let TaskOutcome::Complete { value, .. } = report.outcome else {
                panic!("expected complete outcome, got {:?}", report.outcome);
            };
            value
                .with_list(|values| {
                    assert_eq!(values[0], Value::string("failed"));
                    assert_eq!(values[1], Value::string("synthetic failure"));
                })
                .expect("expected index status tuple");
        });
        let _ = fs::remove_file(index_path);
    }

    #[test]
    fn rust_analyzer_definition_accepts_identifier_start_offset() {
        if std::process::Command::new("rust-analyzer")
            .arg("--version")
            .output()
            .is_err()
        {
            return;
        }

        with_source_provider_env(|| {
            let mut runner = SourceRunner::new_empty();
            load_source_relations(&mut runner);
            let source = fs::read_to_string("src/source_provider.rs").unwrap();
            let offset = source.find("fn read_utf8_file").unwrap() + "fn ".len();

            let report = runner
            .run_source(&format!(
                "for def in source/DefinitionAt(#repo, #rev, \"src/source_provider.rs\", {offset}, ?symbol, ?name, ?kind, ?target_path, ?start_line, ?end_line, ?start_byte, ?end_byte, ?provider)\n\
                   if def[:target_path] == \"src/source_provider.rs\"\n\
                     return def[:provider]\n\
                   end\n\
                 end\n\
                 return nothing"
            ))
            .unwrap();
            assert!(matches!(
                report.outcome,
                TaskOutcome::Complete { value, .. } if value.with_str(|provider| provider.contains("rust-analyzer")).unwrap_or(false)
            ));
        });
    }

    #[test]
    fn rust_analyzer_module_definition_can_link_to_target_file() {
        if std::process::Command::new("rust-analyzer")
            .arg("--version")
            .output()
            .is_err()
        {
            return;
        }

        with_source_provider_env(|| {
            let mut runner = SourceRunner::new_empty();
            load_source_relations(&mut runner);
            let source = fs::read_to_string("src/lib.rs").unwrap();
            let offset = source.find("mod source_provider").unwrap() + "mod ".len();

            let report = runner
            .run_source(&format!(
                "for def in source/DefinitionAt(#repo, #rev, \"src/lib.rs\", {offset}, ?symbol, ?name, ?kind, ?target_path, ?start_line, ?end_line, ?start_byte, ?end_byte, ?provider)\n\
                   if def[:target_path] == \"src/source_provider.rs\"\n\
                     return def[:start_line]\n\
                   end\n\
                 end\n\
                 return nothing"
            ))
            .unwrap();
            assert!(matches!(
                report.outcome,
                TaskOutcome::Complete { value, .. } if value.as_int() == Some(1)
            ));
        });
    }

    #[test]
    fn rust_analyzer_module_definition_uses_workspace_relative_path() {
        if std::process::Command::new("rust-analyzer")
            .arg("--version")
            .output()
            .is_err()
            || env::var_os("MICA_SOURCE_ROOT").is_none()
        {
            return;
        }

        with_source_provider_env(|| {
            let workspace = env::current_dir()
                .unwrap()
                .parent()
                .and_then(Path::parent)
                .unwrap()
                .display()
                .to_string();
            let mut runner = SourceRunner::new_empty();
            load_source_relations_at(&mut runner, &workspace);
            let source = fs::read_to_string("../runtime/src/lib.rs").unwrap();
            let offset = source.find("mod source_provider").unwrap() + "mod ".len();

            let report = runner
            .run_source(&format!(
                "for def in source/DefinitionAt(#repo, #rev, \"crates/runtime/src/lib.rs\", {offset}, ?symbol, ?name, ?kind, ?target_path, ?start_line, ?end_line, ?start_byte, ?end_byte, ?provider)\n\
                   if def[:target_path] == \"crates/runtime/src/source_provider.rs\"\n\
                     return def[:start_line]\n\
                   end\n\
                 end\n\
                 return nothing"
            ))
            .unwrap();
            assert!(matches!(
                report.outcome,
                TaskOutcome::Complete { value, .. } if value.as_int() == Some(1)
            ));
        });
    }

    #[test]
    fn source_app_select_symbol_sync_event_updates_session_state() {
        let root_path = env::current_dir().unwrap().join(".cache").join(format!(
            "source-app-fixture-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        let src_dir = root_path.join("src");
        fs::create_dir_all(&src_dir).unwrap();
        fs::write(
            src_dir.join("lib.rs"),
            "mod source_provider;\nfn call_provider() { source_provider::boot(); }\n",
        )
        .unwrap();
        fs::write(src_dir.join("source_provider.rs"), "pub fn boot() {}\n").unwrap();
        let root = root_path.display().to_string();
        let source_path = "src/lib.rs";
        let source_text_path = root_path.join(source_path);
        let index_path = temp_index_path("source-app");
        build_source_index_file(Path::new(&root), &index_path).unwrap();
        with_source_index_env(&index_path, Some(Path::new("/bin/false")), || {
            let mut runner = SourceRunner::new_empty();
            for filein in [
                include_str!("../../../apps/shared/sync-host.mica"),
                include_str!("../../../apps/shared/sync-dom.mica"),
                include_str!("../../../apps/source/core.mica"),
                include_str!("../../../apps/source/ui-session.mica"),
                include_str!("../../../apps/source/ui-compose.mica"),
                include_str!("../../../apps/source/http.mica"),
            ] {
                runner.run_filein(filein).unwrap();
            }
            runner
                .run_source(&format!(
                    "retract source/RepositoryRoot(#source/repo_mica, _)\n\
                     assert source/RepositoryRoot(#source/repo_mica, {root:?})"
                ))
                .unwrap();

            let source = fs::read_to_string(&source_text_path).unwrap();
            let offset = source.find("boot").unwrap();
            let report = runner
                .run_source(&format!(
                    "let fields = {{:path -> {source_path:?}, :byte -> {byte:?}}}\n\
                     let handled = sync_event(endpoint(), nothing, 31, \"submit\", \"\", \"source_select_symbol\", fields)\n\
                     let references_handled = sync_event(endpoint(), nothing, 31, \"submit\", \"\", \"source_find_references\", {{}})\n\
                     let path = one source/SelectedPath(endpoint(), ?path)\n\
                     let symbol = one source/SelectedSymbol(endpoint(), ?symbol)\n\
                     let revision = sync_view_revision(31)\n\
                     let payload = dom_snapshot_payload(31, revision, sync_view_tree(31, revision))\n\
                     return [handled, references_handled, path, symbol != nothing, string_contains(payload, \"source index\"), string_contains(payload, \"static-analysis\"), string_contains(payload, \"source_provider::boot\")]",
                    source_path = source_path,
                    byte = offset.to_string()
                ))
                .unwrap();
            let TaskOutcome::Complete { value, .. } = report.outcome else {
                panic!("expected complete outcome, got {:?}", report.outcome);
            };
            value
                .with_list(|values| {
                    assert_eq!(values[0], Value::bool(true));
                    assert_eq!(values[1], Value::bool(true));
                    assert_eq!(values[2], Value::string("src/source_provider.rs"));
                    assert_eq!(values[3], Value::bool(true));
                    assert_eq!(values[4], Value::bool(true));
                    assert_eq!(values[5], Value::bool(false));
                    assert_eq!(values[6], Value::bool(true));
                })
                .expect("expected select-symbol state tuple");
        });
        let _ = fs::remove_file(index_path);
        let _ = fs::remove_dir_all(root_path);
    }

    #[test]
    fn source_app_unknown_rust_symbol_is_noop() {
        let root_path = env::current_dir().unwrap().join(".cache").join(format!(
            "source-app-unknown-symbol-fixture-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        let src_dir = root_path.join("src");
        fs::create_dir_all(&src_dir).unwrap();
        fs::write(
            src_dir.join("lib.rs"),
            "pub fn call_provider() -> Result<String, String> { Ok(String::new()) }\n",
        )
        .unwrap();
        let root = root_path.display().to_string();
        let source_path = "src/lib.rs";
        let source_text_path = root_path.join(source_path);
        let index_path = temp_index_path("source-app-unknown-symbol");
        build_source_index_file(Path::new(&root), &index_path).unwrap();
        with_source_index_env(&index_path, Some(Path::new("/bin/false")), || {
            let mut runner = SourceRunner::new_empty();
            for filein in [
                include_str!("../../../apps/shared/sync-host.mica"),
                include_str!("../../../apps/shared/sync-dom.mica"),
                include_str!("../../../apps/source/core.mica"),
                include_str!("../../../apps/source/ui-session.mica"),
                include_str!("../../../apps/source/ui-compose.mica"),
                include_str!("../../../apps/source/http.mica"),
            ] {
                runner.run_filein(filein).unwrap();
            }
            runner
                .run_source(&format!(
                    "retract source/RepositoryRoot(#source/repo_mica, _)\n\
                     assert source/RepositoryRoot(#source/repo_mica, {root:?})"
                ))
                .unwrap();

            let source = fs::read_to_string(&source_text_path).unwrap();
            let offset = source.find("String").unwrap();
            let report = runner
                .run_source(&format!(
                    "let fields = {{:path -> {source_path:?}, :byte -> {byte:?}}}\n\
                     let handled = sync_event(endpoint(), nothing, 31, \"submit\", \"\", \"source_select_symbol\", fields)\n\
                     let symbol = one source/SelectedSymbol(endpoint(), ?symbol)\n\
                     let revision = sync_view_revision(31)\n\
                     return [handled, symbol == nothing, revision]",
                    source_path = source_path,
                    byte = offset.to_string()
                ))
                .unwrap();
            let TaskOutcome::Complete { value, .. } = report.outcome else {
                panic!("expected complete outcome, got {:?}", report.outcome);
            };
            value
                .with_list(|values| {
                    assert_eq!(values[0], Value::bool(false));
                    assert_eq!(values[1], Value::bool(true));
                    assert_eq!(values[2].as_int(), Some(1));
                })
                .expect("expected unknown-symbol noop tuple");
        });
        let _ = fs::remove_file(index_path);
        let _ = fs::remove_dir_all(root_path);
    }

    #[test]
    fn source_app_open_directory_sync_event_updates_session_state() {
        let root_path = env::current_dir().unwrap().join(".cache").join(format!(
            "source-app-dir-fixture-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        let src_dir = root_path.join("src").join("deep");
        fs::create_dir_all(&src_dir).unwrap();
        fs::write(root_path.join("src").join("lib.rs"), "mod deep;\n").unwrap();
        fs::write(src_dir.join("leaf.rs"), "pub fn leaf() {}\n").unwrap();
        let root = root_path.display().to_string();
        let index_path = temp_index_path("source-app-dir");
        build_source_index_file(Path::new(&root), &index_path).unwrap();
        with_source_index_env(&index_path, Some(Path::new("/bin/false")), || {
            let mut runner = SourceRunner::new_empty();
            for filein in [
                include_str!("../../../apps/shared/sync-host.mica"),
                include_str!("../../../apps/shared/sync-dom.mica"),
                include_str!("../../../apps/source/core.mica"),
                include_str!("../../../apps/source/ui-session.mica"),
                include_str!("../../../apps/source/ui-compose.mica"),
                include_str!("../../../apps/source/http.mica"),
            ] {
                runner.run_filein(filein).unwrap();
            }
            runner
                .run_source(&format!(
                    "retract source/RepositoryRoot(#source/repo_mica, _)\n\
                     assert source/RepositoryRoot(#source/repo_mica, {root:?})"
                ))
                .unwrap();

            let report = runner
                .run_source(
                    "let file_fields = {:path -> \"src/lib.rs\"}\n\
                     let opened_file = sync_event(endpoint(), nothing, 31, \"submit\", \"\", \"source_open_file\", file_fields)\n\
                     let src_fields = {:path -> \"src\"}\n\
                     let opened_src = sync_event(endpoint(), nothing, 31, \"submit\", \"\", \"source_open_directory\", src_fields)\n\
                     let dir_fields = {:path -> \"src/deep\"}\n\
                     let opened_dir = sync_event(endpoint(), nothing, 31, \"submit\", \"\", \"source_open_directory\", dir_fields)\n\
                     let src_expanded = source/ExpandedDirectory(endpoint(), \"src\")\n\
                     let deep_expanded = source/ExpandedDirectory(endpoint(), \"src/deep\")\n\
                     let revision = sync_view_revision(31)\n\
                     let payload = dom_snapshot_payload(31, revision, sync_view_tree(31, revision))\n\
                     return [opened_file, opened_src, opened_dir, src_expanded, deep_expanded, string_contains(payload, \"leaf.rs\"), string_contains(payload, \"Collapse\")]",
                )
                .unwrap();
            let TaskOutcome::Complete { value, .. } = report.outcome else {
                panic!("expected complete outcome, got {:?}", report.outcome);
            };
            value
                .with_list(|values| {
                    assert_eq!(values[0], Value::bool(true));
                    assert_eq!(values[1], Value::bool(true));
                    assert_eq!(values[2], Value::bool(true));
                    assert_eq!(values[3], Value::bool(true));
                    assert_eq!(values[4], Value::bool(true));
                    assert_eq!(values[5], Value::bool(true));
                    assert_eq!(values[6], Value::bool(true));
                })
                .expect("expected open-directory state tuple");
        });
        let _ = fs::remove_file(index_path);
        let _ = fs::remove_dir_all(root_path);
    }

    #[test]
    fn source_app_hides_dot_entries_until_toggled() {
        let root_path = env::current_dir().unwrap().join("target").join(format!(
            "source-app-hidden-fixture-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        fs::create_dir_all(root_path.join(".secret")).unwrap();
        fs::create_dir_all(root_path.join("src")).unwrap();
        fs::write(root_path.join(".env"), "TOKEN=secret\n").unwrap();
        fs::write(root_path.join(".secret").join("index.json"), "{}\n").unwrap();
        fs::write(
            root_path.join("src").join("lib.rs"),
            "pub fn visible() {}\n",
        )
        .unwrap();
        let root = root_path.display().to_string();
        let index_path = temp_index_path("source-app-hidden");
        build_source_index_file(Path::new(&root), &index_path).unwrap();
        with_source_index_env(&index_path, Some(Path::new("/bin/false")), || {
            let mut runner = SourceRunner::new_empty();
            for filein in [
                include_str!("../../../apps/shared/sync-host.mica"),
                include_str!("../../../apps/shared/sync-dom.mica"),
                include_str!("../../../apps/source/core.mica"),
                include_str!("../../../apps/source/ui-session.mica"),
                include_str!("../../../apps/source/ui-compose.mica"),
                include_str!("../../../apps/source/http.mica"),
            ] {
                runner.run_filein(filein).unwrap();
            }
            runner
                .run_source(&format!(
                    "retract source/RepositoryRoot(#source/repo_mica, _)\n\
                     assert source/RepositoryRoot(#source/repo_mica, {root:?})"
                ))
                .unwrap();

            let report = runner
                .run_source(
                    "let open_fields = {:path -> \"src/lib.rs\"}\n\
                     let opened = sync_event(endpoint(), nothing, 31, \"submit\", \"\", \"source_open_file\", open_fields)\n\
                     let initial_revision = sync_view_revision(31)\n\
                     let initial_payload = dom_snapshot_payload(31, initial_revision, sync_view_tree(31, initial_revision))\n\
                     let fields = {:show_hidden -> \"true\"}\n\
                     let toggled = sync_event(endpoint(), nothing, 31, \"submit\", \"\", \"source_toggle_hidden\", fields)\n\
                     let next_revision = sync_view_revision(31)\n\
                     let next_payload = dom_snapshot_payload(31, next_revision, sync_view_tree(31, next_revision))\n\
                     return [opened, string_contains(initial_payload, \"src\"), string_contains(initial_payload, \".env\"), string_contains(initial_payload, \".secret\"), toggled, string_contains(next_payload, \".env\"), string_contains(next_payload, \".secret\")]",
                )
                .unwrap();
            let TaskOutcome::Complete { value, .. } = report.outcome else {
                panic!("expected complete outcome, got {:?}", report.outcome);
            };
            value
                .with_list(|values| {
                    assert_eq!(values[0], Value::bool(true));
                    assert_eq!(values[1], Value::bool(true));
                    assert_eq!(values[2], Value::bool(false));
                    assert_eq!(values[3], Value::bool(false));
                    assert_eq!(values[4], Value::bool(true));
                    assert_eq!(values[5], Value::bool(true));
                    assert_eq!(values[6], Value::bool(true));
                })
                .expect("expected hidden-toggle state tuple");
        });
        let _ = fs::remove_file(index_path);
        let _ = fs::remove_dir_all(root_path);
    }

    #[test]
    fn source_app_toggles_inspector_sections() {
        let root_path = env::current_dir().unwrap().join("target").join(format!(
            "source-app-inspector-fixture-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        fs::create_dir_all(root_path.join("src")).unwrap();
        fs::write(
            root_path.join("src").join("lib.rs"),
            "pub fn inspector_fixture() {}\n",
        )
        .unwrap();
        let root = root_path.display().to_string();
        let index_path = temp_index_path("source-app-inspector");
        build_source_index_file(Path::new(&root), &index_path).unwrap();
        with_source_index_env(&index_path, Some(Path::new("/bin/false")), || {
            let mut runner = SourceRunner::new_empty();
            for filein in [
                include_str!("../../../apps/shared/sync-host.mica"),
                include_str!("../../../apps/shared/sync-dom.mica"),
                include_str!("../../../apps/source/core.mica"),
                include_str!("../../../apps/source/ui-session.mica"),
                include_str!("../../../apps/source/ui-compose.mica"),
                include_str!("../../../apps/source/http.mica"),
            ] {
                runner.run_filein(filein).unwrap();
            }
            runner
                .run_source(&format!(
                    "retract source/RepositoryRoot(#source/repo_mica, _)\n\
                     assert source/RepositoryRoot(#source/repo_mica, {root:?})"
                ))
                .unwrap();

            let report = runner
                .run_source(
                    "let open_fields = {:path -> \"src/lib.rs\"}\n\
                     let opened = sync_event(endpoint(), nothing, 31, \"submit\", \"\", \"source_open_file\", open_fields)\n\
                     let outline_fields = {:section -> \"outline\", :collapsed -> \"true\"}\n\
                     let outline_toggled = sync_event(endpoint(), nothing, 31, \"submit\", \"\", \"source_toggle_inspector_section\", outline_fields)\n\
                     let annotation_fields = {:section -> \"annotations\", :collapsed -> \"true\"}\n\
                     let annotations_toggled = sync_event(endpoint(), nothing, 31, \"submit\", \"\", \"source_toggle_inspector_section\", annotation_fields)\n\
                     let revision = sync_view_revision(31)\n\
                     let payload = dom_snapshot_payload(31, revision, sync_view_tree(31, revision))\n\
                     return [opened, outline_toggled, annotations_toggled, string_contains(payload, \"source-outline source-inspector-section collapsed\"), string_contains(payload, \"source-spans source-inspector-section collapsed\"), string_contains(payload, \"source-inspector-rail both-collapsed\")]",
                )
                .unwrap();
            let TaskOutcome::Complete { value, .. } = report.outcome else {
                panic!("expected complete outcome, got {:?}", report.outcome);
            };
            value
                .with_list(|values| {
                    assert_eq!(values[0], Value::bool(true));
                    assert_eq!(values[1], Value::bool(true));
                    assert_eq!(values[2], Value::bool(true));
                    assert_eq!(values[3], Value::bool(true));
                    assert_eq!(values[4], Value::bool(true));
                    assert_eq!(values[5], Value::bool(true));
                })
                .expect("expected inspector toggle state tuple");
        });
        let _ = fs::remove_file(index_path);
        let _ = fs::remove_dir_all(root_path);
    }
}
