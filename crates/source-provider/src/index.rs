use crate::navigation::SemanticSymbol;
use crate::syntax::SyntaxDocument;
use crate::util::invalid_relation;
use mica_relation_kernel::{KernelError, RelationId};
use serde_json::{Value as JsonValue, json};
use std::fs;
use std::path::{Component, Path, PathBuf};

const SOURCE_INDEX_ID: &str = "source-index:mica-worktree";
const SOURCE_INDEX_SCHEMA: &str = "mica-source-index-v1";
const SOURCE_INDEX_PROVIDER: &str = "mica-source-index/static-analysis";
const SOURCE_INDEX_VERSION: &str = "2";

#[derive(Clone, Debug)]
pub(crate) struct PersistentSemanticIndex {
    pub(crate) id: String,
    pub(crate) provider: String,
    pub(crate) version: String,
    pub(crate) status: String,
    pub(crate) error: String,
    pub(crate) symbols: Vec<IndexedSymbol>,
    pub(crate) references: Vec<IndexedReference>,
}

#[derive(Clone, Debug)]
pub(crate) struct IndexedSymbol {
    pub(crate) symbol: String,
    pub(crate) name: String,
    pub(crate) kind: String,
    pub(crate) path: String,
    pub(crate) start_line: usize,
    pub(crate) end_line: usize,
    pub(crate) start_byte: usize,
    pub(crate) end_byte: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct IndexedReference {
    pub(crate) symbol: String,
    pub(crate) name: String,
    pub(crate) path: String,
    pub(crate) start_line: usize,
    pub(crate) end_line: usize,
    pub(crate) start_byte: usize,
    pub(crate) end_byte: usize,
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

    pub(crate) fn load(relation: RelationId, path: &Path) -> Result<Self, KernelError> {
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

    pub(crate) fn is_complete(&self) -> bool {
        self.status == "complete"
    }

    pub(crate) fn definition_at(&self, path: &str, byte_offset: usize) -> Vec<IndexedSymbol> {
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

    pub(crate) fn references_of(&self, symbol: &SemanticSymbol) -> Vec<IndexedReference> {
        if !self.is_complete() {
            return Vec::new();
        }
        self.references
            .iter()
            .filter(|reference| reference.symbol == symbol.id || reference.name == symbol.name)
            .cloned()
            .collect()
    }

    pub(crate) fn search(&self, query: &str, limit: usize) -> Vec<IndexedSymbol> {
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
