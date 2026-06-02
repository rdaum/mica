use crate::rust_analyzer::{LspLocation, LspPosition};
use crate::syntax::{SyntaxDocument, line_end_without_newline, line_starts};
use crate::util::{invalid_relation, path_to_mica_string, read_utf8_file};
use mica_relation_kernel::{KernelError, RelationId};
use std::path::Path;

#[derive(Clone, Debug)]
pub(crate) struct SemanticLocation {
    pub(crate) path: String,
    pub(crate) name: String,
    pub(crate) kind: String,
    pub(crate) start_line: usize,
    pub(crate) end_line: usize,
    pub(crate) start_byte: usize,
    pub(crate) end_byte: usize,
    pub(crate) provider: String,
}

#[derive(Clone, Debug)]
pub(crate) struct SemanticSymbol {
    pub(crate) id: String,
    pub(crate) provider: SemanticSymbolProvider,
    pub(crate) path: String,
    pub(crate) start_byte: usize,
    pub(crate) name: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SemanticSymbolProvider {
    Index,
    RustAnalyzer,
}

impl SemanticSymbol {
    pub(crate) fn parse(symbol: &str) -> Option<Self> {
        let parts = symbol.split(':').collect::<Vec<_>>();
        let provider = match parts.first().copied()? {
            "idx" => SemanticSymbolProvider::Index,
            "ra" => SemanticSymbolProvider::RustAnalyzer,
            _ => return None,
        };
        let (path, start_byte, name) = if provider == SemanticSymbolProvider::Index
            && parts.len() >= 6
            && parts
                .get(3)
                .and_then(|part| part.parse::<usize>().ok())
                .is_some()
        {
            (
                (*parts.get(2)?).to_owned(),
                parts.get(3)?.parse().ok()?,
                (*parts.get(5)?).to_owned(),
            )
        } else {
            (
                (*parts.get(1)?).to_owned(),
                parts.get(2)?.parse().ok()?,
                (*parts.get(4)?).to_owned(),
            )
        };
        Some(Self {
            id: symbol.to_owned(),
            provider,
            path,
            start_byte,
            name,
        })
    }
}

pub(crate) fn semantic_location(
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

pub(crate) fn semantic_symbol(location: &SemanticLocation) -> String {
    format!(
        "ra:{}:{}:{}:{}",
        location.path, location.start_byte, location.end_byte, location.name
    )
}

pub(crate) fn byte_offset_to_lsp_position(
    relation: RelationId,
    text: &str,
    byte_offset: usize,
) -> Result<LspPosition, KernelError> {
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
    Ok(LspPosition {
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
