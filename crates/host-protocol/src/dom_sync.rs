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
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <https://www.gnu.org/licenses/>.

use mica_var::{Symbol, Value, ValueKind};
use serde_json::{Map, Value as JsonValue, json};
use std::collections::BTreeMap;

pub const DOM_PATCH_PAYLOAD_TYPE: &str = "dom_patch";
pub const SUPPORTED_DOM_TAGS: &[&str] = &[
    "button", "div", "form", "input", "li", "main", "p", "section", "span", "ul",
];
pub const SUPPORTED_DOM_ATTRIBUTES: &[&str] = &[
    "aria-label",
    "autocomplete",
    "class",
    "data-sync-action",
    "data-sync-event",
    "id",
    "name",
    "placeholder",
    "type",
    "value",
];

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
const SIGNATURE_MASK: u64 = 0x007f_ffff_ffff_ffff;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DomNode {
    Text(String),
    Element {
        tag: String,
        attrs: BTreeMap<String, String>,
        children: Vec<DomNode>,
    },
}

/// DOM patches use child-index paths from the synced root node. The empty path
/// targets the root; `[0]` targets the root's first child.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DomPatch {
    Replace {
        path: Vec<usize>,
        node: DomNode,
    },
    SetText {
        path: Vec<usize>,
        text: String,
    },
    SetAttr {
        path: Vec<usize>,
        name: String,
        value: String,
    },
    RemoveAttr {
        path: Vec<usize>,
        name: String,
    },
    AppendChild {
        path: Vec<usize>,
        node: DomNode,
    },
    RemoveChild {
        path: Vec<usize>,
    },
}

impl DomNode {
    pub fn from_mica_value(value: &Value) -> Result<Self, String> {
        if let Some(text) = value
            .map_get(&Value::symbol(Symbol::intern("text")))
            .and_then(|value| value.with_str(str::to_owned))
        {
            return Ok(Self::Text(text));
        }
        if value
            .map_get(&Value::symbol(Symbol::intern("raw")))
            .is_some()
        {
            return Err("raw DOM nodes are not valid sync payload nodes".to_owned());
        }

        let tag = value
            .map_get(&Value::symbol(Symbol::intern("tag")))
            .and_then(|value| value.with_str(str::to_owned))
            .ok_or_else(|| "DOM element requires string tag".to_owned())?;
        let attrs = value
            .map_get(&Value::symbol(Symbol::intern("attrs")))
            .ok_or_else(|| "DOM element requires attrs map".to_owned())?;
        let children = value
            .map_get(&Value::symbol(Symbol::intern("children")))
            .ok_or_else(|| "DOM element requires children list".to_owned())?;
        validate_dom_tag(&tag)?;
        let attrs = mica_attrs(&attrs)?;
        let children = children
            .with_list(|children| {
                children
                    .iter()
                    .map(Self::from_mica_value)
                    .collect::<Result<Vec<_>, _>>()
            })
            .ok_or_else(|| "DOM element children must be a list".to_owned())??;

        Ok(Self::Element {
            tag,
            attrs,
            children,
        })
    }

    pub fn to_mica_value(&self) -> Value {
        match self {
            Self::Text(text) => {
                Value::map([(Value::symbol(Symbol::intern("text")), Value::string(text))])
            }
            Self::Element {
                tag,
                attrs,
                children,
            } => Value::map([
                (
                    Value::symbol(Symbol::intern("attrs")),
                    Value::map(
                        attrs
                            .iter()
                            .map(|(name, value)| (Value::string(name), Value::string(value))),
                    ),
                ),
                (
                    Value::symbol(Symbol::intern("children")),
                    Value::list(children.iter().map(Self::to_mica_value)),
                ),
                (Value::symbol(Symbol::intern("tag")), Value::string(tag)),
            ]),
        }
    }

    pub fn to_json_value(&self) -> JsonValue {
        match self {
            Self::Text(text) => json!({ "text": text }),
            Self::Element {
                tag,
                attrs,
                children,
            } => {
                let mut object = Map::new();
                object.insert("tag".to_owned(), JsonValue::String(tag.clone()));
                object.insert("attrs".to_owned(), json_attrs(attrs));
                object.insert(
                    "children".to_owned(),
                    JsonValue::Array(children.iter().map(Self::to_json_value).collect()),
                );
                JsonValue::Object(object)
            }
        }
    }
}

impl DomPatch {
    pub fn to_mica_value(&self) -> Value {
        match self {
            Self::Replace { path, node } => dom_patch_value(
                "replace",
                path,
                [(Value::symbol(Symbol::intern("node")), node.to_mica_value())],
            ),
            Self::SetText { path, text } => dom_patch_value(
                "set_text",
                path,
                [(Value::symbol(Symbol::intern("text")), Value::string(text))],
            ),
            Self::SetAttr { path, name, value } => dom_patch_value(
                "set_attr",
                path,
                [
                    (Value::symbol(Symbol::intern("name")), Value::string(name)),
                    (Value::symbol(Symbol::intern("value")), Value::string(value)),
                ],
            ),
            Self::RemoveAttr { path, name } => dom_patch_value(
                "remove_attr",
                path,
                [(Value::symbol(Symbol::intern("name")), Value::string(name))],
            ),
            Self::AppendChild { path, node } => dom_patch_value(
                "append_child",
                path,
                [(Value::symbol(Symbol::intern("node")), node.to_mica_value())],
            ),
            Self::RemoveChild { path } => dom_patch_value("remove_child", path, []),
        }
    }

    pub fn to_json_value(&self) -> JsonValue {
        match self {
            Self::Replace { path, node } => json!({
                "op": "replace",
                "path": path,
                "node": node.to_json_value(),
            }),
            Self::SetText { path, text } => json!({
                "op": "set_text",
                "path": path,
                "text": text,
            }),
            Self::SetAttr { path, name, value } => json!({
                "op": "set_attr",
                "path": path,
                "name": name,
                "value": value,
            }),
            Self::RemoveAttr { path, name } => json!({
                "op": "remove_attr",
                "path": path,
                "name": name,
            }),
            Self::AppendChild { path, node } => json!({
                "op": "append_child",
                "path": path,
                "node": node.to_json_value(),
            }),
            Self::RemoveChild { path } => json!({
                "op": "remove_child",
                "path": path,
            }),
        }
    }
}

pub fn diff_dom_nodes(before: &DomNode, after: &DomNode) -> Vec<DomPatch> {
    let mut patches = Vec::new();
    diff_dom_node(before, after, &mut Vec::new(), &mut patches);
    patches
}

pub fn snapshot_payload_json(view: u64, revision: u64, root: &DomNode) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "view": view,
        "revision": revision,
        "root": root.to_json_value(),
    }))
    .expect("DOM snapshot payload should serialize")
}

pub fn dom_patch_payload_json(view: u64, revision: u64, patches: &[DomPatch]) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "type": DOM_PATCH_PAYLOAD_TYPE,
        "view": view,
        "revision": revision,
        "patches": patches.iter().map(DomPatch::to_json_value).collect::<Vec<_>>(),
    }))
    .expect("DOM patch payload should serialize")
}

/// Hashes a revision and canonical state payload. Delta envelopes carry the
/// signature of the resulting rendered state, not the delta payload bytes.
pub fn sync_payload_signature(revision: u64, payload: &[u8]) -> u64 {
    let mut hash = FNV_OFFSET;
    for byte in revision.to_le_bytes().iter().chain(payload) {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash & SIGNATURE_MASK
}

fn mica_attrs(value: &Value) -> Result<BTreeMap<String, String>, String> {
    value
        .with_map(|entries| {
            entries
                .iter()
                .map(|(key, value)| Ok((mica_attr_name(key)?, mica_attr_value(value)?)))
                .collect::<Result<BTreeMap<_, _>, String>>()
        })
        .ok_or_else(|| "DOM element attrs must be a map".to_owned())?
}

fn mica_attr_name(value: &Value) -> Result<String, String> {
    if let Some(text) = value.with_str(str::to_owned) {
        validate_dom_attr(&text)?;
        return Ok(text);
    }
    if let Some(symbol) = value.as_symbol()
        && let Some(name) = symbol.name()
    {
        validate_dom_attr(name)?;
        return Ok(name.to_owned());
    }
    Err("DOM attribute names must be strings or named symbols".to_owned())
}

fn mica_attr_value(value: &Value) -> Result<String, String> {
    match value.kind() {
        ValueKind::String => Ok(value.with_str(str::to_owned).unwrap()),
        ValueKind::Bool => Ok(value.as_bool().unwrap().to_string()),
        ValueKind::Int => Ok(value.as_int().unwrap().to_string()),
        _ => Err("DOM attribute values must be strings, booleans, or integers".to_owned()),
    }
}

fn validate_dom_tag(tag: &str) -> Result<(), String> {
    SUPPORTED_DOM_TAGS
        .contains(&tag)
        .then_some(())
        .ok_or_else(|| format!("unsupported DOM sync tag: {tag}"))
}

fn validate_dom_attr(name: &str) -> Result<(), String> {
    SUPPORTED_DOM_ATTRIBUTES
        .contains(&name)
        .then_some(())
        .ok_or_else(|| format!("unsupported DOM sync attribute: {name}"))
}

fn diff_dom_node(
    before: &DomNode,
    after: &DomNode,
    path: &mut Vec<usize>,
    patches: &mut Vec<DomPatch>,
) {
    if before == after {
        return;
    }
    match (before, after) {
        (DomNode::Text(before), DomNode::Text(after)) => {
            if before != after {
                patches.push(DomPatch::SetText {
                    path: path.clone(),
                    text: after.clone(),
                });
            }
        }
        (
            DomNode::Element {
                tag: before_tag,
                attrs: before_attrs,
                children: before_children,
            },
            DomNode::Element {
                tag: after_tag,
                attrs: after_attrs,
                children: after_children,
            },
        ) if before_tag == after_tag => {
            diff_dom_attrs(before_attrs, after_attrs, path, patches);
            diff_dom_children(before_children, after_children, path, patches);
        }
        _ => patches.push(DomPatch::Replace {
            path: path.clone(),
            node: after.clone(),
        }),
    }
}

fn diff_dom_attrs(
    before: &BTreeMap<String, String>,
    after: &BTreeMap<String, String>,
    path: &[usize],
    patches: &mut Vec<DomPatch>,
) {
    for (name, before_value) in before {
        match after.get(name) {
            Some(after_value) if after_value == before_value => {}
            Some(after_value) => patches.push(DomPatch::SetAttr {
                path: path.to_vec(),
                name: name.clone(),
                value: after_value.clone(),
            }),
            None => patches.push(DomPatch::RemoveAttr {
                path: path.to_vec(),
                name: name.clone(),
            }),
        }
    }

    for (name, after_value) in after {
        if !before.contains_key(name) {
            patches.push(DomPatch::SetAttr {
                path: path.to_vec(),
                name: name.clone(),
                value: after_value.clone(),
            });
        }
    }
}

fn diff_dom_children(
    before: &[DomNode],
    after: &[DomNode],
    path: &mut Vec<usize>,
    patches: &mut Vec<DomPatch>,
) {
    let shared = before.len().min(after.len());
    for index in 0..shared {
        path.push(index);
        diff_dom_node(&before[index], &after[index], path, patches);
        path.pop();
    }
    for child in &after[shared..] {
        patches.push(DomPatch::AppendChild {
            path: path.clone(),
            node: child.clone(),
        });
    }
    for index in (after.len()..before.len()).rev() {
        let mut child_path = path.clone();
        child_path.push(index);
        patches.push(DomPatch::RemoveChild { path: child_path });
    }
}

fn json_attrs(attrs: &BTreeMap<String, String>) -> JsonValue {
    JsonValue::Object(
        attrs
            .iter()
            .map(|(name, value)| (name.clone(), JsonValue::String(value.clone())))
            .collect(),
    )
}

fn dom_patch_value(
    op: &str,
    path: &[usize],
    entries: impl IntoIterator<Item = (Value, Value)>,
) -> Value {
    let mut fields = vec![
        (Value::symbol(Symbol::intern("op")), Value::string(op)),
        (
            Value::symbol(Symbol::intern("path")),
            Value::list(path.iter().map(|index| {
                Value::int(i64::try_from(*index).expect("DOM path index should fit in i64"))
                    .unwrap()
            })),
        ),
    ];
    fields.extend(entries);
    Value::map(fields)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diff_dom_nodes_emits_stable_patch_ops() {
        let before = DomNode::Element {
            tag: "ul".to_owned(),
            attrs: BTreeMap::new(),
            children: Vec::new(),
        };
        let after = DomNode::Element {
            tag: "ul".to_owned(),
            attrs: BTreeMap::from([("id".to_owned(), "messages".to_owned())]),
            children: vec![DomNode::Text("hello".to_owned())],
        };

        assert_eq!(
            diff_dom_nodes(&before, &after),
            vec![
                DomPatch::SetAttr {
                    path: vec![],
                    name: "id".to_owned(),
                    value: "messages".to_owned(),
                },
                DomPatch::AppendChild {
                    path: vec![],
                    node: DomNode::Text("hello".to_owned()),
                },
            ]
        );
    }

    #[test]
    fn payload_signature_is_stable() {
        assert_eq!(
            sync_payload_signature(2, br#"{"type":"dom_patch"}"#),
            sync_payload_signature(2, br#"{"type":"dom_patch"}"#)
        );
        assert_ne!(
            sync_payload_signature(2, br#"{"type":"dom_patch"}"#),
            sync_payload_signature(3, br#"{"type":"dom_patch"}"#)
        );
    }

    #[test]
    fn snapshot_payload_has_protocol_shape() {
        let payload = snapshot_payload_json(11, 20, &DomNode::Text("hello".to_owned()));
        let json: JsonValue = serde_json::from_slice(&payload).unwrap();

        assert_eq!(json["view"], 11);
        assert_eq!(json["revision"], 20);
        assert_eq!(json["root"]["text"], "hello");
    }
}
