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
use std::collections::{BTreeMap, BTreeSet};

pub const DOM_PATCH_PAYLOAD_TYPE: &str = "dom_patch";
pub const DOM_EVENT_PAYLOAD_TYPE: &str = "dom_event";
pub const SUPPORTED_DOM_TAGS: &[&str] = &[
    "aside", "button", "div", "form", "h1", "h2", "header", "input", "li", "main", "nav", "p",
    "section", "span", "strong", "ul",
];
pub const SUPPORTED_DOM_ATTRIBUTES: &[&str] = &[
    "aria-label",
    "aria-live",
    "autocomplete",
    "class",
    "data-command",
    "data-entity",
    "data-sync-action",
    "data-sync-event",
    "data-sync-key",
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
    InsertChild {
        path: Vec<usize>,
        index: usize,
        node: DomNode,
    },
    RemoveChild {
        path: Vec<usize>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DomEventPayload {
    pub session_id: u64,
    pub view_id: u64,
    pub revision: u64,
    pub signature: u64,
    pub event: String,
    pub target: String,
    pub action: String,
    pub fields: BTreeMap<String, String>,
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
            Self::InsertChild { path, index, node } => dom_patch_value(
                "insert_child",
                path,
                [
                    (
                        Value::symbol(Symbol::intern("index")),
                        Value::int(
                            i64::try_from(*index).expect("DOM child index should fit in i64"),
                        )
                        .unwrap(),
                    ),
                    (Value::symbol(Symbol::intern("node")), node.to_mica_value()),
                ],
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
            Self::InsertChild { path, index, node } => json!({
                "op": "insert_child",
                "path": path,
                "index": index,
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

pub fn dom_event_payload_json(event: &DomEventPayload) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "type": DOM_EVENT_PAYLOAD_TYPE,
        "session": event.session_id,
        "view": event.view_id,
        "revision": event.revision,
        "signature": event.signature,
        "event": event.event,
        "target": event.target,
        "action": event.action,
        "fields": event.fields,
    }))
    .expect("DOM event payload should serialize")
}

pub fn decode_dom_event_payload(bytes: &[u8]) -> Result<Option<DomEventPayload>, String> {
    let Ok(value) = serde_json::from_slice::<JsonValue>(bytes) else {
        return Ok(None);
    };
    let Some(object) = value.as_object() else {
        return Ok(None);
    };
    if object.get("type").and_then(JsonValue::as_str) != Some(DOM_EVENT_PAYLOAD_TYPE) {
        return Ok(None);
    }

    let fields = object
        .get("fields")
        .and_then(JsonValue::as_object)
        .ok_or_else(|| "dom_event requires fields object".to_owned())?
        .iter()
        .map(|(key, value)| {
            let value = value
                .as_str()
                .ok_or_else(|| format!("dom_event field {key} must be a string"))?;
            Ok((key.clone(), value.to_owned()))
        })
        .collect::<Result<BTreeMap<_, _>, String>>()?;

    Ok(Some(DomEventPayload {
        session_id: required_u64(object, "session")?,
        view_id: required_u64(object, "view")?,
        revision: required_u64(object, "revision")?,
        signature: required_u64(object, "signature")?,
        event: required_string(object, "event")?,
        target: required_string(object, "target")?,
        action: object
            .get("action")
            .and_then(JsonValue::as_str)
            .unwrap_or("")
            .to_owned(),
        fields,
    }))
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

fn required_u64(object: &Map<String, JsonValue>, field: &str) -> Result<u64, String> {
    let Some(value) = object.get(field) else {
        return Err(format!("dom_event requires numeric {field}"));
    };
    if let Some(raw) = value.as_u64() {
        return Ok(raw);
    }
    if let Some(text) = value.as_str()
        && let Ok(raw) = text.parse()
    {
        return Ok(raw);
    }
    Err(format!("dom_event requires numeric {field}"))
}

fn required_string(object: &Map<String, JsonValue>, field: &str) -> Result<String, String> {
    object
        .get(field)
        .and_then(JsonValue::as_str)
        .map(str::to_owned)
        .ok_or_else(|| format!("dom_event requires string {field}"))
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
    if let (Some(before_keys), Some(after_keys)) = (child_keys(before), child_keys(after)) {
        diff_keyed_dom_children(before, &before_keys, after, &after_keys, path, patches);
        return;
    }

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

fn diff_keyed_dom_children(
    before: &[DomNode],
    before_keys: &[String],
    after: &[DomNode],
    after_keys: &[String],
    path: &mut Vec<usize>,
    patches: &mut Vec<DomPatch>,
) {
    let after_key_set = after_keys.iter().collect::<BTreeSet<_>>();
    let mut current = before.to_vec();
    let mut current_keys = before_keys.to_vec();

    for index in (0..current.len()).rev() {
        if !after_key_set.contains(&current_keys[index]) {
            let mut child_path = path.clone();
            child_path.push(index);
            patches.push(DomPatch::RemoveChild { path: child_path });
            current.remove(index);
            current_keys.remove(index);
        }
    }

    for (index, (after_child, after_key)) in after.iter().zip(after_keys).enumerate() {
        if current_keys.get(index) == Some(after_key) {
            path.push(index);
            diff_dom_node(&current[index], after_child, path, patches);
            path.pop();
            current[index] = after_child.clone();
            continue;
        }

        if let Some(found) = current_keys
            .iter()
            .enumerate()
            .skip(index + 1)
            .find_map(|(found, key)| (key == after_key).then_some(found))
        {
            let mut child_path = path.clone();
            child_path.push(found);
            patches.push(DomPatch::RemoveChild { path: child_path });
            current.remove(found);
            current_keys.remove(found);
        }

        if index == current.len() {
            patches.push(DomPatch::AppendChild {
                path: path.clone(),
                node: after_child.clone(),
            });
        } else {
            patches.push(DomPatch::InsertChild {
                path: path.clone(),
                index,
                node: after_child.clone(),
            });
        }
        current.insert(index, after_child.clone());
        current_keys.insert(index, after_key.clone());
    }
}

fn child_keys(children: &[DomNode]) -> Option<Vec<String>> {
    let mut seen = BTreeSet::new();
    let mut keys = Vec::with_capacity(children.len());
    for child in children {
        let key = dom_node_key(child)?;
        if !seen.insert(key.clone()) {
            return None;
        }
        keys.push(key);
    }
    Some(keys)
}

fn dom_node_key(node: &DomNode) -> Option<String> {
    let DomNode::Element { attrs, .. } = node else {
        return None;
    };
    attrs
        .get("data-sync-key")
        .or_else(|| attrs.get("id"))
        .cloned()
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

    #[test]
    fn keyed_children_insert_without_replacing_existing_nodes() {
        let before = DomNode::Element {
            tag: "ul".to_owned(),
            attrs: BTreeMap::from([("id".to_owned(), "messages".to_owned())]),
            children: vec![DomNode::Element {
                tag: "li".to_owned(),
                attrs: BTreeMap::from([("data-sync-key".to_owned(), "msg-2".to_owned())]),
                children: vec![DomNode::Text("second".to_owned())],
            }],
        };
        let after = DomNode::Element {
            tag: "ul".to_owned(),
            attrs: BTreeMap::from([("id".to_owned(), "messages".to_owned())]),
            children: vec![
                DomNode::Element {
                    tag: "li".to_owned(),
                    attrs: BTreeMap::from([("data-sync-key".to_owned(), "msg-1".to_owned())]),
                    children: vec![DomNode::Text("first".to_owned())],
                },
                DomNode::Element {
                    tag: "li".to_owned(),
                    attrs: BTreeMap::from([("data-sync-key".to_owned(), "msg-2".to_owned())]),
                    children: vec![DomNode::Text("second updated".to_owned())],
                },
            ],
        };

        assert_eq!(
            diff_dom_nodes(&before, &after),
            vec![
                DomPatch::InsertChild {
                    path: vec![],
                    index: 0,
                    node: DomNode::Element {
                        tag: "li".to_owned(),
                        attrs: BTreeMap::from([("data-sync-key".to_owned(), "msg-1".to_owned())]),
                        children: vec![DomNode::Text("first".to_owned())],
                    },
                },
                DomPatch::SetText {
                    path: vec![1, 0],
                    text: "second updated".to_owned(),
                },
            ]
        );
    }

    #[test]
    fn dom_event_payload_round_trips() {
        let event = DomEventPayload {
            session_id: 7,
            view_id: 11,
            revision: 13,
            signature: 17,
            event: "submit".to_owned(),
            target: "chat-composer".to_owned(),
            action: "chat_post".to_owned(),
            fields: BTreeMap::from([
                ("actor".to_owned(), "bob".to_owned()),
                ("text".to_owned(), "hello".to_owned()),
            ]),
        };
        let payload = dom_event_payload_json(&event);

        assert_eq!(decode_dom_event_payload(&payload).unwrap(), Some(event));
        assert_eq!(
            decode_dom_event_payload(br#"{"type":"other","fields":{}}"#).unwrap(),
            None
        );
        assert!(decode_dom_event_payload(br#"{"type":"dom_event"}"#).is_err());
        assert!(
            decode_dom_event_payload(
                br#"{"type":"dom_event","session":7,"view":11,"revision":1,"signature":2,"event":"submit","target":"chat-composer","fields":{"text":2}}"#
            )
            .is_err()
        );
    }
}
