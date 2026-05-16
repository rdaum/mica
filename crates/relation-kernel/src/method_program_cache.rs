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

use crate::RelationId;
use arc_swap::ArcSwap;
use mica_var::Value;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug)]
pub(crate) struct MethodProgramCache {
    entries: Arc<ArcSwap<BTreeMap<MethodProgramCacheKey, Option<Value>>>>,
    publish_lock: Arc<Mutex<()>>,
}

impl MethodProgramCache {
    pub(crate) fn new() -> Self {
        Self {
            entries: Arc::new(ArcSwap::from_pointee(BTreeMap::new())),
            publish_lock: Arc::new(Mutex::new(())),
        }
    }

    pub(crate) fn get(&self, relation: RelationId, method: &Value) -> Option<Option<Value>> {
        let key = MethodProgramCacheKey::new(relation, method);
        let entries = self.entries.load();
        entries.get(&key).cloned()
    }

    pub(crate) fn insert(&self, relation: RelationId, method: &Value, program: Option<Value>) {
        let key = MethodProgramCacheKey::new(relation, method);
        let _guard = self.publish_lock.lock().unwrap();
        let entries = self.entries.load_full();
        if entries.contains_key(&key) {
            return;
        }
        let mut next = (*entries).clone();
        next.insert(key, program);
        self.entries.store(Arc::new(next));
    }
}

impl Default for MethodProgramCache {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct MethodProgramCacheKey {
    relation: RelationId,
    method: Value,
}

impl MethodProgramCacheKey {
    fn new(relation: RelationId, method: &Value) -> Self {
        Self {
            relation,
            method: method.clone(),
        }
    }
}
