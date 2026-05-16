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

use crate::{ApplicableMethodCall, DispatchRelations};
use arc_swap::ArcSwap;
use mica_var::Value;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug)]
pub(crate) struct DispatchCache {
    entries: Arc<ArcSwap<BTreeMap<DispatchCacheKey, Arc<[ApplicableMethodCall]>>>>,
    publish_lock: Arc<Mutex<()>>,
}

impl DispatchCache {
    pub(crate) fn new() -> Self {
        Self {
            entries: Arc::new(ArcSwap::from_pointee(BTreeMap::new())),
            publish_lock: Arc::new(Mutex::new(())),
        }
    }

    pub(crate) fn get(
        &self,
        relations: DispatchRelations,
        selector: &Value,
        roles: &[(Value, Value)],
    ) -> Option<Vec<ApplicableMethodCall>> {
        let key = DispatchCacheKey::new(relations, selector, roles);
        self.get_key(&key)
    }

    pub(crate) fn get_normalized(
        &self,
        relations: DispatchRelations,
        selector: &Value,
        roles: &[(Value, Value)],
    ) -> Option<Vec<ApplicableMethodCall>> {
        let key = DispatchCacheKey::new_normalized(relations, selector, roles);
        self.get_key(&key)
    }

    fn get_key(&self, key: &DispatchCacheKey) -> Option<Vec<ApplicableMethodCall>> {
        let entries = self.entries.load();
        entries.get(key).map(|methods| methods.to_vec())
    }

    pub(crate) fn insert(
        &self,
        relations: DispatchRelations,
        selector: &Value,
        roles: &[(Value, Value)],
        methods: Vec<ApplicableMethodCall>,
    ) {
        let key = DispatchCacheKey::new(relations, selector, roles);
        self.insert_key(key, methods);
    }

    pub(crate) fn insert_normalized(
        &self,
        relations: DispatchRelations,
        selector: &Value,
        roles: &[(Value, Value)],
        methods: Vec<ApplicableMethodCall>,
    ) {
        let key = DispatchCacheKey::new_normalized(relations, selector, roles);
        self.insert_key(key, methods);
    }

    fn insert_key(&self, key: DispatchCacheKey, methods: Vec<ApplicableMethodCall>) {
        let _guard = self.publish_lock.lock().unwrap();
        let entries = self.entries.load_full();
        if entries.contains_key(&key) {
            return;
        }
        let methods = Arc::<[ApplicableMethodCall]>::from(methods);
        let mut next = (*entries).clone();
        next.insert(key, methods);
        self.entries.store(Arc::new(next));
    }
}

impl Default for DispatchCache {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct DispatchCacheKey {
    relations: DispatchRelationsKey,
    selector: Value,
    roles: Vec<(Value, Value)>,
}

impl DispatchCacheKey {
    fn new(relations: DispatchRelations, selector: &Value, roles: &[(Value, Value)]) -> Self {
        let mut roles = roles.to_vec();
        crate::normalize_dispatch_roles(&mut roles);
        Self::from_normalized_roles(relations, selector, roles)
    }

    fn new_normalized(
        relations: DispatchRelations,
        selector: &Value,
        roles: &[(Value, Value)],
    ) -> Self {
        Self::from_normalized_roles(relations, selector, roles.to_vec())
    }

    fn from_normalized_roles(
        relations: DispatchRelations,
        selector: &Value,
        roles: Vec<(Value, Value)>,
    ) -> Self {
        Self {
            relations: DispatchRelationsKey::from(relations),
            selector: selector.clone(),
            roles,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct DispatchRelationsKey {
    method_selector: crate::RelationId,
    param: crate::RelationId,
    delegates: crate::RelationId,
}

impl From<DispatchRelations> for DispatchRelationsKey {
    fn from(value: DispatchRelations) -> Self {
        Self {
            method_selector: value.method_selector,
            param: value.param,
            delegates: value.delegates,
        }
    }
}
