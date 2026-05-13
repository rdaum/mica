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

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::{OnceLock, RwLock};

/// Interned symbol id.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Symbol(pub(crate) u32);

impl Symbol {
    pub const fn from_id(id: u32) -> Self {
        Self(id)
    }

    pub fn intern(name: &str) -> Self {
        symbol_table().intern(name)
    }

    pub const fn id(self) -> u32 {
        self.0
    }

    pub fn name(self) -> Option<&'static str> {
        symbol_table().name(self)
    }

    pub fn metadata(self) -> Option<SymbolMetadata> {
        symbol_table().metadata(self)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct SymbolMetadata {
    pub byte_len: usize,
    pub char_len: usize,
    pub is_ascii: bool,
}

const SYMBOL_CACHE_SIZE: usize = 16;

#[derive(Clone, Copy)]
struct SymbolCacheEntry {
    name: &'static str,
    symbol: Symbol,
}

struct SymbolCache {
    entries: [Option<SymbolCacheEntry>; SYMBOL_CACHE_SIZE],
    next_slot: usize,
}

impl SymbolCache {
    const fn new() -> Self {
        const NONE: Option<SymbolCacheEntry> = None;
        Self {
            entries: [NONE; SYMBOL_CACHE_SIZE],
            next_slot: 0,
        }
    }

    fn get(&self, name: &str) -> Option<Symbol> {
        self.entries
            .iter()
            .flatten()
            .find(|entry| entry.name == name)
            .map(|entry| entry.symbol)
    }

    fn insert(&mut self, name: &'static str, symbol: Symbol) {
        if let Some(entry) = self
            .entries
            .iter_mut()
            .flatten()
            .find(|entry| entry.name == name)
        {
            entry.symbol = symbol;
            return;
        }

        self.entries[self.next_slot] = Some(SymbolCacheEntry { name, symbol });
        self.next_slot = (self.next_slot + 1) % SYMBOL_CACHE_SIZE;
    }
}

thread_local! {
    static SYMBOL_CACHE: RefCell<SymbolCache> = const { RefCell::new(SymbolCache::new()) };
}

struct SymbolTable {
    inner: RwLock<SymbolTableInner>,
}

#[derive(Default)]
struct SymbolTableInner {
    by_name: HashMap<&'static str, u32>,
    by_id: Vec<SymbolData>,
}

struct SymbolData {
    name: &'static str,
    metadata: SymbolMetadata,
}

impl SymbolTable {
    fn intern(&self, name: &str) -> Symbol {
        if let Some(symbol) = SYMBOL_CACHE.with(|cache| cache.borrow().get(name)) {
            return symbol;
        }

        if let Some((id, interned_name)) = {
            let inner = self.inner.read().unwrap();
            inner
                .by_name
                .get(name)
                .copied()
                .map(|id| (id, inner.by_id[id as usize].name))
        } {
            let symbol = Symbol(id);
            SYMBOL_CACHE.with(|cache| cache.borrow_mut().insert(interned_name, symbol));
            return symbol;
        }

        let mut inner = self.inner.write().unwrap();
        if let Some(id) = inner.by_name.get(name).copied() {
            let symbol = Symbol(id);
            let name = inner.by_id[id as usize].name;
            SYMBOL_CACHE.with(|cache| cache.borrow_mut().insert(name, symbol));
            return symbol;
        }

        let id = inner.by_id.len() as u32;
        let name: &'static str = Box::leak(Box::<str>::from(name));
        inner.by_name.insert(name, id);
        inner.by_id.push(SymbolData {
            name,
            metadata: SymbolMetadata {
                byte_len: name.len(),
                char_len: if name.is_ascii() {
                    name.len()
                } else {
                    name.chars().count()
                },
                is_ascii: name.is_ascii(),
            },
        });
        let symbol = Symbol(id);
        SYMBOL_CACHE.with(|cache| cache.borrow_mut().insert(name, symbol));
        symbol
    }

    fn name(&self, symbol: Symbol) -> Option<&'static str> {
        self.inner
            .read()
            .unwrap()
            .by_id
            .get(symbol.id() as usize)
            .map(|data| data.name)
    }

    fn metadata(&self, symbol: Symbol) -> Option<SymbolMetadata> {
        self.inner
            .read()
            .unwrap()
            .by_id
            .get(symbol.id() as usize)
            .map(|data| data.metadata)
    }
}

fn symbol_table() -> &'static SymbolTable {
    static SYMBOLS: OnceLock<SymbolTable> = OnceLock::new();
    SYMBOLS.get_or_init(|| SymbolTable {
        inner: RwLock::new(SymbolTableInner::default()),
    })
}
