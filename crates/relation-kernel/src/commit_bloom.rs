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

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

const BLOOM_BITS: usize = 2048;
const BLOOM_BYTES: usize = BLOOM_BITS / 8;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommitBloom {
    bits: Box<[u8; BLOOM_BYTES]>,
}

impl CommitBloom {
    pub(crate) fn new() -> Self {
        Self {
            bits: Box::new([0; BLOOM_BYTES]),
        }
    }

    pub(crate) fn insert<K: Hash>(&mut self, key: &K) {
        let (h1, h2) = double_hash(key);
        self.set_bit(h1 % BLOOM_BITS);
        self.set_bit(h2 % BLOOM_BITS);
    }

    #[cfg(test)]
    pub(crate) fn might_intersect(&self, other: &Self) -> bool {
        self.bits
            .iter()
            .zip(other.bits.iter())
            .any(|(left, right)| left & right != 0)
    }

    fn set_bit(&mut self, bit: usize) {
        self.bits[bit / 8] |= 1 << (bit % 8);
    }
}

impl Default for CommitBloom {
    fn default() -> Self {
        Self::new()
    }
}

fn double_hash<K: Hash>(key: &K) -> (usize, usize) {
    let mut h1 = DefaultHasher::new();
    key.hash(&mut h1);
    let v1 = h1.finish() as usize;

    let mut h2 = DefaultHasher::new();
    key.hash(&mut h2);
    v1.hash(&mut h2);
    let v2 = h2.finish() as usize;

    (v1, v2)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bloom_detects_possible_intersection() {
        let mut left = CommitBloom::new();
        let mut right = CommitBloom::new();
        left.insert(&("relation", 1_u64));
        right.insert(&("relation", 1_u64));

        assert!(left.might_intersect(&right));
    }

    #[test]
    fn empty_bloom_is_disjoint_from_nonempty_bloom() {
        let mut left = CommitBloom::new();
        let right = CommitBloom::new();
        left.insert(&("relation", 1_u64));

        assert!(!left.might_intersect(&right));
    }
}
