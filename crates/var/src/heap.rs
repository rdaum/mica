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

use crate::value::{
    ErrorValue, TAG_BYTES, TAG_ERROR, TAG_LIST, TAG_MAP, TAG_RANGE, TAG_STRING, Value,
};

pub(crate) enum HeapValue {
    String(Box<str>),
    Bytes(Box<[u8]>),
    List(Box<[Value]>),
    Map(Box<[(Value, Value)]>),
    Range { start: Value, end: Option<Value> },
    Error(ErrorValue),
}

impl HeapValue {
    pub(crate) fn tag(&self) -> u8 {
        match self {
            Self::String(_) => TAG_STRING,
            Self::Bytes(_) => TAG_BYTES,
            Self::List(_) => TAG_LIST,
            Self::Map(_) => TAG_MAP,
            Self::Range { .. } => TAG_RANGE,
            Self::Error(_) => TAG_ERROR,
        }
    }
}
