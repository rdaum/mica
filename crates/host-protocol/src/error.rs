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

use mica_var::ValueCodecError;
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HostProtocolError {
    FrameTooLarge(usize),
    InvalidMagic([u8; 4]),
    InvalidFrameLength {
        declared: usize,
        actual: usize,
    },
    UnexpectedEnd {
        needed: usize,
        offset: usize,
        len: usize,
    },
    TrailingFrameBytes(usize),
    TrailingPayload(usize),
    UnsupportedFlags(u16),
    InvalidUtf8(String),
    InvalidIdentity(u64),
    InvalidOptionTag(u8),
    SymbolNameUnavailable(u32),
    UnknownMessageType(u16),
    Value(ValueCodecError),
    OffsetOverflow,
}

impl fmt::Display for HostProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FrameTooLarge(len) => write!(f, "host frame length {len} exceeds u32"),
            Self::InvalidMagic(magic) => write!(f, "invalid host frame magic {magic:?}"),
            Self::InvalidFrameLength { declared, actual } => {
                write!(
                    f,
                    "invalid host frame length: declared {declared}, actual {actual}"
                )
            }
            Self::UnexpectedEnd {
                needed,
                offset,
                len,
            } => write!(
                f,
                "host frame ended early: need {needed} bytes at offset {offset}, len {len}"
            ),
            Self::TrailingFrameBytes(count) => {
                write!(f, "trailing bytes after host frame: {count}")
            }
            Self::TrailingPayload(count) => {
                write!(f, "trailing bytes in host message payload: {count}")
            }
            Self::UnsupportedFlags(flags) => {
                write!(f, "unsupported host frame flags 0x{flags:04x}")
            }
            Self::InvalidUtf8(error) => write!(f, "invalid utf-8: {error}"),
            Self::InvalidIdentity(raw) => write!(f, "identity {raw} is out of range"),
            Self::InvalidOptionTag(tag) => write!(f, "invalid option tag {tag}"),
            Self::SymbolNameUnavailable(symbol_id) => {
                write!(f, "symbol id {symbol_id} has no name")
            }
            Self::UnknownMessageType(message_type) => {
                write!(f, "unknown host message type 0x{message_type:04x}")
            }
            Self::Value(error) => write!(f, "invalid host value payload: {error}"),
            Self::OffsetOverflow => f.write_str("host frame offset overflow"),
        }
    }
}

impl std::error::Error for HostProtocolError {}

impl From<ValueCodecError> for HostProtocolError {
    fn from(error: ValueCodecError) -> Self {
        Self::Value(error)
    }
}
