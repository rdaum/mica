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

//! Language-neutral host protocol framing for Mica.

mod error;
mod frame;
mod message;
mod payload;

pub use error::HostProtocolError;
pub use frame::{
    EncodedFrameSegments, FrameDecoder, FrameRef, decode_frame, encode_frame,
    encode_frame_segments, encoded_frame,
};
pub use message::{HostMessage, MessageType};

pub const MAGIC: [u8; 4] = *b"MHP1";
pub const PROTOCOL_VERSION: u16 = 1;
pub const DEFAULT_MAX_FRAME_LEN: usize = 16 * 1024 * 1024;
