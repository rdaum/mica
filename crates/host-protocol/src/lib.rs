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

mod dom_sync;
mod error;
mod frame;
mod message;
mod payload;
mod sync;

pub use dom_sync::{
    DOM_EVENT_PAYLOAD_TYPE, DOM_PATCH_PAYLOAD_TYPE, DomEventPayload, DomNode, DomPatch,
    SUPPORTED_DOM_ATTRIBUTES, SUPPORTED_DOM_TAGS, decode_dom_event_payload, diff_dom_nodes,
    dom_event_payload_json, dom_patch_payload_json, snapshot_payload_json, sync_payload_signature,
};
pub use error::HostProtocolError;
pub use frame::{
    EncodedFrameSegments, FrameDecoder, FrameRef, decode_frame, encode_frame,
    encode_frame_segments, encoded_frame,
};
pub use message::{HostMessage, MessageType};
pub use sync::{
    SYNC_ENVELOPE_HEADER_LEN, SYNC_ENVELOPE_MAGIC, SYNC_HAVE_VIEW_SELECTOR,
    SYNC_NEED_VIEW_SELECTOR, SYNC_ROLE_CLIENT_REVISION, SYNC_ROLE_CLIENT_SIGNATURE,
    SYNC_ROLE_PAYLOAD, SYNC_ROLE_SERVER_REVISION, SYNC_ROLE_SERVER_SIGNATURE, SYNC_ROLE_SESSION,
    SYNC_ROLE_VIEW, SYNC_VALUE_HAVE_VIEW, SYNC_VALUE_NEED_VIEW, SYNC_VALUE_VIEW_DELTA,
    SYNC_VALUE_VIEW_SNAPSHOT, SyncEnvelope, SyncEnvelopeRef, SyncMessageKind, decode_sync_envelope,
    encode_sync_envelope, encoded_sync_envelope, sync_emission_value, sync_envelope_from_value,
    sync_invocation_roles, sync_invocation_selector, sync_u64_from_value, sync_u64_value,
    sync_value_kind_symbol,
};

pub const MAGIC: [u8; 4] = *b"MHP1";
pub const PROTOCOL_VERSION: u16 = 1;
pub const DEFAULT_MAX_FRAME_LEN: usize = 16 * 1024 * 1024;
