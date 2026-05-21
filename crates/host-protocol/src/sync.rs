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

use crate::HostProtocolError;

pub const SYNC_ENVELOPE_MAGIC: [u8; 4] = *b"MSY1";
pub const SYNC_ENVELOPE_HEADER_LEN: usize = 56;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum SyncMessageKind {
    HaveView = 0x01,
    NeedView = 0x02,
    ViewSnapshot = 0x03,
    ViewDelta = 0x04,
}

impl SyncMessageKind {
    pub const fn raw(self) -> u8 {
        self as u8
    }

    pub const fn from_raw(raw: u8) -> Option<Self> {
        match raw {
            0x01 => Some(Self::HaveView),
            0x02 => Some(Self::NeedView),
            0x03 => Some(Self::ViewSnapshot),
            0x04 => Some(Self::ViewDelta),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncEnvelope {
    pub kind: SyncMessageKind,
    pub session_id: u64,
    pub view_id: u64,
    pub client_revision: u64,
    pub client_signature: u64,
    pub server_revision: u64,
    pub server_signature: u64,
    pub payload: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SyncEnvelopeRef<'a> {
    pub kind: SyncMessageKind,
    pub session_id: u64,
    pub view_id: u64,
    pub client_revision: u64,
    pub client_signature: u64,
    pub server_revision: u64,
    pub server_signature: u64,
    pub payload: &'a [u8],
}

impl SyncEnvelope {
    pub fn as_ref(&self) -> SyncEnvelopeRef<'_> {
        SyncEnvelopeRef {
            kind: self.kind,
            session_id: self.session_id,
            view_id: self.view_id,
            client_revision: self.client_revision,
            client_signature: self.client_signature,
            server_revision: self.server_revision,
            server_signature: self.server_signature,
            payload: &self.payload,
        }
    }
}

pub fn encode_sync_envelope(envelope: SyncEnvelopeRef<'_>, out: &mut Vec<u8>) {
    out.extend_from_slice(&SYNC_ENVELOPE_MAGIC);
    out.push(envelope.kind.raw());
    out.push(0);
    out.extend_from_slice(&0u16.to_le_bytes());
    out.extend_from_slice(&envelope.session_id.to_le_bytes());
    out.extend_from_slice(&envelope.view_id.to_le_bytes());
    out.extend_from_slice(&envelope.client_revision.to_le_bytes());
    out.extend_from_slice(&envelope.client_signature.to_le_bytes());
    out.extend_from_slice(&envelope.server_revision.to_le_bytes());
    out.extend_from_slice(&envelope.server_signature.to_le_bytes());
    out.extend_from_slice(envelope.payload);
}

pub fn encoded_sync_envelope(envelope: SyncEnvelopeRef<'_>) -> Vec<u8> {
    let mut out = Vec::with_capacity(SYNC_ENVELOPE_HEADER_LEN + envelope.payload.len());
    encode_sync_envelope(envelope, &mut out);
    out
}

pub fn decode_sync_envelope(bytes: &[u8]) -> Result<SyncEnvelope, HostProtocolError> {
    if bytes.len() < SYNC_ENVELOPE_HEADER_LEN {
        return Err(HostProtocolError::UnexpectedEnd {
            needed: SYNC_ENVELOPE_HEADER_LEN,
            offset: 0,
            len: bytes.len(),
        });
    }
    let magic: [u8; 4] = bytes[..4].try_into().unwrap();
    if magic != SYNC_ENVELOPE_MAGIC {
        return Err(HostProtocolError::InvalidSyncEnvelopeMagic(magic));
    }
    let kind = SyncMessageKind::from_raw(bytes[4])
        .ok_or(HostProtocolError::UnknownSyncMessageKind(bytes[4]))?;
    if bytes[5] != 0 {
        return Err(HostProtocolError::UnsupportedSyncEnvelopeFlags(bytes[5]));
    }
    let reserved = u16::from_le_bytes(bytes[6..8].try_into().unwrap());
    if reserved != 0 {
        return Err(HostProtocolError::UnsupportedSyncEnvelopeReserved(reserved));
    }
    Ok(SyncEnvelope {
        kind,
        session_id: read_u64(bytes, 8),
        view_id: read_u64(bytes, 16),
        client_revision: read_u64(bytes, 24),
        client_signature: read_u64(bytes, 32),
        server_revision: read_u64(bytes, 40),
        server_signature: read_u64(bytes, 48),
        payload: bytes[SYNC_ENVELOPE_HEADER_LEN..].to_vec(),
    })
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn envelope(payload: &[u8]) -> SyncEnvelope {
        SyncEnvelope {
            kind: SyncMessageKind::NeedView,
            session_id: 7,
            view_id: 11,
            client_revision: 13,
            client_signature: 17,
            server_revision: 19,
            server_signature: 23,
            payload: payload.to_vec(),
        }
    }

    #[test]
    fn sync_envelope_matches_golden_bytes() {
        let encoded = encoded_sync_envelope(envelope(b"dom").as_ref());

        assert_eq!(
            encoded,
            vec![
                b'M', b'S', b'Y', b'1', 2, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 11, 0, 0, 0, 0, 0, 0,
                0, 13, 0, 0, 0, 0, 0, 0, 0, 17, 0, 0, 0, 0, 0, 0, 0, 19, 0, 0, 0, 0, 0, 0, 0, 23,
                0, 0, 0, 0, 0, 0, 0, b'd', b'o', b'm',
            ]
        );
        assert_eq!(decode_sync_envelope(&encoded).unwrap(), envelope(b"dom"));
    }

    #[test]
    fn sync_envelope_round_trips_empty_payload() {
        let encoded = encoded_sync_envelope(envelope(b"").as_ref());

        assert_eq!(encoded.len(), SYNC_ENVELOPE_HEADER_LEN);
        assert_eq!(decode_sync_envelope(&encoded).unwrap(), envelope(b""));
    }

    #[test]
    fn sync_envelope_rejects_bad_headers() {
        assert!(matches!(
            decode_sync_envelope(&[0; 4]),
            Err(HostProtocolError::UnexpectedEnd { .. })
        ));

        let mut encoded = encoded_sync_envelope(envelope(b"dom").as_ref());
        encoded[0] = b'X';
        assert_eq!(
            decode_sync_envelope(&encoded),
            Err(HostProtocolError::InvalidSyncEnvelopeMagic(*b"XSY1"))
        );

        let mut encoded = encoded_sync_envelope(envelope(b"dom").as_ref());
        encoded[4] = 0xff;
        assert_eq!(
            decode_sync_envelope(&encoded),
            Err(HostProtocolError::UnknownSyncMessageKind(0xff))
        );

        let mut encoded = encoded_sync_envelope(envelope(b"dom").as_ref());
        encoded[5] = 1;
        assert_eq!(
            decode_sync_envelope(&encoded),
            Err(HostProtocolError::UnsupportedSyncEnvelopeFlags(1))
        );

        let mut encoded = encoded_sync_envelope(envelope(b"dom").as_ref());
        encoded[6..8].copy_from_slice(&1u16.to_le_bytes());
        assert_eq!(
            decode_sync_envelope(&encoded),
            Err(HostProtocolError::UnsupportedSyncEnvelopeReserved(1))
        );
    }
}
