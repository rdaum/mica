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

use mica_var::{Identity, Symbol, Value};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u16)]
pub enum MessageType {
    Hello = 0x0001,
    HelloAck = 0x0002,
    RequestAccepted = 0x0003,
    RequestRejected = 0x0004,
    OpenEndpoint = 0x0100,
    CloseEndpoint = 0x0101,
    ResolveIdentity = 0x0102,
    IdentityResolved = 0x0103,
    SubmitSource = 0x0200,
    SubmitInput = 0x0201,
    OutputReady = 0x0300,
    DrainOutput = 0x0301,
    OutputBatch = 0x0302,
    EndpointClosed = 0x0303,
    TaskCompleted = 0x0400,
    TaskFailed = 0x0401,
}

impl MessageType {
    pub const fn raw(self) -> u16 {
        self as u16
    }

    pub const fn from_raw(raw: u16) -> Option<Self> {
        match raw {
            0x0001 => Some(Self::Hello),
            0x0002 => Some(Self::HelloAck),
            0x0003 => Some(Self::RequestAccepted),
            0x0004 => Some(Self::RequestRejected),
            0x0100 => Some(Self::OpenEndpoint),
            0x0101 => Some(Self::CloseEndpoint),
            0x0102 => Some(Self::ResolveIdentity),
            0x0103 => Some(Self::IdentityResolved),
            0x0200 => Some(Self::SubmitSource),
            0x0201 => Some(Self::SubmitInput),
            0x0300 => Some(Self::OutputReady),
            0x0301 => Some(Self::DrainOutput),
            0x0302 => Some(Self::OutputBatch),
            0x0303 => Some(Self::EndpointClosed),
            0x0400 => Some(Self::TaskCompleted),
            0x0401 => Some(Self::TaskFailed),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum HostMessage {
    Hello {
        protocol_version: u16,
        min_protocol_version: u16,
        feature_bits: u64,
        host_name: String,
    },
    HelloAck {
        protocol_version: u16,
        feature_bits: u64,
    },
    RequestAccepted {
        request_id: u64,
        task_id: Option<u64>,
    },
    RequestRejected {
        request_id: u64,
        code: Symbol,
        message: String,
    },
    OpenEndpoint {
        request_id: u64,
        endpoint: Identity,
        actor: Option<Identity>,
        protocol: String,
        grant_token: Option<String>,
    },
    CloseEndpoint {
        request_id: u64,
        endpoint: Identity,
    },
    ResolveIdentity {
        request_id: u64,
        name: Symbol,
    },
    IdentityResolved {
        request_id: u64,
        name: Symbol,
        identity: Identity,
    },
    SubmitSource {
        request_id: u64,
        endpoint: Identity,
        actor: Identity,
        source: String,
    },
    SubmitInput {
        request_id: u64,
        endpoint: Identity,
        value: Value,
    },
    OutputReady {
        endpoint: Identity,
        buffered: u32,
    },
    DrainOutput {
        request_id: u64,
        endpoint: Identity,
        limit: u32,
    },
    OutputBatch {
        endpoint: Identity,
        values: Vec<Value>,
    },
    EndpointClosed {
        endpoint: Identity,
        reason: String,
    },
    TaskCompleted {
        task_id: u64,
        value: Value,
    },
    TaskFailed {
        task_id: u64,
        error: Value,
    },
}

impl HostMessage {
    pub const fn message_type(&self) -> MessageType {
        match self {
            Self::Hello { .. } => MessageType::Hello,
            Self::HelloAck { .. } => MessageType::HelloAck,
            Self::RequestAccepted { .. } => MessageType::RequestAccepted,
            Self::RequestRejected { .. } => MessageType::RequestRejected,
            Self::OpenEndpoint { .. } => MessageType::OpenEndpoint,
            Self::CloseEndpoint { .. } => MessageType::CloseEndpoint,
            Self::ResolveIdentity { .. } => MessageType::ResolveIdentity,
            Self::IdentityResolved { .. } => MessageType::IdentityResolved,
            Self::SubmitSource { .. } => MessageType::SubmitSource,
            Self::SubmitInput { .. } => MessageType::SubmitInput,
            Self::OutputReady { .. } => MessageType::OutputReady,
            Self::DrainOutput { .. } => MessageType::DrainOutput,
            Self::OutputBatch { .. } => MessageType::OutputBatch,
            Self::EndpointClosed { .. } => MessageType::EndpointClosed,
            Self::TaskCompleted { .. } => MessageType::TaskCompleted,
            Self::TaskFailed { .. } => MessageType::TaskFailed,
        }
    }
}
