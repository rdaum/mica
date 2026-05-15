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

//! ZeroMQ carrier for Mica Host Protocol frames.

use compio::runtime::fd::PollFd;
use mica_host_protocol::{HostMessage, HostProtocolError, decode_frame, encoded_frame};
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PeerId(Vec<u8>);

impl PeerId {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RoutedFrame {
    pub peer: PeerId,
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RoutedMessage {
    pub peer: PeerId,
    pub message: HostMessage,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ZmqSocketOptions {
    pub linger_millis: i32,
    pub send_high_water: i32,
    pub recv_high_water: i32,
}

impl Default for ZmqSocketOptions {
    fn default() -> Self {
        Self {
            linger_millis: 0,
            send_high_water: 1024,
            recv_high_water: 1024,
        }
    }
}

pub struct ZmqHostSocket {
    socket: PollFd<zmq::Socket>,
}

#[derive(Debug)]
pub enum ZmqTransportError {
    Zmq(zmq::Error),
    Readiness(std::io::Error),
    Protocol(HostProtocolError),
    EmptyMessage,
    MissingRoute,
}

impl fmt::Display for ZmqTransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Zmq(error) => write!(f, "ZeroMQ error: {error}"),
            Self::Readiness(error) => write!(f, "ZeroMQ readiness error: {error}"),
            Self::Protocol(error) => write!(f, "host protocol error: {error}"),
            Self::EmptyMessage => f.write_str("ZeroMQ message had no frames"),
            Self::MissingRoute => f.write_str("ZeroMQ routed message had no route frame"),
        }
    }
}

impl std::error::Error for ZmqTransportError {}

impl From<zmq::Error> for ZmqTransportError {
    fn from(error: zmq::Error) -> Self {
        Self::Zmq(error)
    }
}

impl From<HostProtocolError> for ZmqTransportError {
    fn from(error: HostProtocolError) -> Self {
        Self::Protocol(error)
    }
}

impl ZmqHostSocket {
    pub fn bind(
        context: &zmq::Context,
        socket_type: zmq::SocketType,
        endpoint: &str,
        options: ZmqSocketOptions,
    ) -> Result<Self, ZmqTransportError> {
        let socket = context.socket(socket_type)?;
        configure_socket(&socket, options)?;
        socket.bind(endpoint)?;
        Self::from_socket(socket)
    }

    pub fn connect(
        context: &zmq::Context,
        socket_type: zmq::SocketType,
        endpoint: &str,
        options: ZmqSocketOptions,
    ) -> Result<Self, ZmqTransportError> {
        let socket = context.socket(socket_type)?;
        configure_socket(&socket, options)?;
        socket.connect(endpoint)?;
        Self::from_socket(socket)
    }

    pub fn from_socket(socket: zmq::Socket) -> Result<Self, ZmqTransportError> {
        Ok(Self {
            socket: PollFd::new(socket).map_err(ZmqTransportError::Readiness)?,
        })
    }

    pub async fn recv_multipart(&self) -> Result<Vec<Vec<u8>>, ZmqTransportError> {
        loop {
            if self.socket.get_events()?.contains(zmq::POLLIN) {
                match self.socket.recv_multipart(zmq::DONTWAIT) {
                    Ok(parts) if parts.is_empty() => return Err(ZmqTransportError::EmptyMessage),
                    Ok(parts) => return Ok(parts),
                    Err(zmq::Error::EAGAIN) => {}
                    Err(error) => return Err(error.into()),
                }
            }
            self.socket
                .read_ready()
                .await
                .map_err(ZmqTransportError::Readiness)?;
        }
    }

    pub async fn send_multipart(&self, parts: &[&[u8]]) -> Result<(), ZmqTransportError> {
        loop {
            if self.socket.get_events()?.contains(zmq::POLLOUT) {
                match self
                    .socket
                    .send_multipart(parts.iter().copied(), zmq::DONTWAIT)
                {
                    Ok(()) => return Ok(()),
                    Err(zmq::Error::EAGAIN) => {}
                    Err(error) => return Err(error.into()),
                }
            }
            self.socket
                .write_ready()
                .await
                .map_err(ZmqTransportError::Readiness)?;
        }
    }

    pub async fn recv_frame(&self) -> Result<Vec<u8>, ZmqTransportError> {
        let parts = self.recv_multipart().await?;
        parts
            .into_iter()
            .last()
            .ok_or(ZmqTransportError::EmptyMessage)
    }

    pub async fn send_frame(&self, frame: &[u8]) -> Result<(), ZmqTransportError> {
        self.send_multipart(&[frame]).await
    }

    pub async fn recv_message(&self) -> Result<HostMessage, ZmqTransportError> {
        let frame = self.recv_frame().await?;
        decode_frame(&frame).map_err(Into::into)
    }

    pub fn try_recv_message(&self) -> Result<Option<HostMessage>, ZmqTransportError> {
        if !self.socket.get_events()?.contains(zmq::POLLIN) {
            return Ok(None);
        }
        match self.socket.recv_multipart(zmq::DONTWAIT) {
            Ok(parts) if parts.is_empty() => Err(ZmqTransportError::EmptyMessage),
            Ok(parts) => {
                let frame = parts
                    .into_iter()
                    .last()
                    .ok_or(ZmqTransportError::EmptyMessage)?;
                decode_frame(&frame).map(Some).map_err(Into::into)
            }
            Err(zmq::Error::EAGAIN) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    pub async fn send_message(&self, message: &HostMessage) -> Result<(), ZmqTransportError> {
        let frame = encoded_frame(message)?;
        self.send_frame(&frame).await
    }

    pub async fn recv_routed_frame(&self) -> Result<RoutedFrame, ZmqTransportError> {
        let mut parts = self.recv_multipart().await?;
        let payload = parts.pop().ok_or(ZmqTransportError::EmptyMessage)?;
        let peer = parts
            .into_iter()
            .next()
            .map(PeerId::new)
            .ok_or(ZmqTransportError::MissingRoute)?;
        Ok(RoutedFrame { peer, payload })
    }

    pub async fn send_routed_frame(
        &self,
        peer: &PeerId,
        frame: &[u8],
    ) -> Result<(), ZmqTransportError> {
        self.send_multipart(&[peer.as_bytes(), frame]).await
    }

    pub async fn recv_routed_message(&self) -> Result<RoutedMessage, ZmqTransportError> {
        let frame = self.recv_routed_frame().await?;
        let message = decode_frame(&frame.payload)?;
        Ok(RoutedMessage {
            peer: frame.peer,
            message,
        })
    }

    pub async fn send_routed_message(
        &self,
        peer: &PeerId,
        message: &HostMessage,
    ) -> Result<(), ZmqTransportError> {
        let frame = encoded_frame(message)?;
        self.send_routed_frame(peer, &frame).await
    }
}

fn configure_socket(
    socket: &zmq::Socket,
    options: ZmqSocketOptions,
) -> Result<(), ZmqTransportError> {
    socket.set_linger(options.linger_millis)?;
    socket.set_sndhwm(options.send_high_water)?;
    socket.set_rcvhwm(options.recv_high_water)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use mica_host_protocol::PROTOCOL_VERSION;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_ENDPOINT: AtomicU64 = AtomicU64::new(1);

    #[test]
    fn ipc_router_dealer_round_trips_host_messages() {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let endpoint = ipc_endpoint();
            let _cleanup = IpcCleanup::new(endpoint.path.clone());
            let context = zmq::Context::new();
            let router = ZmqHostSocket::bind(
                &context,
                zmq::ROUTER,
                &endpoint.uri,
                ZmqSocketOptions::default(),
            )
            .unwrap();
            let dealer = ZmqHostSocket::connect(
                &context,
                zmq::DEALER,
                &endpoint.uri,
                ZmqSocketOptions::default(),
            )
            .unwrap();
            let hello = HostMessage::Hello {
                protocol_version: PROTOCOL_VERSION,
                min_protocol_version: PROTOCOL_VERSION,
                feature_bits: 0,
                host_name: "test".to_owned(),
            };

            dealer.send_message(&hello).await.unwrap();
            let routed = router.recv_routed_message().await.unwrap();
            assert_eq!(routed.message, hello);

            let ack = HostMessage::HelloAck {
                protocol_version: PROTOCOL_VERSION,
                feature_bits: 1,
            };
            assert_eq!(dealer.try_recv_message().unwrap(), None);
            router
                .send_routed_message(&routed.peer, &ack)
                .await
                .unwrap();
            assert_eq!(dealer.recv_message().await.unwrap(), ack);
            assert_eq!(dealer.try_recv_message().unwrap(), None);
        });
    }

    struct IpcEndpoint {
        uri: String,
        path: PathBuf,
    }

    struct IpcCleanup {
        path: PathBuf,
    }

    impl IpcCleanup {
        fn new(path: PathBuf) -> Self {
            let _ = std::fs::remove_file(&path);
            Self { path }
        }
    }

    impl Drop for IpcCleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
        }
    }

    fn ipc_endpoint() -> IpcEndpoint {
        let index = NEXT_ENDPOINT.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("mica-host-zmq-{}-{index}.sock", std::process::id()));
        IpcEndpoint {
            uri: format!("ipc://{}", path.display()),
            path,
        }
    }
}
