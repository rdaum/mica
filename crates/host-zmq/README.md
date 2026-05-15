# mica-host-zmq

`mica-host-zmq` is the ZeroMQ carrier for Mica Host Protocol frames. It keeps
ZeroMQ socket handling separate from the daemon/runtime so that MHP remains the
semantic protocol and ZeroMQ remains the transport.

The crate currently provides:

- explicit socket construction for ROUTER/DEALER/PUB/SUB-style endpoints;
- compio readiness waiting around ZeroMQ signalling file descriptors;
- non-blocking receive/send loops using `ZMQ_EVENTS` and `DONTWAIT`;
- routed MHP helpers for ROUTER/DEALER request handling.

IPC endpoints do not use CURVE/ZAP. They are admitted by local operating-system
socket permissions. TCP endpoints should add CURVE/ZAP configuration before
being treated as remotely safe.

## Licence

Mica is licensed under the GNU Affero General Public License v3.0. See the
repository root `LICENSE`.
