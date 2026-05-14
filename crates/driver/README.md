# mica-driver

`mica-driver` is the first event-loop layer above `mica-runtime`.

The runtime task manager is synchronous: each call runs a task until
completion, abort, commit, or suspension. This crate owns the next layer of
behaviour and schedules task-manager work as compio tasks:

- source, invocation, explicit resume, and input resume work launched with
  `compio::runtime::spawn`;
- compio timer tasks for `suspend(seconds)` wakeups;
- endpoint input waiters for `read()`;
- effect and task lifecycle events.

The driver intentionally keeps sockets and protocols out of this crate. A
daemon or listener can translate network input into `DriverHandle::input(...)`
and route `DriverEvent::Effect` values back to endpoints, actors, or other
sinks.
