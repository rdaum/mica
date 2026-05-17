# Runtime Overview

The Mica runtime executes compiled tasks against a live relation store. The
runtime is responsible for making the language feel direct while preserving the
transactional rules that keep a shared world coherent.

The core runtime concepts are:

- a relation kernel that stores facts, relation metadata, and rules;
- tasks that run bytecode over a transaction;
- a task manager that owns task state and suspended continuations;
- a driver that resumes tasks after timers, input, child-spawn completion, or
  mailbox readiness;
- hosts that translate protocol traffic into task submissions, input, and
  effects.

The runtime is transactional by default. Code can feel direct and live while
still committing state changes, effects, and mailbox sends at explicit
boundaries.

A typical flow looks like this:

1. A host or REPL submits source or a verb invocation.
2. The compiler produces bytecode for a task.
3. The task runs against a transaction and authority context.
4. If the task commits, relation writes become visible and effects are routed.
5. If the task suspends, the driver records why and resumes it later.

The runtime does not require all state to be durable. Endpoint state,
capabilities, and mailboxes are runtime concerns. Durable state stores the
world's facts, rules, definitions, and policy.
