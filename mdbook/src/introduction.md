# Mica Reference

This is the initial reference for Mica's language, syntax, and runtime model.
It is not trying to be a tutorial book yet, but it should still be readable by
someone meeting the system for the first time. The goal is to explain the
surface area that already exists: what the code looks like, what the runtime
does with it, and what mental model makes the pieces fit together.

Mica is a programming language and runtime for shared programmable memory. A
Mica world is a live environment where durable identities, facts, rules, verbs,
authority, effects, and tasks can all change while the system is running. Code
does not sit outside the data as a separate application layer. Behaviour is
installed into the world alongside the facts it reads and writes.

The reference is organized in two parts:

- the language surface: values, expressions, relations, rules, verbs, dispatch,
  authority, and effects;
- the runtime surface: tasks, transactions, suspension, mailboxes, filein, and
  fileout.

Many chapters are still incomplete. When adding detail, prefer concrete syntax,
current runtime behaviour, and runnable examples over abstract promises.
