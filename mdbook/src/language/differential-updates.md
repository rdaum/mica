# Changing Worlds and Live Updates

A Mica world is live: facts change while the system runs, and rules re-derive their consequences on
every commit. Most of the time, you care about a small part of that world — Alice's view of a room,
the messages in one chat, the status of a single tool call — and you want to react when _that_ part
changes. The changes themselves are usually small: a few facts about a few objects (identities such
as `#alice` or `#lamp`), while the rest of the world stays the same. You do not want to poll the
whole world on a timer, and you do not want to re-check everything yourself.

Mica gives you two things for this:

- **`subscribe_changes`**, a runtime API that delivers exactly the facts that became true or stopped
  being true after each commit that touched them; and
- **differential evaluation**, the engine technique that makes those deliveries cheap enough to use
  on a large world by computing changes from changes rather than from scratch.

This chapter is mostly about the first. The engine technique has a name — _differential Datalog_ —
and gets a section of its own, because knowing the name helps you read the rest of the docs and the
runtime source. But the user-facing idea is simple: tell Mica what you care about, and Mica tells
you when it changed.

## Listening For Changes

`subscribe_changes` sends settled changes to a mailbox after commit. It works for both stored
relations (facts you asserted) and derived relations (facts your rules produced). It is the primary
way to react to a live world from task code.

This example subscribes to the `VisibleTo` results for Alice:

```mica
let [receiver, sender] = mailbox()
let subscription = subscribe_changes(
  sender,
  :relation,
  :VisibleTo,
  [#alice, nothing],
  :snapshot
)

commit()

let ready = mailbox_recv([receiver])
```

The binding list has one entry for each relation column. A value fixes that column; `nothing` leaves
it open. Here the subscription covers every tuple whose first value is `#alice`.

The subject controls what is observed:

| Subject      | Meaning                                                                 |
| ------------ | ----------------------------------------------------------------------- |
| `:facts`     | changes to asserted facts in a stored relation                          |
| `:relation`  | changes to the relation's complete visible result, including rule facts |
| `:catalogue` | relation and rule catalogue changes; reserved for root authority        |

Use `:facts` when the application cares about the stored statements themselves. Use `:relation` when
it cares whether a stored or derived answer became true or false.

The initial-delivery argument is either:

- `:snapshot`, which first delivers the current matching rows and then later changes; or
- `:changes`, which starts with changes committed after registration.

With `:snapshot`, the first message has the same shape as a change message but reports the current
matching rows as assertions and has empty retractions:

```mica
{
  :kind -> :changes,
  :subscription -> subscription,
  :cursor -> 41,
  :subject -> :snapshot,
  :assertions -> [[#alice, #lamp]],
  :retractions -> []
}
```

Subsequent change messages contain a commit cursor plus `assertions` and `retractions`. For example,
Alice losing sight of the lamp can produce a message shaped like this:

```mica
{
  :kind -> :changes,
  :subscription -> subscription,
  :cursor -> 42,
  :subject -> :relation,
  :assertions -> [],
  :retractions -> [[#alice, #lamp]]
}
```

Catalogue subscriptions deliver their initial snapshot in a different shape (`:kind -> :snapshot`,
`:subject -> :catalogue`, with an `:entries` list). That is a runtime inconsistency we expect to
clean up; if you write code against catalogue snapshots, check the actual message shape rather than
assuming it matches the relation form.

Registration and cancellation take effect at commit boundaries:

```mica
cancel_subscription(subscription)
commit()
```

Subscriptions obey the reader's authority. If the actor or principal loses permission to read the
relation, the runtime sends `:revoked` rather than leaking later changes. Queues are bounded; if a
consumer falls too far behind, it receives `:resynchronize` and must read a fresh snapshot instead
of assuming it still has an unbroken change history. A changes-only subscription may also resume
from a retained commit cursor.

Mailboxes and their capabilities are ephemeral. Store durable progress as relation facts, not as a
subscription or mailbox value. See [Task Control](../runtime/task-control.md) for the mailbox model.

### A Worked Example: Reacting When Something Becomes Visible

The snippets above show the call shapes in isolation. To see how `subscribe_changes` is used in a
task, consider a small watcher that tells Alice whenever an object enters or leaves her sight.

The watcher will subscribe to a derived relation that pairs each visible object with its current
name. Carrying the name in the derived row is deliberate: a retraction message contains the row as
it was _before_ it disappeared, so the name is preserved in the message even if the object's `Name`
fact was removed in the same commit. If the verb instead looked the name up fresh on retraction, it
might find nothing.

This example assumes that an object's `Name` does not change while it remains visible. A rename
while visible would show up as a retraction/assertion pair on `VisibleNamedObject` (the old name row
leaving, the new name row arriving) and the watcher below would announce it as "no longer see … see
…" rather than as a rename. A production watcher that cares about renames should subscribe to the
stable `VisibleTo` identity instead and render names from a separate, snapshot-derived read, or
detect adjacent retraction/assertion pairs with the same `object` value and collapse them.

```mica
VisibleTo(actor, object) :-
  Actor(actor),
  Object(object),
  LocatedIn(actor, place),
  LocatedIn(object, place)

VisibleNamedObject(actor, object, name) :-
  VisibleTo(actor, object),
  Name(object, name)
```

The verb subscribes to `VisibleNamedObject` rows where Alice is the actor, then loops on the
mailbox. For each change message it reads the `:kind` field, dispatches on it, and emits one line
per object that appeared or disappeared:

```mica
verb watch_sight(actor @ #player)
  let [receiver, sender] = mailbox()
  let subscription = subscribe_changes(
    sender,
    :relation,
    :VisibleNamedObject,
    [actor, nothing, nothing],
    :snapshot
  )
  commit()

  while true
    let ready = mailbox_recv([receiver])
    for group in ready
      let messages = group[1]
      for message in messages
        let kind = message[:kind]
        if kind == :changes
          for row in message[:assertions]
            let name = row[2]
            emit(actor, string_concat("You see ", name, "."))
          end
          for row in message[:retractions]
            let name = row[2]
            emit(actor, string_concat("You no longer see ", name, "."))
          end
        elseif kind == :resynchronize
          emit(actor, "Sight changes came too fast; stopping. Restart to reread the room.")
          cancel_subscription(subscription)
          commit()
          return
        elseif kind == :revoked
          emit(actor, "You may no longer observe that.")
          cancel_subscription(subscription)
          commit()
          return
        end
      end
    end
  end
end
```

A few things this example is meant to show:

- The subscription is registered with a _bound_ first column (`actor`) and _open_ later columns
  (`nothing`). It fires only when a `VisibleNamedObject` row whose actor is Alice changes. A
  subscription with `[nothing, nothing, nothing]` would fire for every actor's visibility changes,
  which is more traffic than this verb needs.
- `:snapshot` causes the first delivered message to list everything Alice currently sees as
  assertions. The loop treats that initial batch the same way it treats later changes, so the verb
  announces her starting view and then keeps announcing changes without a separate initial case.
- Both terminal branches cancel the subscription before returning. `:resynchronize` and `:revoked`
  stop further delivery, but they do not by themselves remove the runtime registration; an explicit
  `cancel_subscription` followed by `commit()` does. The `commit()` also publishes the final `emit`
  so the host sees the parting line.
- There is no explicit `commit()` inside the loop body. `mailbox_recv` is itself a commit boundary:
  it commits the current transaction before waiting, so the `emit` calls from one iteration are
  published when the next `mailbox_recv` runs. An explicit `commit()` between iterations would only
  force publication slightly earlier and is not needed for the effects to reach the host.

A more durable consumer could remember the last commit cursor it processed (the `:cursor` field of
each change message) and, on `:resynchronize`, register a new `:changes`-only subscription starting
from that cursor. If the runtime no longer retains history that far back, the new subscription will
itself signal `:resynchronize`, and the consumer must fall back to a fresh `:snapshot`.

Subscription authority comes from the task's actor (when present) or its principal, not from the
`actor` argument passed into the verb. That argument selects the bound column and is used for
dispatch; the runtime's read authority check uses whatever authority context the task was started or
resumed with, and it checks the relation surface being subscribed to — here, `VisibleNamedObject` —
not every relation used inside its rule. So this verb is expected to run _as_ `actor` (for example,
dispatched through a session that has that actor's authority), and the task's authority must permit
reading `VisibleNamedObject`. If that authority is later revoked, the runtime sends `:revoked`
instead of leaking further changes, as the branch above handles.

This is the shape `subscribe_changes` is designed for: a small piece of derived state tied to one
identity, observed continuously, with effects published as that state changes.

## Differential DOM Updates

Mica's synchronized DOM views use the same change path without putting subscription plumbing into
each application. A view declares which relation patterns determine its rendered tree:

```mica
verb sync_view_dependencies(view)
  let room = one ChatView(view, ?room)
  return [{
    :subject -> :relation,
    :relation -> :ChatRenderedMessage,
    :bindings -> [room, nothing, nothing, nothing, nothing]
  }]
end
```

The host subscribes on behalf of the view. When a relevant commit changes `ChatRenderedMessage`, the
host renders `sync_view_tree(view)` again, compares the new tree with the previous tree, and sends
the browser a DOM patch.

This separates three jobs:

1. rules decide what is true;
2. subscriptions say which truth changes can affect a view; and
3. the DOM synchronizer turns the new view tree into a browser update.

The current DOM renderer is not itself evaluated differentially. It rerenders the affected view and
computes a structural diff. The subscription still removes the need for polling, global refresh
triggers, and application-maintained revision counters: unrelated fact commits and task completions
do not schedule that view. (Catalogue and read-authority policy changes can still wake non-root
subscriptions, because they might change what a subscription is allowed to see.)

Long-running tasks can publish progress the same way. The agent application commits after adding a
message or changing a tool-call status. Its `sync_event` task may still be waiting for a model or
tool, but the committed relation change wakes the subscribed view and the user sees the partial
result.

## Why The Changes Are Cheap: Differential Datalog

So far this chapter has shown the API. Underneath it, Mica uses an evaluation strategy whose name
carries some history, and that history is worth a short background paragraph so the rest of the
section makes sense.

Mica's rules are a form of **Datalog** — a small, old, well-studied declarative language for saying
"these facts imply those facts". You have already been writing it:

```mica
VisibleTo(actor, object) :-
  LocatedIn(actor, place),
  LocatedIn(object, place)
```

You do not need to know Datalog's theory to write Mica rules. The reason the name comes up here is
that there is a set of techniques around one specific question: _given that the rules stay the same
but the facts change, how do the derived answers change?_ Those techniques are called
**differential** Datalog, and they are what Mica uses to keep a live world's derived relations
current without recomputing them from scratch on every commit.

We can frame the difference as two questions. Datalog answers the first:

> Given these facts and rules, what else is true?

Differential Datalog answers the second:

> After the facts changed, what became true and what stopped being true?

The rest of this section is about that second question: how Mica answers it mechanically, and what
it keeps between commits to do so.

You do not need any special rule syntax to get this behaviour. It is how Mica evaluates suitable
rules and publishes their changes. The relations still behave like ordinary sets of facts.

## From Facts to Consequences

Suppose a world stores who is an actor, what is an object, and where each is located:

```mica
Actor(#alice)
Actor(#bob)
Object(#lamp)
LocatedIn(#alice, #workshop)
LocatedIn(#bob, #courtyard)
LocatedIn(#lamp, #workshop)
```

A rule says that an actor can see an object in the same place:

```mica
VisibleTo(actor, object) :-
  Actor(actor),
  Object(object),
  LocatedIn(actor, place),
  LocatedIn(object, place)
```

From the initial facts, Mica derives exactly one visibility fact:

```mica
VisibleTo(#alice, #lamp)
```

Now a task moves the lamp:

```mica
retract LocatedIn(#lamp, #workshop)
assert LocatedIn(#lamp, #courtyard)
```

When the task commits, two stored facts change. Those changes in turn alter the result of
`VisibleTo`:

| Relation    | Became true                    | Stopped being true            |
| ----------- | ------------------------------ | ----------------------------- |
| `LocatedIn` | `LocatedIn(#lamp, #courtyard)` | `LocatedIn(#lamp, #workshop)` |
| `VisibleTo` | `VisibleTo(#bob, #lamp)`       | `VisibleTo(#alice, #lamp)`    |

The naive way to get the new `VisibleTo` is to forget every previous answer and recompute visibility
for the whole world. That works, and Mica will do it when it has to. But it is wasteful when only
two rows of `LocatedIn` changed. The differential approach starts from those two changed rows and
asks: which `VisibleTo` results could possibly have changed because of _these_ edits? It then
recomputes only those.

You will sometimes see this written in the literature as `+1` and `-1` changes, meaning a fact (or
tuple) entered or left a relation. That notation is not Mica syntax; it is just a compact way to
talk about the same idea.

## Why A Retraction Is Not Just Deletion

A derived fact can have more than one reason to be true. Consider reachability:

```mica
Reachable(from, to) :-
  Passage(from, to)

Reachable(from, to) :-
  Passage(from, middle),
  Reachable(middle, to)
```

There may be several paths from `#square` to `#station`. Removing one passage must not retract
`Reachable(#square, #station)` if another path still supports it.

Mica handles this differently depending on the kind of rule.

For non-recursive rules — joins, and unions of multiple rule heads — Mica tracks how many distinct
derivations support each derived fact. A public Mica relation still has set semantics: a fact (a
tuple) is either present or absent. It becomes visible when its support count crosses from zero to
non-zero, and disappears only when the count crosses back to zero. This lets additions and removals
work through joins and multiple proofs without exposing duplicate facts to Mica code.

Recursive rules need a different approach. Mica stores each derived recursive fact with simple
presence, not a count of alternative recursive paths. When an input is retracted, the runtime runs
an **overdelete** pass that tentatively removes everything that depended on the retracted fact, then
a **rederivation** pass that puts back any of those facts which still have an independent proof.
This avoids both losing a still-supported fact and keeping a fact with no remaining proof. Recursive
maintenance proceeds in frontiers: Mica starts with the changed rows, follows the part of the
recursive relation they affect, and stops when an iteration produces no further changes. This is the
change-oriented counterpart to computing a complete recursive fixpoint from an empty slate.

## What Mica Keeps Between Commits

To reuse work between commits, Mica keeps some execution state for rule results that have been
warmed for differential maintenance:

- **arrangements** are indexes used to find the rows that can join with a changed row;
- **traces** retain consolidated, versioned relation changes; and
- **support weights** distinguish a fact with one proof from a fact with several proofs (for
  non-recursive rules, as above).

These are execution structures, not durable world facts. The committed relation store remains
authoritative. A snapshot presents ordinary stored and derived relations, and a task continues to
read one coherent snapshot as described in
[Tasks and Transactions](../runtime/tasks-and-transactions.md).

If maintained state is cold or unsuitable, Mica can fall back to computing the complete rule result.
Rule and relation catalogue changes can also require rebuilding maintained state. Differential
maintenance is an evaluation strategy, not a different meaning for the program: the answers are the
same either way.

## Costs And Good Fits

Differential maintenance works best when the world is large, commits are small, rules remain stable,
and derived results are read or observed repeatedly. Reachability, visibility, authority, dependency
analysis, live work queues, and collaborative views often have this shape.

It is not a promise that every small input has a small result. Removing one important edge can
change an entire reachability graph. Differential maintenance still has to process every genuinely
affected result.

There are also real costs:

- maintained traces and arrangements use memory;
- commits do work to propagate changes through warmed rule dependencies;
- broad subscriptions wake more often than subscriptions with useful bound columns; and
- a consumer that cannot keep up must resynchronize.

Small or rarely read relations may be cheaper to evaluate in full. Mica keeps that path available
and does not require authors to mark rules as "differential". The ordinary relation and rule model
comes first; incremental work is used where it can preserve the same answers while avoiding work on
the unchanged majority.
