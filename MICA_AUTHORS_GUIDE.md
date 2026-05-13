# Mica Author's Guide

This guide is for world-builders, game designers, and authors coming from "image-based" systems like MOO, Self, or Smalltalk. It explains how to build and evolve a live world using Mica’s relational model.

## The Mental Model Shift

| Concept | The MOO/Self Way | The Mica Way |
| :--- | :--- | :--- |
| **Object** | A container of slots/properties. | A **stable identity** (a unique anchor). |
| **State** | Values inside the object's box. | **Facts** in relations about an identity. |
| **Inheritance** | Searching parent boxes for a slot. | Matchers against **Delegates** facts. |
| **Verbs/Methods** | Functions stored "on" the object. | Independent identities matching **Roles**. |
| **Mutation** | Direct assignment: `obj.prop = x`. | **Fact Changes**: `assert` and `retract`. |

---

## 1. Working with Identities

In Mica, an object is a **stable identity value** (e.g., `#lamp42`). You don't "allocate" it in the traditional sense; you create it by asserting its existence and describing it with facts.

### Creating an Object
For convenience, Mica provides command sugar for creating identities and their initial facts:

```mica
object #brass_lamp extends #portable_item
  name = "brass lamp"
  description = "A tarnished but sturdy lamp."
  lit = false
end
```

**Under the hood:** This sugar expands into a transaction of ordinary fact changes:
- `assert Object(#brass_lamp)`
- `assert Delegates(#brass_lamp, #portable_item, 0)`
- `assert Name(#brass_lamp, "brass lamp")`
- `assert Description(#brass_lamp, "A tarnished but sturdy lamp.")`
- `assert Lit(#brass_lamp, false)`

### Inspecting an Object
If you "inspect" `#brass_lamp`, the system shows you a **Fact Neighborhood**. It gathers all facts where `#brass_lamp` is a primary key. It looks like an object, but it is a computed view over the relational world.

---

## 2. Properties and Relations

### Named Relations
Core concepts like `Name`, `LocatedIn`, and `Owner` are **named relations**. These are fast, indexed, and subject to schema constraints.

### Dot Sugar
Mica allows the `obj.prop` syntax as an ergonomic shortcut **only for declared binary functional relations**. 

```mica
#brass_lamp.located_in = #study_room
```

This expands to ordinary fact changes:

```mica
retract LocatedIn(#brass_lamp, _)
assert LocatedIn(#brass_lamp, #study_room)
```

**Note:** If a relation is not declared as functional for the object, dot-assignment is not available. This prevents accidental "silent" creation of data in the wrong place.

### Ad-hoc Slots
If you need an ad-hoc property that doesn't have a dedicated relation, you must use the `Slot` relation explicitly:

```mica
assert Slot(#brass_lamp, :polish_level, 10)
```

---

## 3. Writing Verbs (Methods)

In image-based systems, a verb is "on" an object. In Mica, a method is an independent identity that matches **Invocation Roles**.

### Defining a Verb
Use the `verb` syntax to create behavior. Instead of a privileged `this`, you name the roles that the method requires.

```mica
verb light(actor: #player, target: #brass_lamp)
  require Lit(target, false)
  require HasItem(actor, #matches)
  
  target.lit = true
  assert Event(:lit, actor, target)
end
```

### Key Differences:
- **Role Binding:** Instead of `dobj` and `iobj`, you use meaningful names like `target`, `item`, or `destination`.
- **Requirements:** The `require` keyword checks a fact (or a rule) before the method runs. 
- **Self:** There is no magic `self`. However, a method can declare a receiver role (e.g., `receiver target`) to allow the `target:light()` syntax. In the body, `self` is simply an alias for that role.

---

## 4. Delegation and Dispatch

It is important to distinguish between how **state** is found and how **behavior** is found.

### State (Properties)
Some declared properties are **effective** properties. When you query an effective property like `target.lit`, the backing relation may follow `Delegates`: if the local identity does not have a value, the effective relation checks prototypes according to its declared policy.

This is not automatic for every relation. Plain relation queries only ask the relation you named. Delegation participates when the relation or dot name is explicitly defined in terms of an effective relation.

### Behavior (Dispatch)
Methods are **not** found by walking up a parent chain. Instead, when an invocation occurs, the system finds all methods whose **parameters** match the roles of the call. 
- A method requiring `target: #portable` matches `#brass_lamp` because `#brass_lamp` delegates to `#portable`.
- The method is an independent identity; it is not "inside" the `#portable` object.

---

## 5. The REPL Lifecycle

When you type a command in Mica, it follows a rigorous lifecycle:
1. **Parse:** Your text is turned into an **Invocation** with role bindings.
2. **Match:** The system derives the set of **Applicable Methods**.
3. **Execute:** Selected methods record assertions, retractions, events, or effects in the transaction.
4. **Validate:** The system checks authority and world constraints.
5. **Commit:** The world state is updated atomically.

To you, the author, it feels like an immediate update. To the system, it is a checked transition.

---

## 6. Rosetta Stone for MOO Programmers

| MOO Concept | Mica Translation |
| :--- | :--- |
| `obj.prop` | A declared dot name backed by a binary functional relation, such as `Name(obj, val)`. |
| `parent(obj)` | `Delegates(obj, p, 0)` |
| `children(obj)` | `Delegates(c, obj, _)` |
| `obj:verb(...)` | `:verb(receiver_role: obj, ...)` |
| `move obj to dest` | `retract LocatedIn(obj, _)`, `assert LocatedIn(obj, dest)` |
| `player` | `actor` (A standard role binding). |
| `dobj / iobj` | Named roles (e.g., `item`, `target`) in the method signature. |
| `property_info` | Facts about the relation (Owner, Constraints). |
| `#0` (System) | Core identities and the active world relations. |

---

## Summary for Authors

Mica gives you the same "live" feel as a MOO, but with a foundation built on **Representational Independence**.

1. **Identity first.** Create the anchor.
2. **Fact-based state.** Use declared relations for structure, and `Slot` for the ad-hoc.
3. **Behavior through roles.** Methods match your world’s facts; they don't live inside boxes.
4. **Checked transitions.** Your changes are validated before they become the world's history.
