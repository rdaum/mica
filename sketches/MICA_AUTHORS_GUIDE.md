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

In Mica, an object is a **stable identity value** (e.g., `#lamp42`). You do not allocate a hidden record, vtable, slot dictionary, or storage box. You create a durable identity and then describe it with facts.

### Creating an Object

The current executable surface uses ordinary Mica code. In the REPL or in a
filein, create identities and relations with builtins, then assert facts:

```mica
make_identity(:brass_lamp)
make_identity(:portable_item)
make_identity(:study_room)

make_relation(:Object, 1)
make_relation(:Delegates, 3)
make_relation(:Name, 2)
make_relation(:Description, 2)
make_relation(:Lit, 2)
make_relation(:LocatedIn, 2)

assert Object(#brass_lamp)
assert Delegates(#brass_lamp, #portable_item, 0)
assert Name(#brass_lamp, "brass lamp")
assert Description(#brass_lamp, "A tarnished but sturdy lamp.")
assert Lit(#brass_lamp, false)
assert LocatedIn(#brass_lamp, #study_room)
```

Creating an object-shaped world entity means creating a stable identity and
describing it through ordinary relations and facts.

### Inspecting an Object
If you "inspect" `#brass_lamp`, the system shows you a **fact neighbourhood**. It gathers facts where `#brass_lamp` appears in important subject positions, especially argument 0. It looks like an object in an outliner, but it is a computed view over the relational world.

---

## 2. Properties and Relations

### Named Relations
Core concepts like `Name`, `LocatedIn`, and `Owner` are **named relations**. These are fast, indexed, and subject to schema constraints.

### Dot Sugar
Mica supports `obj.prop` as sugar over a binary relation when the matching
relation exists in the compile context. The current conventional mapping is
`#brass_lamp.location` to `Location(#brass_lamp, ?location)`.

```mica
#brass_lamp.location
```

This reads one value:

```mica
one Location(#brass_lamp, ?location)
```

Assignment replaces the old tuple for that first argument:

```mica
#brass_lamp.location = #study_room
```

which is equivalent to replacing `Location(#brass_lamp, _)` with
`Location(#brass_lamp, #study_room)`.

The stricter schema story is not finished. The intended rule remains that dot
names are for binary functional relations, with no silent fallback to `Slot`.

### Ad-hoc Slots
If you need an ad-hoc property that doesn't have a dedicated relation, you must use the `Slot` relation explicitly:

```mica
assert Slot(#brass_lamp, :polish_level, 10)
```

### Queries
Relation atoms with query variables return bindings:

```mica
Location(#brass_lamp, ?room)      // [[:room: #study_room]]
Location(?thing, ?room)           // every matching thing/room pair
one Location(#brass_lamp, ?room)  // #study_room, or an error if not unique
```

---

## 3. Writing Verbs (Methods)

In image-based systems, a verb is "on" an object. In Mica, a method is an independent identity that matches **invocation roles**.

### Defining a Verb
Use the `verb` syntax to create behaviour. Instead of a privileged `this`, you name the roles that the method requires.

```mica
verb light(actor @ #player, target @ #brass_lamp)
  require Lit(target, false)
  require HasItem(actor, #matches)

  retract Lit(target, _)
  assert Lit(target, true)
  assert Event(:lit, actor, target)
end
```

### Key Differences:
- **Role Binding:** Instead of `dobj` and `iobj`, you use meaningful names like `target`, `item`, or `destination`.
- **Requirements:** The `require` keyword checks a fact (or a rule) before the method runs. 
- **Self:** There is no magic `self` in the current surface. A receiver call either binds the conventional `receiver` role in named calls or supplies the first positional method argument; method bodies still use declared role names.

---

## 4. Delegation and Dispatch

It is important to distinguish between how **state** is found and how **behaviour** is found.

### State (Properties)
Some declared properties are **effective** properties. When you query an effective property like `target.lit`, the backing relation may follow `Delegates`: if the local identity does not have a value, the effective relation checks prototypes according to its declared policy.

This is not automatic for every relation. Plain relation queries only ask the relation you named. Delegation participates when the relation or dot name is explicitly defined in terms of an effective relation.

### Behaviour (Dispatch)
Methods are **not** found by walking up a parent chain. Instead, when an invocation occurs, the system finds all methods whose **parameters** match the roles of the call. 
- A method requiring `target @ #portable` matches `#brass_lamp` because `#brass_lamp` delegates to `#portable`.
- The method is an independent identity; it is not "inside" the `#portable` object.

---

## 5. The REPL Lifecycle

When you type a command in Mica, it follows a rigorous lifecycle:
1. **Compile:** Your text is parsed, lowered, and compiled as ordinary Mica source.
2. **Match:** The system derives the set of **Applicable Methods**.
3. **Execute:** Selected methods record assertions, retractions, events, or effects in the transaction.
4. **Validate:** The system checks authority and world constraints.
5. **Commit:** The world state is updated atomically.

To you, the author, it feels like an immediate update. To the system, it is a checked transition.

---

## 6. Rosetta Stone for MOO Programmers

| MOO Concept | Mica Translation |
| :--- | :--- |
| `obj.prop` | Sugar over a binary relation, such as `Name(obj, val)`, with the current conventional mapping `obj.name` -> `Name(obj, ?name)`. |
| `parent(obj)` | `Delegates(obj, p, 0)` |
| `children(obj)` | `Delegates(c, obj, _)` |
| `obj:verb(...)` | Receiver-call syntax. Named calls bind `receiver: obj`; positional calls pass `obj` as the first method argument. |
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
3. **Behaviour through roles.** Methods match your world’s facts; they don't live inside boxes.
4. **Checked transitions.** Your changes are validated before they become the world's history.
