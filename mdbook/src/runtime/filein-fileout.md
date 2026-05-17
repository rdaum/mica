# Filein and Fileout

Filein and fileout provide a human-readable import/export surface for live
world state.

Filein runs ordinary Mica source:

```mica
make_identity(:lamp)
make_functional_relation(:Name, 2, [0])
assert Name(#lamp, "brass lamp")

verb look(actor, item)
  return one Name(item, ?name)
end
```

Fileout emits readable source that can be reviewed, edited, version controlled,
and filed back in.

This is useful for more than object worlds. A fileout can capture the schema,
rules, seed facts, and verb definitions for an agent workspace, including
relations such as `Task`, `Artifact`, `Observation`, `ToolResult`,
`AssignedTo`, and `DependsOn`. The result is an auditable bootstrap and review
format for live memory, not a copy of a hidden object heap.

Units group filed-in state so replacement workflows can update an imported
source unit over top of a live workspace. The runtime stores the resulting
identities, relations, facts, rules, and verb definitions. It does not rely on
storing the original file text as the source of truth.

Replacement should be atomic at the unit boundary: a failed filein should not
leave half of the replacement visible. When a filein spans many definitions,
the practical implementation may use smaller internal steps, but the authoring
contract should remain "the unit replacement committed" or "it did not".
