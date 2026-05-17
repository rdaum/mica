## Compiler/Runtime Gap List

These are not reference semantics; they are bugs or design debts exposed by
the cross-check.

1. Rules and method/verb definitions cannot be mixed with executable task code
    in one compiled chunk.

2. Contextual actor submissions cannot install rules or methods.
