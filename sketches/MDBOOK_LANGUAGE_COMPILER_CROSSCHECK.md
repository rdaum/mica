## Compiler/Runtime Gap List

These are not reference semantics; they are bugs or design debts exposed by
the cross-check.

1. Argument splices parse in every argument list, but backend only implements
   them for list literals, direct local function calls, registered runtime
   builtin calls, function-value calls, relation atoms, task-control calls,
   positional dispatch, receiver positional dispatch, `invoke`, and positional
   spawn. Role-named dispatch and role-named spawn call paths still reject
   them.

2. Rules and method/verb definitions cannot be mixed with executable task code
    in one compiled chunk.

3. Contextual actor submissions cannot install rules or methods.
