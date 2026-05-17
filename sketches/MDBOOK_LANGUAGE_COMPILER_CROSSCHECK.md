## Compiler/Runtime Gap List

These are not reference semantics; they are bugs or design debts exposed by
the cross-check.

1. Function/lambda values now compile to ephemeral runtime function handles,
   including closures with captured locals, but function values currently
   support only required positional parameters.

2. Argument splices parse in every argument list, but backend only implements
   them for list literals, direct local function calls, registered runtime
   builtin calls, and relation atoms. Function-value calls, dispatch, spawn,
   and task-control call paths still reject them.

3. `spawn` parses any expression target, but backend only accepts role or
   receiver dispatch targets. Role dispatch spawn targets still require
   explicit role names.

4. A single compiled eval task cannot define a relation/identity and use the
    new name later in the same source body because compile context is resolved
    before execution. Filein chunking hides this for import files, but the REPL
    and `eval` semantics remain surprising.

5. Rules and method/verb definitions cannot be mixed with executable task code
    in one compiled chunk.

6. Contextual actor submissions cannot install rules or methods.
