## Compiler/Runtime Gap List

These are not reference semantics; they are bugs or design debts exposed by
the cross-check.

1. Anonymous function/lambda values parse, but backend rejects them unless the
   function literal is immediately bound as a direct local function.

2. Closures are detected by semantic analysis, but backend rejects captured
   locals.

3. Argument splices parse in every argument list, but backend only implements
   them for list literals, direct local function calls, registered runtime
   builtin calls, and relation atoms. Dispatch, spawn, and task-control call
   paths still reject them.

4. Named arguments parse in ordinary call syntax, but most call paths reject
   them. Named arguments are meaningful for role dispatch, not generic calls.

5. Receiver calls parse positional arguments, but backend dispatch requires all
   non-receiver arguments to have explicit role names.

6. `spawn` parses any expression target, but backend only accepts role or
   receiver dispatch targets with explicit role names.

7. Query variables parse anywhere, but backend only accepts them in relation
    queries and rule terms.

8. A single compiled eval task cannot define a relation/identity and use the
    new name later in the same source body because compile context is resolved
    before execution. Filein chunking hides this for import files, but the REPL
    and `eval` semantics remain surprising.

9. Rules and method/verb definitions cannot be mixed with executable task code
    in one compiled chunk.

10. Contextual actor submissions cannot install rules or methods.

11. Error catch syntax parses broader conditions, but backend currently
    accepts only literal error-code matches or catch-all clauses.
