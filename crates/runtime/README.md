# mica-runtime

`mica-runtime` is the live Mica runtime environment. It wires the compiler,
task manager, relation kernel, VM, and core builtins into the programmable
system that drivers and command-line tools execute against.

It owns the layer above the bytecode VM and relation kernel, and below drivers
or command-line tools:

- `SourceRunner`;
- task request, continuation, task manager, and task outcome types;
- bootstrap catalogue relations;
- builtin registration;
- method and rule installation;
- filein/fileout ownership;
- authority construction for actor execution;
- report rendering and identity/relation display names.

The bytecode execution core lives in `mica-vm`. The command-line REPL lives in
`mica-runner`. The compio task driver lives in `mica-driver`.
