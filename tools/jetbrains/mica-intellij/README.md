# Mica JetBrains Plugin

Native JetBrains Platform language support for Mica files in IntelliJ IDEA and
RustRover.

## Features

- `.mica` file association.
- Syntax highlighting for Mica keywords, comments, strings, numbers, sigils,
  operators, identifiers, and error-code literals.
- `//` line comments.
- Permissive parser and PSI for editor structure.
- Structure view entries for verbs, methods, and relation rules.
- Indentation and reformat support for common block forms, relation rule
  bodies, and comma continuations.

This plugin does not yet provide compiler diagnostics, completion, semantic
go-to-definition, find usages, or live Mica server editing.

## Build

From this directory:

```sh
./gradlew test
./gradlew buildPlugin
```

The installable plugin zip is written to:

```text
build/distributions/mica-intellij-0.1.0.zip
```

## Install Locally

In IntelliJ IDEA or RustRover:

1. Open `Settings`.
2. Go to `Plugins`.
3. Use the gear menu and choose `Install Plugin from Disk...`.
4. Select `tools/jetbrains/mica-intellij/build/distributions/mica-intellij-0.1.0.zip`.
5. Restart the IDE if prompted.

After installation, `.mica` files in opened projects should be recognized as
Mica files automatically.

## Sandbox Run

To launch a sandbox IntelliJ IDEA instance with the plugin installed:

```sh
./gradlew runIde
```

Use this path when checking highlighting, Structure view, and indentation
interactively.

## Generated Files

Grammar-Kit generates parser and lexer output under:

```text
src/main/gen/
```

That directory is ignored by Git. Regenerate it with Gradle rather than editing
generated files by hand.
