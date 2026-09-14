# CHASM development notes

## Adding a library under `chasm/lib/`

1. Export `NewNilLibrary()`, returning an instance with nil handlers and nil config.
2. Add a `<pkg>.NewNilLibrary()` line to the `libs` slice in `chasm/lib/all/all.go`.

`chasm/lib/all` lets callers such as tdbg decode persisted CHASM trees without linking
production dependencies. `TestAllNilLibrariesRegistered` fails if step 2 is missed.

Since `NewNilLibrary` has no config or handlers, read paths those callers reach must not
consult library config or schedule tasks. Keep config lookups on write paths.

## Reading a persisted tree outside the history service

Use `chasm.NewDetachedTree` rather than `NewTreeFromDB` with a hand rolled backend. It takes
the facts living outside the tree (`chasm.ExecutionMetadata`) explicitly, and panics on write
paths rather than fabricating state.
