# CHASM development notes

## Adding a library under `chasm/lib/`

1. Export `NewNilLibrary()`, returning an instance with nil handlers and nil config.
2. Add a `<pkg>.NewNilLibrary()` line to the `libs` slice in `chasm/lib/all/all.go`.

`chasm/lib/all` lets callers such as tdbg decode persisted CHASM trees without linking
production dependencies. `TestAllNilLibrariesRegistered` fails if step 2 is missed.

Since `NewNilLibrary` has no config or handlers, read paths those callers reach must not
consult library config or schedule tasks. Keep config lookups on write paths.

Keep `Tasks()` reachable from the nil constructor. Handlers being nil is fine, but the
registrations are what let offline readers resolve and decode the logical tasks a tree carries.
`TestNewRegistry_TasksDecodable` covers this.

## Reading a persisted tree outside the history service

Use `chasm.DetachedRootComponent` to decode a persisted `WorkflowMutableState` into a typed
root component, or `chasm.NewDetachedTree` when you need the tree itself. Both panic on write
paths rather than fabricating state.

Pass the whole record, not just the nodes. Only `ChasmNodes` is required; `ExecutionInfo` and
`ExecutionState` supply the namespace, business ID, run ID, close time, and transition count,
which live on the record rather than in the tree. Omit them and everything still decodes, with
those fields reading zero. A caller that stores CHASM nodes for later offline reads should
store those two alongside, since nothing recovers them from the tree.
