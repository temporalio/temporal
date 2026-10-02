# CHASM development notes

## Adding a library under `chasm/lib/`

1. Export `NewNilLibrary()`, returning an instance with nil handlers and nil config.
2. Add a `<pkg>.NewNilLibrary()` line to the `libs` slice in `chasm/lib/all/all.go`.

`chasm/lib/all` lets callers such as tdbg decode persisted CHASM trees without linking
production dependencies. `TestAllNilLibrariesRegistered` fails if step 2 is missed.

Offline readers only ever see the nil library, which has no config and no handlers. So any
code they can reach, such as describing a component, must not read library config or
schedule tasks, or it fails there even though it works in the history service. Read config on
write paths, which offline readers never run.

Keep `Tasks()` reachable from the nil constructor. Handlers being nil is fine, but the
registrations are what let offline readers resolve and decode the logical tasks a tree carries.
`TestNewNilRegistry_TasksDecodable` covers this.

## Reading a persisted tree outside the history service

Use `chasm.NewDetachedExecution` to decode a persisted `WorkflowMutableState` into a typed
root component. It panics on write paths rather than fabricating state. To route a record
before decoding it, `chasm.ExecutionArchetypeID` reads the archetype ID from the root node alone,
without a registry.

Pass the whole record, not just the nodes. `ChasmNodes`, `ExecutionInfo`, and `ExecutionState`
are all required: the latter two supply the namespace, business ID, run ID, close time, and
transition history, which live on the record rather than in the tree. A caller that stores
CHASM nodes for later offline reads must store those two alongside, since nothing recovers them
from the tree.
