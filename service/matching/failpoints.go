package matching

import "go.temporal.io/testx/failpoint"

// SkipSyncMatchKey skips sync matching of a task that history added to a waiting poller, persisting
// it to the backlog instead as if no poller was waiting. Tests use On to force the backlog path.
var SkipSyncMatchKey = failpoint.NewKey[bool]("matching.skipSyncMatch")
