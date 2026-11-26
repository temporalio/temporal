package matching

import "go.temporal.io/testx/failpoint"

// ForwardTaskKey fires before matching forwards a workflow, activity, query or Nexus task to the
// parent partition. Tests use Pause to make forwarding latency observable.
var ForwardTaskKey = failpoint.NewKey[error]("matching.forwardTask")

// SkipSyncMatchKey skips sync matching of a task that history added to a waiting poller, persisting
// it to the backlog instead as if no poller was waiting. Tests use On to force the backlog path.
var SkipSyncMatchKey = failpoint.NewKey[bool]("matching.skipSyncMatch")
