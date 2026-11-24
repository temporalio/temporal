// Package failpoints declares the failpoints of the Temporal server. Server code executes them
// through its test hooks, scoped to a namespace ID; they only fire in builds with the test_dep tag.
package failpoints

import "go.temporal.io/testx/failpoint"

// Failpoints in server code. Each key documents where it fires and what tests use it for.
var (
	// MatchingForwardTask fires before matching forwards a workflow, activity, query or Nexus task
	// to the parent partition. Tests use Pause to make forwarding latency observable.
	MatchingForwardTask = failpoint.NewKey("matching.forwardTask")
	// UpdateWithStartBetweenLockAndStart fires after Update-with-Start released the workflow lock
	// and before it starts the workflow. Tests use Call to start a conflicting workflow in this
	// window.
	UpdateWithStartBetweenLockAndStart = failpoint.NewKey("history.updateWithStart.betweenLockAndStart")
	// UpdateWithStartClosingWorkflowRetry fires before Update-with-Start retries after its update
	// was aborted by a closing workflow. Tests use Call to start a new workflow before the retry.
	UpdateWithStartClosingWorkflowRetry = failpoint.NewKey("history.updateWithStart.closingWorkflowRetry")
)

// PersistenceKey returns the key of the failpoint that fires before a persistence store method
// runs, for example PersistenceKey("ExecutionStore", "UpdateWorkflowExecution").
func PersistenceKey(store, method string) failpoint.Key {
	return failpoint.KeyFromName("persistence:"+store+"."+method, "")
}
