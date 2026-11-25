// Package failpoints declares the failpoints of the Temporal server. Server code executes them
// through the server's failpoint Source, scoped to a namespace ID; they only fire if the startup switch
// enabled failpoints (see Decide).
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
