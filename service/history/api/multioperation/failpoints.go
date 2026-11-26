package multioperation

import "go.temporal.io/testx/failpoint"

// Failpoints of Update-with-Start. Each key documents where it fires and what tests use it for.
var (
	// BetweenLockAndStartKey fires after Update-with-Start released the workflow lock and before it
	// starts the workflow. Tests use Call to start a conflicting workflow in this window.
	BetweenLockAndStartKey = failpoint.NewKey[error]("history.updateWithStart.betweenLockAndStart")
	// ClosingWorkflowRetryKey fires before Update-with-Start retries after its update was aborted by
	// a closing workflow. Tests use Call to start a new workflow before the retry.
	ClosingWorkflowRetryKey = failpoint.NewKey[error]("history.updateWithStart.closingWorkflowRetry")
)
