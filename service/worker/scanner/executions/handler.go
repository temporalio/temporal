package executions

import "github.com/temporalio/temporal/service/worker/scanner/executor"

type handlerStatus = executor.TaskStatus

const (
	handlerStatusDone  = executor.TaskStatusDone
	handlerStatusErr   = executor.TaskStatusErr
	handlerStatusDefer = executor.TaskStatusDefer
)

const scannerTaskListPrefix = "temporal-sys-executions-scanner"

// validateHandler validates a single execution.
// It operates in two phases: collection step and validation step.
// During collection step information from persistence is read for this workflow execution.
// During validation step invariants are asserted over everything that was read.
// In the future its possible to add a third step here which will additionally take automatic recovery actions if validation failed.
func (s *Scavenger) validateHandler(key *executionKey) handlerStatus {
	// TODO: implement this
	return handlerStatusDone
}
