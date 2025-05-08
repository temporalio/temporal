package queues

import (
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/tasks"
)

var _ Executable = (*speculativeWorkflowTaskTimeoutExecutable)(nil)

type (
	speculativeWorkflowTaskTimeoutExecutable struct {
		Executable
		workflowTaskTimeoutTask *tasks.WorkflowTaskTimeoutTask
	}
)

func newSpeculativeWorkflowTaskTimeoutExecutable(
	executable Executable,
	workflowTaskTimeoutTask *tasks.WorkflowTaskTimeoutTask,
) *speculativeWorkflowTaskTimeoutExecutable {

	return &speculativeWorkflowTaskTimeoutExecutable{
		Executable:              executable,
		workflowTaskTimeoutTask: workflowTaskTimeoutTask,
	}
}

// ctasks.Task interface overrides.

func (e *speculativeWorkflowTaskTimeoutExecutable) IsRetryableError(_ error) bool {
	// Speculative WT lives in memory representation of mutable state.
	// Almost any error cause workflow context (and mutable state) to be reloaded from database.
	// This clears speculative WT information and retrying corresponding timeout tasks doesn't make sense.
	return false
}

func (e *speculativeWorkflowTaskTimeoutExecutable) Abort() {
}

func (e *speculativeWorkflowTaskTimeoutExecutable) Cancel() {
	e.workflowTaskTimeoutTask.Cancel()
}

func (e *speculativeWorkflowTaskTimeoutExecutable) Ack() {
}

func (e *speculativeWorkflowTaskTimeoutExecutable) Nack(err error) {
}

func (e *speculativeWorkflowTaskTimeoutExecutable) Reschedule() {
}

func (e *speculativeWorkflowTaskTimeoutExecutable) State() ctasks.State {
	return e.workflowTaskTimeoutTask.State()
}
