package chasm

import "go.temporal.io/api/serviceerror"

// ErrRequestIDAlreadyUsed is returned by UpdateComponent when the request ID supplied via
// WithRequestID has already been recorded on the execution.
var ErrRequestIDAlreadyUsed = serviceerror.NewFailedPrecondition("request ID has already been used for this execution")

type ExecutionAlreadyStartedError struct {
	Message          string
	CurrentRequestID string
	CurrentRunID     string
}

func NewExecutionAlreadyStartedErr(
	message, currentRequestID, currentRunID string,
) *ExecutionAlreadyStartedError {
	return &ExecutionAlreadyStartedError{
		Message:          message,
		CurrentRequestID: currentRequestID,
		CurrentRunID:     currentRunID,
	}
}

func (e *ExecutionAlreadyStartedError) Error() string {
	return e.Message
}

// TaskNotInvalidatedError indicates a CHASM task executed successfully but its
// validator still reports the task as valid. Retrying such a task can loop
// forever, so queue executors should treat it as terminal and send it to DLQ
// when task DLQ is enabled.
type TaskNotInvalidatedError struct {
	TaskKind string
	TaskInfo string
}

func NewTaskNotInvalidatedError(
	taskKind string,
	taskInfo string,
) *TaskNotInvalidatedError {
	return &TaskNotInvalidatedError{
		TaskKind: taskKind,
		TaskInfo: taskInfo,
	}
}

func (e *TaskNotInvalidatedError) Error() string {
	return "CHASM " + e.TaskKind + " task remained valid after successful execution: " + e.TaskInfo
}

func (e *TaskNotInvalidatedError) IsTerminalTaskError() bool {
	return true
}
