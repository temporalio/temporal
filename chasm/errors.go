package chasm

import "go.temporal.io/api/serviceerror"

// ErrRequestIDAlreadyUsed is returned by UpdateComponent when the request ID supplied via
// WithRequestID has already been recorded on the execution - i.e. a retry of an update that already
// succeeded.
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
