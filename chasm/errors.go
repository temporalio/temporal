package chasm

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
