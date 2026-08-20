package errors

var _ error = (*DestinationDownError)(nil)

// DestinationDownError indicates the destination is down and wraps another error.
// It is a useful specific error that can be used, for example, in a circuit breaker
// to distinguish when a destination service is down and an internal error.
type DestinationDownError struct {
	Message string
	err     error
}

func NewDestinationDownError(msg string, err error) *DestinationDownError {
	return &DestinationDownError{
		Message: "destination down: " + msg,
		err:     err,
	}
}

func (e *DestinationDownError) Error() string {
	msg := e.Message
	if e.err != nil {
		msg += "\n" + e.err.Error()
	}
	return msg
}

func (e *DestinationDownError) Unwrap() error {
	return e.err
}

var _ error = (*UnprocessableTaskError)(nil)

// UnprocessableTaskError is an error type that indicates a task cannot be processed
// and should be marked as failed without retrying.
type UnprocessableTaskError struct {
	Message string
}

// NewUnprocessableTaskError returns a new UnprocessableTaskError from given message.
func NewUnprocessableTaskError(message string) *UnprocessableTaskError {
	return &UnprocessableTaskError{Message: message}
}

func (e UnprocessableTaskError) Error() string {
	return "unprocessable task: " + e.Message
}
