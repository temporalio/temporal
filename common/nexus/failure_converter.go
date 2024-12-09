package nexus

import "github.com/nexus-rpc/sdk-go/nexus"

type failureConverter struct{}

type FailureError struct {
	Failure nexus.Failure
}

func (e FailureError) Error() string {
	return e.Failure.Message
}

// ErrorToFailure implements nexus.FailureConverter.
func (f failureConverter) ErrorToFailure(err error) nexus.Failure {
	// If we got this error from the wire, it must be a FailureError.
	if failureErr, ok := err.(FailureError); ok {
		return failureErr.Failure
	}
	// Fallback to just sending the message, e.g. when an error is generated server side.
	return nexus.Failure{
		Message: err.Error(),
	}
}

// FailureToError implements nexus.FailureConverter.
func (f failureConverter) FailureToError(failure nexus.Failure) error {
	return FailureError{failure}
}

var FailureConverter nexus.FailureConverter = failureConverter{}
