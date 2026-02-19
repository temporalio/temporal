package serviceerror

import (
	"go.temporal.io/api/serviceerror"
)

// AbortedByServer wraps a serviceerror.Aborted and is returned when a workflow update
// is aborted by the server (e.g., when the update registry is cleared).
// SDKs will automatically retry this error. This error will also be retried internally
// by the server without forcing all serviceerror.Aborted errors to be retried.
type AbortedByServer struct {
	*serviceerror.Aborted
}

// NewAbortedByServer creates an AbortedByServer error with the given message.
func NewAbortedByServer(msg string) *AbortedByServer {
	return &AbortedByServer{Aborted: serviceerror.NewAborted(msg).(*serviceerror.Aborted)}
}
