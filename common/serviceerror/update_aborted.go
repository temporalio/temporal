package serviceerror

import (
	"go.temporal.io/api/serviceerror"
	errordetailsspb "go.temporal.io/server/api/errordetails/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// AbortedByServer wraps a serviceerror.Aborted and is returned when a workflow update
// is aborted by the server (e.g., when the update registry is cleared).
// This error will be retried internally by the server without forcing all serviceerror.Aborted errors to be retried.
type AbortedByServer struct {
	*serviceerror.Aborted
}

// NewAbortedByServer creates an AbortedByServer error with the given message.
func NewAbortedByServer(msg string) *AbortedByServer {
	return &AbortedByServer{Aborted: serviceerror.NewAborted(msg).(*serviceerror.Aborted)}
}

// Status overrides the embedded Aborted.Status() to include the UpdateAbortedByServerFailure
// detail, so the error survives gRPC serialization and can be reconstructed on the receiving side.
func (e *AbortedByServer) Status() *status.Status {
	st := status.New(codes.Aborted, e.Error())
	st, _ = st.WithDetails(&errordetailsspb.UpdateAbortedByServerFailure{})
	return st
}
