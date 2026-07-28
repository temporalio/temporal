package nexus

import (
	"github.com/nexus-rpc/sdk-go/nexus"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/server/api/matchingservice/v1"
)

// MatchingDispatchResponseToError converts a DispatchNexusTaskResponse proto into a Go error.
// Returns nil if the response indicates success.
//
// For failure cases (worker explicitly returned an error), the Temporal SDK's failure
// converter is used to produce standard Go errors (ApplicationError, CanceledError).
// For transport-level issues (timeout, internal), a nexus.HandlerError is returned
// so the caller can check Retryable().
func MatchingDispatchResponseToError(resp *matchingservice.DispatchNexusTaskResponse) error {
	switch t := resp.GetOutcome().(type) {
	case *matchingservice.DispatchNexusTaskResponse_Failure:
		// Worker received the task and explicitly failed it (via RespondNexusTaskFailed).
		return temporal.GetDefaultFailureConverter().FailureToError(t.Failure)
	case *matchingservice.DispatchNexusTaskResponse_RequestTimeout:
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeUpstreamTimeout, "upstream timeout")
	case *matchingservice.DispatchNexusTaskResponse_Response:
		return StartOperationResponseToError(t.Response.GetStartOperation())
	default:
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "empty or unknown dispatch outcome")
	}
}

// StartOperationResponseToError converts a StartOperationResponse proto into a Go error.
// Returns nil for success variants (SyncSuccess, AsyncSuccess).
func StartOperationResponseToError(resp *nexuspb.StartOperationResponse) error {
	switch t := resp.GetVariant().(type) {
	case *nexuspb.StartOperationResponse_SyncSuccess:
		return nil
	case *nexuspb.StartOperationResponse_AsyncSuccess:
		return nil
	case *nexuspb.StartOperationResponse_Failure:
		// Operation processed but failed — the worker returned an explicit failure.
		return temporal.GetDefaultFailureConverter().FailureToError(t.Failure)
	default:
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "empty or unknown start operation response variant")
	}
}
