package history

import (
	"fmt"

	"github.com/nexus-rpc/sdk-go/nexus"
	failurepb "go.temporal.io/api/failure/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
)

// convertTemporalFailure converts a Temporal API Failure proto into a Go error
// via the Nexus SDK failure converter.
func convertTemporalFailure(failure *failurepb.Failure) (nexusErr error, err error) {
	nexusFailure, err := commonnexus.TemporalFailureToNexusFailure(failure)
	if err != nil {
		return nil, fmt.Errorf("failed to convert Temporal failure: %w", err)
	}
	nexusErr, err = nexusrpc.DefaultFailureConverter().FailureToError(nexusFailure)
	if err != nil {
		return nil, fmt.Errorf("failed to convert Nexus failure to error: %w", err)
	}
	return nexusErr, nil
}

// dispatchResponseToError converts a DispatchNexusTaskResponse proto into a Nexus SDK error.
// Returns nil if the response indicates success.
func dispatchResponseToError(resp *matchingservice.DispatchNexusTaskResponse) error {
	switch t := resp.GetOutcome().(type) {
	// Worker received the task and explicitly failed it (via RespondNexusTaskFailed).
	case *matchingservice.DispatchNexusTaskResponse_Failure:
		converted, err := convertTemporalFailure(t.Failure)
		if err != nil {
			return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "%v", err)
		}
		return converted
	case *matchingservice.DispatchNexusTaskResponse_RequestTimeout:
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeUpstreamTimeout, "upstream timeout")
	// Worker responded successfully; check the inner StartOperation response.
	case *matchingservice.DispatchNexusTaskResponse_Response:
		return startOperationResponseToError(t.Response.GetStartOperation())
	default:
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "empty or unknown dispatch outcome")
	}
}

// startOperationResponseToError converts a StartOperationResponse proto into a Nexus SDK error.
// Returns nil for success variants (SyncSuccess, AsyncSuccess).
func startOperationResponseToError(resp *nexuspb.StartOperationResponse) error {
	switch t := resp.GetVariant().(type) {
	case *nexuspb.StartOperationResponse_SyncSuccess:
		return nil
	case *nexuspb.StartOperationResponse_AsyncSuccess:
		return nil
	// Operation processed but failed — the worker returned an explicit failure.
	case *nexuspb.StartOperationResponse_Failure:
		return operationErrorFromFailure(t.Failure)
	default:
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "empty or unknown start operation response variant")
	}
}

// operationErrorFromFailure converts a Temporal API Failure into a Nexus SDK operation error.
func operationErrorFromFailure(failure *failurepb.Failure) error {
	state := nexus.OperationStateFailed
	if failure.GetCanceledFailureInfo() != nil {
		state = nexus.OperationStateCanceled
	}
	cause, err := convertTemporalFailure(failure)
	if err != nil {
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "%v", err)
	}
	opError := &nexus.OperationError{
		State:   state,
		Message: fmt.Sprintf("operation error: %s", failure.GetMessage()),
		Cause:   cause,
	}
	if err := nexusrpc.MarkAsWrapperError(nexusrpc.DefaultFailureConverter(), opError); err != nil {
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "failed to mark operation error as wrapper: %v", err)
	}
	return opError
}
