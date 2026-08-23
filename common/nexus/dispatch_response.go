package nexus

import (
	"github.com/nexus-rpc/sdk-go/nexus"
	enumspb "go.temporal.io/api/enums/v1"
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
	case *matchingservice.DispatchNexusTaskResponse_HandlerError:
		//nolint:staticcheck // Deprecated, still sent by older workers.
		return protoHandlerErrorToError(t.HandlerError)
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
	case *nexuspb.StartOperationResponse_OperationError: //nolint:staticcheck // Deprecated, still sent by older workers.
		//nolint:staticcheck // Deprecated fields on a deprecated variant.
		cause := ProtoFailureToNexusFailure(t.OperationError.GetFailure())
		return &nexus.OperationError{
			//nolint:staticcheck // Deprecated fields on a deprecated variant.
			State: nexus.OperationState(t.OperationError.GetOperationState()),
			// OperationError.Error() does not include the cause, so carry the worker's message here
			// too. Otherwise callers that record err.Error() lose it.
			Message: cause.Message,
			Cause:   &nexus.FailureError{Failure: cause},
		}
	default:
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "empty or unknown start operation response variant")
	}
}

// protoHandlerErrorToError converts the deprecated HandlerError outcome into a nexus.HandlerError, so that
// the worker's error type and retry behavior are preserved rather than collapsed into an internal error.
func protoHandlerErrorToError(handlerErr *nexuspb.HandlerError) error {
	var retryBehavior nexus.HandlerErrorRetryBehavior
	//nolint:exhaustive // Unspecified defers to the error type's default.
	switch handlerErr.GetRetryBehavior() {
	case enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_RETRYABLE:
		retryBehavior = nexus.HandlerErrorRetryBehaviorRetryable
	case enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_NON_RETRYABLE:
		retryBehavior = nexus.HandlerErrorRetryBehaviorNonRetryable
	}
	//nolint:staticcheck // Deprecated function still in use for backward compatibility.
	cause := ProtoFailureToNexusFailure(handlerErr.GetFailure())
	return &nexus.HandlerError{
		Type:          nexus.HandlerErrorType(handlerErr.GetErrorType()),
		RetryBehavior: retryBehavior,
		// HandlerError.Error() does not include the cause, so carry the worker's message here too.
		// Otherwise callers that record err.Error() lose it.
		Message: cause.Message,
		Cause:   &nexus.FailureError{Failure: cause},
	}
}
