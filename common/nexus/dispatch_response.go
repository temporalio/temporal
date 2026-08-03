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
		// Legacy wire format: a worker that does not support Temporal failures reports its handler
		// error as a proto HandlerError instead. Preserved so that such a worker's retry behavior is
		// still honored rather than being read as an unknown outcome.
		// nolint:staticcheck // Deprecated variant, kept for backwards compatibility with older SDKs.
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
	case *nexuspb.StartOperationResponse_OperationError:
		// Legacy wire format for the case above, used by workers that do not support Temporal
		// failures. Also an answer from the handler, not a delivery failure.
		// nolint:staticcheck // Deprecated variant, kept for backwards compatibility with older SDKs.
		return protoOperationErrorToError(t.OperationError)
	default:
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "empty or unknown start operation response variant")
	}
}

// StartOperationResponseFailed reports whether the response says the handler ran and failed the
// operation, as opposed to the task never being handled. Both wire formats are recognized.
func StartOperationResponseFailed(resp *nexuspb.StartOperationResponse) bool {
	switch resp.GetVariant().(type) {
	// nolint:staticcheck // Deprecated variant, kept for backwards compatibility with older SDKs.
	case *nexuspb.StartOperationResponse_Failure, *nexuspb.StartOperationResponse_OperationError:
		return true
	default:
		return false
	}
}

// protoHandlerErrorToError converts a legacy proto HandlerError into a [nexus.HandlerError], keeping
// the type and retry behavior that decide whether the caller retries.
func protoHandlerErrorToError(he *nexuspb.HandlerError) error {
	var retryBehavior nexus.HandlerErrorRetryBehavior
	// nolint:exhaustive // The default arm covers every remaining value.
	switch he.GetRetryBehavior() {
	case enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_RETRYABLE:
		retryBehavior = nexus.HandlerErrorRetryBehaviorRetryable
	case enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_NON_RETRYABLE:
		retryBehavior = nexus.HandlerErrorRetryBehaviorNonRetryable
	default:
		// Unspecified: leave it unset so the error type's own default decides.
	}
	return &nexus.HandlerError{
		Type:          nexus.HandlerErrorType(he.GetErrorType()),
		RetryBehavior: retryBehavior,
		// Carried on the error itself as well as its cause: callers that record only the error string
		// would otherwise lose what the worker said.
		Message: he.GetFailure().GetMessage(),
		// nolint:staticcheck // Deprecated function, kept for backwards compatibility with older SDKs.
		Cause: &nexus.FailureError{Failure: ProtoFailureToNexusFailure(he.GetFailure())},
	}
}

// protoOperationErrorToError converts a legacy proto UnsuccessfulOperationError into a
// [nexus.OperationError].
func protoOperationErrorToError(oe *nexuspb.UnsuccessfulOperationError) error {
	state := nexus.OperationState(oe.GetOperationState())
	return &nexus.OperationError{
		State:   state,
		Message: oe.GetFailure().GetMessage(),
		// nolint:staticcheck // Deprecated function, kept for backwards compatibility with older SDKs.
		Cause: &nexus.FailureError{Failure: ProtoFailureToNexusFailure(oe.GetFailure())},
	}
}
