package nexus

import (
	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/server/api/matchingservice/v1"
)

// MatchingDispatchResponseToError converts a DispatchNexusTaskResponse proto into a Go error.
// Returns nil if the response indicates success.
//
// For failure cases (worker explicitly returned an error), the Temporal SDK's failure
// converter is used to produce standard Go errors (temporal.ApplicationError, temporal.CanceledError).
// For transport-level issues (timeout, internal), a nexus.HandlerError is returned
// so the caller can check Retryable().
func MatchingDispatchResponseToError(resp *matchingservice.DispatchNexusTaskResponse) error {
	return DispatchResultToError(ClassifyStartOperationDispatch(resp))
}

// DispatchResultToError converts a classified dispatch into a Go error for server-internal
// consumption. Returns nil for the success outcomes.
//
// The errors this produces are SDK-shaped: a failure the worker reported becomes a
// *temporal.ApplicationError or *temporal.CanceledError, and a Nexus handler error becomes a
// *nexus.HandlerError.
//
// Wire-facing paths convert through nexusrpc instead. The Nexus HTTP handler re-serializes a returned
// error with nexusrpc's failure converter, which flattens an SDK-shaped cause to just its message and
// drops the failure type and details.
func DispatchResultToError(result DispatchResult) error {
	if result.Outcome.Succeeded() {
		return nil
	}

	switch result.Outcome {
	case DispatchOutcomeHandlerFailure,
		DispatchOutcomeWorkerFailure,
		DispatchOutcomeOperationFailure:
		// The worker either failed the task (via RespondNexusTaskFailed) or answered with a failed
		// operation. Either way the failure reports what the worker said.
		return temporal.GetDefaultFailureConverter().FailureToError(result.Failure)

	case DispatchOutcomeRequestTimeout:
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeUpstreamTimeout, "upstream timeout")

	default:
		return nexus.NewHandlerErrorf(
			nexus.HandlerErrorTypeInternal,
			"unsupported or unrecognized dispatch outcome: %s", result.Outcome,
		)
	}
}
