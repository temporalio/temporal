package frontend

import (
	"github.com/nexus-rpc/sdk-go/nexus"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/log/tag"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
)

// handleStartOperationResponse converts matching's response to a StartOperation dispatch into the result the
// Nexus SDK expects, recording the metrics outcome tag and the failure-source response header along
// the way.
//
// Links are returned to the caller instead of being attached here: nexus.AddHandlerLinks requires the
// SDK's handler context.
func (c *operationContext) handleStartOperationResponse(
	resp *matchingservice.DispatchNexusTaskResponse,
	operation string,
) (nexus.HandlerStartOperationResult[any], []nexus.Link, error) {
	result := commonnexus.ClassifyStartOperationDispatch(resp)
	c.recordDispatchOutcome(result)

	switch result.Outcome {
	case commonnexus.DispatchOutcomeSyncSuccess:
		return &nexus.HandlerStartOperationResultSync[any]{
			Value: result.OperationResult,
		}, parseLinks(result.Links, c.logger), nil

	case commonnexus.DispatchOutcomeAsyncSuccess:
		return &nexus.HandlerStartOperationResultAsync{
			OperationToken: result.OperationToken,
		}, parseLinks(result.Links, c.logger), nil

	case commonnexus.DispatchOutcomeOperationFailure:
		// The worker ran the operation and it came back failed or canceled. That is a legitimate
		// answer, reported to the caller as a Nexus operation error rather than a handler error.
		cause, internalErr := c.convertWorkerFailure(result.Failure, operation)
		if internalErr != nil {
			return nil, nil, internalErr
		}
		state := nexus.OperationStateFailed
		if result.Failure.GetCanceledFailureInfo() != nil {
			state = nexus.OperationStateCanceled
		}
		return nil, nil, c.operationError(state, cause, operation)

	default:
		return nil, nil, c.failedDispatchToNexusError(result, operation)
	}
}

// handleCancelOperationResponse converts matching's response to a CancelOperation dispatch into the error the
// Nexus SDK expects, recording the metrics outcome tag and the failure-source response header along
// the way. A nil error means the cancel was accepted.
func (c *operationContext) handleCancelOperationResponse(
	resp *matchingservice.DispatchNexusTaskResponse,
	operation string,
) error {
	result := commonnexus.ClassifyCancelOperationDispatch(resp)
	c.recordDispatchOutcome(result)

	if result.Outcome == commonnexus.DispatchOutcomeCancelAccepted {
		return nil
	}
	return c.failedDispatchToNexusError(result, operation)
}

// failedDispatchToNexusError converts the outcomes that mean the task was never handled, or was
// refused outright, into the error to report to the caller. These arms are identical for every kind of
// dispatched request, so both handler methods share them.
func (c *operationContext) failedDispatchToNexusError(
	result commonnexus.DispatchResult,
	operation string,
) error {
	switch result.Outcome {
	case commonnexus.DispatchOutcomeHandlerFailure, commonnexus.DispatchOutcomeWorkerFailure:
		// A handler error round-trips as a handler error. Anything else the worker used to fail the
		// task converts to an opaque failure error, which the SDK reports to the caller as internal.
		// Neither is wrapped, so both errors are reported to the caller unchanged.
		cause, internalErr := c.convertWorkerFailure(result.Failure, operation)
		if internalErr != nil {
			return internalErr
		}
		return cause

	case commonnexus.DispatchOutcomeRequestTimeout:
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeUpstreamTimeout, "upstream timeout")

	default:
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "empty outcome")
	}
}

// convertWorkerFailure converts a Temporal failure a worker reported into its Nexus-shaped
// equivalent, preserving the cause chain in the form the wire needs.
//
// Exactly one of the returned errors is non-nil. cause is the worker's own failure, which the caller
// may return directly or use as the cause of an OperationError. internalErr means the worker sent a
// failure the server could not parse; the caller must return it as-is, since the operation's outcome
// is unknown at that point.
func (c *operationContext) convertWorkerFailure(
	failure *failurepb.Failure,
	operation string,
) (cause error, internalErr error) {
	// Converting in place is safe here: the failure belongs to a response this request owns.
	nf, err := commonnexus.TemporalFailureToNexusFailureInPlace(failure)
	if err != nil {
		c.logger.Error("error converting Temporal failure to Nexus failure",
			tag.Error(err), tag.Operation(operation), tag.WorkflowNamespace(c.namespaceName))
		return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "internal error")
	}
	cause, err = nexusrpc.DefaultFailureConverter().FailureToError(nf)
	if err != nil {
		c.logger.Error("error converting Nexus failure to Nexus error",
			tag.Error(err), tag.Operation(operation), tag.WorkflowNamespace(c.namespaceName))
		return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "internal error")
	}
	return cause, nil
}

// operationError builds the envelope that tells a Nexus caller the operation itself failed, marked so
// that a Temporal caller unwraps to the real cause instead of recording the synthetic envelope.
func (c *operationContext) operationError(
	state nexus.OperationState,
	cause error,
	operation string,
) error {
	opErr := &nexus.OperationError{
		State:   state,
		Message: "operation error",
		Cause:   cause,
	}
	if err := nexusrpc.MarkAsWrapperError(nexusrpc.DefaultFailureConverter(), opErr); err != nil {
		c.logger.Error("error converting OperationError to Nexus failure",
			tag.Error(err), tag.Operation(operation), tag.WorkflowNamespace(c.namespaceName))
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "internal error")
	}
	return opErr
}

// recordDispatchOutcome tags the request's metrics with the dispatch outcome and, when the dispatch
// did not succeed, attributes the failure to the worker in the response header.
func (c *operationContext) recordDispatchOutcome(result commonnexus.DispatchResult) {
	c.metricsHandler = c.metricsHandler.WithTags(result.OutcomeTag())
	if !result.Outcome.Succeeded() {
		c.setFailureSource(commonnexus.FailureSourceWorker)
	}
}
