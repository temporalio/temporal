package frontend

import (
	"github.com/nexus-rpc/sdk-go/nexus"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
)

// startOperationOutcome converts matching's response to a StartOperation dispatch into the result the
// Nexus SDK expects, recording the metrics outcome tag and the failure-source response header along
// the way.
//
// Links are returned to the caller instead of being attached here: nexus.AddHandlerLinks requires the
// SDK's handler context.
func (c *operationContext) startOperationOutcome(
	resp *matchingservice.DispatchNexusTaskResponse,
	operation string,
) (nexus.HandlerStartOperationResult[any], []nexus.Link, error) {
	switch t := resp.GetOutcome().(type) {
	case *matchingservice.DispatchNexusTaskResponse_Failure:
		// Set the failure source to "worker" if we've reached this case.
		// Failure conversions errors below are the user's fault, as it implies that malformed completions were sent from
		// the worker.
		c.setFailureSource(commonnexus.FailureSourceWorker)
		c.metricsHandler = c.metricsHandler.WithTags(metrics.OutcomeTag("handler_error:" + t.Failure.GetNexusHandlerFailureInfo().GetType()))
		he, internalErr := c.convertWorkerFailure(t.Failure, operation)
		if internalErr != nil {
			return nil, nil, internalErr
		}
		return nil, nil, he

	case *matchingservice.DispatchNexusTaskResponse_HandlerError: //nolint:staticcheck // Deprecated, still sent by older workers.
		// Deprecated case. Replaced with DispatchNexusTaskResponse_Failure
		//nolint:staticcheck // Deprecated field on a deprecated variant.
		c.metricsHandler = c.metricsHandler.WithTags(metrics.OutcomeTag("handler_error:" + t.HandlerError.GetErrorType()))
		c.setFailureSource(commonnexus.FailureSourceWorker)
		return nil, nil, convertOutcomeToNexusHandlerError(t)

	case *matchingservice.DispatchNexusTaskResponse_RequestTimeout:
		c.metricsHandler = c.metricsHandler.WithTags(metrics.OutcomeTag("handler_timeout"))
		c.setFailureSource(commonnexus.FailureSourceWorker)
		return nil, nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeUpstreamTimeout, "upstream timeout")

	case *matchingservice.DispatchNexusTaskResponse_Response:
		switch t := t.Response.GetStartOperation().GetVariant().(type) {
		case *nexuspb.StartOperationResponse_SyncSuccess:
			c.metricsHandler = c.metricsHandler.WithTags(metrics.OutcomeTag("sync_success"))
			return &nexus.HandlerStartOperationResultSync[any]{
				Value: t.SyncSuccess.GetPayload(),
			}, parseLinks(t.SyncSuccess.GetLinks(), c.logger), nil

		case *nexuspb.StartOperationResponse_AsyncSuccess:
			c.metricsHandler = c.metricsHandler.WithTags(metrics.OutcomeTag("async_success"))
			token := t.AsyncSuccess.GetOperationToken()
			if token == "" {
				// Workers predating the operation-token rename only set the operation ID.
				//nolint:staticcheck // Deprecated, still sent by older workers.
				token = t.AsyncSuccess.GetOperationId()
			}
			return &nexus.HandlerStartOperationResultAsync{
				OperationToken: token,
			}, parseLinks(t.AsyncSuccess.GetLinks(), c.logger), nil

		case *nexuspb.StartOperationResponse_OperationError: //nolint:staticcheck // Deprecated, still sent by older workers.
			c.metricsHandler = c.metricsHandler.WithTags(metrics.OutcomeTag("operation_error"))
			c.setFailureSource(commonnexus.FailureSourceWorker)
			cause := &nexus.FailureError{
				//nolint:staticcheck // Deprecated function still in use for backward compatibility.
				Failure: commonnexus.ProtoFailureToNexusFailure(t.OperationError.GetFailure()),
			}
			//nolint:staticcheck // Deprecated field on a deprecated variant.
			state := nexus.OperationState(t.OperationError.GetOperationState())
			return nil, nil, c.operationError(state, cause, operation)

		case *nexuspb.StartOperationResponse_Failure:
			// Set the failure source to "worker" if we've reached this case.
			// Failure conversions errors below are the user's fault, as it implies that malformed completions were sent from
			// the worker.
			c.metricsHandler = c.metricsHandler.WithTags(metrics.OutcomeTag("failure"))
			c.setFailureSource(commonnexus.FailureSourceWorker)
			cause, internalErr := c.convertWorkerFailure(t.Failure, operation)
			if internalErr != nil {
				return nil, nil, internalErr
			}
			state := nexus.OperationStateFailed
			if t.Failure.GetCanceledFailureInfo() != nil {
				state = nexus.OperationStateCanceled
			}
			return nil, nil, c.operationError(state, cause, operation)
		}
	}
	// This is the worker's fault.
	c.metricsHandler = c.metricsHandler.WithTags(metrics.OutcomeTag("handler_error:EMPTY_OUTCOME"))
	c.setFailureSource(commonnexus.FailureSourceWorker)

	return nil, nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "empty outcome")
}

// cancelOperationOutcome converts matching's response to a CancelOperation dispatch into the error the
// Nexus SDK expects, recording the metrics outcome tag and the failure-source response header along
// the way. A nil error means the cancel was accepted.
func (c *operationContext) cancelOperationOutcome(
	resp *matchingservice.DispatchNexusTaskResponse,
	operation string,
) error {
	switch t := resp.GetOutcome().(type) {
	case *matchingservice.DispatchNexusTaskResponse_Failure:
		c.metricsHandler = c.metricsHandler.WithTags(metrics.OutcomeTag("handler_error:" + t.Failure.GetNexusHandlerFailureInfo().GetType()))
		// Set the failure source to "worker" if we've reached this case.
		// Failure conversions errors below are the user's fault, as it implies that malformed completions were sent from
		// the worker.
		c.setFailureSource(commonnexus.FailureSourceWorker)
		he, internalErr := c.convertWorkerFailure(t.Failure, operation)
		if internalErr != nil {
			return internalErr
		}
		return he

	case *matchingservice.DispatchNexusTaskResponse_HandlerError: //nolint:staticcheck // Deprecated, still sent by older workers.
		// Deprecated case. Replaced with DispatchNexusTaskResponse_Failure
		//nolint:staticcheck // Deprecated field on a deprecated variant.
		c.metricsHandler = c.metricsHandler.WithTags(metrics.OutcomeTag("handler_error:" + t.HandlerError.GetErrorType()))
		c.setFailureSource(commonnexus.FailureSourceWorker)
		return convertOutcomeToNexusHandlerError(t)

	case *matchingservice.DispatchNexusTaskResponse_RequestTimeout:
		c.metricsHandler = c.metricsHandler.WithTags(metrics.OutcomeTag("handler_timeout"))
		c.setFailureSource(commonnexus.FailureSourceWorker)
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeUpstreamTimeout, "upstream timeout")

	case *matchingservice.DispatchNexusTaskResponse_Response:
		// A cancel response carries no fields, so any response means the worker accepted.
		c.metricsHandler = c.metricsHandler.WithTags(metrics.OutcomeTag("success"))
		return nil
	}
	// This is the worker's fault.
	c.metricsHandler = c.metricsHandler.WithTags(metrics.OutcomeTag("handler_error:EMPTY_OUTCOME"))
	c.setFailureSource(commonnexus.FailureSourceWorker)

	return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "empty outcome")
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

// convertOutcomeToNexusHandlerError converts the deprecated handler error outcome into the Nexus
// handler error to report to the caller.
func convertOutcomeToNexusHandlerError(resp *matchingservice.DispatchNexusTaskResponse_HandlerError) *nexus.HandlerError { //nolint:staticcheck // Deprecated, still sent by older workers.
	var retryBehavior nexus.HandlerErrorRetryBehavior
	// nolint:exhaustive // unspecified is the default
	switch resp.HandlerError.RetryBehavior {
	case enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_RETRYABLE:
		retryBehavior = nexus.HandlerErrorRetryBehaviorRetryable
	case enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_NON_RETRYABLE:
		retryBehavior = nexus.HandlerErrorRetryBehaviorNonRetryable
	}
	// nolint:staticcheck // Deprecated function still in use for backward compatibility.
	cause := commonnexus.ProtoFailureToNexusFailure(resp.HandlerError.GetFailure())
	return &nexus.HandlerError{
		// nolint:staticcheck // Deprecated function still in use for backward compatibility.
		Type:          nexus.HandlerErrorType(resp.HandlerError.GetErrorType()),
		RetryBehavior: retryBehavior,
		Cause:         &nexus.FailureError{Failure: cause},
	}
}
