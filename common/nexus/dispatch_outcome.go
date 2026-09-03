package nexus

import (
	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/metrics"
)

// DispatchOutcome names the arm of matching's DispatchNexusTaskResponse that came back from a
// DispatchNexusTask call. The nested oneofs and the deprecated variants collapse into one flat set of
// cases. (Including those from a worker sending the deprecated failure responses, those will get
// silently converted into the newer forms.)
//
// The zero value is the empty string and is not a valid outcome, so a switch over a DispatchOutcome
// needs a default clause. For metric tags use DispatchResult.OutcomeTag, not the string value.
type DispatchOutcome string

const (
	// DispatchOutcomeUnrecognized is a response this build cannot interpret: no outcome set, an
	// outcome variant added after this build, or a worker answer that carries nothing usable.
	DispatchOutcomeUnrecognized DispatchOutcome = "unrecognized-outcome"

	// DispatchOutcomeSyncSuccess means the worker ran the operation to completion inline.
	DispatchOutcomeSyncSuccess DispatchOutcome = "sync-success"

	// DispatchOutcomeAsyncSuccess means the worker started the operation; the result arrives later,
	// out of band.
	DispatchOutcomeAsyncSuccess DispatchOutcome = "async-success"

	// DispatchOutcomeCancelAccepted means the worker accepted the cancellation request.
	DispatchOutcomeCancelAccepted DispatchOutcome = "cancel-accepted"

	// DispatchOutcomeOperationFailure means the worker ran the operation and it resolved as failed
	// or canceled. The task was delivered and answered; just the operation itself did not succeed.
	//
	// e.g. the Nexus handler fails with:
	//     nexus.NewOperationFailedError("insufficient funds")
	//     nexus.NewOperationCanceledError("already canceled upstream")
	DispatchOutcomeOperationFailure DispatchOutcome = "operation-failure"

	// DispatchOutcomeHandlerFailure means the worker answered with a Nexus handler error, whose retry
	// behavior says whether another attempt is worthwhile. The handler can return one itself, or the
	// worker can fail the task before the handler runs.
	//
	// e.g.
	//     nexus.NewHandlerErrorf(nexus.HandlerErrorTypeNotFound, "no such workflow")
	//     nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "cannot deserialize input")
	DispatchOutcomeHandlerFailure DispatchOutcome = "nexus-handler-failure"

	// DispatchOutcomeWorkerFailure means the worker failed the task with a failure carrying no Nexus
	// handler failure info. RespondNexusTaskFailed rejects such a response, so matching cannot produce
	// this outcome today. It exists so that a malformed response is classified rather than read as a
	// handler error with no type.
	DispatchOutcomeWorkerFailure DispatchOutcome = "worker-failure"

	// DispatchOutcomeRequestTimeout means matching gave up before the task was answered: no worker
	// was polling the task queue, or a worker took the task and never responded.
	DispatchOutcomeRequestTimeout DispatchOutcome = "request-timeout"
)

// Succeeded reports whether the worker accepted the request. An asynchronous start counts: the worker
// took responsibility for the operation, even though it has not finished it.
func (o DispatchOutcome) Succeeded() bool {
	switch o {
	case DispatchOutcomeSyncSuccess, DispatchOutcomeAsyncSuccess, DispatchOutcomeCancelAccepted:
		return true
	default:
		return false
	}
}

// DispatchResult is the classified form of a DispatchNexusTaskResponse: the outcome, plus whatever
// that arm of the response carried, hoisted out of the nested oneofs.
type DispatchResult struct {
	Outcome DispatchOutcome

	// OperationResult is the operation's result. Set for DispatchOutcomeSyncSuccess, where it may still
	// be nil: an operation is allowed to succeed with no value.
	OperationResult *commonpb.Payload

	// OperationToken is set for DispatchOutcomeAsyncSuccess.
	OperationToken string

	// Links are the handler links the worker attached to a successful start. Set for
	// DispatchOutcomeSyncSuccess and DispatchOutcomeAsyncSuccess.
	Links []*nexuspb.Link

	// Failure is the Temporal failure the worker reported. Set for DispatchOutcomeHandlerFailure,
	// DispatchOutcomeWorkerFailure and DispatchOutcomeOperationFailure.
	//
	// IMPORTANT: For the current response formats this aliases the proto inside the response rather
	// than copying it, so callers that convert it in place mutate the response too.
	Failure *failurepb.Failure

	// usedDeprecatedFormat records that the worker answered in a format predating Temporal failure
	// responses. Outcome and Failure are normalized either way; only the metric tag differs.
	usedDeprecatedFormat bool
}

// baseClassifyDispatchNexusTaskResponse converts a DispatchNexusTaskResponse into a DispatchResult.
//
// onResponseFn classifies a Response outcome. The DispatchNexusTaskResponse proto is shared by every
// kind of Nexus task, so only the caller knows how to interpret a succesful response.
func baseClassifyDispatchNexusTaskResponse(
	resp *matchingservice.DispatchNexusTaskResponse,
	onResponseFn func(*nexuspb.StartOperationResponse) DispatchResult,
) DispatchResult {
	switch t := resp.GetOutcome().(type) {
	case *matchingservice.DispatchNexusTaskResponse_Failure:
		// A handler error is a Nexus-level refusal whose retry behavior is meaningful; anything else
		// is an arbitrary failure the worker chose to report.
		outcome := DispatchOutcomeWorkerFailure
		if t.Failure.GetNexusHandlerFailureInfo() != nil {
			outcome = DispatchOutcomeHandlerFailure
		}
		return DispatchResult{Outcome: outcome, Failure: t.Failure}

	case *matchingservice.DispatchNexusTaskResponse_HandlerError: //nolint:staticcheck // Deprecated, still sent by older workers.
		return DispatchResult{
			Outcome: DispatchOutcomeHandlerFailure,
			//nolint:staticcheck // Deprecated field on a deprecated variant.
			Failure:              deprecatedHandlerErrorToFailure(t.HandlerError),
			usedDeprecatedFormat: true,
		}

	case *matchingservice.DispatchNexusTaskResponse_RequestTimeout:
		return DispatchResult{Outcome: DispatchOutcomeRequestTimeout}

	case *matchingservice.DispatchNexusTaskResponse_Response:
		// Only the caller knows how to interpret a successful response, so defer to onResponseFn.
		return onResponseFn(t.Response.GetStartOperation())

	default:
		return DispatchResult{Outcome: DispatchOutcomeUnrecognized}
	}
}

// classifyStartOperationResponse classifies the response a worker gave to a StartOperation request.
func classifyStartOperationResponse(resp *nexuspb.StartOperationResponse) DispatchResult {
	switch t := resp.GetVariant().(type) {
	case *nexuspb.StartOperationResponse_SyncSuccess:
		return DispatchResult{
			Outcome:         DispatchOutcomeSyncSuccess,
			OperationResult: t.SyncSuccess.GetPayload(),
			Links:           t.SyncSuccess.GetLinks(),
		}

	case *nexuspb.StartOperationResponse_AsyncSuccess:
		token := t.AsyncSuccess.GetOperationToken()
		if token == "" {
			// Workers predating the operation-token rename only set the operation ID.
			//nolint:staticcheck // Deprecated, still sent by older workers.
			token = t.AsyncSuccess.GetOperationId()
		}
		return DispatchResult{
			Outcome:        DispatchOutcomeAsyncSuccess,
			OperationToken: token,
			Links:          t.AsyncSuccess.GetLinks(),
		}

	case *nexuspb.StartOperationResponse_Failure:
		return DispatchResult{
			Outcome: DispatchOutcomeOperationFailure,
			Failure: t.Failure,
		}

	case *nexuspb.StartOperationResponse_OperationError: //nolint:staticcheck // Deprecated, still sent by older workers.
		return DispatchResult{
			Outcome: DispatchOutcomeOperationFailure,
			//nolint:staticcheck // Deprecated field on a deprecated variant.
			Failure:              deprecatedOperationErrorToFailure(t.OperationError),
			usedDeprecatedFormat: true,
		}

	default:
		return DispatchResult{Outcome: DispatchOutcomeUnrecognized}
	}
}

// ClassifyStartOperationDispatch classifies matching's response to a dispatched StartOperation task.
func ClassifyStartOperationDispatch(resp *matchingservice.DispatchNexusTaskResponse) DispatchResult {
	return baseClassifyDispatchNexusTaskResponse(resp, classifyStartOperationResponse)
}

// ClassifyCancelOperationDispatch classifies matching's response to a dispatched CancelOperation task.
func ClassifyCancelOperationDispatch(resp *matchingservice.DispatchNexusTaskResponse) DispatchResult {
	return baseClassifyDispatchNexusTaskResponse(
		resp,
		func(*nexuspb.StartOperationResponse) DispatchResult {
			// A cancel response carries no fields, so any response means the worker accepted.
			return DispatchResult{Outcome: DispatchOutcomeCancelAccepted}
		})
}

// deprecatedHandlerErrorToFailure converts the deprecated nexuspb.HandlerError into the new format,
// a Temporal failurepb.Failure.
func deprecatedHandlerErrorToFailure(handlerErr *nexuspb.HandlerError) *failurepb.Failure {
	failure := &failurepb.Failure{
		FailureInfo: &failurepb.Failure_NexusHandlerFailureInfo{
			NexusHandlerFailureInfo: &failurepb.NexusHandlerFailureInfo{
				Type:          handlerErr.GetErrorType(),
				RetryBehavior: handlerErr.GetRetryBehavior(),
			},
		},
	}
	if handlerErr.GetFailure() != nil {
		failure.Cause = convertNexusFailureToTemporalFailure(handlerErr.GetFailure())
	}
	return failure
}

// deprecatedOperationErrorToFailure converts the deprecated nexuspb.UnsuccessfulOperationError into
// the new format, a Temporal failurepb.Failure proto.
//
// The deprecated variant reports the operation state in a field of its own and sends the handler's
// failure bare. The current format has no state field: the failure that wraps the handler's failure is
// what reports the state, canceled or failed. So the wrapper is rebuilt here from the state field.
// Workers converting in the other direction drop the wrapper and keep its cause.
func deprecatedOperationErrorToFailure(opErr *nexuspb.UnsuccessfulOperationError) *failurepb.Failure {
	cause := convertNexusFailureToTemporalFailure(opErr.GetFailure())
	// The deprecated variant carries no message of its own, so the wrapper repeats the cause's.
	failure := &failurepb.Failure{Message: cause.GetMessage(), Cause: cause}

	if nexus.OperationState(opErr.GetOperationState()) == nexus.OperationStateCanceled {
		failure.FailureInfo = &failurepb.Failure_CanceledFailureInfo{
			CanceledFailureInfo: &failurepb.CanceledFailureInfo{},
		}
	} else {
		failure.FailureInfo = &failurepb.Failure_ApplicationFailureInfo{
			ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
				Type:         "OperationError",
				NonRetryable: true,
			},
		}
	}
	return failure
}

// convertNexusFailureToTemporalFailure converts the deprecated nexuspb.Failure into the new format,
// a Temporal failurepb.Failure.
func convertNexusFailureToTemporalFailure(nexusFailure *nexuspb.Failure) *failurepb.Failure {
	//nolint:staticcheck // Deprecated function still in use for backward compatibility.
	converted, err := NexusFailureToTemporalFailure(ProtoFailureToNexusFailure(nexusFailure))
	// A failure that cannot be re-encoded falls back to its message. We know the operation failed,
	// we just don't recognize the format of the data in its cause/details.
	if err != nil {
		return &failurepb.Failure{
			Message: nexusFailure.GetMessage(),
			FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
				ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
					Type:         "NexusFailure",
					NonRetryable: false,
				},
			},
		}
	}
	return converted
}

// OutcomeTag returns the metrics outcome tag for a dispatch.
//
// A handler failure reports its error type as the tag's suffix. A type outside the Nexus spec, or a
// failure with no type at all, reports "handler_error:UNKNOWN" so that a worker cannot mint new time
// series.
func (r DispatchResult) OutcomeTag() metrics.Tag {
	return metrics.OutcomeTag(r.metricOutcome())
}

func (r DispatchResult) metricOutcome() string {
	// NOTE: Some of these are confusing (e.g. "success" for CancelAccepted), but
	// changing these would break existing dashboards.
	switch r.Outcome {
	case DispatchOutcomeSyncSuccess:
		return "sync_success"
	case DispatchOutcomeAsyncSuccess:
		return "async_success"
	case DispatchOutcomeCancelAccepted:
		return "success"
	case DispatchOutcomeOperationFailure:
		if r.usedDeprecatedFormat {
			return "operation_error"
		}
		return "failure"
	case DispatchOutcomeHandlerFailure,
		DispatchOutcomeWorkerFailure:
		// A worker failure has no handler error type to report and will map to UNKNOWN.
		hErrType := r.Failure.GetNexusHandlerFailureInfo().GetType()
		return "handler_error:" + boundHandlerErrorType(hErrType)
	case DispatchOutcomeRequestTimeout:
		return "handler_timeout"
	case DispatchOutcomeUnrecognized:
		fallthrough
	default:
		return "handler_error:EMPTY_OUTCOME"
	}
}

// handlerErrorTypes are the handler error types that may appear verbatim in a metric tag.
// Keep in sync with the HandlerErrorType consts in nexus-rpc/sdk-go/nexus/errors.go.
var handlerErrorTypes = map[string]struct{}{
	string(nexus.HandlerErrorTypeBadRequest):        {},
	string(nexus.HandlerErrorTypeUnauthenticated):   {},
	string(nexus.HandlerErrorTypeUnauthorized):      {},
	string(nexus.HandlerErrorTypeNotFound):          {},
	string(nexus.HandlerErrorTypeRequestTimeout):    {},
	string(nexus.HandlerErrorTypeConflict):          {},
	string(nexus.HandlerErrorTypeResourceExhausted): {},
	string(nexus.HandlerErrorTypeInternal):          {},
	string(nexus.HandlerErrorTypeNotImplemented):    {},
	string(nexus.HandlerErrorTypeUnavailable):       {},
	string(nexus.HandlerErrorTypeUpstreamTimeout):   {},
}

// boundHandlerErrorType bounds the metric cardinality a worker can introduce through a handler error
// type. Types in the Nexus spec pass through; anything else, including the empty string, collapses to
// UNKNOWN.
func boundHandlerErrorType(errType string) string {
	if _, ok := handlerErrorTypes[errType]; ok {
		return errType
	}
	return "UNKNOWN"
}
