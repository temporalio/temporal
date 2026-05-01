package nexusoperation

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"go.temporal.io/server/common/backoff"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/softassert"
	queueserrors "go.temporal.io/server/service/history/queues/errors"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	EndpointSearchAttribute  = chasm.NewSearchAttributeKeyword("Endpoint", chasm.SearchAttributeFieldKeyword01)
	ServiceSearchAttribute   = chasm.NewSearchAttributeKeyword("Service", chasm.SearchAttributeFieldKeyword02)
	OperationSearchAttribute = chasm.NewSearchAttributeKeyword("Operation", chasm.SearchAttributeFieldKeyword03)
	RequestIDSearchAttribute = chasm.NewSearchAttributeKeyword("RequestId", chasm.SearchAttributeFieldKeyword04)
	StatusSearchAttribute    = chasm.NewSearchAttributeKeyword("ExecutionStatus", chasm.SearchAttributeFieldLowCardinalityKeyword01)
)

var _ chasm.Component = (*Operation)(nil)
var _ chasm.RootComponent = (*Operation)(nil)
var _ chasm.StateMachine[nexusoperationpb.OperationStatus] = (*Operation)(nil)
var _ chasm.VisibilitySearchAttributesProvider = (*Operation)(nil)
var _ chasm.NexusCompletionHandler = (*Operation)(nil)

// ErrCancellationAlreadyRequested is returned when a cancellation has already been requested for an operation.
var ErrCancellationAlreadyRequested = serviceerror.NewFailedPrecondition("cancellation already requested")

// ErrOperationAlreadyCompleted is returned when trying to cancel an operation that has already completed.
var ErrOperationAlreadyCompleted = serviceerror.NewFailedPrecondition("operation already completed")

// InvocationData contains data needed to invoke a Nexus operation.
type InvocationData struct {
	// Input is the operation input payload.
	Input *commonpb.Payload
	// Header contains the Nexus headers for the operation.
	Header map[string]string
	// NexusLinks are the links to the caller(s) that scheduled this operation.
	NexusLinks []nexus.Link
}

// TaskGroupName groups invocation and cancellation together for the outbound queue
const TaskGroupName = "nexus"

// OperationStore defines the interface that must be implemented by any parent component that wants to manage Nexus operations.
// It's the responsibility of the parent component to apply the appropriate state transitions to the operation.
type OperationStore interface {
	OnNexusOperationStarted(ctx chasm.MutableContext, operation *Operation, operationToken string, startTime *time.Time, links []*commonpb.Link) error
	OnNexusOperationCanceled(ctx chasm.MutableContext, operation *Operation, cause *failurepb.Failure) error
	OnNexusOperationFailed(ctx chasm.MutableContext, operation *Operation, cause *failurepb.Failure) error
	OnNexusOperationTimedOut(ctx chasm.MutableContext, operation *Operation, cause *failurepb.Failure, fromAttempt bool) error
	OnNexusOperationCompleted(ctx chasm.MutableContext, operation *Operation, result *commonpb.Payload, links []*commonpb.Link) error
	OnNexusOperationCancellationCompleted(ctx chasm.MutableContext, operation *Operation) error
	OnNexusOperationCancellationFailed(ctx chasm.MutableContext, operation *Operation, cause *failurepb.Failure) error
	// NexusOperationInvocationData loads invocation data (Input, Header, NexusLinks) from the scheduled history event.
	NexusOperationInvocationData(ctx chasm.Context, operation *Operation) (InvocationData, error)
}

// Operation is a CHASM component that represents a Nexus operation.
type Operation struct {
	chasm.UnimplementedComponent

	// Persisted internal state
	*nexusoperationpb.OperationState

	// Pointer to an implementation of the "store". For a workflow-based Nexus operation
	// this is a parent pointer back to the workflow. For a standalone Nexus operation this is nil.
	Store chasm.ParentPtr[OperationStore]

	RequestData chasm.Field[*nexusoperationpb.OperationRequestData]
	// Cancellation is a child component that manages sending the cancel request to the Nexus endpoint.
	// Created when cancellation is requested, nil otherwise.
	Cancellation chasm.Field[*Cancellation]
	Outcome      chasm.Field[*nexusoperationpb.OperationOutcome]
	Visibility   chasm.Field[*chasm.Visibility]
}

// NewOperation creates a new Operation component with the given persisted state.
func NewOperation(state *nexusoperationpb.OperationState) *Operation {
	return &Operation{OperationState: state}
}

func newStandaloneOperation(
	ctx chasm.MutableContext,
	req *nexusoperationpb.StartNexusOperationRequest,
) (*Operation, error) {
	frontendReq := req.GetFrontendRequest()
	op := NewOperation(&nexusoperationpb.OperationState{
		EndpointId:             req.GetEndpointId(),
		Endpoint:               frontendReq.GetEndpoint(),
		Service:                frontendReq.GetService(),
		Operation:              frontendReq.GetOperation(),
		ScheduleToCloseTimeout: frontendReq.GetScheduleToCloseTimeout(),
		ScheduleToStartTimeout: frontendReq.GetScheduleToStartTimeout(),
		StartToCloseTimeout:    frontendReq.GetStartToCloseTimeout(),
		ScheduledTime:          timestamppb.New(ctx.Now(nil)),
		RequestId:              uuid.NewString(),
	})
	op.RequestData = chasm.NewDataField(ctx, &nexusoperationpb.OperationRequestData{
		Input:        frontendReq.GetInput(),
		NexusHeader:  frontendReq.GetNexusHeader(),
		UserMetadata: frontendReq.GetUserMetadata(),
		Identity:     frontendReq.GetIdentity(),
	})
	op.Outcome = chasm.NewDataField(ctx, &nexusoperationpb.OperationOutcome{})
	op.Visibility = chasm.NewComponentField(ctx, chasm.NewVisibilityWithData(
		ctx,
		frontendReq.GetSearchAttributes().GetIndexedFields(),
		nil,
	))
	if err := TransitionScheduled.Apply(op, ctx, EventScheduled{}); err != nil {
		return nil, err
	}
	return op, nil
}

// LifecycleState maps the operation's status to a CHASM lifecycle state.
func (o *Operation) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	switch o.Status {
	case nexusoperationpb.OPERATION_STATUS_SUCCEEDED:
		return chasm.LifecycleStateCompleted
	case nexusoperationpb.OPERATION_STATUS_FAILED,
		nexusoperationpb.OPERATION_STATUS_CANCELED,
		nexusoperationpb.OPERATION_STATUS_TIMED_OUT,
		nexusoperationpb.OPERATION_STATUS_TERMINATED:
		return chasm.LifecycleStateFailed
	default:
		return chasm.LifecycleStateRunning
	}
}

func (o *Operation) ContextMetadata(_ chasm.Context) map[string]string {
	return nil
}

// StateMachineState returns the current operation status.
func (o *Operation) StateMachineState() nexusoperationpb.OperationStatus {
	return o.Status
}

// SetStateMachineState sets the operation status.
func (o *Operation) SetStateMachineState(status nexusoperationpb.OperationStatus) {
	o.Status = status
}

// RequestCancel requests cancellation of the operation. It creates a Cancellation child component and, if the
// operation has already started, schedules the cancellation request to be sent to the Nexus endpoint.
func (o *Operation) RequestCancel(
	ctx chasm.MutableContext,
	req *nexusoperationpb.CancellationState,
) error {
	if !TransitionCanceled.Possible(o) {
		return ErrOperationAlreadyCompleted
	}

	if existingCancellation, ok := o.Cancellation.TryGet(ctx); ok {
		existingReqID := existingCancellation.GetRequestId()
		newReqID := req.GetRequestId()
		if existingReqID != newReqID {
			return fmt.Errorf("%w with request ID %s", ErrCancellationAlreadyRequested, existingReqID)
		}
		return nil
	}

	cancel := newCancellation(req)
	o.Cancellation = chasm.NewComponentField(ctx, cancel)
	// Once started, the handler returns a token that can be used in the cancellation request.
	// Until then, no need to schedule the cancellation.
	if o.Status == nexusoperationpb.OPERATION_STATUS_STARTED {
		return TransitionCancellationScheduled.Apply(cancel, ctx, EventCancellationScheduled{
			Destination: o.GetEndpoint(),
		})
	}
	return nil
}

// onStarted applies the started transition or delegates to the store if one is present.
func (o *Operation) onStarted(ctx chasm.MutableContext, operationToken string, startTime *time.Time, links []*commonpb.Link) error {
	if store, ok := o.Store.TryGet(ctx); ok {
		return store.OnNexusOperationStarted(ctx, o, operationToken, startTime, links)
	}
	o.Links = append(o.Links, links...)
	return TransitionStarted.Apply(o, ctx, EventStarted{
		OperationToken: operationToken,
		StartTime:      startTime,
	})
}

// onCompleted applies the succeeded transition or delegates to the store if one is present.
func (o *Operation) onCompleted(ctx chasm.MutableContext, result *commonpb.Payload, links []*commonpb.Link) error {
	if store, ok := o.Store.TryGet(ctx); ok {
		return store.OnNexusOperationCompleted(ctx, o, result, links)
	}
	o.Links = append(o.Links, links...)
	return TransitionSucceeded.Apply(o, ctx, EventSucceeded{Result: result})
}

// onFailed applies the failed transition or delegates to the store if one is present.
func (o *Operation) onFailed(ctx chasm.MutableContext, cause *failurepb.Failure) error {
	if store, ok := o.Store.TryGet(ctx); ok {
		return store.OnNexusOperationFailed(ctx, o, cause)
	}
	return TransitionFailed.Apply(o, ctx, EventFailed{Failure: cause})
}

// onCanceled applies the canceled transition or delegates to the store if one is present.
func (o *Operation) onCanceled(ctx chasm.MutableContext, cause *failurepb.Failure) error {
	if store, ok := o.Store.TryGet(ctx); ok {
		return store.OnNexusOperationCanceled(ctx, o, cause)
	}
	return TransitionCanceled.Apply(o, ctx, EventCanceled{Failure: cause})
}

// onTimedOut applies the timed out transition or delegates to the store if one is present.
func (o *Operation) onTimedOut(ctx chasm.MutableContext, cause *failurepb.Failure, fromAttempt bool) error {
	if store, ok := o.Store.TryGet(ctx); ok {
		return store.OnNexusOperationTimedOut(ctx, o, cause, fromAttempt)
	}
	return TransitionTimedOut.Apply(o, ctx, EventTimedOut{
		Failure:     cause,
		FromAttempt: fromAttempt,
	})
}

// HandleNexusCompletion handles the outcome of an asynchronous completion callback.
func (o *Operation) HandleNexusCompletion(
	ctx chasm.MutableContext,
	completion *persistencespb.ChasmNexusCompletion,
) error {
	// Request ID lets us reject a stale or misrouted completion.
	if completion.GetRequestId() != "" && o.GetRequestId() != completion.GetRequestId() {
		return serviceerror.NewNotFound("operation not found")
	}

	links := completion.GetLinks()

	// For completion-before-start, apply the started transition first.
	if o.GetStatus() == nexusoperationpb.OPERATION_STATUS_SCHEDULED {
		startTime := timestamp.TimeValuePtr(completion.GetStartTime())
		if err := o.onStarted(ctx, completion.GetOperationToken(), startTime, links); err != nil {
			return err
		}
		// Links belong only to the synthetic started event.
		links = nil
	}

	switch outcome := completion.Outcome.(type) {
	case *persistencespb.ChasmNexusCompletion_Success:
		return o.onCompleted(ctx, outcome.Success, nil)
	case *persistencespb.ChasmNexusCompletion_Failure:
		if outcome.Failure.GetCanceledFailureInfo() != nil {
			return o.onCanceled(ctx, outcome.Failure)
		}
		return o.onFailed(ctx, outcome.Failure)
	default:
		return serviceerror.NewInvalidArgument("invalid completion outcome")
	}
}

// loadStartArgs is a ReadComponent callback that loads the start arguments from the operation.
func (o *Operation) loadStartArgs(
	ctx chasm.Context,
	_ chasm.NoValue,
) (startArgs, error) {
	var (
		invocationData InvocationData
		err            error
	)
	if store, ok := o.Store.TryGet(ctx); ok {
		invocationData, err = store.NexusOperationInvocationData(ctx, o)
		if err != nil {
			return startArgs{}, err
		}
	} else {
		requestData := o.RequestData.Get(ctx)
		invocationData = InvocationData{
			Input:  requestData.GetInput(),
			Header: requestData.GetNexusHeader(),
		}
	}
	invocationData.NexusLinks = append(invocationData.NexusLinks,
		commonnexus.ConvertLinkNexusOperationToNexusLink(&commonpb.Link_NexusOperation{
			Namespace:   ctx.NamespaceEntry().Name().String(),
			OperationId: ctx.ExecutionKey().BusinessID,
			RunId:       ctx.ExecutionKey().RunID,
		}))

	serializedRef, err := ctx.Ref(o)
	if err != nil {
		return startArgs{}, err
	}

	return startArgs{
		endpointName:           o.GetEndpoint(),
		endpointID:             o.GetEndpointId(),
		service:                o.GetService(),
		operation:              o.GetOperation(),
		requestID:              o.GetRequestId(),
		currentTime:            ctx.Now(o),
		scheduledTime:          o.GetScheduledTime().AsTime(),
		scheduleToCloseTimeout: o.GetScheduleToCloseTimeout().AsDuration(),
		scheduleToStartTimeout: o.GetScheduleToStartTimeout().AsDuration(),
		startToCloseTimeout:    o.GetStartToCloseTimeout().AsDuration(),
		payload:                invocationData.Input,
		header:                 invocationData.Header,
		nexusLinks:             invocationData.NexusLinks,
		serializedRef:          serializedRef,
	}, nil
}

// saveInvocationResultInput is the input to the Operation.saveInvocationResult method used in UpdateComponent.
type saveInvocationResultInput struct {
	result      invocationResult
	retryPolicy backoff.RetryPolicy
}

// saveInvocationResult handles the outcome of the initial start call.
func (o *Operation) saveInvocationResult(
	ctx chasm.MutableContext,
	input saveInvocationResultInput,
) (chasm.NoValue, error) {
	switch r := input.result.(type) {
	case invocationResultOK:
		links := convertResponseLinks(r.response.Links, ctx.Logger())
		if r.response.Pending != nil {
			// An async operation transitions to STARTED here;
			// HandleNexusCompletion will apply its outcome from the completion callback.
			return nil, o.onStarted(ctx, r.response.Pending.Token, nil, links)
		}
		return nil, o.onCompleted(ctx, r.response.Successful, links)
	case invocationResultCancel:
		return nil, o.onCanceled(ctx, r.failure)
	case invocationResultFail:
		return nil, o.onFailed(ctx, r.failure)
	case invocationResultTimeout:
		return nil, o.onTimedOut(ctx, r.failure, true)
	case invocationResultRetry:
		return nil, transitionAttemptFailed.Apply(o, ctx, EventAttemptFailed{
			Failure:     r.failure,
			RetryPolicy: input.retryPolicy,
		})
	default:
		return nil, queueserrors.NewUnprocessableTaskError(fmt.Sprintf("unrecognized invocation result %T", r))
	}
}

// resolveUnsuccessfully finalizes the operation. When fromAttempt is true, the failure is recorded as
// LastAttemptFailure. Otherwise the failure is recorded as the terminal Outcome.
func (o *Operation) resolveUnsuccessfully(ctx chasm.MutableContext, failure *failurepb.Failure, closeTime time.Time, fromAttempt bool) error {
	softassert.That(ctx.Logger(), failure != nil, "resolveUnsuccessfully called with nil failure")
	if fromAttempt {
		o.LastAttemptCompleteTime = timestamppb.New(ctx.Now(o))
		o.LastAttemptFailure = failure
	} else {
		o.getOrCreateOutcome(ctx).Variant = &nexusoperationpb.OperationOutcome_Failed_{
			Failed: &nexusoperationpb.OperationOutcome_Failed{Failure: failure},
		}
	}
	o.ClosedTime = timestamppb.New(closeTime)

	// NextAttemptScheduleTime is only valid in BACKING_OFF; clear on close
	o.NextAttemptScheduleTime = nil
	return nil
}

func (o *Operation) getOrCreateOutcome(ctx chasm.MutableContext) *nexusoperationpb.OperationOutcome {
	if outcome, ok := o.Outcome.TryGet(ctx); ok {
		return outcome
	}
	outcome := &nexusoperationpb.OperationOutcome{}
	o.Outcome = chasm.NewDataField(ctx, outcome)
	return outcome
}

func (o *Operation) Terminate(
	ctx chasm.MutableContext,
	req chasm.TerminateComponentRequest,
) (chasm.TerminateComponentResponse, error) {
	if o.GetTerminateState() != nil {
		if existingReqID := o.TerminateState.GetRequestId(); existingReqID != req.RequestID {
			return chasm.TerminateComponentResponse{},
				serviceerror.NewFailedPreconditionf("already terminated with request ID %s", existingReqID)
		}
		return chasm.TerminateComponentResponse{}, nil
	}
	return chasm.TerminateComponentResponse{}, TransitionTerminated.Apply(o, ctx, EventTerminated{TerminateComponentRequest: req})
}

func (o *Operation) SearchAttributes(_ chasm.Context) []chasm.SearchAttributeKeyValue {
	return []chasm.SearchAttributeKeyValue{
		EndpointSearchAttribute.Value(o.Endpoint),
		ServiceSearchAttribute.Value(o.Service),
		OperationSearchAttribute.Value(o.Operation),
		RequestIDSearchAttribute.Value(o.RequestId),
		StatusSearchAttribute.Value(operationExecutionStatus(o.Status).String()),
	}
}

func (o *Operation) buildDescribeResponse(
	ctx chasm.Context,
	req *nexusoperationpb.DescribeNexusOperationRequest,
) (*nexusoperationpb.DescribeNexusOperationResponse, error) {
	token, err := ctx.Ref(o)
	if err != nil {
		return nil, err
	}

	resp := &workflowservice.DescribeNexusOperationExecutionResponse{
		RunId:         ctx.ExecutionKey().RunID,
		Info:          o.buildExecutionInfo(ctx),
		LongPollToken: token,
	}
	if req.GetFrontendRequest().GetIncludeInput() {
		resp.Input = o.RequestData.Get(ctx).GetInput()
	}
	if req.GetFrontendRequest().GetIncludeOutcome() && o.isClosed() {
		if successful, failure := o.outcome(ctx); successful != nil {
			resp.Outcome = &workflowservice.DescribeNexusOperationExecutionResponse_Result{
				Result: successful,
			}
		} else if failure != nil {
			resp.Outcome = &workflowservice.DescribeNexusOperationExecutionResponse_Failure{
				Failure: failure,
			}
		}
	}
	return &nexusoperationpb.DescribeNexusOperationResponse{FrontendResponse: resp}, nil
}

func (o *Operation) buildPollResponse(
	ctx chasm.Context,
) *nexusoperationpb.PollNexusOperationResponse {
	resp := &workflowservice.PollNexusOperationExecutionResponse{
		RunId:          ctx.ExecutionKey().RunID,
		OperationToken: o.OperationToken,
	}

	if o.isClosed() {
		resp.WaitStage = enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED
		if successful, failure := o.outcome(ctx); successful != nil {
			resp.Outcome = &workflowservice.PollNexusOperationExecutionResponse_Result{
				Result: successful,
			}
		} else if failure != nil {
			resp.Outcome = &workflowservice.PollNexusOperationExecutionResponse_Failure{
				Failure: failure,
			}
		}
	} else {
		resp.WaitStage = enumspb.NEXUS_OPERATION_WAIT_STAGE_STARTED
	}

	return &nexusoperationpb.PollNexusOperationResponse{
		FrontendResponse: resp,
	}
}

func (o *Operation) outcome(ctx chasm.Context) (*commonpb.Payload, *failurepb.Failure) {
	if !o.isClosed() {
		return nil, nil
	}

	outcome, hasOutcome := o.Outcome.TryGet(ctx)

	switch {
	case !hasOutcome:
		return nil, o.LastAttemptFailure
	case outcome.GetSuccessful() != nil:
		return outcome.GetSuccessful().GetResult(), nil
	case outcome.GetFailed() != nil:
		return nil, outcome.GetFailed().GetFailure()
	default:
		softassert.Fail(ctx.Logger(), "operation outcome has no variant set")
		return nil, o.LastAttemptFailure
	}
}

func (o *Operation) isWaitStageReached(_ chasm.Context, waitStage enumspb.NexusOperationWaitStage) bool {
	switch waitStage {
	case enumspb.NEXUS_OPERATION_WAIT_STAGE_STARTED:
		return o.Status == nexusoperationpb.OPERATION_STATUS_STARTED || o.isClosed()
	case enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED:
		return o.isClosed()
	default:
		return false
	}
}

func (o *Operation) isClosed() bool {
	return o.LifecycleState(nil).IsClosed()
}

func (o *Operation) buildExecutionInfo(ctx chasm.Context) *nexuspb.NexusOperationExecutionInfo {
	requestData := o.RequestData.Get(ctx)
	key := ctx.ExecutionKey()
	info := &nexuspb.NexusOperationExecutionInfo{
		OperationId:             key.BusinessID,
		RunId:                   key.RunID,
		Endpoint:                o.Endpoint,
		Service:                 o.Service,
		Operation:               o.Operation,
		Status:                  operationExecutionStatus(o.Status),
		State:                   PendingOperationState(o.Status),
		ScheduleToCloseTimeout:  o.ScheduleToCloseTimeout,
		ScheduleToStartTimeout:  o.ScheduleToStartTimeout,
		StartToCloseTimeout:     o.StartToCloseTimeout,
		Attempt:                 o.Attempt,
		ScheduleTime:            o.ScheduledTime,
		LastAttemptCompleteTime: o.LastAttemptCompleteTime,
		LastAttemptFailure:      o.LastAttemptFailure,
		NextAttemptScheduleTime: o.NextAttemptScheduleTime,
		RequestId:               o.RequestId,
		OperationToken:          o.OperationToken,
		StateTransitionCount:    ctx.ExecutionInfo().StateTransitionCount,
		SearchAttributes: &commonpb.SearchAttributes{
			IndexedFields: o.Visibility.Get(ctx).CustomSearchAttributes(ctx),
		},
		NexusHeader:  requestData.GetNexusHeader(),
		UserMetadata: requestData.GetUserMetadata(),
		Links:        o.Links,
		Identity:     requestData.GetIdentity(),
	}

	if o.ScheduledTime != nil {
		if o.ScheduleToCloseTimeout != nil {
			info.ExpirationTime = timestamppb.New(o.ScheduledTime.AsTime().Add(o.ScheduleToCloseTimeout.AsDuration()))
		}
		if closeTime := o.closeTime(ctx); closeTime != nil {
			info.CloseTime = closeTime
			info.ExecutionDuration = durationpb.New(closeTime.AsTime().Sub(o.ScheduledTime.AsTime()))
		} else {
			info.ExecutionDuration = durationpb.New(ctx.Now(o).Sub(o.ScheduledTime.AsTime()))
		}
	}

	return info
}

func (o *Operation) closeTime(ctx chasm.Context) *timestamppb.Timestamp {
	if !o.LifecycleState(ctx).IsClosed() {
		return nil
	}
	return o.ClosedTime
}

func operationExecutionStatus(status nexusoperationpb.OperationStatus) enumspb.NexusOperationExecutionStatus {
	switch status {
	case nexusoperationpb.OPERATION_STATUS_SCHEDULED,
		nexusoperationpb.OPERATION_STATUS_BACKING_OFF,
		nexusoperationpb.OPERATION_STATUS_STARTED:
		return enumspb.NEXUS_OPERATION_EXECUTION_STATUS_RUNNING
	case nexusoperationpb.OPERATION_STATUS_SUCCEEDED:
		return enumspb.NEXUS_OPERATION_EXECUTION_STATUS_COMPLETED
	case nexusoperationpb.OPERATION_STATUS_FAILED:
		return enumspb.NEXUS_OPERATION_EXECUTION_STATUS_FAILED
	case nexusoperationpb.OPERATION_STATUS_CANCELED:
		return enumspb.NEXUS_OPERATION_EXECUTION_STATUS_CANCELED
	case nexusoperationpb.OPERATION_STATUS_TIMED_OUT:
		return enumspb.NEXUS_OPERATION_EXECUTION_STATUS_TIMED_OUT
	case nexusoperationpb.OPERATION_STATUS_TERMINATED:
		return enumspb.NEXUS_OPERATION_EXECUTION_STATUS_TERMINATED
	default:
		return enumspb.NEXUS_OPERATION_EXECUTION_STATUS_UNSPECIFIED
	}
}

// PendingOperationState maps a nexus operation status to the corresponding pending API state.
// Returns PENDING_NEXUS_OPERATION_STATE_UNSPECIFIED for non-pending or unspecified statuses.
func PendingOperationState(status nexusoperationpb.OperationStatus) enumspb.PendingNexusOperationState {
	switch status {
	case nexusoperationpb.OPERATION_STATUS_SCHEDULED:
		return enumspb.PENDING_NEXUS_OPERATION_STATE_SCHEDULED
	case nexusoperationpb.OPERATION_STATUS_BACKING_OFF:
		return enumspb.PENDING_NEXUS_OPERATION_STATE_BACKING_OFF
	case nexusoperationpb.OPERATION_STATUS_STARTED:
		return enumspb.PENDING_NEXUS_OPERATION_STATE_STARTED
	default:
		return enumspb.PENDING_NEXUS_OPERATION_STATE_UNSPECIFIED
	}
}
