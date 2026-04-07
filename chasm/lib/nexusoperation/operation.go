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
	"go.temporal.io/server/chasm"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"go.temporal.io/server/common/backoff"
	queueserrors "go.temporal.io/server/service/history/queues/errors"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	EndpointSearchAttribute  = chasm.NewSearchAttributeKeyword("Endpoint", chasm.SearchAttributeFieldKeyword01)
	ServiceSearchAttribute   = chasm.NewSearchAttributeKeyword("Service", chasm.SearchAttributeFieldKeyword02)
	OperationSearchAttribute = chasm.NewSearchAttributeKeyword("Operation", chasm.SearchAttributeFieldKeyword03)
	StatusSearchAttribute    = chasm.NewSearchAttributeKeyword("ExecutionStatus", chasm.SearchAttributeFieldLowCardinalityKeyword01)
)

var _ chasm.Component = (*Operation)(nil)
var _ chasm.RootComponent = (*Operation)(nil)
var _ chasm.StateMachine[nexusoperationpb.OperationStatus] = (*Operation)(nil)
var _ chasm.VisibilitySearchAttributesProvider = (*Operation)(nil)

var ErrCancellationAlreadyRequested = serviceerror.NewFailedPrecondition("cancellation already requested")
var ErrOperationAlreadyCompleted = serviceerror.NewFailedPrecondition("operation already completed")

type InvocationData struct {
	Input     *commonpb.Payload
	Header    map[string]string
	NexusLink nexus.Link
}

type OperationStore interface {
	OnNexusOperationStarted(ctx chasm.MutableContext, operation *Operation, operationToken string, links []*commonpb.Link) error
	OnNexusOperationCanceled(ctx chasm.MutableContext, operation *Operation, cause *failurepb.Failure) error
	OnNexusOperationFailed(ctx chasm.MutableContext, operation *Operation, cause *failurepb.Failure) error
	OnNexusOperationTimedOut(ctx chasm.MutableContext, operation *Operation, cause *failurepb.Failure) error
	OnNexusOperationCompleted(ctx chasm.MutableContext, operation *Operation, result *commonpb.Payload, links []*commonpb.Link) error
	OnNexusOperationCancellationCompleted(ctx chasm.MutableContext, operation *Operation) error
	OnNexusOperationCancellationFailed(ctx chasm.MutableContext, operation *Operation, cause *failurepb.Failure) error
	// NexusOperationInvocationData loads invocation data (Input, Header, NexusLink) from the scheduled history event.
	NexusOperationInvocationData(ctx chasm.Context, operation *Operation) (InvocationData, error)
}

type Operation struct {
	chasm.UnimplementedComponent
	*nexusoperationpb.OperationState

	Store chasm.ParentPtr[OperationStore]

	RequestData  chasm.Field[*nexusoperationpb.OperationRequestData]
	Cancellation chasm.Field[*Cancellation]
	Outcome      chasm.Field[*nexusoperationpb.OperationOutcome]
	Visibility   chasm.Field[*chasm.Visibility]
}

func NewOperation(state *nexusoperationpb.OperationState) *Operation {
	return &Operation{OperationState: state}
}

func newStandaloneOperation(
	ctx chasm.MutableContext,
	req *nexusoperationpb.StartNexusOperationRequest,
) (*Operation, error) {
	frontendReq := req.GetFrontendRequest()
	op := NewOperation(&nexusoperationpb.OperationState{
		Endpoint:               frontendReq.GetEndpoint(),
		Service:                frontendReq.GetService(),
		Operation:              frontendReq.GetOperation(),
		ScheduleToCloseTimeout: frontendReq.GetScheduleToCloseTimeout(),
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

func (o *Operation) StateMachineState() nexusoperationpb.OperationStatus {
	return o.Status
}

func (o *Operation) SetStateMachineState(status nexusoperationpb.OperationStatus) {
	o.Status = status
}

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
	if o.Status == nexusoperationpb.OPERATION_STATUS_STARTED {
		return TransitionCancellationScheduled.Apply(cancel, ctx, EventCancellationScheduled{
			Destination: o.GetEndpoint(),
		})
	}
	return nil
}

func (o *Operation) onStarted(ctx chasm.MutableContext, operationToken string, links []*commonpb.Link) error {
	if store, ok := o.Store.TryGet(ctx); ok {
		return store.OnNexusOperationStarted(ctx, o, operationToken, links)
	}
	return TransitionStarted.Apply(o, ctx, EventStarted{OperationToken: operationToken})
}

func (o *Operation) onCompleted(ctx chasm.MutableContext, result *commonpb.Payload, links []*commonpb.Link) error {
	if store, ok := o.Store.TryGet(ctx); ok {
		return store.OnNexusOperationCompleted(ctx, o, result, links)
	}
	outcome := o.outcome(ctx)
	outcome.Variant = &nexusoperationpb.OperationOutcome_Successful_{
		Successful: &nexusoperationpb.OperationOutcome_Successful{Result: result},
	}
	return TransitionSucceeded.Apply(o, ctx, EventSucceeded{})
}

func (o *Operation) onFailed(ctx chasm.MutableContext, cause *failurepb.Failure) error {
	if store, ok := o.Store.TryGet(ctx); ok {
		return store.OnNexusOperationFailed(ctx, o, cause)
	}
	if cause != nil {
		o.outcome(ctx).Variant = &nexusoperationpb.OperationOutcome_Failed_{
			Failed: &nexusoperationpb.OperationOutcome_Failed{Failure: cause},
		}
	}
	return TransitionFailed.Apply(o, ctx, EventFailed{Failure: cause})
}

func (o *Operation) onCanceled(ctx chasm.MutableContext, cause *failurepb.Failure) error {
	if store, ok := o.Store.TryGet(ctx); ok {
		return store.OnNexusOperationCanceled(ctx, o, cause)
	}
	if cause != nil {
		o.outcome(ctx).Variant = &nexusoperationpb.OperationOutcome_Failed_{
			Failed: &nexusoperationpb.OperationOutcome_Failed{Failure: cause},
		}
	}
	return TransitionCanceled.Apply(o, ctx, EventCanceled{Failure: cause})
}

func (o *Operation) onTimedOut(ctx chasm.MutableContext, cause *failurepb.Failure) error {
	if store, ok := o.Store.TryGet(ctx); ok {
		return store.OnNexusOperationTimedOut(ctx, o, cause)
	}
	if cause != nil {
		o.outcome(ctx).Variant = &nexusoperationpb.OperationOutcome_Failed_{
			Failed: &nexusoperationpb.OperationOutcome_Failed{Failure: cause},
		}
	}
	return TransitionTimedOut.Apply(o, ctx, EventTimedOut{})
}

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
		nexusLink:              invocationData.NexusLink,
		serializedRef:          serializedRef,
	}, nil
}

type saveInvocationResultInput struct {
	result      invocationResult
	retryPolicy backoff.RetryPolicy
}

func (o *Operation) saveInvocationResult(
	ctx chasm.MutableContext,
	input saveInvocationResultInput,
) (chasm.NoValue, error) {
	switch r := input.result.(type) {
	case invocationResultOK:
		links := convertResponseLinks(r.response.Links, ctx.Logger())
		if r.response.Pending != nil {
			return nil, o.onStarted(ctx, r.response.Pending.Token, links)
		}
		return nil, o.onCompleted(ctx, r.response.Successful, links)
	case invocationResultCancel:
		return nil, o.onCanceled(ctx, r.failure)
	case invocationResultFail:
		return nil, o.onFailed(ctx, r.failure)
	case invocationResultTimeout:
		return nil, o.onTimedOut(ctx, r.failure)
	case invocationResultRetry:
		return nil, transitionAttemptFailed.Apply(o, ctx, EventAttemptFailed{
			Failure:     r.failure,
			RetryPolicy: input.retryPolicy,
		})
	default:
		return nil, queueserrors.NewUnprocessableTaskError(fmt.Sprintf("unrecognized invocation result %T", r))
	}
}

func (o *Operation) resolveUnsuccessfully(ctx chasm.MutableContext, failure *failurepb.Failure, closeTime time.Time) error {
	if o.GetStatus() == nexusoperationpb.OPERATION_STATUS_SCHEDULED {
		o.LastAttemptCompleteTime = timestamppb.New(ctx.Now(o))
		o.LastAttemptFailure = failure
	}
	o.ClosedTime = timestamppb.New(closeTime)
	o.NextAttemptScheduleTime = nil
	if failure != nil {
		o.outcome(ctx).Variant = &nexusoperationpb.OperationOutcome_Failed_{
			Failed: &nexusoperationpb.OperationOutcome_Failed{Failure: failure},
		}
	}
	return nil
}

func (o *Operation) outcome(ctx chasm.MutableContext) *nexusoperationpb.OperationOutcome {
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
		if successful, failure := o.describeOutcome(ctx); successful != nil {
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
		if successful, failure := o.describeOutcome(ctx); successful != nil {
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

func (o *Operation) describeOutcome(ctx chasm.Context) (*commonpb.Payload, *failurepb.Failure) {
	outcome := o.Outcome.Get(ctx)
	if successful := outcome.GetSuccessful(); successful != nil {
		return successful.GetResult(), nil
	}
	if failure := outcome.GetFailed().GetFailure(); failure != nil {
		return nil, failure
	}
	return nil, o.LastAttemptFailure
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
		State:                   pendingOperationState(o.Status),
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

func pendingOperationState(status nexusoperationpb.OperationStatus) enumspb.PendingNexusOperationState {
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
