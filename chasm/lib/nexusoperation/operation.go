package nexusoperation

import (
	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"google.golang.org/protobuf/types/known/anypb"
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

// ErrCancellationAlreadyRequested is returned when a cancellation has already been requested for an operation.
var ErrCancellationAlreadyRequested = serviceerror.NewFailedPrecondition("cancellation already requested")

// ErrOperationAlreadyCompleted is returned when trying to cancel an operation that has already completed.
var ErrOperationAlreadyCompleted = serviceerror.NewFailedPrecondition("operation already completed")

// OperationStore defines the interface that must be implemented by any parent component that wants to manage Nexus operations.
// It's the responsibility of the parrent component to apply the appropriate state transitions to the operation.
type OperationStore interface {
	OnNexusOperationStarted(ctx chasm.MutableContext, operation *Operation, operationToken string, links []*commonpb.Link) error
	OnNexusOperationCancelled(ctx chasm.MutableContext, operation *Operation, cause *failurepb.Failure) error
	OnNexusOperationFailed(ctx chasm.MutableContext, operation *Operation, cause *failurepb.Failure) error
	OnNexusOperationTimedOut(ctx chasm.MutableContext, operation *Operation, cause *failurepb.Failure) error
	OnNexusOperationCompleted(ctx chasm.MutableContext, operation *Operation, result *commonpb.Payload, links []*commonpb.Link) error
}

// Operation is a CHASM component that represents a Nexus operation.
type Operation struct {
	chasm.UnimplementedComponent

	// Persisted internal state
	*nexusoperationpb.OperationState

	// Pointer to an implementation of the "store". For a workflow-based Nexus operation
	// this is a parent pointer back to the workflow. For a standalone Nexus operation this is nil.
	Store chasm.ParentPtr[OperationStore]

	// RequestData stores the original request data (input, headers, user metadata).
	// Only used for standalone Nexus operations; for workflow-embedded operations,
	// request data is stored in the workflow's history events.
	RequestData chasm.Field[*nexusoperationpb.OperationRequestData]

	// Cancellation is a child component that manages sending the cancel request to the Nexus endpoint.
	Cancellation chasm.Field[*Cancellation]

	// Visibility holds custom search attributes for this operation.
	Visibility chasm.Field[*chasm.Visibility]
}

// NewOperation creates a new Operation component with the given persisted state.
func NewOperation(state *nexusoperationpb.OperationState) *Operation {
	return &Operation{OperationState: state}
}

// newStandaloneOperation creates a standalone Nexus operation from a frontend Start request.
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
		RequestId:              uuid.NewString(), // different from client-supplied RequestID!
	})
	op.RequestData = chasm.NewDataField(ctx, &nexusoperationpb.OperationRequestData{
		Input:        frontendReq.GetInput(),
		NexusHeader:  frontendReq.GetNexusHeader(),
		UserMetadata: frontendReq.GetUserMetadata(),
		Identity:     frontendReq.GetIdentity(),
	})
	op.Visibility = chasm.NewComponentField(ctx, chasm.NewVisibilityWithData(
		ctx,
		frontendReq.GetSearchAttributes().GetIndexedFields(),
		nil,
	))
	if err := transitionScheduled.Apply(op, ctx, EventScheduled{}); err != nil {
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
		nexusoperationpb.OPERATION_STATUS_TIMED_OUT:
		return chasm.LifecycleStateFailed
	default:
		return chasm.LifecycleStateRunning
	}
}

// StateMachineState returns the current operation status.
func (o *Operation) StateMachineState() nexusoperationpb.OperationStatus {
	return o.Status
}

// SetStateMachineState sets the operation status.
func (o *Operation) SetStateMachineState(status nexusoperationpb.OperationStatus) {
	o.Status = status
}

// Cancel requests cancellation of the operation. It creates a Cancellation child component and, if the
// operation has already started, schedules the cancellation request to be sent to the Nexus endpoint.
// parentData is opaque data injected by the parent (e.g. workflow) for its own bookkeeping.
func (o *Operation) Cancel(ctx chasm.MutableContext, parentData *anypb.Any) error {
	if !TransitionCanceled.Possible(o) {
		return ErrOperationAlreadyCompleted
	}
	if _, ok := o.Cancellation.TryGet(ctx); ok {
		return ErrCancellationAlreadyRequested
	}

	cancellation := newCancellation(&nexusoperationpb.CancellationState{
		ParentData: parentData,
	})
	o.Cancellation = chasm.NewComponentField(ctx, cancellation)

	// Once started, the handler returns a token that can be used in the cancelation request.
	// Until then, no need to schedule the cancelation.
	if o.Status == nexusoperationpb.OPERATION_STATUS_STARTED {
		return transitionCancellationScheduled.Apply(cancellation, ctx, EventCancellationScheduled{})
	}

	return nil
}

func (o *Operation) Terminate(_ chasm.MutableContext, _ chasm.TerminateComponentRequest) (chasm.TerminateComponentResponse, error) {
	return chasm.TerminateComponentResponse{}, serviceerror.NewUnimplemented("not implemented")
}

// SearchAttributes implements chasm.VisibilitySearchAttributesProvider interface.
// Returns the current search attribute values for this operation.
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
	var input *commonpb.Payload
	if req.GetFrontendRequest().GetIncludeInput() {
		input = o.RequestData.Get(ctx).GetInput()
	}

	return &nexusoperationpb.DescribeNexusOperationResponse{
		FrontendResponse: &workflowservice.DescribeNexusOperationExecutionResponse{
			RunId: ctx.ExecutionKey().RunID,
			Info:  o.buildExecutionInfo(ctx),
			Input: input,
			// TODO: Add Outcome
			// TODO: Add LongPollToken support.
		},
	}, nil
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
		// TODO CancellationInfo
		RequestId:            o.RequestId,
		OperationToken:       o.OperationToken,
		StateTransitionCount: ctx.StateTransitionCount(),
		SearchAttributes: &commonpb.SearchAttributes{
			IndexedFields: o.Visibility.Get(ctx).CustomSearchAttributes(ctx),
		},
		NexusHeader:  requestData.GetNexusHeader(),
		UserMetadata: requestData.GetUserMetadata(),
		// TODO Links
		Identity: requestData.GetIdentity(),
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

// closeTime returns the close time of the operation (only for terminal states).
func (o *Operation) closeTime(ctx chasm.Context) *timestamppb.Timestamp {
	if !o.LifecycleState(ctx).IsClosed() {
		return nil
	}
	return o.LastAttemptCompleteTime
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

// OnStarted applies the started transition or delegates to the store if one is present.
func (o *Operation) OnStarted(ctx chasm.MutableContext, _ *Operation, operationToken string, links []*commonpb.Link) error {
	store, ok := o.Store.TryGet(ctx)
	if ok {
		return store.OnNexusOperationStarted(ctx, o, operationToken, links)
	}
	return transitionStarted.Apply(o, ctx, EventStarted{
		OperationToken: operationToken,
		FromBackingOff: o.Status == nexusoperationpb.OPERATION_STATUS_BACKING_OFF,
	})
}

// OnCompleted applies the succeeded transition or delegates to the store if one is present.
func (o *Operation) OnCompleted(ctx chasm.MutableContext, _ *Operation, result *commonpb.Payload, links []*commonpb.Link) error {
	store, ok := o.Store.TryGet(ctx)
	if ok {
		return store.OnNexusOperationCompleted(ctx, o, result, links)
	}
	return transitionSucceeded.Apply(o, ctx, EventSucceeded{})
}

// OnFailed applies the failed transition or delegates to the store if one is present.
func (o *Operation) OnFailed(ctx chasm.MutableContext, _ *Operation, cause *failurepb.Failure) error {
	store, ok := o.Store.TryGet(ctx)
	if ok {
		return store.OnNexusOperationFailed(ctx, o, cause)
	}
	return transitionFailed.Apply(o, ctx, EventFailed{})
}

// OnCancelled applies the canceled transition or delegates to the store if one is present.
func (o *Operation) OnCancelled(ctx chasm.MutableContext, _ *Operation, cause *failurepb.Failure) error {
	store, ok := o.Store.TryGet(ctx)
	if ok {
		return store.OnNexusOperationCancelled(ctx, o, cause)
	}
	return TransitionCanceled.Apply(o, ctx, EventCanceled{})
}

// OnTimedOut applies the timed out transition or delegates to the store if one is present.
func (o *Operation) OnTimedOut(ctx chasm.MutableContext, _ *Operation, cause *failurepb.Failure) error {
	store, ok := o.Store.TryGet(ctx)
	if ok {
		return store.OnNexusOperationTimedOut(ctx, o, cause)
	}
	return transitionTimedOut.Apply(o, ctx, EventTimedOut{})
}
