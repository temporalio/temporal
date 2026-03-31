package nexusoperation

import (
	"fmt"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	queueserrors "go.temporal.io/server/service/history/queues/errors"
	"google.golang.org/protobuf/types/known/anypb"
)

var _ chasm.Component = (*Operation)(nil)
var _ chasm.StateMachine[nexusoperationpb.OperationStatus] = (*Operation)(nil)

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
	// NexusLink is the link to the caller that scheduled this operation.
	NexusLink nexus.Link
}

// OperationStore defines the interface that must be implemented by any parent component that wants to manage Nexus operations.
// It's the responsibility of the parrent component to apply the appropriate state transitions to the operation.
type OperationStore interface {
	OnNexusOperationStarted(ctx chasm.MutableContext, operation *Operation, operationToken string, links []*commonpb.Link) error
	OnNexusOperationCancelled(ctx chasm.MutableContext, operation *Operation, cause *failurepb.Failure) error
	OnNexusOperationFailed(ctx chasm.MutableContext, operation *Operation, cause *failurepb.Failure) error
	OnNexusOperationTimedOut(ctx chasm.MutableContext, operation *Operation, cause *failurepb.Failure) error
	OnNexusOperationCompleted(ctx chasm.MutableContext, operation *Operation, result *commonpb.Payload, links []*commonpb.Link) error
	// GetNexusOperationInvocationData loads invocation data (Input, Header, NexusLink) from the scheduled history event.
	GetNexusOperationInvocationData(ctx chasm.Context, operation *Operation) (InvocationData, error)
}

// Operation is a CHASM component that represents a Nexus operation.
type Operation struct {
	chasm.UnimplementedComponent

	// Persisted internal state
	*nexusoperationpb.OperationState

	// Pointer to an implementation of the "store". For a workflow-based Nexus operation
	// this is a parent pointer back to the workflow. For a standalone Nexus operation this is nil.
	Store chasm.ParentPtr[OperationStore]

	// Cancellation is a child component that manages sending the cancel request to the Nexus endpoint.
	Cancellation chasm.Field[*Cancellation]
}

// NewOperation creates a new Operation component with the given persisted state.
func NewOperation(state *nexusoperationpb.OperationState) *Operation {
	return &Operation{OperationState: state}
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
		return TransitionCancellationScheduled.Apply(cancellation, ctx, EventCancellationScheduled{})
	}

	return nil
}

// OnStarted applies the started transition or delegates to the store if one is present.
func (o *Operation) OnStarted(ctx chasm.MutableContext, operationToken string, links []*commonpb.Link) error {
	store, ok := o.Store.TryGet(ctx)
	if ok {
		return store.OnNexusOperationStarted(ctx, o, operationToken, links)
	}
	return TransitionStarted.Apply(o, ctx, EventStarted{
		OperationToken: operationToken,
	})
}

// OnCompleted applies the succeeded transition or delegates to the store if one is present.
func (o *Operation) OnCompleted(ctx chasm.MutableContext, result *commonpb.Payload, links []*commonpb.Link) error {
	store, ok := o.Store.TryGet(ctx)
	if ok {
		return store.OnNexusOperationCompleted(ctx, o, result, links)
	}
	return TransitionSucceeded.Apply(o, ctx, EventSucceeded{})
}

// OnFailed applies the failed transition or delegates to the store if one is present.
func (o *Operation) OnFailed(ctx chasm.MutableContext, cause *failurepb.Failure) error {
	store, ok := o.Store.TryGet(ctx)
	if ok {
		return store.OnNexusOperationFailed(ctx, o, cause)
	}
	return TransitionFailed.Apply(o, ctx, EventFailed{})
}

// OnCancelled applies the canceled transition or delegates to the store if one is present.
func (o *Operation) OnCancelled(ctx chasm.MutableContext, cause *failurepb.Failure) error {
	store, ok := o.Store.TryGet(ctx)
	if ok {
		return store.OnNexusOperationCancelled(ctx, o, cause)
	}
	return TransitionCanceled.Apply(o, ctx, EventCanceled{})
}

// OnTimedOut applies the timed out transition or delegates to the store if one is present.
func (o *Operation) OnTimedOut(ctx chasm.MutableContext, cause *failurepb.Failure) error {
	store, ok := o.Store.TryGet(ctx)
	if ok {
		return store.OnNexusOperationTimedOut(ctx, o, cause)
	}
	return TransitionTimedOut.Apply(o, ctx, EventTimedOut{})
}

// loadStartArgs is a ReadComponent callback that loads the start arguments from the operation.
func (o *Operation) loadStartArgs(
	ctx chasm.Context,
	_ chasm.NoValue,
) (startArgs, error) {
	store, ok := o.Store.TryGet(ctx)
	var invocationData InvocationData
	if ok {
		var err error
		invocationData, err = store.GetNexusOperationInvocationData(ctx, o)
		if err != nil {
			return startArgs{}, err
		}
	} else {
		// TODO: For standalone operations, load invocation data from the operation state.
		return startArgs{}, serviceerror.NewInternal("no store available to load invocation data")
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

// saveResult is an UpdateComponent callback that saves the invocation outcome.
func (o *Operation) saveResult(
	ctx chasm.MutableContext,
	input saveResultInput,
) (chasm.NoValue, error) {
	switch r := input.result.(type) {
	case invocationResultOK:
		if r.response.Pending != nil {
			return nil, o.OnStarted(ctx, r.response.Pending.Token, r.links)
		}
		return nil, o.OnCompleted(ctx, r.response.Successful, r.links)
	case invocationResultFail:
		return nil, o.OnFailed(ctx, r.failure)
	case invocationResultCanceled:
		return nil, o.OnCancelled(ctx, r.failure)
	case invocationResultRetry:
		return nil, transitionAttemptFailed.Apply(o, ctx, EventAttemptFailed{
			Failure:     r.failure,
			RetryPolicy: input.retryPolicy(),
		})
	case invocationResultTimeout:
		return nil, o.OnTimedOut(ctx, &failurepb.Failure{
			Message: "operation timed out",
			FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
				TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
					TimeoutType: r.timeoutType,
				},
			},
		})
	default:
		return nil, queueserrors.NewUnprocessableTaskError(
			fmt.Sprintf("unrecognized invocation result %T", input.result),
		)
	}
}
