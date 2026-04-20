package nexusoperation

import (
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"go.temporal.io/server/common/backoff"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

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

// TaskGroupName groups invocation and cancellation together for the outbound queue
const TaskGroupName = "nexus"

// OperationStore defines the interface that must be implemented by any parent component that wants to manage Nexus operations.
// It's the responsibility of the parrent component to apply the appropriate state transitions to the operation.
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

// Operation is a CHASM component that represents a Nexus operation.
type Operation struct {
	chasm.UnimplementedComponent

	// Persisted internal state
	*nexusoperationpb.OperationState

	// Pointer to an implementation of the "store". For a workflow-based Nexus operation
	// this is a parent pointer back to the workflow. For a standalone Nexus operation this is nil.
	Store chasm.ParentPtr[OperationStore]

	// Cancellation is a child component that manages sending the cancel request to the Nexus endpoint.
	// Created when cancelation is requested, nil otherwise.
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
		RequestedTime: timestamppb.New(ctx.Now(o)),
		ParentData:    parentData,
	})
	o.Cancellation = chasm.NewComponentField(ctx, cancellation)

	// Once started, the handler returns a token that can be used in the cancelation request.
	// Until then, no need to schedule the cancelation.
	if o.Status == nexusoperationpb.OPERATION_STATUS_STARTED {
		return TransitionCancellationScheduled.Apply(cancellation, ctx, EventCancellationScheduled{
			Destination: o.GetEndpoint(),
		})
	}

	return nil
}

// onStarted applies the started transition or delegates to the store if one is present.
func (o *Operation) onStarted(ctx chasm.MutableContext, operationToken string, links []*commonpb.Link) error {
	store, ok := o.Store.TryGet(ctx)
	if ok {
		return store.OnNexusOperationStarted(ctx, o, operationToken, links)
	}
	// TODO(stephan): for standalone, store links
	return TransitionStarted.Apply(o, ctx, EventStarted{
		OperationToken: operationToken,
	})
}

// onCompleted applies the succeeded transition or delegates to the store if one is present.
func (o *Operation) onCompleted(ctx chasm.MutableContext, result *commonpb.Payload, links []*commonpb.Link) error {
	store, ok := o.Store.TryGet(ctx)
	if ok {
		return store.OnNexusOperationCompleted(ctx, o, result, links)
	}
	// TODO(stephan): for standalone, store result and links
	return TransitionSucceeded.Apply(o, ctx, EventSucceeded{})
}

// onFailed applies the failed transition or delegates to the store if one is present.
func (o *Operation) onFailed(ctx chasm.MutableContext, cause *failurepb.Failure) error {
	store, ok := o.Store.TryGet(ctx)
	if ok {
		return store.OnNexusOperationFailed(ctx, o, cause)
	}
	return TransitionFailed.Apply(o, ctx, EventFailed{Failure: cause})
}

// onCanceled applies the canceled transition or delegates to the store if one is present.
func (o *Operation) onCanceled(ctx chasm.MutableContext, cause *failurepb.Failure) error {
	store, ok := o.Store.TryGet(ctx)
	if ok {
		return store.OnNexusOperationCanceled(ctx, o, cause)
	}
	return TransitionCanceled.Apply(o, ctx, EventCanceled{Failure: cause})
}

// onTimedOut applies the timed out transition or delegates to the store if one is present.
func (o *Operation) onTimedOut(ctx chasm.MutableContext, cause *failurepb.Failure) error {
	store, ok := o.Store.TryGet(ctx)
	if ok {
		return store.OnNexusOperationTimedOut(ctx, o, cause)
	}
	// TODO(stephan): for standalone, store failure
	return TransitionTimedOut.Apply(o, ctx, EventTimedOut{})
}

// loadStartArgs is a ReadComponent callback that loads the start arguments from the operation.
func (o *Operation) loadStartArgs(
	ctx chasm.Context,
	_ chasm.NoValue,
) (startArgs, error) {
	var invocationData InvocationData

	if store, ok := o.Store.TryGet(ctx); ok {
		var err error
		invocationData, err = store.NexusOperationInvocationData(ctx, o)
		if err != nil {
			return startArgs{}, err
		}
	}
	// TODO(stephan): for standalone, populate the invocationData fields.

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

// saveInvocationResultInput is the input to the Operation.saveResult method used in UpdateComponent.
type saveInvocationResultInput struct {
	result      startResult
	retryPolicy backoff.RetryPolicy
}

func (o *Operation) saveInvocationResult(
	ctx chasm.MutableContext,
	input saveInvocationResultInput,
) (chasm.NoValue, error) {
	switch r := input.result.(type) {
	case startResultOK:
		links := convertResponseLinks(r.response.Links, ctx.Logger())
		if r.response.Pending != nil {
			return nil, o.onStarted(ctx, r.response.Pending.Token, links)
		}
		return nil, o.onCompleted(ctx, r.response.Successful, links)
	case startResultCancel:
		return nil, o.onCanceled(ctx, r.failure)
	case startResultFail:
		return nil, o.onFailed(ctx, r.failure)
	case startResultTimeout:
		return nil, o.onTimedOut(ctx, r.failure)
	case startResultRetry:
		return nil, transitionAttemptFailed.Apply(o, ctx, EventAttemptFailed{
			Failure:     r.failure,
			RetryPolicy: input.retryPolicy,
		})
	default:
		return nil, serviceerror.NewInternalf("cannot save invocation result of type %T", r)
	}
}

func (o *Operation) resolveUnsuccessfully(ctx chasm.MutableContext, failure *failurepb.Failure, closeTime time.Time) error {
	// When we transition from scheduled to failed it is always due to the attempt failing with a non
	// retryable failure. The failure should be recorded in the state for standalone Nexus operations.
	// Workflow operations will be deleted immediately after this transition leaving no trace of the failure
	// object.
	if o.GetStatus() == nexusoperationpb.OPERATION_STATUS_SCHEDULED {
		o.LastAttemptCompleteTime = timestamppb.New(ctx.Now(o))
		o.LastAttemptFailure = failure
	}
	// TODO(stephan): store failure when unsuccessful outside of an attempt.

	o.ClosedTime = timestamppb.New(closeTime)

	// Clear the next attempt schedule time when leaving BACKING_OFF state. This field is only valid in
	// BACKING_OFF state.
	o.NextAttemptScheduleTime = nil
	// Terminal state - no tasks to emit.
	return nil
}
