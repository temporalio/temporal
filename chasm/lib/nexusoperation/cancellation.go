package nexusoperation

import (
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"go.temporal.io/server/common/backoff"
)

var _ chasm.Component = (*Cancellation)(nil)
var _ chasm.StateMachine[nexusoperationpb.CancellationStatus] = (*Cancellation)(nil)

// Cancellation is a CHASM component that represents a pending cancellation of a Nexus operation.
type Cancellation struct {
	chasm.UnimplementedComponent

	// Persisted internal state
	*nexusoperationpb.CancellationState

	// Operation is a pointer to the parent Operation component.
	Operation chasm.ParentPtr[*Operation]
}

func newCancellation(state *nexusoperationpb.CancellationState) *Cancellation {
	return &Cancellation{CancellationState: state}
}

// LifecycleState maps the cancellation's status to a CHASM lifecycle state.
func (c *Cancellation) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	switch c.Status {
	case nexusoperationpb.CANCELLATION_STATUS_SUCCEEDED:
		return chasm.LifecycleStateCompleted
	case nexusoperationpb.CANCELLATION_STATUS_FAILED,
		nexusoperationpb.CANCELLATION_STATUS_TIMED_OUT:
		return chasm.LifecycleStateFailed
	default:
		return chasm.LifecycleStateRunning
	}
}

// StateMachineState returns the current cancellation status.
func (c *Cancellation) StateMachineState() nexusoperationpb.CancellationStatus {
	return c.Status
}

// SetStateMachineState sets the cancellation status.
func (c *Cancellation) SetStateMachineState(status nexusoperationpb.CancellationStatus) {
	c.Status = status
}

// cancelArgs holds the arguments needed to cancel a Nexus operation.
type cancelArgs struct {
	service                string
	operation              string
	token                  string
	requestID              string
	endpointName           string
	endpointID             string
	currentTime            time.Time
	scheduledTime          time.Time
	startedTime            time.Time
	scheduleToCloseTimeout time.Duration
	startToCloseTimeout    time.Duration
	headers                map[string]string
	payload                *commonpb.Payload
}

func (c *Cancellation) onCompleted(ctx chasm.MutableContext) error {
	op := c.Operation.Get(ctx)
	if store, ok := op.Store.TryGet(ctx); ok {
		return store.OnNexusOperationCancellationCompleted(ctx, op)
	}
	return TransitionCancellationSucceeded.Apply(c, ctx, EventCancellationSucceeded{})
}

func (c *Cancellation) onFailed(ctx chasm.MutableContext, failure *failurepb.Failure) error {
	op := c.Operation.Get(ctx)
	if store, ok := op.Store.TryGet(ctx); ok {
		return store.OnNexusOperationCancellationFailed(ctx, op, failure)
	}
	return TransitionCancellationFailed.Apply(c, ctx, EventCancellationFailed{
		Failure: failure,
	})
}

// loadArgs loads the cancel arguments from the cancellation and its parent operation.
func (c *Cancellation) loadArgs(
	ctx chasm.Context,
	_ chasm.NoValue,
) (cancelArgs, error) {
	op := c.Operation.Get(ctx)

	var invocationData InvocationData
	if store, ok := op.Store.TryGet(ctx); ok {
		var err error
		invocationData, err = store.NexusOperationInvocationData(ctx, op)
		if err != nil {
			return cancelArgs{}, err
		}
	} else {
		requestData := op.RequestData.Get(ctx)
		invocationData = InvocationData{
			Input:  requestData.GetInput(),
			Header: requestData.GetNexusHeader(),
		}
	}

	return cancelArgs{
		service:                op.GetService(),
		operation:              op.GetOperation(),
		token:                  op.GetOperationToken(),
		requestID:              op.GetRequestId(),
		endpointName:           op.GetEndpoint(),
		endpointID:             op.GetEndpointId(),
		currentTime:            ctx.Now(c),
		scheduledTime:          op.GetScheduledTime().AsTime(),
		startedTime:            op.GetStartedTime().AsTime(),
		scheduleToCloseTimeout: op.GetScheduleToCloseTimeout().AsDuration(),
		startToCloseTimeout:    op.GetStartToCloseTimeout().AsDuration(),
		headers:                invocationData.Header,
		payload:                invocationData.Input,
	}, nil
}

// saveCancellationResultInput is the input to the Cancellation.saveResult method.
type saveCancellationResultInput struct {
	result      cancellationResult
	retryPolicy func() backoff.RetryPolicy
}

// saveResult applies the outcome of a cancel operation call to the cancellation state machine.
func (c *Cancellation) saveResult(
	ctx chasm.MutableContext,
	input saveCancellationResultInput,
) (chasm.NoValue, error) {
	switch r := input.result.(type) {
	case cancellationResultOK:
		return nil, c.onCompleted(ctx)
	case cancellationResultFail:
		return nil, c.onFailed(ctx, r.failure)
	case cancellationResultRetry:
		return nil, transitionCancellationAttemptFailed.Apply(c, ctx, EventCancellationAttemptFailed{
			Failure:     r.failure,
			RetryPolicy: input.retryPolicy(),
		})
	default:
		return nil, serviceerror.NewInternalf("cannot save cancellation result of type %T", r)
	}
}

func CancellationAPIState(status nexusoperationpb.CancellationStatus) enumspb.NexusOperationCancellationState {
	switch status {
	case nexusoperationpb.CANCELLATION_STATUS_SCHEDULED:
		return enumspb.NEXUS_OPERATION_CANCELLATION_STATE_SCHEDULED
	case nexusoperationpb.CANCELLATION_STATUS_BACKING_OFF:
		return enumspb.NEXUS_OPERATION_CANCELLATION_STATE_BACKING_OFF
	case nexusoperationpb.CANCELLATION_STATUS_SUCCEEDED:
		return enumspb.NEXUS_OPERATION_CANCELLATION_STATE_SUCCEEDED
	case nexusoperationpb.CANCELLATION_STATUS_FAILED:
		return enumspb.NEXUS_OPERATION_CANCELLATION_STATE_FAILED
	case nexusoperationpb.CANCELLATION_STATUS_TIMED_OUT:
		return enumspb.NEXUS_OPERATION_CANCELLATION_STATE_TIMED_OUT
	case nexusoperationpb.CANCELLATION_STATUS_BLOCKED:
		return enumspb.NEXUS_OPERATION_CANCELLATION_STATE_BLOCKED
	default:
		return enumspb.NEXUS_OPERATION_CANCELLATION_STATE_UNSPECIFIED
	}
}
