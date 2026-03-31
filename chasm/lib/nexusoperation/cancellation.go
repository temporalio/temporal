package nexusoperation

import (
	"errors"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
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
func (o *Cancellation) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	switch o.Status {
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
func (o *Cancellation) StateMachineState() nexusoperationpb.CancellationStatus {
	return o.Status
}

// SetStateMachineState sets the cancellation status.
func (o *Cancellation) SetStateMachineState(status nexusoperationpb.CancellationStatus) {
	o.Status = status
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
	scheduleToCloseTimeout time.Duration
	startToCloseTimeout    time.Duration
	headers                map[string]string
	payload                *commonpb.Payload
}

// loadCancelArgs is a ReadComponent callback that loads the cancel arguments from the cancellation
// and its parent operation.
func (o *Cancellation) loadCancelArgs(
	ctx chasm.Context,
	_ chasm.NoValue,
) (cancelArgs, error) {
	op := o.Operation.Get(ctx)

	store, ok := op.Store.TryGet(ctx)
	if !ok {
		// TODO: For standalone operations, load invocation data from the operation state.
		return cancelArgs{}, serviceerror.NewInternal("no store available to load invocation data")
	}
	invocationData, err := store.NexusOperationInvocationData(ctx, op)
	if err != nil {
		return cancelArgs{}, err
	}

	return cancelArgs{
		service:                op.GetService(),
		operation:              op.GetOperation(),
		token:                  op.GetOperationToken(),
		requestID:              op.GetRequestId(),
		endpointName:           op.GetEndpoint(),
		endpointID:             op.GetEndpointId(),
		currentTime:            ctx.Now(o),
		scheduledTime:          op.GetScheduledTime().AsTime(),
		scheduleToCloseTimeout: op.GetScheduleToCloseTimeout().AsDuration(),
		startToCloseTimeout:    op.GetStartToCloseTimeout().AsDuration(),
		headers:                invocationData.Header,
		payload:                invocationData.Input,
	}, nil
}

// saveCancellationResultInput is the input to the Cancellation.applyCancellationResult method.
type saveCancellationResultInput struct {
	callErr     error
	retryPolicy func() backoff.RetryPolicy
}

// applyCancellationResult applies the outcome of a cancel operation call to the cancellation state machine.
func (o *Cancellation) applyCancellationResult(
	ctx chasm.MutableContext,
	input saveCancellationResultInput,
) (chasm.NoValue, error) {
	if input.callErr != nil {
		var handlerErr *nexus.HandlerError
		var opTimeoutBelowMinErr *operationTimeoutBelowMinError
		isRetryable := !errors.As(input.callErr, &opTimeoutBelowMinErr) &&
			(!errors.As(input.callErr, &handlerErr) || handlerErr.Retryable())

		failure, err := callErrToFailure(input.callErr, isRetryable)
		if err != nil {
			failure = &failurepb.Failure{Message: input.callErr.Error()}
		}

		if !isRetryable {
			// TODO: Emit EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_FAILED history event
			// when AddHistoryEvent is available on MutableContext.
			return nil, TransitionCancellationFailed.Apply(o, ctx, EventCancellationFailed{})
		}

		return nil, transitionCancellationAttemptFailed.Apply(o, ctx, EventCancellationAttemptFailed{
			Failure:     failure,
			RetryPolicy: input.retryPolicy(),
		})
	}

	// Cancellation request transmitted successfully.
	// The operation is not yet canceled and may ignore our request, the outcome will be known via the
	// completion callback.
	// TODO: Emit EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_COMPLETED history event
	// when AddHistoryEvent is available on MutableContext.
	return nil, TransitionCancellationSucceeded.Apply(o, ctx, EventCancellationSucceeded{})
}
