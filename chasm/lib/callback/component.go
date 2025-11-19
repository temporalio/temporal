package callback

import (
	"fmt"
	"time"

	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/service/history/queues"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	Archetype chasm.Archetype = "Callback"
)

type CompletionSource interface {
	GetNexusCompletion(ctx chasm.Context, requestID string) (nexusrpc.OperationCompletion, error)
}

var _ chasm.Component = (*Callback)(nil)
var _ chasm.StateMachine[callbackspb.CallbackStatus] = (*Callback)(nil)

// Callback represents a callback component in CHASM.
type Callback struct {
	chasm.UnimplementedComponent

	// Persisted internal state
	*callbackspb.CallbackState

	// Interface to retrieve Nexus operation completion data
	CompletionSource chasm.Field[CompletionSource]
}

func NewCallback(
	requestID string,
	registrationTime *timestamppb.Timestamp,
	state *callbackspb.CallbackState,
	cb *callbackspb.Callback,
) *Callback {
	return &Callback{
		CallbackState: &callbackspb.CallbackState{
			RequestId:        requestID,
			RegistrationTime: registrationTime,
			Callback:         cb,
			Status:           callbackspb.CALLBACK_STATUS_STANDBY,
		},
	}
}

func (c *Callback) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	// TODO (seankane): implement lifecycle state
	return chasm.LifecycleStateRunning
}

func (c *Callback) StateMachineState() callbackspb.CallbackStatus {
	return c.Status
}

func (c *Callback) SetStateMachineState(status callbackspb.CallbackStatus) {
	c.Status = status
}

func (c *Callback) recordAttempt(ts time.Time) {
	c.Attempt++
	c.LastAttemptCompleteTime = timestamppb.New(ts)
}

//nolint:revive // context.Context is an input parameter for chasm.ReadComponent, not a function parameter
func (c *Callback) loadInvocationArgs(
	ctx chasm.Context,
	_ chasm.NoValue,
) (callbackInvokable, error) {
	target, err := c.CompletionSource.Get(ctx)
	if err != nil {
		return nil, err
	}

	completion, err := target.GetNexusCompletion(ctx, c.RequestId)
	if err != nil {
		return nil, err
	}

	variant := c.GetCallback().GetNexus()
	if variant == nil {
		return nil, queues.NewUnprocessableTaskError(
			fmt.Sprintf("unprocessable callback variant: %v", variant),
		)
	}

	if variant.Url == chasm.NexusCompletionHandlerURL {
		return chasmInvocation{
			nexus:      variant,
			attempt:    c.Attempt,
			completion: completion,
			requestID:  c.RequestId,
		}, nil
	}
	return nexusInvocation{
		nexus:      variant,
		completion: completion,
		workflowID: ctx.ExecutionKey().BusinessID,
		runID:      ctx.ExecutionKey().EntityID,
		attempt:    c.Attempt,
	}, nil
}

func (c *Callback) saveResult(
	ctx chasm.MutableContext,
	result invocationResult,
) (chasm.NoValue, error) {
	switch r := result.(type) {
	case invocationResultOK:
		err := TransitionSucceeded.Apply(ctx, c, EventSucceeded{Time: ctx.Now(c)})
		return nil, err
	case invocationResultRetry:
		err := TransitionAttemptFailed.Apply(ctx, c, EventAttemptFailed{
			Time:        ctx.Now(c),
			Err:         r.err,
			RetryPolicy: r.retryPolicy,
		})
		return nil, err
	case invocationResultFail:
		err := TransitionFailed.Apply(ctx, c, EventFailed{
			Time: ctx.Now(c),
			Err:  r.err,
		})
		return nil, err
	default:
		return nil, queues.NewUnprocessableTaskError(
			fmt.Sprintf("unrecognized callback result %v", result),
		)
	}
}
