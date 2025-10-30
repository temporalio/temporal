package callback

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/service/history/queues"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	Archetype chasm.Archetype = "Callback"
)

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
	chasmCtx chasm.Context,
	ctx context.Context,
) (callbackInvokable, error) {
	target, err := c.CompletionSource.Get(chasmCtx)
	if err != nil {
		return nil, err
	}

	// TODO (seankane,yichao): we should be able to use the chasm context here.
	completion, err := target.GetNexusCompletion(ctx, c.RequestId)
	if err != nil {
		return nil, err
	}

	switch variant := c.GetCallback().GetVariant().(type) {
	case *callbackspb.Callback_Nexus_:
		if variant.Nexus.Url == chasm.NexusCompletionHandlerURL {
			return chasmInvocation{
				nexus:      variant.Nexus,
				attempt:    c.Attempt,
				completion: completion,
				requestID:  c.RequestId,
			}, nil
		}
		return nexusInvocation{
			nexus:      variant.Nexus,
			completion: completion,
			// workflowID: c.WorkflowId,
			workflowID: chasmCtx.ExecutionKey().BusinessID,
			runID:      chasmCtx.ExecutionKey().EntityID,
			attempt:    c.Attempt,
		}, nil
	default:
		return nil, queues.NewUnprocessableTaskError(
			fmt.Sprintf("unprocessable callback variant: %v", variant),
		)
	}
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
