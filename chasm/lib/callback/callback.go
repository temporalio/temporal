package callback

import (
	"fmt"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/nexus/nexusrpc"
	queueserrors "go.temporal.io/server/service/history/queues/errors"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type CompletionSource interface {
	GetNexusCompletion(ctx chasm.Context, requestID string) (nexusrpc.CompleteOperationOptions, error)
}

var _ chasm.Component = (*Callback)(nil)
var _ chasm.StateMachine[callbackspb.CallbackStatus] = (*Callback)(nil)

// Callback represents a callback component in CHASM.
//
// Note that there is a separate CHASM component, CallbackExecution, which represents
// a callback that needs to be triggered from outside of Temporal. (An external service
// reporting some operation has completed.)
//
// Whereas Callback is used for things that are entirely within CHASM, and supports
// durably sending an HTTP POST request in response to something. (e.g. a Workflow
// completing, to mark the Nexus operation has having finished.)
type Callback struct {
	chasm.UnimplementedComponent

	// Persisted internal state
	*callbackspb.CallbackState

	// Interface to retrieve Nexus operation completion data
	CompletionSource chasm.ParentPtr[CompletionSource]
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
	switch c.Status {
	case callbackspb.CALLBACK_STATUS_SUCCEEDED:
		return chasm.LifecycleStateCompleted
	case callbackspb.CALLBACK_STATUS_FAILED,
		callbackspb.CALLBACK_STATUS_TERMINATED:
		// TODO: Use chasm.LifecycleStateTerminated when it's available (currently commented out
		// in chasm/component.go:70). For now, LifecycleStateFailed is functionally correct
		// as IsClosed() returns true for all states >= LifecycleStateCompleted.
		return chasm.LifecycleStateFailed
	default:
		return chasm.LifecycleStateRunning
	}
}

func (c *Callback) StateMachineState() callbackspb.CallbackStatus {
	return c.Status
}

func (c *Callback) SetStateMachineState(status callbackspb.CallbackStatus) {
	c.Status = status
}

// GetFailure returns the failure for a closed callback. It checks the dedicated Failure field
// (set by external failures like timeout or terminate) first, then falls back to LastAttemptFailure
// (set by invocation attempt failures).
//
// IMPORTANT: Don't conflate this with `c.Failure`, which is the raw data from the `CallbackState`,
// and does not take `c.LastFailureAttempt` into account.
func (c *Callback) GetFailure() *failurepb.Failure {
	if c.Failure != nil {
		return c.Failure
	}
	return c.LastAttemptFailure
}

func (c *Callback) recordAttempt(ts time.Time) {
	c.Attempt++
	c.LastAttemptCompleteTime = timestamppb.New(ts)
}

//nolint:revive // context.Context is an input parameter for chasm.ReadComponent, not a function parameter
func (c *Callback) loadInvocationArgs(
	ctx chasm.Context,
	_ chasm.NoValue,
) (invocable, error) {
	target := c.CompletionSource.Get(ctx)

	completion, err := target.GetNexusCompletion(ctx, c.RequestId)
	if err != nil {
		return nil, err
	}

	callback := c.GetCallback().GetNexus()
	if callback == nil {
		return nil, queueserrors.NewUnprocessableTaskError(
			fmt.Sprintf("unprocessable callback variant: %v", callback),
		)
	}

	if callback.Url == chasm.NexusCompletionHandlerURL {
		return invocableInternal{
			callback:   callback,
			attempt:    c.Attempt,
			completion: completion,
			requestID:  c.RequestId,
		}, nil
	}
	return invocableOutbound{
		callback:   callback,
		completion: completion,
		workflowID: ctx.ExecutionKey().BusinessID,
		runID:      ctx.ExecutionKey().RunID,
		attempt:    c.Attempt,
	}, nil
}

type saveResultInput struct {
	result      invocationResult
	retryPolicy backoff.RetryPolicy
}

func (c *Callback) saveResult(
	ctx chasm.MutableContext,
	input saveResultInput,
) (chasm.NoValue, error) {
	// If the callback was terminated while the invocation was in-flight,
	// the result is no longer relevant — drop it silently.
	//
	// TODO(chrsmith): Unresolved comment: https://github.com/temporalio/temporal/pull/9805/changes#r3105945291
	// > Roey: The transitions should already validate that the callback is in a correct state
	// > 	and the task will fail with a warning in the log (IIRC). It's an edge case that I am
	// > 	happy to ignore since the error would be benign.
	// > Quinn: Why would we want benign warning logs in production? Why not just not process it and skip the log?
	// > Roey: It's rare enough that I think it doesn't matter that much. I'm not sure if there's a log or not in the task processing
	// > 	stack. It could be useful for debugging to know that we sent out a request but ended up not recording the outcome.
	if c.LifecycleState(ctx).IsClosed() {
		return nil, nil
	}

	switch r := input.result.(type) {
	case invocationResultOK:
		err := TransitionSucceeded.Apply(c, ctx, EventSucceeded{Time: ctx.Now(c)})
		return nil, err
	case invocationResultRetry:
		err := TransitionAttemptFailed.Apply(c, ctx, EventAttemptFailed{
			Time:        ctx.Now(c),
			Err:         r.err,
			RetryPolicy: input.retryPolicy,
		})
		return nil, err
	case invocationResultRetryNoCB:
		err := TransitionAttemptFailed.Apply(c, ctx, EventAttemptFailed{
			Time:        ctx.Now(c),
			Err:         r.err,
			RetryPolicy: input.retryPolicy,
		})
		return nil, err
	case invocationResultFail:
		err := TransitionFailed.Apply(c, ctx, EventFailed{
			Time: ctx.Now(c),
			Err:  r.err,
		})
		return nil, err
	default:
		return nil, queueserrors.NewUnprocessableTaskError(
			fmt.Sprintf("unrecognized callback result %v", input.result),
		)
	}
}

// ToAPICallback converts a CHASM callback to API callback proto.
func (c *Callback) ToAPICallback() (*commonpb.Callback, error) {
	// Convert CHASM callback proto to API callback proto
	chasmCB := c.GetCallback()
	res := &commonpb.Callback{
		Links: chasmCB.GetLinks(),
	}

	// CHASM currently only supports Nexus callbacks
	if variant, ok := chasmCB.Variant.(*callbackspb.Callback_Nexus_); ok {
		res.Variant = &commonpb.Callback_Nexus_{
			Nexus: &commonpb.Callback_Nexus{
				Url:    variant.Nexus.GetUrl(),
				Header: variant.Nexus.GetHeader(),
			},
		}
		return res, nil
	}

	// This should not happen as CHASM only supports Nexus callbacks currently.
	return nil, serviceerror.NewInternal("unsupported CHASM callback type")
}

// ScheduleStandbyCallbacks transitions all STANDBY callbacks to SCHEDULED state,
// triggering their invocation. Used by both workflows and standalone activities
// when the execution reaches a terminal state.
func ScheduleStandbyCallbacks(ctx chasm.MutableContext, callbacks chasm.Map[string, *Callback]) error {
	for _, field := range callbacks {
		cb := field.Get(ctx)
		if cb.Status != callbackspb.CALLBACK_STATUS_STANDBY {
			continue
		}
		if err := TransitionScheduled.Apply(cb, ctx, EventScheduled{}); err != nil {
			return err
		}
	}
	return nil
}
