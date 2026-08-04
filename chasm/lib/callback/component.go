package callback

import (
	"fmt"
	"maps"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common"
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
	case callbackspb.CALLBACK_STATUS_FAILED:
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

func (c *Callback) recordAttempt(ts time.Time) {
	c.Attempt++
	c.LastAttemptCompleteTime = timestamppb.New(ts)
}

//nolint:revive // context.Context is an input parameter for chasm.ReadComponent, not a function parameter
func (c *Callback) loadInvocationArgs(
	ctx chasm.Context,
	_ chasm.NoValue,
) (invocable, error) {
	// Only Nexus-variant callbacks are supported.
	callback := c.GetCallback().GetNexus()
	if callback == nil {
		return nil, queueserrors.NewUnprocessableTaskError(
			fmt.Sprintf("unprocessable callback variant: %T", c.GetCallback().GetVariant()),
		)
	}

	target := c.CompletionSource.Get(ctx)
	completion, err := target.GetNexusCompletion(ctx, c.RequestId)
	if err != nil {
		return nil, err
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
		Links: common.CloneProtoSlice(chasmCB.GetLinks()),
	}

	switch variant := chasmCB.GetVariant().(type) {
	case *callbackspb.Callback_Nexus_:
		res.Variant = &commonpb.Callback_Nexus_{
			Nexus: &commonpb.Callback_Nexus{
				Url:    variant.Nexus.GetUrl(),
				Header: maps.Clone(variant.Nexus.GetHeader()),
			},
		}
		return res, nil
	case *callbackspb.Callback_Worker_:
		res.Variant = &commonpb.Callback_Worker_{
			Worker: &commonpb.Callback_Worker{
				TaskQueueName: variant.Worker.GetTaskQueueName(),
				Service:       variant.Worker.GetService(),
				Operation:     variant.Worker.GetOperation(),
				SourceContext: common.CloneProto(variant.Worker.GetSourceContext()),
			},
		}
		return res, nil
	default:
		return nil, serviceerror.NewInternalf("unsupported CHASM callback type: %T", variant)
	}
}

// FromAPICallback converts an API callback into a CHASM callback proto.
func FromAPICallback(cb *commonpb.Callback) (*callbackspb.Callback, error) {
	res := &callbackspb.Callback{
		Links: common.CloneProtoSlice(cb.GetLinks()),
	}

	switch variant := cb.GetVariant().(type) {
	case *commonpb.Callback_Nexus_:
		res.Variant = &callbackspb.Callback_Nexus_{
			Nexus: &callbackspb.Callback_Nexus{
				Url:    variant.Nexus.GetUrl(),
				Header: maps.Clone(variant.Nexus.GetHeader()),
			},
		}
		return res, nil
	case *commonpb.Callback_Worker_:
		// Conversion is supported so worker callbacks can be persisted, but
		// invoking them is not yet implemented.
		res.Variant = &callbackspb.Callback_Worker_{
			Worker: &callbackspb.Callback_Worker{
				TaskQueueName: variant.Worker.GetTaskQueueName(),
				Service:       variant.Worker.GetService(),
				Operation:     variant.Worker.GetOperation(),
				SourceContext: common.CloneProto(variant.Worker.GetSourceContext()),
			},
		}
		return res, nil
	default:
		return nil, serviceerror.NewInvalidArgumentf("unsupported callback variant: %T", variant)
	}
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
