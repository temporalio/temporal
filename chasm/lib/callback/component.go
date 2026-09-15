package callback

import (
	"fmt"
	"maps"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	callbackpb "go.temporal.io/api/callback/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/log/tag"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/softassert"
	queueserrors "go.temporal.io/server/service/history/queues/errors"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// CompletionSource is the interface different kinds of executions implement so that their result can be
// delivered to waiting callback handlers.
type CompletionSource interface {
	// GetNexusCompletion returns the execution's result. Links on the returned CompleteOperationOptions
	// are the "backlinks", so a Nexus handler receiving the completion can link to its source.
	GetNexusCompletion(ctx chasm.Context, requestID string) (nexusrpc.CompleteOperationOptions, error)

	// GetComponentExecutionPath returns the type of execution the source belongs to, along with the
	// component path addressing the source within it. A source that is itself the root component of
	// its execution has no path. A Workflow Update returns [ "Updates", updateID ].
	GetComponentExecutionPath() (enumspb.ExecutionType, []string)
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
	// Reject unknown/unsupported callback variants.
	switch c.GetCallback().GetVariant().(type) {
	case *callbackspb.Callback_Nexus_, *callbackspb.Callback_NexusHandler_:
		// OK
	default:
		return nil, queueserrors.NewUnprocessableTaskError(
			fmt.Sprintf("unprocessable callback variant: %T", c.GetCallback().GetVariant()),
		)
	}

	// Get the parent CHASM object's Nexus result to be delivered.
	target := c.CompletionSource.Get(ctx)
	completion, err := target.GetNexusCompletion(ctx, c.RequestId)
	if err != nil {
		return nil, err
	}

	// NexusHandler callbacks, deliver the result by invoking a Nexus handler.
	if nexusHandler := c.GetCallback().GetNexusHandler(); nexusHandler != nil {
		// Generate a backlink pointing to this CHASM Callback. NexusHandler-variant callbacks do not invoke the
		// targeted Nexus handler using the links carried on the CompletionSource, because that would point to the
		// source execution. Instead, we use the Callback-variant link to identify a particular completion callback
		// _attached to_ the source execution.
		//
		// Links are supplementary, so a source reporting an execution type with no link representation still has
		// its completion delivered, just without one.
		var backlinks []*nexuspb.Link
		if backlink, err := c.buildCallbackBacklink(ctx); err == nil {
			backlinks = commonnexus.ConvertLinksToProto([]nexus.Link{backlink})
		} else {
			softassert.Fail(
				ctx.Logger(),
				"failed to build the callback backlink",
				tag.Error(err),
				tag.NexusCompletionSource(c.CompletionSource.Fqn()),
			)
		}

		return invocableNexusHandler{
			callback:            nexusHandler,
			completion:          completion,
			callbackBacklinks:   backlinks,
			completionSourceTag: c.CompletionSource.Fqn(),
			businessID:          ctx.ExecutionKey().BusinessID,
			runID:               ctx.ExecutionKey().RunID,
			requestID:           c.RequestId,
			attempt:             c.Attempt,
		}, nil
	}

	// Nexus callbacks deliver results by also invoking a Nexus handler, but using HTTP.
	callback := c.GetCallback().GetNexus()
	if callback.GetUrl() == chasm.NexusCompletionHandlerURL {
		return invocableInternal{
			callback:   callback,
			attempt:    c.Attempt,
			completion: completion,
			requestID:  c.RequestId,
		}, nil
	}
	return invocableOutbound{
		callback:            callback,
		completion:          completion,
		completionSourceTag: c.CompletionSource.Fqn(),
		businessID:          ctx.ExecutionKey().BusinessID,
		runID:               ctx.ExecutionKey().RunID,
		attempt:             c.Attempt,
	}, nil
}

// recordHandlerLinks stores the links the callback's target returned when it accepted the delivery.
// For a NexusHandler callback these are the handler links the worker attached to its StartOperation
// response, e.g. a workflow_event link to the workflow it started to process the completion.
func (c *Callback) recordHandlerLinks(ctx chasm.MutableContext, links []nexus.Link) error {
	if len(links) == 0 {
		return nil
	}
	// Unconvertible links are dropped with a warning rather than failing the callback. The callback
	// has already been delivered at this point, so returning an error would fail the transition and
	// leave the delivered callback retrying forever.
	protoLinks := commonnexus.ConvertNexusLinksToProtoLinks(links, ctx.Logger())
	if len(protoLinks) == 0 {
		return nil
	}
	return ctx.SetRequestLinks(c, c.RequestId, protoLinks)
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
		// Persist any links returned from the callback's invocation.
		// This is only applicable to the NexusHandler-callback case.
		if err := c.recordHandlerLinks(ctx, r.links); err != nil {
			return nil, err
		}
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
	case *callbackspb.Callback_NexusHandler_:
		res.Variant = &commonpb.Callback_NexusHandler_{
			NexusHandler: &commonpb.Callback_NexusHandler{
				TaskQueueName: variant.NexusHandler.GetTaskQueueName(),
				Service:       variant.NexusHandler.GetService(),
				Operation:     variant.NexusHandler.GetOperation(),
				SourceContext: common.CloneProto(variant.NexusHandler.GetSourceContext()),
			},
		}
		return res, nil
	default:
		return nil, serviceerror.NewInternalf("unsupported CHASM callback type: %T", variant)
	}
}

// APIState converts the CHASM callback status to the API CallbackState enum along with the relevant
// circuit breaker's blocking status.
func (c *Callback) APIState(ctx chasm.Context) (enumspb.CallbackState, string, error) {
	state, err := c.apiStatus()
	if err != nil {
		return enumspb.CALLBACK_STATE_UNSPECIFIED, "", err
	}

	// The circuit breaker is only relevant for scheduled callbacks.
	if state != enumspb.CALLBACK_STATE_SCHEDULED {
		return state, "", nil
	}

	cbCtx := callbackContextFromChasm(ctx)
	destination, err := callbackDestination(c.GetCallback())
	if err != nil {
		return enumspb.CALLBACK_STATE_UNSPECIFIED, "", err
	}
	if !cbCtx.destinationBlocked(ctx.ExecutionKey().NamespaceID, destination) {
		return state, "", nil
	}
	return enumspb.CALLBACK_STATE_BLOCKED, "The circuit breaker is open.", nil
}

func (c *Callback) apiStatus() (enumspb.CallbackState, error) {
	switch c.Status {
	case callbackspb.CALLBACK_STATUS_STANDBY:
		return enumspb.CALLBACK_STATE_STANDBY, nil
	case callbackspb.CALLBACK_STATUS_SCHEDULED:
		return enumspb.CALLBACK_STATE_SCHEDULED, nil
	case callbackspb.CALLBACK_STATUS_BACKING_OFF:
		return enumspb.CALLBACK_STATE_BACKING_OFF, nil
	case callbackspb.CALLBACK_STATUS_FAILED:
		return enumspb.CALLBACK_STATE_FAILED, nil
	case callbackspb.CALLBACK_STATUS_SUCCEEDED:
		return enumspb.CALLBACK_STATE_SUCCEEDED, nil
	case callbackspb.CALLBACK_STATUS_UNSPECIFIED:
		return enumspb.CALLBACK_STATE_UNSPECIFIED, serviceerror.NewInternal("callback with UNSPECIFIED state")
	default:
		return enumspb.CALLBACK_STATE_UNSPECIFIED, serviceerror.NewInternalf("unknown callback state: %v", c.Status)
	}
}

// ToAPICallbackInfo returns the API CallbackInfo based on the current state of the CHASM component.
func (c *Callback) ToAPICallbackInfo(ctx chasm.Context) (*callbackpb.CallbackInfo, error) {
	apiCb, err := c.ToAPICallback()
	if err != nil {
		return nil, err
	}
	// Merge the static links that were part of the callback's creation (apiCb.Links) with
	// any new links picked up as part of the callback's execution.
	existingLinks := ctx.Links(c)
	apiCb.Links = append(apiCb.Links, common.CloneProtoSlice(existingLinks)...)
	apiState, blockedReason, err := c.APIState(ctx)
	if err != nil {
		return nil, err
	}

	info := &callbackpb.CallbackInfo{
		Callback:                apiCb,
		RegistrationTime:        common.CloneProto(c.RegistrationTime),
		State:                   apiState,
		BlockedReason:           blockedReason,
		RequestId:               c.RequestId,
		Attempt:                 c.Attempt,
		LastAttemptCompleteTime: common.CloneProto(c.LastAttemptCompleteTime),
		LastAttemptFailure:      common.CloneProto(c.LastAttemptFailure),
		NextAttemptScheduleTime: common.CloneProto(c.NextAttemptScheduleTime),
	}
	return info, nil
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
	case *commonpb.Callback_NexusHandler_:
		res.Variant = &callbackspb.Callback_NexusHandler_{
			NexusHandler: &callbackspb.Callback_NexusHandler{
				TaskQueueName: variant.NexusHandler.GetTaskQueueName(),
				Service:       variant.NexusHandler.GetService(),
				Operation:     variant.NexusHandler.GetOperation(),
				SourceContext: common.CloneProto(variant.NexusHandler.GetSourceContext()),
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

// buildCallbackBacklink returns a commonpb.Link_Callback encoded as a nexus.Link, addressing this
// callback within the execution of its completion source.
func (c *Callback) buildCallbackBacklink(ctx chasm.Context) (nexus.Link, error) {
	exKey := ctx.ExecutionKey()
	exType, componentPath := c.CompletionSource.Get(ctx).GetComponentExecutionPath()
	link, err := commonnexus.ConvertLinkCallbackToNexusLink(&commonpb.Link_Callback{
		Namespace: ctx.NamespaceEntry().Name().String(),
		Execution: &commonpb.Execution{
			Type:       exType,
			BusinessId: exKey.BusinessID,
			RunId:      exKey.RunID,
		},
		ComponentPath: componentPath,
		RequestId:     c.GetRequestId(),
	})
	if err != nil {
		return nexus.Link{}, fmt.Errorf("converting to nexus.Link: %w", err)
	}
	return link, nil
}
