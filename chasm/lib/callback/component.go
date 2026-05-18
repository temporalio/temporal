package callback

import (
	"fmt"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	callbackpb "go.temporal.io/api/callback/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common/backoff"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	queueserrors "go.temporal.io/server/service/history/queues/errors"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type CompletionSource interface {
	GetNexusCompletion(ctx chasm.Context, requestID string) (nexusrpc.CompleteOperationOptions, error)
}

var (
	_ chasm.RootComponent                            = (*Callback)(nil)
	_ chasm.StateMachine[callbackspb.CallbackStatus] = (*Callback)(nil)
	_ chasm.VisibilitySearchAttributesProvider       = (*Callback)(nil)

	// The Callback itself is a completion source. Either delegating to its parent
	// or returning the user-supplied completion.
	_ CompletionSource = (*Callback)(nil)
)

var executionStatusSearchAttribute = chasm.NewSearchAttributeKeyword(
	"ExecutionStatus",
	chasm.SearchAttributeFieldLowCardinalityKeyword01,
)

// Callback represents a callback component in CHASM.
type Callback struct {
	chasm.UnimplementedComponent

	// Persisted internal state
	*callbackspb.CallbackState

	// Failure from an external termination (timeout or terminate), stored separately because
	// of its potential size, and to not overload CallbackState::LastAttemptFailure.
	TerminalFailure chasm.Field[*failurepb.Failure]

	// For embedded callbacks, the completion result is obtained from the parent component.
	// e.g. the Workflow result to be delivered.
	ParentCompletionSource chasm.ParentPtr[CompletionSource]
	// The user-supplied completion for standalone callbacks.
	SuppliedCompletion chasm.Field[*callbackpb.CallbackExecutionCompletion]

	// Visibility sub-component for search attributes and memo indexing.
	Visibility chasm.Field[*chasm.Visibility]
}

// NewEmbeddedCallback returns a Callback component, which will deliver the completion from
// its parent CHASM component. The parent must implement CompletionSource.
func NewEmbeddedCallback(
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

// newStandaloneCallbackInput is the bundle of inputs to the CHASM execution.
type newStandaloneCallbackInput struct {
	RequestID              string
	Callback               *callbackspb.Callback
	ScheduleToCloseTimeout *durationpb.Duration
	Completion             *callbackpb.CallbackExecutionCompletion
	SearchAttributes       map[string]*commonpb.Payload
}

// newStandaloneCallback constructs a new Callback component in standalone mode.
// The Callback is immediately transitioned to SCHEDULED state to begin invocation.
func newStandaloneCallback(
	ctx chasm.MutableContext,
	input *newStandaloneCallbackInput,
) (*Callback, error) {
	now := timestamppb.Now()

	// Create Callback component.
	cb := NewEmbeddedCallback(input.RequestID, now, input.Callback)
	cb.ScheduleToCloseTimeout = input.ScheduleToCloseTimeout
	cb.SuppliedCompletion = chasm.NewDataField(ctx, input.Completion)

	visibility := chasm.NewVisibilityWithData(ctx, input.SearchAttributes, nil)
	cb.Visibility = chasm.NewComponentField(ctx, visibility)

	// Immediately schedule the callback for invocation.
	if err := TransitionScheduled.Apply(cb, ctx, EventScheduled{}); err != nil {
		return nil, fmt.Errorf("failed to schedule callback: %w", err)
	}

	// Schedule the timeout as applicable.
	if durationProto := input.ScheduleToCloseTimeout; durationProto != nil {
		if duration := durationProto.AsDuration(); duration > 0 {
			timeoutTime := now.AsTime().Add(duration)
			ctx.AddTask(
				cb,
				chasm.TaskAttributes{ScheduledTime: timeoutTime},
				&callbackspb.ScheduleToCloseTimeoutTask{},
			)
		}
	}

	return cb, nil
}

func (c *Callback) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	switch c.Status {
	case callbackspb.CALLBACK_STATUS_SUCCEEDED:
		return chasm.LifecycleStateCompleted
	case callbackspb.CALLBACK_STATUS_FAILED,
		callbackspb.CALLBACK_STATUS_TERMINATED,
		callbackspb.CALLBACK_STATUS_TIMED_OUT:
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

// ContextMetadata is used for root CHASM components, so this is only applicable
// for the standalone callback case.
func (c *Callback) ContextMetadata(ctx chasm.Context) map[string]string {
	return map[string]string{
		"request-id":  c.RequestId,
		"callback-id": ctx.ExecutionKey().BusinessID,
	}
}

// SearchAttributes implements chasm.VisibilitySearchAttributesProvider.
func (c *Callback) SearchAttributes(ctx chasm.Context) []chasm.SearchAttributeKeyValue {
	apiStatus := c.statusAsAPIExecutionStatus()
	return []chasm.SearchAttributeKeyValue{
		executionStatusSearchAttribute.Value(apiStatus.String()),
	}
}

// Terminate forcefully terminates the callback execution.
//
// If already terminated with the same request ID, this is a no-op.
// If already terminated with a different request ID, returns FailedPrecondition.
func (c *Callback) Terminate(
	ctx chasm.MutableContext,
	req chasm.TerminateComponentRequest,
) (chasm.TerminateComponentResponse, error) {
	if c.LifecycleState(ctx).IsClosed() {
		if c.TerminateRequestId == "" {
			// Completed organically (succeeded/failed/timed out), not via Terminate.
			err := serviceerror.NewFailedPreconditionf("callback execution already in terminal state %v", c.Status)
			return chasm.TerminateComponentResponse{}, err
		}
		if c.TerminateRequestId != req.RequestID {
			err := serviceerror.NewFailedPreconditionf("already terminated with request ID %s", c.TerminateRequestId)
			return chasm.TerminateComponentResponse{}, err
		}
		return chasm.TerminateComponentResponse{}, nil
	}
	event := EventTerminated{
		Identity: req.Identity,
		Reason:   req.Reason,
	}
	if err := TransitionTerminated.Apply(c, ctx, event); err != nil {
		return chasm.TerminateComponentResponse{}, fmt.Errorf("failed to terminate callback: %w", err)
	}

	c.TerminateRequestId = req.RequestID
	// c.TerminalFailure is set in the transition handler.
	return chasm.TerminateComponentResponse{}, nil
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
	// Get the completion result to be delivered.
	completion, err := c.GetNexusCompletion(ctx, c.RequestId)
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

	// This should not happen as CHASM only supports Nexus callbacks currently
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

func callbackCompletionToNexusCompleteOperationOpts(
	cb *Callback,
	completion *callbackpb.CallbackExecutionCompletion) (nexusrpc.CompleteOperationOptions, error) {

	nexusCompletion := nexusrpc.CompleteOperationOptions{
		StartTime: cb.GetRegistrationTime().AsTime(),
		CloseTime: cb.CloseTime.AsTime(),
	}

	switch completion.Result.(type) {
	case *callbackpb.CallbackExecutionCompletion_Success:
		nexusCompletion.Result = completion.GetSuccess()
		return nexusCompletion, nil

	case *callbackpb.CallbackExecutionCompletion_Failure:
		f, err := commonnexus.TemporalFailureToNexusFailure(completion.GetFailure())
		if err != nil {
			wrappedErr := fmt.Errorf("failed to convert failure: %w", err)
			return nexusrpc.CompleteOperationOptions{}, wrappedErr
		}
		opErr := &nexus.OperationError{
			State:   nexus.OperationStateFailed,
			Message: "operation failed",
			Cause:   &nexus.FailureError{Failure: f},
		}
		if err := nexusrpc.MarkAsWrapperError(nexusrpc.DefaultFailureConverter(), opErr); err != nil {
			wrappedErr := fmt.Errorf("failed to mark wrapper error: %w", err)
			return nexusrpc.CompleteOperationOptions{}, wrappedErr
		}
		nexusCompletion.Error = opErr
		return nexusCompletion, nil

	default:
		return nexusrpc.CompleteOperationOptions{}, serviceerror.NewInternal("no completion result provided")
	}
}

// GetNexusCompletion returns the Nexus completion to be delivered for the callback.
func (c *Callback) GetNexusCompletion(ctx chasm.Context, requestID string) (nexusrpc.CompleteOperationOptions, error) {
	// Embedded callbacks use their parent component as a CompletionSource.
	if source, ok := c.ParentCompletionSource.TryGet(ctx); ok {
		return source.GetNexusCompletion(ctx, requestID)
	}

	// For standalone completions, get the user-supplied value and convert it into the Nexus API type.
	suppliedCompletion, ok := c.SuppliedCompletion.TryGet(ctx)
	if !ok {
		return nexusrpc.CompleteOperationOptions{}, serviceerror.NewInvalidArgument("no completion result provided")
	}
	return callbackCompletionToNexusCompleteOperationOpts(c, suppliedCompletion)
}

// describe returns the CallbackExecutionInfo for the describe RPC. Only applies to standalone callbacks.
func (c *Callback) describe(ctx chasm.Context) (*callbackpb.CallbackExecutionInfo, error) {
	apiCb, err := c.ToAPICallback()
	if err != nil {
		return nil, err
	}

	exInfo := ctx.ExecutionInfo()
	exKey := ctx.ExecutionKey()
	info := &callbackpb.CallbackExecutionInfo{
		CallbackId:              exKey.BusinessID,
		RunId:                   exKey.RunID,
		Callback:                apiCb,
		Status:                  c.statusAsAPIExecutionStatus(),
		State:                   c.statusAsAPIState(),
		Attempt:                 c.Attempt,
		CreateTime:              c.RegistrationTime,
		LastAttemptCompleteTime: c.LastAttemptCompleteTime,
		LastAttemptFailure:      c.LastAttemptFailure,
		NextAttemptScheduleTime: c.NextAttemptScheduleTime,
		CloseTime:               c.CloseTime,
		ScheduleToCloseTimeout:  c.ScheduleToCloseTimeout,
		StateTransitionCount:    exInfo.StateTransitionCount,
	}
	return info, nil
}

// outcome returns the callback execution outcome if the execution is in a terminal state. (Otherwise, nil.)
//
// IMPORTANT: This is specific to the callback delivery, and not the actual completion. The outcome will be
// a success even if it was to deliver a failed completion result.
func (c *Callback) outcome(ctx chasm.Context) *callbackpb.CallbackExecutionOutcome {
	switch c.Status {
	case callbackspb.CALLBACK_STATUS_SUCCEEDED:
		val := &callbackpb.CallbackExecutionOutcome_Success{}
		return &callbackpb.CallbackExecutionOutcome{
			Value: val,
		}

	case callbackspb.CALLBACK_STATUS_FAILED:
		// Organic failures leave TerminalFailure nil, and just set LastAttemptFailure.
		val := &callbackpb.CallbackExecutionOutcome_Failure{
			Failure: c.LastAttemptFailure,
		}
		return &callbackpb.CallbackExecutionOutcome{
			Value: val,
		}

	case callbackspb.CALLBACK_STATUS_TERMINATED,
		callbackspb.CALLBACK_STATUS_TIMED_OUT:
		val := &callbackpb.CallbackExecutionOutcome_Failure{
			Failure: c.TerminalFailure.Get(ctx),
		}
		return &callbackpb.CallbackExecutionOutcome{
			Value: val,
		}

	default:
		return nil
	}
}

func (c *Callback) statusAsAPIExecutionStatus() enumspb.CallbackExecutionStatus {
	switch c.Status {
	case callbackspb.CALLBACK_STATUS_STANDBY,
		callbackspb.CALLBACK_STATUS_SCHEDULED,
		callbackspb.CALLBACK_STATUS_BACKING_OFF:
		return enumspb.CALLBACK_EXECUTION_STATUS_RUNNING
	case callbackspb.CALLBACK_STATUS_FAILED,
		callbackspb.CALLBACK_STATUS_TIMED_OUT:
		return enumspb.CALLBACK_EXECUTION_STATUS_FAILED
	case callbackspb.CALLBACK_STATUS_SUCCEEDED:
		return enumspb.CALLBACK_EXECUTION_STATUS_SUCCEEDED
	case callbackspb.CALLBACK_STATUS_TERMINATED:
		return enumspb.CALLBACK_EXECUTION_STATUS_TERMINATED
	default:
		return enumspb.CALLBACK_EXECUTION_STATUS_UNSPECIFIED
	}
}

func (c *Callback) statusAsAPIState() enumspb.CallbackState {
	switch c.Status {
	case callbackspb.CALLBACK_STATUS_STANDBY:
		return enumspb.CALLBACK_STATE_STANDBY
	case callbackspb.CALLBACK_STATUS_SCHEDULED:
		return enumspb.CALLBACK_STATE_SCHEDULED
	case callbackspb.CALLBACK_STATUS_BACKING_OFF:
		return enumspb.CALLBACK_STATE_BACKING_OFF
	case callbackspb.CALLBACK_STATUS_FAILED:
		return enumspb.CALLBACK_STATE_FAILED
	case callbackspb.CALLBACK_STATUS_SUCCEEDED:
		return enumspb.CALLBACK_STATE_SUCCEEDED
	case callbackspb.CALLBACK_STATUS_TIMED_OUT:
		return enumspb.CALLBACK_STATE_TIMED_OUT
	case callbackspb.CALLBACK_STATUS_TERMINATED:
		return enumspb.CALLBACK_STATE_TERMINATED
	default:
		return enumspb.CALLBACK_STATE_UNSPECIFIED
	}
}
