package callback

import (
	"fmt"

	"github.com/nexus-rpc/sdk-go/nexus"
	callbackpb "go.temporal.io/api/callback/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	executionStatusSearchAttribute = chasm.NewSearchAttributeKeyword(
		"ExecutionStatus",
		chasm.SearchAttributeFieldLowCardinalityKeyword01,
	)

	_ chasm.RootComponent                      = (*CallbackExecution)(nil)
	_ CompletionSource                         = (*CallbackExecution)(nil)
	_ chasm.VisibilitySearchAttributesProvider = (*CallbackExecution)(nil)
	_ chasm.VisibilityMemoProvider             = (*CallbackExecution)(nil)
)

// CallbackExecution is a top-level CHASM entity that manages a standalone callback.
// It owns a child Callback component and implements CompletionSource to provide
// stored Nexus completion data for invocation.
type CallbackExecution struct {
	chasm.UnimplementedComponent

	// Persisted state
	*callbackspb.CallbackExecutionState

	// Child callback component
	Callback chasm.Field[*Callback]

	// Visibility sub-component for search attributes and memo indexing.
	Visibility chasm.Field[*chasm.Visibility]
}

// StartCallbackExecutionInput contains validated fields from the start request.
type StartCallbackExecutionInput struct {
	CallbackID             string
	RequestID              string
	Callback               *callbackspb.Callback
	SuccessCompletion      *commonpb.Payload
	FailureCompletion      *failurepb.Failure
	ScheduleToCloseTimeout *durationpb.Duration //nolint:revive // keeping full type name for clarity
	SearchAttributes       map[string]*commonpb.Payload
}

// CreateCallbackExecution constructs a new CallbackExecution entity with a child Callback.
// The child Callback is immediately transitioned to SCHEDULED state to begin invocation.
func CreateCallbackExecution(
	ctx chasm.MutableContext,
	input *StartCallbackExecutionInput,
) (*CallbackExecution, error) {
	now := timestamppb.Now()

	state := &callbackspb.CallbackExecutionState{
		CallbackId:             input.CallbackID,
		CreateTime:             now,
		ScheduleToCloseTimeout: input.ScheduleToCloseTimeout,
	}

	// Store the completion payload.
	if input.SuccessCompletion != nil {
		state.Completion = &callbackspb.CallbackExecutionState_SuccessCompletion{
			SuccessCompletion: input.SuccessCompletion,
		}
	} else if input.FailureCompletion != nil {
		state.Completion = &callbackspb.CallbackExecutionState_FailureCompletion{
			FailureCompletion: input.FailureCompletion,
		}
	}

	// Create child Callback component.
	cb := NewCallback(
		input.RequestID,
		now,
		&callbackspb.CallbackState{},
		input.Callback,
	)

	exec := &CallbackExecution{
		CallbackExecutionState: state,
	}
	exec.Callback = chasm.NewComponentField(ctx, cb)

	visibility := chasm.NewVisibilityWithData(ctx, input.SearchAttributes, nil)
	exec.Visibility = chasm.NewComponentField(ctx, visibility)

	// Immediately schedule the callback for invocation.
	if err := TransitionScheduled.Apply(cb, ctx, EventScheduled{}); err != nil {
		return nil, fmt.Errorf("failed to schedule callback: %w", err)
	}

	// Schedule the timeout task if ScheduleToCloseTimeout is set.
	if input.ScheduleToCloseTimeout != nil {
		if timeout := input.ScheduleToCloseTimeout.AsDuration(); timeout > 0 {
			ctx.AddTask(
				cb,
				chasm.TaskAttributes{
					ScheduledTime: now.AsTime().Add(timeout),
				},
				&callbackspb.ScheduleToCloseTimeoutTask{},
			)
		}
	}

	return exec, nil
}

// LifecycleState delegates to the child Callback's lifecycle state.
func (e *CallbackExecution) LifecycleState(ctx chasm.Context) chasm.LifecycleState {
	cb := e.Callback.Get(ctx)
	return cb.LifecycleState(ctx)
}

// Terminate forcefully terminates the callback execution.
// If already terminated with the same request ID, this is a no-op.
// If already terminated with a different request ID, returns FailedPrecondition.
func (e *CallbackExecution) Terminate(
	ctx chasm.MutableContext,
	req chasm.TerminateComponentRequest,
) (chasm.TerminateComponentResponse, error) {
	cb := e.Callback.Get(ctx)
	if cb.LifecycleState(ctx).IsClosed() {
		if e.TerminateRequestId == "" {
			// Completed organically (succeeded/failed/timed out), not via Terminate.
			return chasm.TerminateComponentResponse{}, serviceerror.NewFailedPreconditionf(
				"callback execution already in terminal state %v", cb.Status)
		}
		if e.TerminateRequestId != req.RequestID {
			return chasm.TerminateComponentResponse{}, serviceerror.NewFailedPreconditionf(
				"already terminated with request ID %s", e.TerminateRequestId)
		}
		return chasm.TerminateComponentResponse{}, nil
	}
	if err := TransitionTerminated.Apply(cb, ctx, EventTerminated{Reason: req.Reason}); err != nil {
		return chasm.TerminateComponentResponse{}, fmt.Errorf("failed to terminate callback: %w", err)
	}
	e.TerminateRequestId = req.RequestID
	return chasm.TerminateComponentResponse{}, nil
}

// Describe returns CallbackExecutionInfo for the describe RPC.
func (e *CallbackExecution) Describe(ctx chasm.Context) (*callbackpb.CallbackExecutionInfo, error) {
	cb := e.Callback.Get(ctx)
	apiCb, err := cb.ToAPICallback()
	if err != nil {
		return nil, err
	}

	info := &callbackpb.CallbackExecutionInfo{
		CallbackId:              e.CallbackId,
		RunId:                   ctx.ExecutionKey().RunID,
		Callback:                apiCb,
		Status:                  callbackStatusToAPIExecutionStatus(cb.Status),
		State:                   callbackStatusToAPIState(cb.Status),
		Attempt:                 cb.Attempt,
		CreateTime:              e.CreateTime,
		LastAttemptCompleteTime: cb.LastAttemptCompleteTime,
		LastAttemptFailure:      cb.LastAttemptFailure,
		NextAttemptScheduleTime: cb.NextAttemptScheduleTime,
		CloseTime:               cb.CloseTime,
		ScheduleToCloseTimeout:  e.ScheduleToCloseTimeout,
		StateTransitionCount:    ctx.StateTransitionCount(),
	}
	return info, nil
}

// GetOutcome returns the callback execution outcome if the execution is in a terminal state.
func (e *CallbackExecution) GetOutcome(ctx chasm.Context) (*callbackpb.CallbackExecutionOutcome, error) {
	cb := e.Callback.Get(ctx)
	switch cb.Status {
	case callbackspb.CALLBACK_STATUS_SUCCEEDED:
		return &callbackpb.CallbackExecutionOutcome{
			Value: &callbackpb.CallbackExecutionOutcome_Success{},
		}, nil
	case callbackspb.CALLBACK_STATUS_FAILED,
		callbackspb.CALLBACK_STATUS_TERMINATED:
		return &callbackpb.CallbackExecutionOutcome{
			Value: &callbackpb.CallbackExecutionOutcome_Failure{
				Failure: cb.LastAttemptFailure,
			},
		}, nil
	default:
		return nil, nil
	}
}

// callbackStatusToAPIExecutionStatus maps internal CallbackStatus to public API CallbackExecutionStatus.
func callbackStatusToAPIExecutionStatus(status callbackspb.CallbackStatus) enumspb.CallbackExecutionStatus {
	switch status {
	case callbackspb.CALLBACK_STATUS_STANDBY,
		callbackspb.CALLBACK_STATUS_SCHEDULED,
		callbackspb.CALLBACK_STATUS_BACKING_OFF:
		return enumspb.CALLBACK_EXECUTION_STATUS_RUNNING
	case callbackspb.CALLBACK_STATUS_FAILED:
		return enumspb.CALLBACK_EXECUTION_STATUS_FAILED
	case callbackspb.CALLBACK_STATUS_SUCCEEDED:
		return enumspb.CALLBACK_EXECUTION_STATUS_SUCCEEDED
	case callbackspb.CALLBACK_STATUS_TERMINATED:
		return enumspb.CALLBACK_EXECUTION_STATUS_TERMINATED
	default:
		return enumspb.CALLBACK_EXECUTION_STATUS_UNSPECIFIED
	}
}

// callbackStatusToAPIState maps internal CallbackStatus to public API CallbackState.
func callbackStatusToAPIState(status callbackspb.CallbackStatus) enumspb.CallbackState {
	switch status {
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
	case callbackspb.CALLBACK_STATUS_TERMINATED:
		return enumspb.CALLBACK_STATE_TERMINATED
	default:
		return enumspb.CALLBACK_STATE_UNSPECIFIED
	}
}

// SearchAttributes implements chasm.VisibilitySearchAttributesProvider.
func (e *CallbackExecution) SearchAttributes(ctx chasm.Context) []chasm.SearchAttributeKeyValue {
	cb := e.Callback.Get(ctx)
	return []chasm.SearchAttributeKeyValue{
		executionStatusSearchAttribute.Value(callbackStatusToAPIExecutionStatus(cb.Status).String()),
	}
}

// Memo implements chasm.VisibilityMemoProvider. Returns the CallbackExecutionListInfo
// as the memo for visibility queries.
func (e *CallbackExecution) Memo(ctx chasm.Context) proto.Message {
	cb := e.Callback.Get(ctx)
	return &callbackpb.CallbackExecutionListInfo{
		CallbackId: e.CallbackId,
		Status:     callbackStatusToAPIExecutionStatus(cb.Status),
		CreateTime: e.CreateTime,
		CloseTime:  cb.CloseTime,
	}
}

// GetNexusCompletion implements CompletionSource. It converts the stored completion
// payload to nexusrpc.CompleteOperationOptions for use by the Callback invocation logic.
func (e *CallbackExecution) GetNexusCompletion(
	ctx chasm.Context,
	requestID string,
) (nexusrpc.CompleteOperationOptions, error) {
	opts := nexusrpc.CompleteOperationOptions{
		StartTime: e.CreateTime.AsTime(),
	}
	switch c := e.Completion.(type) {
	case *callbackspb.CallbackExecutionState_SuccessCompletion:
		opts.Result = c.SuccessCompletion
		return opts, nil
	case *callbackspb.CallbackExecutionState_FailureCompletion:
		f, err := commonnexus.TemporalFailureToNexusFailure(c.FailureCompletion)
		if err != nil {
			return nexusrpc.CompleteOperationOptions{}, fmt.Errorf("failed to convert failure: %w", err)
		}
		opErr := &nexus.OperationError{
			State:   nexus.OperationStateFailed,
			Message: "operation failed",
			Cause:   &nexus.FailureError{Failure: f},
		}
		if err := nexusrpc.MarkAsWrapperError(nexusrpc.DefaultFailureConverter(), opErr); err != nil {
			return nexusrpc.CompleteOperationOptions{}, fmt.Errorf("failed to mark wrapper error: %w", err)
		}
		opts.Error = opErr
		return opts, nil
	default:
		return nexusrpc.CompleteOperationOptions{}, fmt.Errorf("empty completion payload")
	}
}
