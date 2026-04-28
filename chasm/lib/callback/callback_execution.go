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
//
// TODO(chrsmith): https://github.com/temporalio/temporal/pull/9805/changes#r3105932218
// > Roey: The other components that we support both standalone and embdded are structured differently. We don't
// >   create these wrapper components for them and instead make the embedded component multi-purpose. Not that
// >   what you did is incorrect, it's just different than what we have done in the past and IMHO best to keep
// >   the consistent approach.
// > Quinn: I think that is a bad approach personally, the only precedent we have is for stand alone activities
// >   no? or is there other components that did that?
// > Roey: Standalone Nexus.
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

// createCallbackExecution constructs a new CallbackExecution entity with a child Callback.
// The child Callback is immediately transitioned to SCHEDULED state to begin invocation.
func createCallbackExecution(
	ctx chasm.MutableContext,
	input *StartCallbackExecutionInput,
) (*CallbackExecution, error) {
	// TODO(chrsmith): Should this be `[MutableContext]ctx.Now(chasm.Component)`? But what
	// chasm.Component should be passed to that, since one hasn't been created yet?
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
	//
	// TODO(chrsmith): Unresolved comment.
	// > Roey: When you rewrite to reuse the callback component, make the completion an embedded field
	// > because it can save some memory for transitions and APIs that don't require this potentially large piece of data.
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
		if duration := input.ScheduleToCloseTimeout.AsDuration(); duration > 0 {
			timeoutTime := now.AsTime().Add(duration)
			ctx.AddTask(
				cb,
				chasm.TaskAttributes{ScheduledTime: timeoutTime},
				&callbackspb.ScheduleToCloseTimeoutTask{},
			)
		}
	}

	return exec, nil
}

func (e *CallbackExecution) ContextMetadata(ctx chasm.Context) map[string]string {
	md := map[string]string{
		"CallbackID": e.GetCallbackId(),
	}
	return md
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
			err := serviceerror.NewFailedPreconditionf("callback execution already in terminal state %v", cb.Status)
			return chasm.TerminateComponentResponse{}, err
		}
		if e.TerminateRequestId != req.RequestID {
			err := serviceerror.NewFailedPreconditionf("already terminated with request ID %s", e.TerminateRequestId)
			return chasm.TerminateComponentResponse{}, err
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
		CallbackId: e.CallbackId,
		RunId:      ctx.ExecutionKey().RunID,
		Callback:   apiCb,
		// QUIRK: The outcome of the CallbackExecution may have been a failure,
		// but the Status/State may report success. (Since the callback was
		// successfully delivered within the Temporal machineary.)
		Status:                  callbackStatusToAPIExecutionStatus(cb.Status),
		State:                   callbackStatusToAPIState(cb.Status),
		Attempt:                 cb.Attempt,
		CreateTime:              e.CreateTime,
		LastAttemptCompleteTime: cb.LastAttemptCompleteTime,
		LastAttemptFailure:      cb.LastAttemptFailure,
		NextAttemptScheduleTime: cb.NextAttemptScheduleTime,
		CloseTime:               cb.CloseTime,
		ScheduleToCloseTimeout:  e.ScheduleToCloseTimeout,
		StateTransitionCount:    ctx.ExecutionInfo().StateTransitionCount,
	}
	return info, nil
}

// Outcome returns the callback execution outcome if the execution is in a terminal state. (Otherwise, nil.)
func (e *CallbackExecution) GetOutcome(ctx chasm.Context) (*callbackpb.CallbackExecutionOutcome, error) {

	// BUG: This is returning the Callback's outcome. But this should instead return the CallbackExecution's
	// outcome. (That is, the CallbackExecutionState.Commpletion field.)
	cb := e.Callback.Get(ctx)
	switch cb.Status {
	case callbackspb.CALLBACK_STATUS_SUCCEEDED:
		val := &callbackpb.CallbackExecutionOutcome_Success{}
		return &callbackpb.CallbackExecutionOutcome{
			Value: val,
		}, nil

	case callbackspb.CALLBACK_STATUS_FAILED, callbackspb.CALLBACK_STATUS_TERMINATED:
		val := &callbackpb.CallbackExecutionOutcome_Failure{
			Failure: cb.GetFailure(),
		}
		return &callbackpb.CallbackExecutionOutcome{
			Value: val,
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
	statusStr := callbackStatusToAPIExecutionStatus(cb.Status).String()
	return []chasm.SearchAttributeKeyValue{
		executionStatusSearchAttribute.Value(statusStr),
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

	// TODO(chrsmith): https://github.com/temporalio/temporal/pull/9805/changes#r3105967769
	// > Roey: All of these fields should be available to you. You shouldn't need to store any of this in visibility.
	// > Quinn: Sorry I don't understand are you saying they shouldn't be on the list output?
	// > Roey: Yes in list output, not in the CHASM memo field.
}

// GetNexusCompletion implements CompletionSource. It converts the stored completion
// payload to nexusrpc.CompleteOperationOptions for use by the Callback invocation logic.
func (e *CallbackExecution) GetNexusCompletion(
	ctx chasm.Context,
	requestID string) (nexusrpc.CompleteOperationOptions, error) {

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
		opts.Error = opErr
		return opts, nil
	default:
		return nexusrpc.CompleteOperationOptions{}, fmt.Errorf("empty completion payload")
	}
}
