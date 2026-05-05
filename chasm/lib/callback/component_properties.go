package callback

import (
	"fmt"

	"github.com/nexus-rpc/sdk-go/nexus"
	callbackpb "go.temporal.io/api/callback/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
)

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
		return nexusrpc.CompleteOperationOptions{}, serviceerror.NewInvalidArgument("no completion result provided")
	}
}

// CompletionSource returns the CompletionSource from the callback, which depends on whether it
// is embedded or is running in standalone mode.
func (c *Callback) CompletionSource(ctx chasm.Context) CompletionSource {
	// Embedded callbacks use their parent component as a CompletionSource.
	source, ok := c.ParentCompletionSource.TryGet(ctx)
	if ok {
		return source
	}

	// For standalone completions, get the user-supplied value and convert it
	// into the Nexus API type.
	suppliedCompletion, ok := c.SuppliedCompletion.TryGet(ctx)
	if !ok {
		return CompletionSourceFn(func(_ chasm.Context, _ string) (nexusrpc.CompleteOperationOptions, error) {
			return nexusrpc.CompleteOperationOptions{}, serviceerror.NewInternal("no completion available")
		})
	}

	convertOutcomeProtoFn := func(_ chasm.Context, _ string) (nexusrpc.CompleteOperationOptions, error) {
		return callbackCompletionToNexusCompleteOperationOpts(c, suppliedCompletion)
	}
	return CompletionSourceFn(convertOutcomeProtoFn)
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

// Describe returns the CallbackExecutionInfo for the describe RPC. Only applies to standalone callbacks.
func (c *Callback) Describe(ctx chasm.Context) (*callbackpb.CallbackExecutionInfo, error) {
	apiCb, err := c.ToAPICallback()
	if err != nil {
		return nil, err
	}

	exInfo := ctx.ExecutionInfo()
	info := &callbackpb.CallbackExecutionInfo{
		CallbackId:              c.CallbackId,
		RunId:                   ctx.ExecutionKey().RunID,
		Callback:                apiCb,
		Status:                  callbackStatusToAPIExecutionStatus(c.Status),
		State:                   callbackStatusToAPIState(c.Status),
		Attempt:                 c.Attempt,
		CreateTime:              c.RegistrationTime,
		LastAttemptCompleteTime: c.LastAttemptCompleteTime,
		LastAttemptFailure:      c.LastAttemptFailure,
		NextAttemptScheduleTime: c.NextAttemptScheduleTime,
		CloseTime:               c.CloseTime,
		ScheduleToCloseTimeout:  c.CompletionScheduleToCloseTimeout,
		StateTransitionCount:    exInfo.StateTransitionCount,
	}
	return info, nil
}

// Outcome returns the callback execution outcome if the execution is in a terminal state. (Otherwise, nil.)
//
// IMPORTANT: This is specific to the callback delivery, and not the actual completion. The outcome will be
// a success even if it was to deliver a failed completion result.
func (c *Callback) Outcome(ctx chasm.Context) *callbackpb.CallbackExecutionOutcome {
	switch c.Status {
	case callbackspb.CALLBACK_STATUS_SUCCEEDED:
		val := &callbackpb.CallbackExecutionOutcome_Success{}
		return &callbackpb.CallbackExecutionOutcome{
			Value: val,
		}

	case callbackspb.CALLBACK_STATUS_FAILED,
		callbackspb.CALLBACK_STATUS_TERMINATED:
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
