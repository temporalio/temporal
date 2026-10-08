package activity

import (
	"github.com/nexus-rpc/sdk-go/nexus" //nolint:importas
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/chasm/lib/callback"
	callbackpb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
)

func (a *Activity) CompletionCallbacks() chasm.Map[string, *callback.Callback] {
	if a.Callbacks == nil {
		a.Callbacks = make(chasm.Map[string, *callback.Callback])
	}
	return a.Callbacks
}

func (a *Activity) CompletionCallbackMetadata() *callbackpb.CallbackMetadata {
	if a.CallbackMetadata == nil {
		a.CallbackMetadata = &callbackpb.CallbackMetadata{}
	}
	return a.CallbackMetadata
}

// GetNexusCompletion returns the activity's completion data in the format required by the Nexus callback invocation.
// Implements callback.CompletionSource.
func (a *Activity) GetNexusCompletion(ctx chasm.Context, _ string) (nexusrpc.CompleteOperationOptions, error) {
	if !a.LifecycleState(ctx).IsClosed() {
		return nexusrpc.CompleteOperationOptions{}, serviceerror.NewInternal("activity has not completed yet")
	}

	key := ctx.ExecutionKey()
	backLink := commonnexus.ConvertLinkActivityToNexusLink(&commonpb.Link_Activity{
		Namespace:  ctx.NamespaceEntry().Name().String(),
		ActivityId: key.BusinessID,
		RunId:      key.RunID,
	})

	opts := nexusrpc.CompleteOperationOptions{
		StartTime: a.GetScheduleTime().AsTime(),
		CloseTime: ctx.ExecutionInfo().CloseTime,
		Links:     []nexus.Link{backLink},
	}

	outcome := a.Outcome.Get(ctx)
	if successful := outcome.GetSuccessful(); successful != nil {
		// Successful completion: return the first output payload as the result as Nexus supports only a single payload
		var p *commonpb.Payload
		if payloads := successful.GetOutput().GetPayloads(); len(payloads) > 0 {
			p = payloads[0]
		}
		opts.Result = p
		return opts, nil
	}

	failure := a.terminalFailure(ctx)
	if failure != nil {
		state := nexus.OperationStateFailed
		message := "operation failed"
		if a.Status == activitypb.ACTIVITY_EXECUTION_STATUS_CANCELED {
			state = nexus.OperationStateCanceled
			message = "operation canceled"
		}

		nf, err := commonnexus.TemporalFailureToNexusFailure(failure)
		if err != nil {
			return nexusrpc.CompleteOperationOptions{}, serviceerror.NewInternalf("failed to convert failure: %v", err)
		}

		opErr := &nexus.OperationError{
			State:   state,
			Message: message,
			Cause:   &nexus.FailureError{Failure: nf},
		}
		if err := nexusrpc.MarkAsWrapperError(nexusrpc.DefaultFailureConverter(), opErr); err != nil {
			return nexusrpc.CompleteOperationOptions{}, err
		}
		opts.Error = opErr
		return opts, nil
	}

	return nexusrpc.CompleteOperationOptions{}, serviceerror.NewInternalf("activity in status %v has no outcome", a.Status)
}
