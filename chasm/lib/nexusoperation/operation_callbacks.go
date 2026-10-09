package nexusoperation

import (
	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1" //nolint:importas
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/callback"
	callbackpb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
)

func (o *Operation) CompletionCallbacks() chasm.Map[string, *callback.Callback] {
	if o.Callbacks == nil {
		o.Callbacks = make(chasm.Map[string, *callback.Callback])
	}
	return o.Callbacks
}

func (o *Operation) CompletionCallbackMetadata() *callbackpb.CallbackMetadata {
	if o.CallbackMetadata == nil {
		o.CallbackMetadata = &callbackpb.CallbackMetadata{}
	}
	return o.CallbackMetadata
}

// GetNexusCompletion implements callback.CompletionSource, providing the result of the Nexus operation.
func (o *Operation) GetNexusCompletion(ctx chasm.Context, _ string) (nexusrpc.CompleteOperationOptions, error) {
	if !o.isClosed() {
		return nexusrpc.CompleteOperationOptions{}, serviceerror.NewInternal("nexus operation has not completed yet")
	}

	key := ctx.ExecutionKey()
	backLink := commonnexus.ConvertLinkNexusOperationToNexusLink(&commonpb.Link_NexusOperation{
		Namespace:   ctx.NamespaceEntry().Name().String(),
		OperationId: key.BusinessID,
		RunId:       key.RunID,
	})

	opts := nexusrpc.CompleteOperationOptions{
		StartTime: o.GetScheduledTime().AsTime(),
		CloseTime: ctx.ExecutionInfo().CloseTime,
		Links:     []nexus.Link{backLink},
	}

	result, failure := o.outcome(ctx)
	if o.Status == nexusoperationpb.OPERATION_STATUS_SUCCEEDED {
		opts.Result = result
		return opts, nil
	}
	if failure == nil {
		return nexusrpc.CompleteOperationOptions{},
			serviceerror.NewInternalf("nexus operation in status %v has no outcome", o.Status)
	}

	state := nexus.OperationStateFailed
	message := "operation failed"
	if o.Status == nexusoperationpb.OPERATION_STATUS_CANCELED {
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
