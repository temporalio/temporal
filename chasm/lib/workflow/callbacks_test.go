package workflow

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	commoncallbacks "go.temporal.io/server/common/callbacks"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// nexusHandlerCallback returns a NexusHandler-variant callback whose source context serializes to
// roughly sourceCtxSize bytes.
func nexusHandlerCallback(sourceCtxSize int) *commonpb.Callback {
	return &commonpb.Callback{
		Variant: &commonpb.Callback_NexusHandler_{NexusHandler: &commonpb.Callback_NexusHandler{
			TaskQueueName: "completions",
			Service:       "Adapter",
			Operation:     "Deliver",
			SourceContext: &commonpb.Payload{Data: make([]byte, sourceCtxSize)},
		}},
	}
}

// The frontend bounds the source context carried by one request. Only this check bounds what
// accumulates across the many requests that can attach callbacks to a running workflow, and it
// counts the workflow's own callbacks together with those on its updates.
func TestWorkflowCallbackSourceContextLimit(t *testing.T) {
	limits := commoncallbacks.Limits{MaxCount: 10, MaxSourceContextSize: 1500}
	eventTime := timestamppb.Now()

	t.Run("RejectsASingleOversizedRequest", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		wf := &Workflow{}

		err := wf.AddCompletionCallbacks(ctx, eventTime, "req-1", []*commonpb.Callback{
			nexusHandlerCallback(1600),
		}, limits)
		var failedPreconditionErr *serviceerror.FailedPrecondition
		require.ErrorAs(t, err, &failedPreconditionErr)
		require.ErrorContains(t, err, "cannot attach more than 1500 bytes of callback source_context")
		require.Empty(t, wf.Callbacks)
	})

	t.Run("RejectsExceedingTheLimitWithAlreadyAttachedCallbacks", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		wf := &Workflow{}

		require.NoError(t, wf.AddCompletionCallbacks(ctx, eventTime, "req-1", []*commonpb.Callback{
			nexusHandlerCallback(900),
		}, limits))

		err := wf.AddCompletionCallbacks(ctx, eventTime, "req-2", []*commonpb.Callback{
			nexusHandlerCallback(900),
		}, limits)
		// req-2 is within the limit on its own, so only the accumulated total can have rejected it.
		require.ErrorContains(t, err, "cannot attach more than 1500 bytes of callback source_context")
		require.Len(t, wf.Callbacks, 1)
	})

	// An update's callbacks count against the same budget as the workflow's own.
	t.Run("CountsCallbacksAttachedToUpdates", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		wf := &Workflow{}

		require.NoError(t, wf.AddUpdateCompletionCallbacks(ctx, eventTime, "update-1", "req-1",
			[]*commonpb.Callback{nexusHandlerCallback(900)}, limits, 10))

		err := wf.AddCompletionCallbacks(ctx, eventTime, "req-2", []*commonpb.Callback{
			nexusHandlerCallback(900),
		}, limits)
		require.ErrorContains(t, err, "cannot attach more than 1500 bytes of callback source_context")
		require.Empty(t, wf.Callbacks)
	})

	t.Run("AllowsCallbacksWithinTheLimit", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		wf := &Workflow{}

		require.NoError(t, wf.AddCompletionCallbacks(ctx, eventTime, "req-1", []*commonpb.Callback{
			nexusHandlerCallback(700),
			nexusHandlerCallback(700),
		}, limits))
		require.Len(t, wf.Callbacks, 2)
	})
}
