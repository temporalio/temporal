package workflow

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/chasm"
	chasmworkflowpb "go.temporal.io/server/chasm/lib/workflow/gen/workflowpb/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// unlimitedCallbacks disables the per-workflow and per-update limits that are still checked while
// attaching.
const unlimitedCallbacks = 1000

func newTestWorkflow() *Workflow {
	return &Workflow{
		WorkflowState: &chasmworkflowpb.WorkflowState{},
		MSPointer:     chasm.NewMSPointer(&chasm.MockNodeBackend{}),
	}
}

func nexusCallback(url string) *commonpb.Callback {
	return &commonpb.Callback{
		Variant: &commonpb.Callback_Nexus_{
			Nexus: &commonpb.Callback_Nexus{Url: url},
		},
	}
}

func nexusCallbacks(urls ...string) []*commonpb.Callback {
	cbs := make([]*commonpb.Callback, len(urls))
	for i, url := range urls {
		cbs[i] = nexusCallback(url)
	}
	return cbs
}

func TestAddCompletionCallbacks(t *testing.T) {
	t.Parallel()

	t.Run("AttachesAndTracksTotals", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		wf := newTestWorkflow()
		cbs := nexusCallbacks("http://cb-1", "http://cb-2")

		require.NoError(t, wf.AddCompletionCallbacks(ctx, timestamppb.Now(), "req-1", cbs, unlimitedCallbacks))
		require.Len(t, wf.Callbacks, 2)
		require.Equal(t, "http://cb-1", wf.Callbacks["req-1-0"].Get(ctx).GetCallback().GetNexus().GetUrl())
		require.Equal(t, "http://cb-2", wf.Callbacks["req-1-1"].Get(ctx).GetCallback().GetNexus().GetUrl())

		require.Equal(t, int64(2), wf.GetTotalCallbacksCount())
		require.Equal(t, int64(cbs[0].Size()+cbs[1].Size()), wf.GetTotalCallbacksSize())
	})

	t.Run("EmptyListIsNoOp", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		wf := newTestWorkflow()

		require.NoError(t, wf.AddCompletionCallbacks(ctx, timestamppb.Now(), "req-1", nil, unlimitedCallbacks))
		require.Empty(t, wf.Callbacks)
		require.Zero(t, wf.GetTotalCallbacksCount())
		require.Zero(t, wf.GetTotalCallbacksSize())
	})

	t.Run("ReattachingTheSameRequestIsANoOp", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		wf := newTestWorkflow()
		cbs := nexusCallbacks("http://cb-1", "http://cb-2")

		require.NoError(t, wf.AddCompletionCallbacks(ctx, timestamppb.Now(), "req-1", cbs, unlimitedCallbacks))
		sizeAfterFirstAttach := wf.GetTotalCallbacksSize()

		// The admitted and accepted events of an update both carry the same callbacks; the
		// second attach must not be counted twice.
		require.NoError(t, wf.AddCompletionCallbacks(ctx, timestamppb.Now(), "req-1", cbs, unlimitedCallbacks))
		require.Len(t, wf.Callbacks, 2)
		require.Equal(t, int64(2), wf.GetTotalCallbacksCount())
		require.Equal(t, sizeAfterFirstAttach, wf.GetTotalCallbacksSize())
	})

	t.Run("DistinctRequestsAccumulate", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		wf := newTestWorkflow()

		require.NoError(t, wf.AddCompletionCallbacks(ctx, timestamppb.Now(), "req-1", nexusCallbacks("http://cb-1"), unlimitedCallbacks))
		require.NoError(t, wf.AddCompletionCallbacks(ctx, timestamppb.Now(), "req-2", nexusCallbacks("http://cb-2"), unlimitedCallbacks))
		require.Len(t, wf.Callbacks, 2)
		require.Equal(t, int64(2), wf.GetTotalCallbacksCount())
	})

	t.Run("CountsWorkflowAndUpdateCallbacksTogether", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		wf := newTestWorkflow()

		require.NoError(t, wf.AddCompletionCallbacks(ctx, timestamppb.Now(), "req-1", nexusCallbacks("http://cb-1"), unlimitedCallbacks))
		require.NoError(t, wf.AddUpdateCompletionCallbacks(ctx, timestamppb.Now(), "u1", "req-2", nexusCallbacks("http://cb-2"), unlimitedCallbacks, unlimitedCallbacks))
		require.NoError(t, wf.AddUpdateCompletionCallbacks(ctx, timestamppb.Now(), "u2", "req-3", nexusCallbacks("http://cb-3", "http://cb-4"), unlimitedCallbacks, unlimitedCallbacks))

		require.Len(t, wf.Callbacks, 1)
		require.Len(t, wf.Updates["u1"].Get(ctx).Callbacks, 1)
		require.Len(t, wf.Updates["u2"].Get(ctx).Callbacks, 2)
		require.Equal(t, int64(4), wf.GetTotalCallbacksCount())
	})
}
