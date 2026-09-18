package workflow

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/callback"
	chasmworkflowpb "go.temporal.io/server/chasm/lib/workflow/gen/workflowpb/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func newTotalsTestWorkflow() *Workflow {
	return &Workflow{MSPointer: chasm.NewMSPointer(&chasm.MockNodeBackend{})}
}

// wantSize is the summed size of the callback specifications, which is what the counter tracks
// -- deliberately not the size of the surrounding CallbackState.
func wantSize(t *testing.T, cbs ...*commonpb.Callback) int64 {
	t.Helper()
	var total int64
	for _, cb := range cbs {
		chasmCB, err := callback.FromAPICallback(cb)
		require.NoError(t, err)
		total += int64(chasmCB.Size())
	}
	return total
}

func TestCallbackTotals(t *testing.T) {
	t.Parallel()

	const maxPerWorkflow, maxPerUpdate = 100, 100
	cb1, cb2 := nexusCallback("http://cb-1"), nexusCallback("http://cb-2")

	t.Run("TracksCountAndSizeAcrossWorkflowAndUpdates", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		wf := newTotalsTestWorkflow()

		require.NoError(t, wf.AddCompletionCallbacks(
			ctx, timestamppb.Now(), "req-1", []*commonpb.Callback{cb1}, maxPerWorkflow))
		require.NoError(t, wf.AddUpdateCompletionCallbacks(
			ctx, timestamppb.Now(), "u1", "req-2", []*commonpb.Callback{cb2}, maxPerWorkflow, maxPerUpdate))

		require.Equal(t, int32(2), wf.GetTotalCallbacksCount())
		require.Equal(t, wantSize(t, cb1, cb2), wf.GetTotalCallbacksSize())

		// The denormalized totals must agree with a full walk of the tree.
		count, size := wf.recomputeCallbackTotals(ctx)
		require.Equal(t, 2, count)
		require.Equal(t, wf.GetTotalCallbacksSize(), size)
	})

	t.Run("ReattachingTheSameRequestIDDoesNotDoubleCount", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		wf := newTotalsTestWorkflow()
		cbs := []*commonpb.Callback{cb1, cb2}

		require.NoError(t, wf.AddCompletionCallbacks(ctx, timestamppb.Now(), "req-1", cbs, maxPerWorkflow))
		countAfterFirst, sizeAfterFirst := wf.GetTotalCallbacksCount(), wf.GetTotalCallbacksSize()

		// Same request ID re-derives the same keys, so nothing is inserted.
		require.NoError(t, wf.AddCompletionCallbacks(ctx, timestamppb.Now(), "req-1", cbs, maxPerWorkflow))
		require.Equal(t, countAfterFirst, wf.GetTotalCallbacksCount())
		require.Equal(t, sizeAfterFirst, wf.GetTotalCallbacksSize())
		require.Len(t, wf.Callbacks, 2)
	})

	t.Run("RecomputesForWorkflowsPredatingTheCounters", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		wf := newTotalsTestWorkflow()
		require.NoError(t, wf.AddCompletionCallbacks(
			ctx, timestamppb.Now(), "req-1", []*commonpb.Callback{cb1, cb2}, maxPerWorkflow))

		// Simulate state written before WorkflowState existed: the callbacks are present but
		// the counters decode as zero.
		wf.WorkflowState = &chasmworkflowpb.WorkflowState{}

		count, size := wf.callbackTotals(ctx)
		require.Equal(t, 2, count)
		require.Equal(t, wantSize(t, cb1, cb2), size)
	})

	t.Run("RecomputedTotalIsPersistedSoItIsNotRecomputedAgain", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		wf := newTotalsTestWorkflow()
		require.NoError(t, wf.AddCompletionCallbacks(
			ctx, timestamppb.Now(), "req-1", []*commonpb.Callback{cb1}, maxPerWorkflow))
		wf.WorkflowState = &chasmworkflowpb.WorkflowState{}

		// A no-op attach still writes the recomputed total back.
		require.NoError(t, wf.AddCompletionCallbacks(
			ctx, timestamppb.Now(), "req-1", []*commonpb.Callback{cb1}, maxPerWorkflow))
		require.Equal(t, int32(1), wf.GetTotalCallbacksCount())
		require.Equal(t, wantSize(t, cb1), wf.GetTotalCallbacksSize())
	})

	// Limits are charged against what the call would actually persist. A retry re-derives keys
	// it has already written, so it inserts nothing and must not be rejected for exceeding a cap
	// it does not move.
	t.Run("ARetryThatInsertsNothingIsAcceptedAtTheCap", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		wf := newTotalsTestWorkflow()
		cbs := []*commonpb.Callback{cb1, cb2}

		require.NoError(t, wf.AddCompletionCallbacks(ctx, timestamppb.Now(), "req-1", cbs, 2))
		require.NoError(t, wf.AddCompletionCallbacks(ctx, timestamppb.Now(), "req-1", cbs, 2))
		require.Len(t, wf.Callbacks, 2)
		require.Equal(t, int32(2), wf.GetTotalCallbacksCount())
	})

	t.Run("ARetryOnAnUpdateIsAcceptedAtThePerUpdateCap", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		wf := newTotalsTestWorkflow()
		cbs := []*commonpb.Callback{cb1, cb2}

		require.NoError(t, wf.AddUpdateCompletionCallbacks(
			ctx, timestamppb.Now(), "u1", "req-1", cbs, maxPerWorkflow, 2))
		require.NoError(t, wf.AddUpdateCompletionCallbacks(
			ctx, timestamppb.Now(), "u1", "req-1", cbs, maxPerWorkflow, 2))
		require.Len(t, wf.Updates["u1"].Get(ctx).Callbacks, 2)
		require.Equal(t, int32(2), wf.GetTotalCallbacksCount())
	})

	// A rejected request must not leave a half-built update component behind.
	t.Run("RejectedUpdateDoesNotCreateTheUpdateComponent", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		wf := newTotalsTestWorkflow()

		err := wf.AddUpdateCompletionCallbacks(
			ctx, timestamppb.Now(), "u1", "req-1", []*commonpb.Callback{cb1, cb2}, maxPerWorkflow, 1)
		require.Error(t, err)
		require.NotContains(t, wf.Updates, "u1")
	})

	t.Run("EmptyWorkflowReportsZeroWithoutRecomputing", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		wf := newTotalsTestWorkflow()

		count, size := wf.callbackTotals(ctx)
		require.Zero(t, count)
		require.Zero(t, size)
		// Nothing was written, so a callback-free workflow still persists a nil blob.
		require.Nil(t, wf.WorkflowState)
	})
}
