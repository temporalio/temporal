package workflow

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/callback"
	chasmworkflowpb "go.temporal.io/server/chasm/lib/workflow/gen/workflowpb/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func nexusCallback(url string) *commonpb.Callback {
	return &commonpb.Callback{
		Variant: &commonpb.Callback_Nexus_{
			Nexus: &commonpb.Callback_Nexus{Url: url},
		},
	}
}

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

	cb1, cb2 := nexusCallback("http://cb-1"), nexusCallback("http://cb-2")

	t.Run("TracksCountAndSizeAcrossWorkflowAndUpdates", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		wf := newTotalsTestWorkflow()

		require.NoError(t, wf.AddCompletionCallbacks(
			ctx, timestamppb.Now(), "req-1", []*commonpb.Callback{cb1}))
		require.NoError(t, wf.AddUpdateCompletionCallbacks(
			ctx, timestamppb.Now(), "u1", "req-2", []*commonpb.Callback{cb2}))

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

		require.NoError(t, wf.AddCompletionCallbacks(ctx, timestamppb.Now(), "req-1", cbs))
		countAfterFirst, sizeAfterFirst := wf.GetTotalCallbacksCount(), wf.GetTotalCallbacksSize()

		// Same request ID re-derives the same keys, so nothing is inserted.
		require.NoError(t, wf.AddCompletionCallbacks(ctx, timestamppb.Now(), "req-1", cbs))
		require.Equal(t, countAfterFirst, wf.GetTotalCallbacksCount())
		require.Equal(t, sizeAfterFirst, wf.GetTotalCallbacksSize())
		require.Len(t, wf.Callbacks, 2)
	})

	t.Run("RecomputesForWorkflowsPredatingTheCounters", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		wf := newTotalsTestWorkflow()
		require.NoError(t, wf.AddCompletionCallbacks(
			ctx, timestamppb.Now(), "req-1", []*commonpb.Callback{cb1, cb2}))

		// Simulate state written before WorkflowState existed: the callbacks are present but
		// the counters decode as zero.
		wf.WorkflowState = &chasmworkflowpb.WorkflowState{}

		count, size := wf.CallbackTotals(ctx)
		require.Equal(t, 2, count)
		require.Equal(t, wantSize(t, cb1, cb2), size)
	})

	t.Run("RecomputedTotalIsPersistedSoItIsNotRecomputedAgain", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		wf := newTotalsTestWorkflow()
		require.NoError(t, wf.AddCompletionCallbacks(
			ctx, timestamppb.Now(), "req-1", []*commonpb.Callback{cb1}))
		wf.WorkflowState = &chasmworkflowpb.WorkflowState{}

		// A no-op attach still writes the recomputed total back.
		require.NoError(t, wf.AddCompletionCallbacks(
			ctx, timestamppb.Now(), "req-1", []*commonpb.Callback{cb1}))
		require.Equal(t, int32(1), wf.GetTotalCallbacksCount())
		require.Equal(t, wantSize(t, cb1), wf.GetTotalCallbacksSize())
	})

	// A retry re-derives keys it has already written, so it inserts nothing. The write path
	// relies on this to avoid charging a retry against a cap it does not move; see
	// MutableStateImpl.validateChasmCallbackAttachments.
	t.Run("ARetryInsertsNothingAndLeavesTheCountUnchanged", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		wf := newTotalsTestWorkflow()
		cbs := []*commonpb.Callback{cb1, cb2}

		require.NoError(t, wf.AddCompletionCallbacks(ctx, timestamppb.Now(), "req-1", cbs))
		require.NoError(t, wf.AddCompletionCallbacks(ctx, timestamppb.Now(), "req-1", cbs))
		require.Len(t, wf.Callbacks, 2)
		require.Equal(t, int32(2), wf.GetTotalCallbacksCount())
	})

	t.Run("ARetryOnAnUpdateInsertsNothing", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		wf := newTotalsTestWorkflow()
		cbs := []*commonpb.Callback{cb1, cb2}

		require.NoError(t, wf.AddUpdateCompletionCallbacks(
			ctx, timestamppb.Now(), "u1", "req-1", cbs))
		require.NoError(t, wf.AddUpdateCompletionCallbacks(
			ctx, timestamppb.Now(), "u1", "req-1", cbs))
		require.Len(t, wf.Updates["u1"].Get(ctx).Callbacks, 2)
		require.Equal(t, int32(2), wf.GetTotalCallbacksCount())
	})

	// The metric exists so the fleet-wide distribution can be read before the size limit is
	// given a non-zero default, so it must report the execution's running total, not the
	// increment from a single attach.
	t.Run("RecordsTheCumulativeTotalAsAMetric", func(t *testing.T) {
		captureHandler := metricstest.NewCaptureHandler()
		capture := captureHandler.StartCapture()
		ctx := &chasm.MockMutableContext{
			MockContext: chasm.MockContext{
				HandleMetricsHandler: func() metrics.Handler { return captureHandler },
			},
		}
		wf := newTotalsTestWorkflow()

		require.NoError(t, wf.AddCompletionCallbacks(ctx, timestamppb.Now(), "req-1", []*commonpb.Callback{cb1}))
		require.NoError(t, wf.AddCompletionCallbacks(ctx, timestamppb.Now(), "req-2", []*commonpb.Callback{cb2}))

		recorded := capture.Snapshot()[callback.TotalSizePerExecution.Name()]
		require.Len(t, recorded, 2)
		require.Equal(t, wantSize(t, cb1), recorded[0].Value)
		require.Equal(t, wantSize(t, cb1, cb2), recorded[1].Value,
			"the second sample must be the running total, not just the second callback")
	})

	t.Run("EmptyWorkflowReportsZeroWithoutRecomputing", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		wf := newTotalsTestWorkflow()

		count, size := wf.CallbackTotals(ctx)
		require.Zero(t, count)
		require.Zero(t, size)
		// Nothing was written, so a callback-free workflow still persists a nil blob.
		require.Nil(t, wf.WorkflowState)
	})
}
