// The MIT License (MIT)
//
// Copyright (c) 2024 Temporal Technologies Inc.  ALL RIGHTS RESERVED.
//
// See NOTICE.md for full restrictions.

package workflow

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/components/callbacks"
	"go.temporal.io/server/components/nexusoperations"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

// signalSuggestCANFixture builds a workflowTaskStateMachine whose only variable inputs are
// SignalCount and the two signal-related dynamic-config knobs, with the size/count suggest
// thresholds pushed out of the way so they cannot pollute the asserted reasons slice.
func signalSuggestCANFixture(t *testing.T, signalCount int64, maxSignals int, threshold float64) *workflowTaskStateMachine {
	t.Helper()
	controller := gomock.NewController(t)
	config := tests.NewDynamicConfig()
	// Push the size/count reasons far out so only the signal reason can fire.
	config.HistorySizeSuggestContinueAsNew = func(string) int { return 1 << 60 }
	config.HistoryCountSuggestContinueAsNew = func(string) int { return 1 << 60 }
	config.MaximumSignalsPerExecution = func(string) int { return maxSignals }
	config.MaximumSignalsPerExecutionSuggestContinueAsNewThreshold = func(string) float64 { return threshold }

	mockShard := shard.NewTestContext(controller, &persistencespb.ShardInfo{ShardId: 0, RangeId: 1}, config)
	t.Cleanup(mockShard.StopForTest)

	reg := hsm.NewRegistry()
	require.NoError(t, RegisterStateMachine(reg))
	require.NoError(t, callbacks.RegisterStateMachine(reg))
	require.NoError(t, nexusoperations.RegisterStateMachines(reg))
	mockShard.SetStateMachineRegistry(reg)

	namespaceEntry := tests.GlobalNamespaceEntry
	mockShard.Resource.NamespaceCache.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(namespaceEntry, nil).AnyTimes()
	mockShard.Resource.ClusterMetadata.EXPECT().ClusterNameForFailoverVersion(namespaceEntry.IsGlobalNamespace(), namespaceEntry.FailoverVersion(tests.WorkflowID)).Return(cluster.TestCurrentClusterName).AnyTimes()
	mockShard.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	mockShard.Resource.ClusterMetadata.EXPECT().GetClusterID().Return(int64(1)).AnyTimes()

	ms := NewMutableState(mockShard, events.NewMockCache(controller), mockShard.GetLogger(), namespaceEntry, tests.WorkflowID, tests.RunID, time.Now().UTC())
	// getHistorySizeInfo returns early when ExecutionStats is nil, so set a non-nil one.
	ms.GetExecutionInfo().ExecutionStats = &persistencespb.ExecutionStats{}
	ms.GetExecutionInfo().SignalCount = signalCount
	return newWorkflowTaskStateMachine(ms, mockShard.GetMetricsHandler())
}

func TestGetHistorySizeInfo_SignalSuggestCAN(t *testing.T) {
	const signalReason = enumspb.SUGGEST_CONTINUE_AS_NEW_REASON_TOO_MANY_SIGNALS
	// With max=10, threshold=0.5 the boundary is ceil(10*0.5)=5: the reason fires at SignalCount>=5.
	const maxSignals = 10
	const threshold = 0.5

	t.Run("disabled by zero threshold", func(t *testing.T) {
		// INV-3: threshold 0 -> never suggest, regardless of count.
		m := signalSuggestCANFixture(t, 100, maxSignals, 0)
		_, reasons := m.getHistorySizeInfo()
		require.NotContains(t, reasons, signalReason, "threshold 0 must disable the signal reason")
		require.Empty(t, reasons, "no reason should fire when only the (disabled) signal threshold is configured")
	})

	t.Run("disabled by zero max signals", func(t *testing.T) {
		// INV-3: maxSignals 0 (hard limit disabled) -> signal suggestion also disabled.
		m := signalSuggestCANFixture(t, 100, 0, threshold)
		_, reasons := m.getHistorySizeInfo()
		require.NotContains(t, reasons, signalReason, "maxSignals 0 must disable the signal reason")
		require.Empty(t, reasons)
	})

	t.Run("below boundary does not suggest", func(t *testing.T) {
		// boundary=5; count=4 is one short -> no reason.
		m := signalSuggestCANFixture(t, 4, maxSignals, threshold)
		_, reasons := m.getHistorySizeInfo()
		require.NotContains(t, reasons, signalReason)
		require.Empty(t, reasons)
	})

	t.Run("at boundary suggests (inclusive)", func(t *testing.T) {
		// INV-3: inclusive >= at exactly ceil(max*threshold).
		m := signalSuggestCANFixture(t, 5, maxSignals, threshold)
		_, reasons := m.getHistorySizeInfo()
		require.Contains(t, reasons, signalReason)
	})

	t.Run("above boundary suggests", func(t *testing.T) {
		m := signalSuggestCANFixture(t, 9, maxSignals, threshold)
		_, reasons := m.getHistorySizeInfo()
		require.Contains(t, reasons, signalReason)
	})

	t.Run("threshold 1.0 suggests at hard limit", func(t *testing.T) {
		// INV-3/INV-2: threshold 1.0 -> suggest as soon as count >= max (the hard-limit instant).
		m := signalSuggestCANFixture(t, int64(maxSignals), maxSignals, 1.0)
		_, reasons := m.getHistorySizeInfo()
		require.Contains(t, reasons, signalReason)
	})

	t.Run("threshold 1.0 below hard limit does not suggest", func(t *testing.T) {
		m := signalSuggestCANFixture(t, int64(maxSignals)-1, maxSignals, 1.0)
		_, reasons := m.getHistorySizeInfo()
		require.NotContains(t, reasons, signalReason)
		require.Empty(t, reasons)
	})

	t.Run("coexists with history-size reason", func(t *testing.T) {
		// INV-7: when the history-size threshold AND the signal threshold are both crossed, both
		// reasons must appear in the slice (additive, not mutually exclusive). The shared fixture
		// pushes the size/count limits out of the way, so build a dedicated state machine here that
		// sets a low size threshold alongside a crossed signal threshold.
		controller := gomock.NewController(t)
		config := tests.NewDynamicConfig()
		config.HistorySizeSuggestContinueAsNew = func(string) int { return 1 } // tiny: any history fires it
		config.HistoryCountSuggestContinueAsNew = func(string) int { return 1 << 60 }
		config.MaximumSignalsPerExecution = func(string) int { return maxSignals }
		config.MaximumSignalsPerExecutionSuggestContinueAsNewThreshold = func(string) float64 { return threshold }

		mockShard := shard.NewTestContext(controller, &persistencespb.ShardInfo{ShardId: 0, RangeId: 1}, config)
		t.Cleanup(mockShard.StopForTest)

		reg := hsm.NewRegistry()
		require.NoError(t, RegisterStateMachine(reg))
		require.NoError(t, callbacks.RegisterStateMachine(reg))
		require.NoError(t, nexusoperations.RegisterStateMachines(reg))
		mockShard.SetStateMachineRegistry(reg)

		namespaceEntry := tests.GlobalNamespaceEntry
		mockShard.Resource.NamespaceCache.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(namespaceEntry, nil).AnyTimes()
		mockShard.Resource.ClusterMetadata.EXPECT().ClusterNameForFailoverVersion(namespaceEntry.IsGlobalNamespace(), namespaceEntry.FailoverVersion(tests.WorkflowID)).Return(cluster.TestCurrentClusterName).AnyTimes()
		mockShard.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
		mockShard.Resource.ClusterMetadata.EXPECT().GetClusterID().Return(int64(1)).AnyTimes()

		ms := NewMutableState(mockShard, events.NewMockCache(controller), mockShard.GetLogger(), namespaceEntry, tests.WorkflowID, tests.RunID, time.Now().UTC())
		ms.GetExecutionInfo().ExecutionStats = &persistencespb.ExecutionStats{HistorySize: 1 << 20}
		// SignalCount above the boundary (ceil(10*0.5)=5).
		ms.GetExecutionInfo().SignalCount = int64(maxSignals)
		m := newWorkflowTaskStateMachine(ms, mockShard.GetMetricsHandler())

		_, reasons := m.getHistorySizeInfo()
		require.Contains(t, reasons, signalReason, "signal reason must fire when its threshold is crossed")
		require.Contains(t, reasons, enumspb.SUGGEST_CONTINUE_AS_NEW_REASON_HISTORY_SIZE_TOO_LARGE,
			"history-size reason must coexist when its threshold is also crossed")
		require.Len(t, reasons, 2, "exactly the two crossed reasons, additive")
	})

	t.Run("new run after continue-as-new does not inherit prior signal count", func(t *testing.T) {
		// INV-4: SignalCount is per-execution and resets on continue-as-new (a new run gets a new
		// mutable state). A run whose count crossed the threshold suggests; the successor run, with
		// a fresh low SignalCount, must NOT suggest even though the prior run did. Each call to
		// signalSuggestCANFixture builds an independent mutable state, modelling the two runs.
		priorRun := signalSuggestCANFixture(t, int64(maxSignals), maxSignals, threshold)
		_, priorReasons := priorRun.getHistorySizeInfo()
		require.Contains(t, priorReasons, signalReason, "prior run over the threshold must suggest")

		newRun := signalSuggestCANFixture(t, 0, maxSignals, threshold)
		_, newReasons := newRun.getHistorySizeInfo()
		require.NotContains(t, newReasons, signalReason,
			"new run's fresh SignalCount must not trigger the reason even though the prior run suggested")
		require.Empty(t, newReasons)
	})
}
