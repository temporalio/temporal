package concurrency

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	fcpb "go.temporal.io/server/chasm/lib/flowcontrol/gen/flowcontrolpb/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.uber.org/mock/gomock"
)

func TestBatchSlotMetrics(t *testing.T) {
	t.Parallel()
	handler := metricstest.NewCaptureHandler()
	capture := handler.StartCapture()
	defer handler.StopCapture(capture)
	now := time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)
	key := batchKey{namespaceID: "namespace-id", key: "wholequeue/test/queue/2/0"}
	cctx := newTestMutableContext(now)
	cctx.HandleExecutionKey = func() chasm.ExecutionKey {
		return chasm.ExecutionKey{NamespaceID: key.namespaceID, BusinessID: key.key}
	}
	cctx.HandleMetricsHandler = func() metrics.Handler { return handler }
	component := newTestComponent(2)
	engine := chasm.NewMockEngine(gomock.NewController(t))
	engine.EXPECT().UpdateWithStartExecution(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ chasm.ComponentRef, _ func(chasm.MutableContext) (chasm.RootComponent, error), update func(chasm.MutableContext, chasm.Component) error, _ ...chasm.TransitionOption) (chasm.EngineUpdateWithStartExecutionResult, error) {
			return chasm.EngineUpdateWithStartExecutionResult{}, update(cctx, component)
		})
	ctx := chasm.NewEngineContext(t.Context(), engine)
	h := newTestHandlerContext(t).handler
	results := h.applyBatch(key, []batchReq{{ctx: ctx, req: &fcpb.ConcurrencyBatchRequest{ReserveSlots: []string{"a", "a", "b", "c"}}}})
	require.NoError(t, results[0].err)
	snapshot := capture.Snapshot()
	require.NotEmpty(t, snapshot["flowcontrol_concurrency_slot_operations"])
	counts := map[string]int64{}
	for _, record := range snapshot["flowcontrol_concurrency_slot_operations"] {
		require.Equal(t, "namespace-id", record.Tags["namespace_id"])
		require.Equal(t, "test/queue", record.Tags["taskqueue"])
		require.Equal(t, "Activity", record.Tags["task_type"])
		require.NotContains(t, record.Tags, "business_id")
		counts[record.Tags["outcome"]] += record.Value.(int64)
	}
	require.Equal(t, map[string]int64{"APPLIED": 2, "DUPLICATE": 1, "REJECTED": 1}, counts)
	slots := map[string]float64{}
	for _, record := range snapshot["flowcontrol_concurrency_slots"] {
		slots[record.Tags["state"]] = record.Value.(float64)
	}
	require.Equal(t, map[string]float64{"RESERVED": 2, "COMMITTED": 0, "AVAILABLE": 0, "LIMIT": 2}, slots)
	require.Len(t, snapshot["flowcontrol_concurrency_slots_observed_timestamp"], 1)
}

func TestFailedBatchDoesNotReportSlotChanges(t *testing.T) {
	t.Parallel()
	handler := metricstest.NewCaptureHandler()
	capture := handler.StartCapture()
	defer handler.StopCapture(capture)
	cctx := newTestMutableContext(time.Now())
	cctx.HandleMetricsHandler = func() metrics.Handler { return handler }
	engine := chasm.NewMockEngine(gomock.NewController(t))
	persistenceErr := errors.New("persistence failed")
	engine.EXPECT().UpdateWithStartExecution(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ chasm.ComponentRef, _ func(chasm.MutableContext) (chasm.RootComponent, error), update func(chasm.MutableContext, chasm.Component) error, _ ...chasm.TransitionOption) (chasm.EngineUpdateWithStartExecutionResult, error) {
			require.NoError(t, update(cctx, newTestComponent(1)))
			return chasm.EngineUpdateWithStartExecutionResult{}, persistenceErr
		})
	ctx := chasm.NewEngineContext(t.Context(), engine)
	h := newTestHandlerContext(t).handler
	results := h.applyBatch(batchKey{namespaceID: "namespace-id", key: "wholequeue/queue/2/0"}, []batchReq{{ctx: ctx, req: &fcpb.ConcurrencyBatchRequest{ReserveSlots: []string{"a"}}}})
	require.ErrorIs(t, results[0].err, persistenceErr)
	require.Empty(t, capture.Snapshot())
}

func TestSlotMetricLifecycle(t *testing.T) {
	t.Parallel()
	handler := metricstest.NewCaptureHandler()
	capture := handler.StartCapture()
	defer handler.StopCapture(capture)
	now := time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)
	cctx := newTestMutableContext(now)
	cctx.HandleNow = func(chasm.Component) time.Time { return now }
	cctx.HandleMetricsHandler = func() metrics.Handler { return handler }
	cctx.HandleExecutionKey = func() chasm.ExecutionKey {
		return chasm.ExecutionKey{NamespaceID: "namespace-id", BusinessID: "wholequeue/queue/2/0"}
	}
	c := newTestComponent(2)
	apply := func(req *fcpb.ConcurrencyBatchRequest) {
		t.Helper()
		m := &batchMetrics{}
		_, err := updateFn(c, cctx, chasmReq{getWakeLevel: func(int32) (int64, bool) { return 0, true }, items: []batchReq{{req: req}}, metrics: m})
		require.NoError(t, err)
		m.record()
	}
	apply(&fcpb.ConcurrencyBatchRequest{ReserveSlots: []string{"a", "b"}})
	now = now.Add(100 * time.Millisecond)
	apply(&fcpb.ConcurrencyBatchRequest{CommitSlots: []string{"a", "a", "missing"}})
	apply(&fcpb.ConcurrencyBatchRequest{CancelReservationSlots: []string{"a", "b", "b"}})
	apply(&fcpb.ConcurrencyBatchRequest{ReleaseSlots: []string{"a", "a"}})
	snapshot := capture.Snapshot()
	counts := map[string]int64{}
	for _, r := range snapshot["flowcontrol_concurrency_slot_operations"] {
		counts[r.Tags["operation"]+"/"+r.Tags["outcome"]] += r.Value.(int64)
	}
	require.Equal(t, map[string]int64{"RESERVE/APPLIED": 2, "COMMIT/APPLIED": 1, "COMMIT/DUPLICATE": 1, "COMMIT/REJECTED": 1, "CANCEL/APPLIED": 1, "CANCEL/NOOP": 2, "RELEASE/APPLIED": 1, "RELEASE/NOOP": 1}, counts)
	records := snapshot["flowcontrol_concurrency_reservation_to_commit_latency"]
	require.Len(t, records, 1)
	require.Equal(t, 100*time.Millisecond, records[0].Value)
	last := map[string]float64{}
	for _, r := range snapshot["flowcontrol_concurrency_slots"] {
		last[r.Tags["state"]] = r.Value.(float64)
	}
	require.Equal(t, map[string]float64{"RESERVED": 0, "COMMITTED": 0, "AVAILABLE": 2, "LIMIT": 2}, last)
}

func TestExpirationMetrics(t *testing.T) {
	t.Parallel()
	for _, pureTask := range []bool{false, true} {
		t.Run(map[bool]string{false: "batch", true: "staged wake"}[pureTask], func(t *testing.T) {
			t.Parallel()
			handler := metricstest.NewCaptureHandler()
			capture := handler.StartCapture()
			defer handler.StopCapture(capture)
			now := time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)
			c := newTestComponent(1)
			require.True(t, c.reserve("expired", now.Add(-reserveTimeout-time.Second)))
			cctx := newTestMutableContext(now)
			cctx.HandleMetricsHandler = func() metrics.Handler { return handler }
			if pureTask {
				h := NewStagedWakeHandler(newTestHandlerContext(t).handler)
				require.NoError(t, h.Execute(cctx, c, chasm.TaskAttributes{}, &stagedWake{}))
			} else {
				_, err := updateFn(c, cctx, chasmReq{getWakeLevel: func(int32) (int64, bool) { return 0, true }})
				require.NoError(t, err)
			}
			records := capture.Snapshot()["flowcontrol_concurrency_expiration_attempts"]
			require.Len(t, records, 1)
			require.Equal(t, int64(1), records[0].Value)
			require.Empty(t, c.Slots)
		})
	}
}

func TestLegacyReservationHasNoLatency(t *testing.T) {
	t.Parallel()
	c := newTestComponent(1)
	c.Slots = []*fcpb.ConcurrencyState_Slot{{SlotId: "legacy"}}
	cctx := newTestMutableContext(time.Now())
	handler := metricstest.NewCaptureHandler()
	capture := handler.StartCapture()
	defer handler.StopCapture(capture)
	cctx.HandleMetricsHandler = func() metrics.Handler { return handler }
	m := &batchMetrics{}
	_, err := updateFn(c, cctx, chasmReq{getWakeLevel: func(int32) (int64, bool) { return 0, true }, metrics: m, items: []batchReq{{req: &fcpb.ConcurrencyBatchRequest{CommitSlots: []string{"legacy"}}}}})
	require.NoError(t, err)
	m.record()
	require.True(t, c.Slots[0].Committed)
	require.Empty(t, capture.Snapshot()["flowcontrol_concurrency_reservation_to_commit_latency"])
}

func TestPerTaskLimiterHasNoSlotGauges(t *testing.T) {
	t.Parallel()
	handler := metricstest.NewCaptureHandler()
	capture := handler.StartCapture()
	defer handler.StopCapture(capture)
	cctx := newTestMutableContext(time.Now())
	cctx.HandleMetricsHandler = func() metrics.Handler { return handler }
	cctx.HandleExecutionKey = func() chasm.ExecutionKey {
		return chasm.ExecutionKey{NamespaceID: "namespace-id", BusinessID: "private-business-id"}
	}
	m := &batchMetrics{}
	_, err := updateFn(newTestComponent(1), cctx, chasmReq{getWakeLevel: func(int32) (int64, bool) { return 0, true }, metrics: m, items: []batchReq{{req: &fcpb.ConcurrencyBatchRequest{ReserveSlots: []string{"slot"}}}}})
	require.NoError(t, err)
	m.record()
	snapshot := capture.Snapshot()
	require.NotEmpty(t, snapshot["flowcontrol_concurrency_slot_operations"])
	require.Empty(t, snapshot["flowcontrol_concurrency_slots"])
	require.Empty(t, snapshot["flowcontrol_concurrency_slots_observed_timestamp"])
	for _, records := range snapshot {
		for _, r := range records {
			for _, v := range r.Tags {
				require.NotEqual(t, "private-business-id", v)
			}
		}
	}
}

func TestRetriedTransitionReportsFinalMetricsOnly(t *testing.T) {
	t.Parallel()
	handler := metricstest.NewCaptureHandler()
	capture := handler.StartCapture()
	defer handler.StopCapture(capture)
	cctx := newTestMutableContext(time.Now())
	cctx.HandleMetricsHandler = func() metrics.Handler { return handler }
	cctx.HandleExecutionKey = func() chasm.ExecutionKey {
		return chasm.ExecutionKey{NamespaceID: "namespace-id", BusinessID: "wholequeue/queue/2/0"}
	}
	engine := chasm.NewMockEngine(gomock.NewController(t))
	engine.EXPECT().UpdateWithStartExecution(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ chasm.ComponentRef, _ func(chasm.MutableContext) (chasm.RootComponent, error), update func(chasm.MutableContext, chasm.Component) error, _ ...chasm.TransitionOption) (chasm.EngineUpdateWithStartExecutionResult, error) {
			require.NoError(t, update(cctx, newTestComponent(1)))
			require.Empty(t, capture.Snapshot())
			require.NoError(t, update(cctx, newTestComponent(1)))
			return chasm.EngineUpdateWithStartExecutionResult{}, nil
		})
	ctx := chasm.NewEngineContext(t.Context(), engine)
	h := newTestHandlerContext(t).handler
	result := h.applyBatch(batchKey{namespaceID: "namespace-id", key: "wholequeue/queue/2/0"}, []batchReq{{ctx: ctx, req: &fcpb.ConcurrencyBatchRequest{ReserveSlots: []string{"slot"}}}})
	require.NoError(t, result[0].err)
	snapshot := capture.Snapshot()
	require.Len(t, snapshot["flowcontrol_concurrency_slot_operations"], 1)
	require.Equal(t, int64(1), snapshot["flowcontrol_concurrency_slot_operations"][0].Value)
	require.Len(t, snapshot["flowcontrol_concurrency_slots_observed_timestamp"], 1)
}
