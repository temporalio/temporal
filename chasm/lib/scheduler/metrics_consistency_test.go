package scheduler_test

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Prometheus rejects a metric whose label set differs from its first emission, so fired and invalidated paths must carry identical label keys.

func requireConsistentLabelKeys(t *testing.T, capture *metricstest.Capture, metricName string, wantSamples int) {
	t.Helper()

	recs := capture.Snapshot()[metricName]
	require.GreaterOrEqual(t, len(recs), wantSamples,
		"expected at least %d samples for %s", wantSamples, metricName)

	var want []string
	for i, r := range recs {
		keys := make([]string, 0, len(r.Tags))
		for k := range r.Tags {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		if i == 0 {
			want = keys
			require.Contains(t, keys, "outcome", "%s sample missing outcome label", metricName)
			require.Contains(t, keys, "reason", "%s sample missing reason label", metricName)
			continue
		}
		require.Equal(t, want, keys,
			"%s emitted with inconsistent label keys (%v) for tags %v; Prometheus rejects a metric whose label set changes between emissions",
			metricName, want, r.Tags)
	}
}

func TestScheduleIdleTask_ConsistentLabels(t *testing.T) {
	env := newTestEnv(t)
	rec := metricstest.NewCaptureHandler()
	capture := rec.StartCapture()
	defer rec.StopCapture(capture)

	h := scheduler.NewSchedulerIdleTaskHandler(scheduler.SchedulerIdleTaskHandlerOptions{
		Config:         defaultConfig(),
		MetricsHandler: rec,
		BaseLogger:     env.Logger,
	})

	require.NoError(t, h.Execute(env.MutableContext(), env.Scheduler,
		chasm.TaskAttributes{}, &schedulerpb.SchedulerIdleTask{}))

	// Execute set Closed=true, so Validate now rejects with reason=closed.
	valid, err := h.Validate(env.MutableContext(), env.Scheduler,
		chasm.TaskInvocation{TaskAttributes: chasm.TaskAttributes{ScheduledTime: env.TimeSource.Now()}},
		&schedulerpb.SchedulerIdleTask{IdleTimeTotal: durationpb.New(10 * time.Minute)})
	require.NoError(t, err)
	require.False(t, valid)

	requireConsistentLabelKeys(t, capture, metrics.ScheduleIdleTask.Name(), 2)
}

func TestScheduleInvokerProcessBufferTask_ConsistentLabels(t *testing.T) {
	env := newTestEnv(t)
	rec := metricstest.NewCaptureHandler()
	capture := rec.StartCapture()
	defer rec.StopCapture(capture)

	h := scheduler.NewInvokerProcessBufferTaskHandler(scheduler.InvokerTaskHandlerOptions{
		Config:         defaultConfig(),
		MetricsHandler: rec,
		BaseLogger:     env.Logger,
		SpecProcessor:  env.SpecProcessor,
	})

	ctx := env.MutableContext()
	invoker := env.Scheduler.Invoker.Get(ctx)
	invoker.LastProcessedTime = timestamppb.New(env.TimeSource.Now())

	require.NoError(t, h.Execute(ctx, invoker, chasm.TaskAttributes{}, &schedulerpb.InvokerProcessBufferTask{}))
	require.NoError(t, env.CloseTransaction())

	valid, err := h.Validate(env.ReadContext(), invoker,
		chasm.TaskInvocation{TaskAttributes: chasm.TaskAttributes{ScheduledTime: env.TimeSource.Now().Add(-time.Minute)}},
		&schedulerpb.InvokerProcessBufferTask{})
	require.NoError(t, err)
	require.False(t, valid)

	requireConsistentLabelKeys(t, capture, metrics.ScheduleInvokerProcessBufferTask.Name(), 2)
}

func TestScheduleBackfillerTask_ConsistentLabels(t *testing.T) {
	env := newTestEnv(t)
	rec := metricstest.NewCaptureHandler()
	capture := rec.StartCapture()
	defer rec.StopCapture(capture)

	h := scheduler.NewBackfillerTaskHandler(scheduler.BackfillerTaskHandlerOptions{
		Config:         defaultConfig(),
		MetricsHandler: rec,
		BaseLogger:     env.Logger,
		SpecProcessor:  env.SpecProcessor,
	})

	// A range beyond the buffer limit keeps the Backfiller alive after its first iteration.
	ctx := env.MutableContext()
	schedComponent, err := env.Node.Component(ctx, chasm.ComponentRef{})
	require.NoError(t, err)
	sched := schedComponent.(*scheduler.Scheduler)
	startTime := env.TimeSource.Now()
	backfiller := sched.NewRangeBackfiller(ctx, &schedulepb.BackfillRequest{
		StartTime:     timestamppb.New(startTime),
		EndTime:       timestamppb.New(startTime.Add(1000 * defaultInterval)),
		OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
	})
	require.NoError(t, env.CloseTransaction())

	ctx = env.MutableContext()
	sched.Invoker.Get(ctx).BufferedStarts = nil // make room for the next batch
	require.NoError(t, h.Execute(ctx, backfiller,
		chasm.TaskAttributes{}, &schedulerpb.BackfillerTask{Stamp: backfiller.GetTaskStamp()}))
	require.NoError(t, env.CloseTransaction())

	valid, err := h.Validate(env.ReadContext(), backfiller,
		chasm.TaskInvocation{}, &schedulerpb.BackfillerTask{Stamp: backfiller.GetTaskStamp() - 1})
	require.NoError(t, err)
	require.False(t, valid)

	requireConsistentLabelKeys(t, capture, metrics.ScheduleBackfillerTask.Name(), 2)
}

func TestScheduleInvokerExecuteTask_ConsistentLabels(t *testing.T) {
	env := newInvokerExecuteTestEnv(t)
	rec := metricstest.NewCaptureHandler()
	capture := rec.StartCapture()
	defer rec.StopCapture(capture)

	h := scheduler.NewInvokerExecuteTaskHandler(scheduler.InvokerTaskHandlerOptions{
		Config:         defaultConfig(),
		MetricsHandler: rec,
		BaseLogger:     env.Logger,
		SpecProcessor:  env.SpecProcessor,
		HistoryClient:  env.mockHistoryClient,
		FrontendClient: env.mockFrontendClient,
	})

	ctx := env.MutableContext()
	invoker := env.Scheduler.Invoker.Get(ctx)
	invoker.LastProcessedTime = timestamppb.New(env.TimeSource.Now())

	valid, err := h.Validate(ctx, invoker, chasm.TaskInvocation{}, &schedulerpb.InvokerExecuteTask{})
	require.NoError(t, err)
	require.False(t, valid)

	now := timestamppb.New(env.TimeSource.Now())
	invoker.BufferedStarts = []*schedulespb.BufferedStart{{
		NominalTime:   now,
		ActualTime:    now,
		DesiredTime:   now,
		RequestId:     "req1",
		OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		Attempt:       1,
	}}
	env.mockFrontendClient.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(&workflowservice.StartWorkflowExecutionResponse{RunId: "run-id"}, nil)
	dropped, err := chasmtest.ExecuteSideEffectTask(context.Background(), env.engine, invoker, h,
		chasm.TaskAttributes{}, &schedulerpb.InvokerExecuteTask{})
	require.NoError(t, err)
	require.False(t, dropped)
	require.NoError(t, env.CloseTransaction())

	requireConsistentLabelKeys(t, capture, metrics.ScheduleInvokerExecuteTask.Name(), 2)
}
