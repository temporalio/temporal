package scheduler_test

import (
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// The scheduler task-lifecycle counters are emitted on two paths: "fired"
// (Validate=true, the task executed) and "invalidated" (Validate=false, with a
// ReasonTag explaining why). Prometheus binds a fixed label set to a metric
// name on first emission and rejects later emissions whose label set differs,
// logging a warn that CICD log-scraping treats as a failure. So both paths of a
// given counter must carry an identical set of label keys.
//
// These tests drive each counter's fired and invalidated paths through a
// capturing metrics handler and assert the recorded samples share the same
// label keys (regression guard for the duplicate-descriptor bug where the fired
// path omitted ReasonTag).

// requireConsistentLabelKeys asserts that every sample recorded under metricName
// carries an identical, sorted set of label keys, that at least wantSamples were
// captured, and that the keys include both outcome and reason.
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

	// fired: Execute closes the schedule and records outcome=fired.
	require.NoError(t, h.Execute(env.MutableContext(), env.Scheduler,
		chasm.TaskAttributes{}, &schedulerpb.SchedulerIdleTask{}))

	// invalidated: Execute set Closed=true above, so Validate now rejects with
	// reason=closed.
	valid, err := h.Validate(env.MutableContext(), env.Scheduler,
		chasm.TaskAttributes{ScheduledTime: env.TimeSource.Now()},
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

	// fired: Execute with an empty buffer records outcome=fired.
	require.NoError(t, h.Execute(ctx, invoker, chasm.TaskAttributes{}, &schedulerpb.InvokerProcessBufferTask{}))
	require.NoError(t, env.CloseTransaction())

	// invalidated: a task scheduled before the high water mark is stale.
	valid, err := h.Validate(env.ReadContext(), invoker,
		chasm.TaskAttributes{ScheduledTime: env.TimeSource.Now().Add(-time.Minute)},
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

	// Use a range larger than the backfiller's buffer limit so it does not
	// complete (and delete itself) on the first auto-executed iteration; that
	// leaves a live Backfiller we can drive Execute on through our capturing
	// handler.
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

	// fired: Execute records outcome=fired.
	ctx = env.MutableContext()
	sched.Invoker.Get(ctx).BufferedStarts = nil // make room for the next batch
	require.NoError(t, h.Execute(ctx, backfiller,
		chasm.TaskAttributes{}, &schedulerpb.BackfillerTask{}))
	require.NoError(t, env.CloseTransaction())

	// invalidated: a task scheduled before the high water mark is stale.
	backfiller.LastProcessedTime = timestamppb.New(env.TimeSource.Now())
	valid, err := h.Validate(env.ReadContext(), backfiller,
		chasm.TaskAttributes{ScheduledTime: env.TimeSource.Now().Add(-time.Hour)},
		&schedulerpb.BackfillerTask{})
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

	// invalidated: no terminate/cancel/eligible work records reason=no_work.
	valid, err := h.Validate(ctx, invoker, chasm.TaskAttributes{}, &schedulerpb.InvokerExecuteTask{})
	require.NoError(t, err)
	require.False(t, valid)

	// fired: Execute over an empty work set records outcome=fired (no RPCs).
	env.ExpectReadComponent(ctx, invoker)
	env.ExpectUpdateComponent(ctx, invoker)
	require.NoError(t, h.Execute(env.EngineContext(), chasm.ComponentRef{},
		chasm.TaskAttributes{}, &schedulerpb.InvokerExecuteTask{}))
	require.NoError(t, env.CloseTransaction())

	requireConsistentLabelKeys(t, capture, metrics.ScheduleInvokerExecuteTask.Name(), 2)
}
