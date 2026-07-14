package scheduler_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	queueerrors "go.temporal.io/server/service/history/queues/errors"
	"go.temporal.io/server/service/history/tasks"
	legacyscheduler "go.temporal.io/server/service/worker/scheduler"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type generatorHandlerOption func(*scheduler.GeneratorTaskHandlerOptions)

func withGeneratorConfig(c *scheduler.Config) generatorHandlerOption {
	return func(o *scheduler.GeneratorTaskHandlerOptions) { o.Config = c }
}

func withGeneratorMetrics(h metrics.Handler) generatorHandlerOption {
	return func(o *scheduler.GeneratorTaskHandlerOptions) { o.MetricsHandler = h }
}

func newGeneratorHandler(env *testEnv, opts ...generatorHandlerOption) *scheduler.GeneratorTaskHandler {
	o := scheduler.GeneratorTaskHandlerOptions{
		Config:         defaultConfig(),
		MetricsHandler: metrics.NoopMetricsHandler,
		BaseLogger:     env.Logger,
		SpecProcessor:  env.SpecProcessor,
		SpecBuilder:    legacyscheduler.NewSpecBuilder(),
	}
	for _, opt := range opts {
		opt(&o)
	}
	return scheduler.NewGeneratorTaskHandler(o)
}

func TestGeneratorTask_Execute_ProcessTimeRangeFails(t *testing.T) {
	// Create a custom mock spec processor that fails on ProcessTimeRange.
	ctrl := gomock.NewController(t)
	specProcessor := scheduler.NewMockSpecProcessor(ctrl)
	now := time.Now()

	// First call during newTestEnv's CloseTransaction should succeed.
	specProcessor.EXPECT().ProcessTimeRange(
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
	).Return(&scheduler.ProcessedTimeRange{
		NextWakeupTime: now.Add(defaultInterval),
		LastActionTime: now,
	}, nil).Times(1)

	// Second call during test should fail.
	specProcessor.EXPECT().ProcessTimeRange(
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
	).Return(nil, errors.New("processTimeRange bug"))

	specProcessor.EXPECT().NextTime(gomock.Any(), gomock.Any()).Return(legacyscheduler.GetNextTimeResult{
		Next:    now.Add(defaultInterval),
		Nominal: now.Add(defaultInterval),
	}, nil).AnyTimes()

	env := newTestEnv(t, withSpecProcessor(specProcessor))
	handler := newGeneratorHandler(env)

	ctx := env.MutableContext()
	generator := env.Scheduler.Generator.Get(ctx)

	// If ProcessTimeRange fails, we should fail the task as an internal error.
	err := handler.Execute(ctx, generator, chasm.TaskAttributes{}, &schedulerpb.GeneratorTask{})
	var target *queueerrors.UnprocessableTaskError
	require.ErrorAs(t, err, &target)
	require.Equal(t, "failed to process a time range: processTimeRange bug", target.Message)
}

func TestGeneratorTask_ExecuteBufferTask_Basic(t *testing.T) {
	env := newTestEnv(t)
	handler := newGeneratorHandler(env)

	ctx := env.MutableContext()
	sched := env.Scheduler
	generator := sched.Generator.Get(ctx)

	// Move high water mark back in time (Generator always compares high water mark
	// against system time) to generate buffered actions.
	highWatermark := ctx.Now(generator).UTC().Add(-defaultInterval * 5)
	generator.LastProcessedTime = timestamppb.New(highWatermark)

	// Execute the generate task.
	err := handler.Execute(ctx, generator, chasm.TaskAttributes{}, &schedulerpb.GeneratorTask{})
	require.NoError(t, err)

	// We expect 5 buffered starts.
	invoker := sched.Invoker.Get(ctx)
	require.Len(t, invoker.BufferedStarts, 5)

	// Validate RequestId -> WorkflowId mapping.
	for _, start := range invoker.BufferedStarts {
		require.Equal(t, start.WorkflowId, invoker.RunningWorkflowID(start.RequestId))
	}

	// Generator's high water mark should have advanced.
	newHighWatermark := generator.LastProcessedTime.AsTime()
	require.True(t, newHighWatermark.After(highWatermark))

	// Ensure we scheduled a physical side-effect task on the tree at immediate time.
	// The InvokerExecuteTask is a side-effect task that starts workflows.
	// The InvokerProcessBufferTask (pure) executes inline during CloseTransaction.
	require.NoError(t, env.CloseTransaction())
	require.True(t, env.HasTask(&tasks.ChasmTask{}, chasm.TaskScheduledTimeImmediate))
}

// Once the buffer hits MaxBufferSize, further automated actions are dropped,
// BufferDropped is incremented (accumulating across ticks), and the overrun
// metric is recorded.
func TestGeneratorTask_BufferOverrunDropsActions(t *testing.T) {
	env := newTestEnv(t)

	rec := metricstest.NewCaptureHandler()
	capture := rec.StartCapture()
	defer rec.StopCapture(capture)

	const maxBufferSize = 2
	handler := newGeneratorHandler(env,
		withGeneratorConfig(configWithTweakables(func(tw *scheduler.Tweakables) { tw.MaxBufferSize = maxBufferSize })),
		withGeneratorMetrics(rec))

	ctx := env.MutableContext()
	sched := env.Scheduler
	generator := sched.Generator.Get(ctx)
	invoker := sched.Invoker.Get(ctx)

	// Pull the high water mark back so the range yields 5 actions, exceeding the
	// buffer capacity of 2.
	rewindHWM := func() time.Time {
		hwm := ctx.Now(generator).UTC().Add(-defaultInterval * 5)
		generator.LastProcessedTime = timestamppb.New(hwm)
		return hwm
	}
	rewindHWM()
	require.NoError(t, handler.Execute(ctx, generator, chasm.TaskAttributes{}, &schedulerpb.GeneratorTask{}))

	// Only the capacity is buffered; the 3 remaining actions are dropped.
	require.Len(t, invoker.BufferedStarts, maxBufferSize)
	require.Equal(t, int64(3), sched.Info.BufferDropped)

	// A second tick with the buffer already full drops every action, accumulates
	// BufferDropped, and still advances the high water mark.
	hwm := rewindHWM()
	require.NoError(t, handler.Execute(ctx, generator, chasm.TaskAttributes{}, &schedulerpb.GeneratorTask{}))
	require.Greater(t, sched.Info.BufferDropped, int64(3), "BufferDropped should accumulate across ticks")
	require.True(t, generator.LastProcessedTime.AsTime().After(hwm), "HWM advances even when all actions drop")

	// The overrun counter total tracks BufferDropped.
	recs := capture.Snapshot()[metrics.ScheduleBufferOverruns.Name()]
	require.NotEmpty(t, recs, "expected the buffer overrun counter to be recorded")
	var total int64
	for _, r := range recs {
		v, ok := r.Value.(int64)
		require.True(t, ok, "expected int64 counter value, got %T", r.Value)
		total += v
	}
	require.Equal(t, sched.Info.BufferDropped, total)
}

// A non-positive MaxBufferSize disables the limit: nothing is dropped even over
// a range that would overflow a small buffer.
func TestGeneratorTask_MaxBufferSizeDisabledBuffersAll(t *testing.T) {
	env := newTestEnv(t)

	handler := newGeneratorHandler(env, withGeneratorConfig(configWithTweakables(func(tw *scheduler.Tweakables) { tw.MaxBufferSize = 0 })))

	ctx := env.MutableContext()
	sched := env.Scheduler
	generator := sched.Generator.Get(ctx)
	invoker := sched.Invoker.Get(ctx)

	highWatermark := ctx.Now(generator).UTC().Add(-defaultInterval * 5)
	generator.LastProcessedTime = timestamppb.New(highWatermark)

	require.NoError(t, handler.Execute(ctx, generator, chasm.TaskAttributes{}, &schedulerpb.GeneratorTask{}))

	require.Len(t, invoker.BufferedStarts, 5)
	require.Zero(t, sched.Info.BufferDropped)
}

func TestGeneratorTask_UpdateFutureActionTimes_UnlimitedActions(t *testing.T) {
	env := newTestEnv(t)
	handler := newGeneratorHandler(env)

	ctx := env.MutableContext()
	generator := env.Scheduler.Generator.Get(ctx)

	err := handler.Execute(ctx, generator, chasm.TaskAttributes{}, &schedulerpb.GeneratorTask{})
	require.NoError(t, err)

	require.NotEmpty(t, generator.FutureActionTimes)
	require.Len(t, generator.FutureActionTimes, 10)
}

func TestGeneratorTask_UpdateFutureActionTimes_LimitedActions(t *testing.T) {
	env := newTestEnv(t)
	handler := newGeneratorHandler(env)

	ctx := env.MutableContext()
	sched := env.Scheduler
	generator := sched.Generator.Get(ctx)

	sched.Schedule.State.LimitedActions = true
	sched.Schedule.State.RemainingActions = 2

	err := handler.Execute(ctx, generator, chasm.TaskAttributes{}, &schedulerpb.GeneratorTask{})
	require.NoError(t, err)

	require.Len(t, generator.FutureActionTimes, 2)
}

func TestGeneratorTask_Idle_SetsIdleCloseTime(t *testing.T) {
	env := newTestEnv(t)
	handler := newGeneratorHandler(env)

	ctx := env.MutableContext()
	sched := env.Scheduler
	generator := sched.Generator.Get(ctx)

	// Exhaust the schedule's actions so it goes idle (no more work to do).
	sched.Schedule.State.LimitedActions = true
	sched.Schedule.State.RemainingActions = 0

	err := handler.Execute(ctx, generator, chasm.TaskAttributes{}, &schedulerpb.GeneratorTask{})
	require.NoError(t, err)

	// The idle-close deadline must be recorded so it can be surfaced as the
	// ScheduleIdleCloseTime search attribute.
	require.NotNil(t, sched.IdleCloseTime, "expected IdleCloseTime to be set once idle")
	require.True(t, sched.IdleCloseTime.AsTime().After(ctx.Now(generator)),
		"expected idle-close deadline in the future, got %v", sched.IdleCloseTime.AsTime())
}

func TestGeneratorTask_NonIdle_ClearsIdleCloseTime(t *testing.T) {
	env := newTestEnv(t)
	handler := newGeneratorHandler(env)

	ctx := env.MutableContext()
	sched := env.Scheduler
	generator := sched.Generator.Get(ctx)

	// Simulate a stale deadline left over from a prior idle period.
	sched.IdleCloseTime = timestamppb.New(ctx.Now(generator).Add(7 * 24 * time.Hour))

	// A normal execution generates future actions, so the schedule is not idle.
	err := handler.Execute(ctx, generator, chasm.TaskAttributes{}, &schedulerpb.GeneratorTask{})
	require.NoError(t, err)

	require.NotEmpty(t, generator.FutureActionTimes)
	require.Nil(t, sched.IdleCloseTime, "expected IdleCloseTime to be cleared when the schedule has work")
}

func TestGeneratorTask_UpdateFutureActionTimes_SkipsBeforeUpdateTime(t *testing.T) {
	env := newTestEnv(t)
	handler := newGeneratorHandler(env)

	ctx := env.MutableContext()
	sched := env.Scheduler
	generator := sched.Generator.Get(ctx)

	// UpdateTime acts as a floor - action times at or before it are skipped.
	baseTime := ctx.Now(generator).UTC()
	updateTime := baseTime.Add(defaultInterval / 2)
	sched.Info.UpdateTime = timestamppb.New(updateTime)

	err := handler.Execute(ctx, generator, chasm.TaskAttributes{}, &schedulerpb.GeneratorTask{})
	require.NoError(t, err)

	require.NotEmpty(t, generator.FutureActionTimes)
	for _, futureTime := range generator.FutureActionTimes {
		require.True(t, futureTime.AsTime().After(updateTime))
	}
}

// TestGeneratorTask_PausedDropsActionsAdvancesHWM verifies the "drop, don't
// buffer; keep advancing the HWM" semantic of the always-on GeneratorTask.
// A paused Execute over a non-trivial time range must produce no buffered
// starts and must still advance the high water mark.
func TestGeneratorTask_PausedDropsActionsAdvancesHWM(t *testing.T) {
	env := newTestEnv(t)
	handler := newGeneratorHandler(env)

	ctx := env.MutableContext()
	sched := env.Scheduler
	generator := sched.Generator.Get(ctx)
	invoker := sched.Invoker.Get(ctx)

	// Pause and pull the HWM back so a non-paused run would have buffered
	// several actions across the interval.
	sched.Schedule.State.Paused = true
	pausedStart := ctx.Now(generator).UTC().Add(-defaultInterval * 5)
	generator.LastProcessedTime = timestamppb.New(pausedStart)

	err := handler.Execute(ctx, generator, chasm.TaskAttributes{}, &schedulerpb.GeneratorTask{})
	require.NoError(t, err)

	// No actions buffered while paused - they are dropped.
	require.Empty(t, invoker.BufferedStarts)

	// HWM advanced past the paused window so a future unpause won't replay
	// the dropped window.
	newHWM := generator.LastProcessedTime.AsTime()
	require.True(t, newHWM.After(pausedStart),
		"HWM should advance past the paused start: was %v, now %v", pausedStart, newHWM)
}

func TestUnpause_ResumesProcessing(t *testing.T) {
	env := newTestEnv(t)

	// Pause the schedule.
	env.Scheduler.Schedule.State.Paused = true
	require.NoError(t, env.CloseTransaction())

	// Clear tasks from setup, then unpause. UpdateTime is captured at T0.
	env.NodeBackend.TasksByCategory = nil
	ctx := env.MutableContext()
	_, err := env.Scheduler.Patch(ctx, &schedulerpb.PatchScheduleRequest{
		FrontendRequest: &workflowservice.PatchScheduleRequest{
			Patch: &schedulepb.SchedulePatch{Unpause: "resuming"},
		},
	})
	require.NoError(t, err)

	// Advance time before closing so the generator has actions to process.
	env.TimeSource.Update(env.TimeSource.Now().Add(defaultInterval * 3))
	require.NoError(t, env.CloseTransaction())

	// With the fix, Patch kicks an immediate generator task. During CloseTransaction
	// it processes the elapsed interval, buffers starts, and the invoker schedules
	// side-effect tasks to start workflows. Without the fix, nothing runs.
	require.True(t, env.HasTask(&tasks.ChasmTask{}, chasm.TaskScheduledTimeImmediate),
		"schedule should resume processing after unpause")
}
