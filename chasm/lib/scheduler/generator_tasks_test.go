package scheduler_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/testing/testlogger"
	queueerrors "go.temporal.io/server/service/history/queues/errors"
	"go.temporal.io/server/service/history/tasks"
	legacyscheduler "go.temporal.io/server/service/worker/scheduler"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type generatorHandlerOption func(*scheduler.GeneratorTaskHandlerOptions)

func withTweakables(fn func(*scheduler.Tweakables)) generatorHandlerOption {
	return func(o *scheduler.GeneratorTaskHandlerOptions) {
		tw := o.Config.Tweakables("")
		fn(&tw)
		o.Config.Tweakables = func(string) scheduler.Tweakables { return tw }
	}
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
		SpecBuilder:    newLegacySpecBuilder(0, 0),
	}
	for _, opt := range opts {
		opt(&o)
	}
	return scheduler.NewGeneratorTaskHandler(o)
}

func TestGeneratorTask_Execute_ProcessTimeRangeFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	specProcessor := scheduler.NewMockSpecProcessor(ctrl)
	now := time.Now()

	// Succeeds for newTestEnv's initial CloseTransaction, then fails in the test.
	specProcessor.EXPECT().ProcessTimeRange(
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
	).Return(&scheduler.ProcessedTimeRange{
		NextWakeupTime: now.Add(defaultInterval),
		LastActionTime: now,
	}, nil).Times(1)

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

	// Generator compares the high water mark against system time, so rewinding it yields actions.
	highWatermark := ctx.Now(generator).UTC().Add(-defaultInterval * 5)
	generator.LastProcessedTime = timestamppb.New(highWatermark)

	err := handler.Execute(ctx, generator, chasm.TaskAttributes{}, &schedulerpb.GeneratorTask{})
	require.NoError(t, err)

	invoker := sched.Invoker.Get(ctx)
	require.Len(t, invoker.BufferedStarts, 5)

	for _, start := range invoker.BufferedStarts {
		require.Equal(t, start.WorkflowId, invoker.RunningWorkflowID(start.RequestId))
	}

	newHighWatermark := generator.LastProcessedTime.AsTime()
	require.True(t, newHighWatermark.After(highWatermark))

	// InvokerProcessBufferTask is pure and runs inline in CloseTransaction, leaving the InvokerExecuteTask.
	require.NoError(t, env.CloseTransaction())
	require.True(t, env.HasTask(&tasks.ChasmTask{}, chasm.TaskScheduledTimeImmediate))
}

func TestGeneratorTask_BufferOverrunDropsActions(t *testing.T) {
	env := newTestEnv(t)

	rec := metricstest.NewCaptureHandler()
	capture := rec.StartCapture()
	defer rec.StopCapture(capture)

	const maxBufferSize = 2
	handler := newGeneratorHandler(env,
		withTweakables(func(tw *scheduler.Tweakables) { tw.MaxBufferSize = maxBufferSize }),
		withGeneratorMetrics(rec))

	ctx := env.MutableContext()
	sched := env.Scheduler
	generator := sched.Generator.Get(ctx)
	invoker := sched.Invoker.Get(ctx)

	rewindHWM := func() time.Time {
		hwm := ctx.Now(generator).UTC().Add(-defaultInterval * 5)
		generator.LastProcessedTime = timestamppb.New(hwm)
		return hwm
	}
	rewindHWM()
	require.NoError(t, handler.Execute(ctx, generator, chasm.TaskAttributes{}, &schedulerpb.GeneratorTask{}))

	require.Len(t, invoker.BufferedStarts, maxBufferSize)
	require.Equal(t, int64(3), sched.Info.BufferDropped)

	hwm := rewindHWM()
	require.NoError(t, handler.Execute(ctx, generator, chasm.TaskAttributes{}, &schedulerpb.GeneratorTask{}))
	require.Greater(t, sched.Info.BufferDropped, int64(3), "BufferDropped should accumulate across ticks")
	require.True(t, generator.LastProcessedTime.AsTime().After(hwm), "HWM advances even when all actions drop")

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

func TestGeneratorTask_MaxBufferSizeDisabledBuffersAll(t *testing.T) {
	env := newTestEnv(t)

	handler := newGeneratorHandler(env, withTweakables(func(tw *scheduler.Tweakables) { tw.MaxBufferSize = 0 }))

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

func TestGeneratorTask_UpdateFutureActionTimes_NegativeRemainingActionsDoesNotPanic(t *testing.T) {
	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	engine, engineCtx := newTestEngineContext(t, logger)

	input := defaultSchedule()
	input.State.LimitedActions = true
	input.State.RemainingActions = -1

	result, err := chasm.StartExecution(
		engineCtx,
		chasm.ExecutionKey{NamespaceID: namespaceID, BusinessID: scheduleID},
		scheduler.CreateScheduler,
		&schedulerpb.CreateScheduleRequest{
			NamespaceId: namespaceID,
			FrontendRequest: &workflowservice.CreateScheduleRequest{
				Namespace:  namespace,
				ScheduleId: scheduleID,
				Schedule:   input,
			},
		},
	)
	require.NoError(t, err)

	rootRef := chasm.NewComponentRef[*scheduler.Scheduler](result.ExecutionKey)
	_, err = engine.FirePureTasks(rootRef, time.Now())
	require.NoError(t, err)

	_, err = chasm.ReadComponent(engineCtx, rootRef,
		func(s *scheduler.Scheduler, ctx chasm.Context, _ struct{}) (struct{}, error) {
			require.Empty(t, s.Generator.Get(ctx).FutureActionTimes)
			return struct{}{}, nil
		}, struct{}{})
	require.NoError(t, err)
}

func TestGeneratorTask_Idle_SetsIdleCloseTime(t *testing.T) {
	env := newTestEnv(t)
	handler := newGeneratorHandler(env)

	ctx := env.MutableContext()
	sched := env.Scheduler
	generator := sched.Generator.Get(ctx)

	sched.Schedule.State.LimitedActions = true
	sched.Schedule.State.RemainingActions = 0

	err := handler.Execute(ctx, generator, chasm.TaskAttributes{}, &schedulerpb.GeneratorTask{})
	require.NoError(t, err)

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

	sched.IdleCloseTime = timestamppb.New(ctx.Now(generator).Add(7 * 24 * time.Hour))

	err := handler.Execute(ctx, generator, chasm.TaskAttributes{}, &schedulerpb.GeneratorTask{})
	require.NoError(t, err)

	require.NotEmpty(t, generator.FutureActionTimes)
	require.Nil(t, sched.IdleCloseTime, "expected IdleCloseTime to be cleared when the schedule has work")
}

// Uses the test engine so the watermark write and the task that reads it cross a real transaction boundary.
func TestGeneratorTask_FutureActionTimesRespectLastProcessedTimeWatermark(t *testing.T) {
	env := newSchedulerTestEngine(t, defaultSchedule())

	watermark := env.timeSource.Now().Add(10 * defaultInterval)
	var generator *scheduler.Generator
	require.NoError(t, env.updateScheduler(func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
		generator = s.Generator.Get(ctx)
		generator.LastProcessedTime = timestamppb.New(watermark)
		generator.Generate(ctx)
		return nil
	}))

	handler := scheduler.NewGeneratorTaskHandler(scheduler.GeneratorTaskHandlerOptions{
		Config:         defaultConfig(),
		MetricsHandler: metrics.NoopMetricsHandler,
		BaseLogger:     env.logger,
		SpecProcessor:  scheduler.NewSpecProcessor(defaultConfig(), metrics.NoopMetricsHandler, env.logger, newLegacySpecBuilder(0, 0)),
		SpecBuilder:    newLegacySpecBuilder(0, 0),
	})
	dropped, err := chasmtest.ExecutePureTask(
		context.Background(), env.engine, generator, handler, chasm.TaskAttributes{}, &schedulerpb.GeneratorTask{})
	require.NoError(t, err)
	require.False(t, dropped)

	require.NoError(t, env.readScheduler(func(s *scheduler.Scheduler, ctx chasm.Context) error {
		generator := s.Generator.Get(ctx)
		require.NotEmpty(t, generator.GetFutureActionTimes())
		for _, future := range generator.GetFutureActionTimes() {
			require.True(t, future.AsTime().After(watermark),
				"advertised future action %v is not after watermark %v", future.AsTime(), watermark)
		}
		return nil
	}))
}

func TestGeneratorTask_UpdateFutureActionTimes_SkipsBeforeUpdateTime(t *testing.T) {
	env := newTestEnv(t)
	handler := newGeneratorHandler(env)

	ctx := env.MutableContext()
	sched := env.Scheduler
	generator := sched.Generator.Get(ctx)

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

func TestGeneratorTask_PausedDropsActionsAdvancesHWM(t *testing.T) {
	env := newTestEnv(t)
	handler := newGeneratorHandler(env)

	ctx := env.MutableContext()
	sched := env.Scheduler
	generator := sched.Generator.Get(ctx)
	invoker := sched.Invoker.Get(ctx)

	sched.Schedule.State.Paused = true
	pausedStart := ctx.Now(generator).UTC().Add(-defaultInterval * 5)
	generator.LastProcessedTime = timestamppb.New(pausedStart)

	err := handler.Execute(ctx, generator, chasm.TaskAttributes{}, &schedulerpb.GeneratorTask{})
	require.NoError(t, err)

	require.Empty(t, invoker.BufferedStarts)

	// Advancing past the paused window keeps a later unpause from replaying it.
	newHWM := generator.LastProcessedTime.AsTime()
	require.True(t, newHWM.After(pausedStart),
		"HWM should advance past the paused start: was %v, now %v", pausedStart, newHWM)
}

func TestUnpause_ResumesProcessing(t *testing.T) {
	env := newTestEnv(t)

	env.Scheduler.Schedule.State.Paused = true
	require.NoError(t, env.CloseTransaction())

	env.NodeBackend.TasksByCategory = nil
	ctx := env.MutableContext()
	_, err := env.Scheduler.Patch(ctx, &schedulerpb.PatchScheduleRequest{
		FrontendRequest: &workflowservice.PatchScheduleRequest{
			Patch: &schedulepb.SchedulePatch{Unpause: "resuming"},
		},
	})
	require.NoError(t, err)

	env.TimeSource.Update(env.TimeSource.Now().Add(defaultInterval * 3))
	require.NoError(t, env.CloseTransaction())

	// Patch must kick an immediate generator task; otherwise nothing runs after unpause.
	require.True(t, env.HasTask(&tasks.ChasmTask{}, chasm.TaskScheduledTimeImmediate),
		"schedule should resume processing after unpause")
}

// The 5 actions are 1-5 minutes old, so a window wrongly resolved to MinCatchupWindow (10s) drops them all.
// The below-min case proves the window is enforced, so the zero-missed results are meaningful.
func TestGeneratorTask_CatchupWindowResolvesNonPositiveToDefault(t *testing.T) {
	cases := []struct {
		name          string
		catchupWindow *durationpb.Duration
		caughtUp      bool
	}{
		{"nil resolves to default", nil, true},
		{"zero resolves to default", durationpb.New(0), true},
		{"negative resolves to default", durationpb.New(-5 * time.Second), true},
		{"below-min positive is clamped and enforced", durationpb.New(time.Second), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := newTestEnv(t)
			handler := newGeneratorHandler(env)

			ctx := env.MutableContext()
			sched := env.Scheduler
			sched.Schedule.Policies.CatchupWindow = tc.catchupWindow

			generator := sched.Generator.Get(ctx)
			invoker := sched.Invoker.Get(ctx)

			hwm := ctx.Now(generator).UTC().Add(-defaultInterval * 5)
			generator.LastProcessedTime = timestamppb.New(hwm)

			require.NoError(t, handler.Execute(ctx, generator, chasm.TaskAttributes{}, &schedulerpb.GeneratorTask{}))

			if tc.caughtUp {
				require.Len(t, invoker.BufferedStarts, 5,
					"a non-positive window should resolve to the default and catch up every missed action")
				require.Zero(t, sched.Info.MissedCatchupWindow,
					"no action should be missed when the window resolves to the default")
			} else {
				require.Less(t, len(invoker.BufferedStarts), 5,
					"a below-min window is clamped to MinCatchupWindow and should drop older actions")
				require.Positive(t, sched.Info.MissedCatchupWindow,
					"actions older than the enforced window should be counted as missed")
			}
		})
	}
}
