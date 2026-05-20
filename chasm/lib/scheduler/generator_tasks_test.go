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
	"go.temporal.io/server/common/testing/testlogger"
	queueerrors "go.temporal.io/server/service/history/queues/errors"
	"go.temporal.io/server/service/history/tasks"
	legacyscheduler "go.temporal.io/server/service/worker/scheduler"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func newGeneratorHandler(env *testEnv) *scheduler.GeneratorTaskHandler {
	return scheduler.NewGeneratorTaskHandler(scheduler.GeneratorTaskHandlerOptions{
		Config:         defaultConfig(),
		MetricsHandler: metrics.NoopMetricsHandler,
		BaseLogger:     env.Logger,
		SpecProcessor:  env.SpecProcessor,
		SpecBuilder:    legacyscheduler.NewSpecBuilder(),
	})
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

// idleSchedule returns a schedule configured with LimitedActions=true,
// RemainingActions=0. This causes the generator to transition the scheduler
// into idle state (scheduling an idle close timer instead of a next-run timer)
// once all in-flight backfillers have completed.
func idleSchedule() *schedulepb.Schedule {
	s := defaultSchedule()
	s.State.LimitedActions = true
	s.State.RemainingActions = 0
	return s
}

// newIdleScheduleEngine creates a chasmtest.Engine, starts an idle scheduler on it,
// and returns the engine, a pre-wired engine context, a handler, and a ref for checking tasks.
func newIdleScheduleEngine(t *testing.T) (*chasmtest.Engine, context.Context, *scheduler.Handler, chasm.ComponentRef) {
	t.Helper()
	ctrl := gomock.NewController(t)
	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	registry := chasm.NewRegistry(logger)
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(newTestLibrary(logger, newRealSpecProcessor(ctrl, logger))))

	engine := chasmtest.NewEngine(t, registry)
	engineCtx := chasm.NewEngineContext(context.Background(), engine)

	result, err := chasm.StartExecution(
		engineCtx,
		chasm.ExecutionKey{NamespaceID: namespaceID, BusinessID: scheduleID},
		func(ctx chasm.MutableContext, _ struct{}) (*scheduler.Scheduler, error) {
			return scheduler.NewScheduler(ctx, namespace, namespaceID, scheduleID, idleSchedule(), nil)
		},
		struct{}{},
	)
	require.NoError(t, err)

	ref, err := chasm.DeserializeComponentRef(result.ExecutionRef)
	require.NoError(t, err)

	return engine, engineCtx, scheduler.NewTestHandler(logger), ref
}

func TestPatch_IdleSchedule_RetainsTimerTask(t *testing.T) {
	t.Run("TriggerImmediately", func(t *testing.T) {
		engine, engineCtx, h, ref := newIdleScheduleEngine(t)

		_, err := h.PatchSchedule(engineCtx, &schedulerpb.PatchScheduleRequest{
			NamespaceId: namespaceID,
			FrontendRequest: &workflowservice.PatchScheduleRequest{
				Namespace:  namespace,
				ScheduleId: scheduleID,
				Patch: &schedulepb.SchedulePatch{
					TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{},
				},
			},
		})
		require.NoError(t, err)

		allTasks, err := engine.Tasks(ref)
		require.NoError(t, err)
		require.NotEmpty(t, allTasks[tasks.CategoryTimer])
	})

	t.Run("BackfillRequest", func(t *testing.T) {
		engine, engineCtx, h, ref := newIdleScheduleEngine(t)
		now := engine.TimeSource().Now()

		_, err := h.PatchSchedule(engineCtx, &schedulerpb.PatchScheduleRequest{
			NamespaceId: namespaceID,
			FrontendRequest: &workflowservice.PatchScheduleRequest{
				Namespace:  namespace,
				ScheduleId: scheduleID,
				Patch: &schedulepb.SchedulePatch{
					BackfillRequest: []*schedulepb.BackfillRequest{
						{
							StartTime: timestamppb.New(now.Add(-2 * defaultInterval)),
							EndTime:   timestamppb.New(now.Add(-defaultInterval)),
						},
					},
				},
			},
		})
		require.NoError(t, err)

		allTasks, err := engine.Tasks(ref)
		require.NoError(t, err)
		require.NotEmpty(t, allTasks[tasks.CategoryTimer])
	})
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
