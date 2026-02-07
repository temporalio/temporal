package scheduler_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/metrics"
	queueerrors "go.temporal.io/server/service/history/queues/errors"
	"go.temporal.io/server/service/history/tasks"
	legacyscheduler "go.temporal.io/server/service/worker/scheduler"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func newGeneratorExecutor(env *testEnv) *scheduler.GeneratorTaskExecutor {
	return scheduler.NewGeneratorTaskExecutor(scheduler.GeneratorTaskExecutorOptions{
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
	executor := newGeneratorExecutor(env)

	ctx := env.MutableContext()
	generator := env.Scheduler.Generator.Get(ctx)

	// If ProcessTimeRange fails, we should fail the task as an internal error.
	err := executor.Execute(ctx, generator, chasm.TaskAttributes{}, &schedulerpb.GeneratorTask{})
	var target *queueerrors.UnprocessableTaskError
	require.ErrorAs(t, err, &target)
	require.Equal(t, "failed to process a time range: processTimeRange bug", target.Message)
}

func TestGeneratorTask_ExecuteBufferTask_Basic(t *testing.T) {
	env := newTestEnv(t)
	executor := newGeneratorExecutor(env)

	ctx := env.MutableContext()
	sched := env.Scheduler
	generator := sched.Generator.Get(ctx)

	// Move high water mark back in time (Generator always compares high water mark
	// against system time) to generate buffered actions.
	highWatermark := ctx.Now(generator).UTC().Add(-defaultInterval * 5)
	generator.LastProcessedTime = timestamppb.New(highWatermark)

	// Execute the generate task.
	err := executor.Execute(ctx, generator, chasm.TaskAttributes{}, &schedulerpb.GeneratorTask{})
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
	executor := newGeneratorExecutor(env)

	ctx := env.MutableContext()
	generator := env.Scheduler.Generator.Get(ctx)

	err := executor.Execute(ctx, generator, chasm.TaskAttributes{}, &schedulerpb.GeneratorTask{})
	require.NoError(t, err)

	require.NotEmpty(t, generator.FutureActionTimes)
	require.Len(t, generator.FutureActionTimes, 10)
}

func TestGeneratorTask_UpdateFutureActionTimes_LimitedActions(t *testing.T) {
	env := newTestEnv(t)
	executor := newGeneratorExecutor(env)

	ctx := env.MutableContext()
	sched := env.Scheduler
	generator := sched.Generator.Get(ctx)

	sched.Schedule.State.LimitedActions = true
	sched.Schedule.State.RemainingActions = 2

	err := executor.Execute(ctx, generator, chasm.TaskAttributes{}, &schedulerpb.GeneratorTask{})
	require.NoError(t, err)

	require.Len(t, generator.FutureActionTimes, 2)
}

func TestGeneratorTask_UpdateFutureActionTimes_SkipsBeforeUpdateTime(t *testing.T) {
	env := newTestEnv(t)
	executor := newGeneratorExecutor(env)

	ctx := env.MutableContext()
	sched := env.Scheduler
	generator := sched.Generator.Get(ctx)

	// UpdateTime acts as a floor - action times at or before it are skipped.
	baseTime := ctx.Now(generator).UTC()
	updateTime := baseTime.Add(defaultInterval / 2)
	sched.Info.UpdateTime = timestamppb.New(updateTime)

	err := executor.Execute(ctx, generator, chasm.TaskAttributes{}, &schedulerpb.GeneratorTask{})
	require.NoError(t, err)

	require.NotEmpty(t, generator.FutureActionTimes)
	for _, futureTime := range generator.FutureActionTimes {
		require.True(t, futureTime.AsTime().After(updateTime))
	}
}
