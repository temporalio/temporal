package scheduler_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestNewSentinel(t *testing.T) {
	sentinel, ctx, _ := setupSentinelForTest(t)

	require.True(t, sentinel.IsSentinel())
	require.NotNil(t, sentinel.Info.GetCreateTime())
	require.False(t, sentinel.Info.CreateTime.AsTime().IsZero())

	// Sentinels should have no Visibility component, which prevents them from
	// appearing in ListSchedules results.
	_, ok := sentinel.Visibility.TryGet(ctx)
	require.False(t, ok)
}

func TestSentinelIdleTask_Validate_Valid(t *testing.T) {
	sentinel, ctx, _ := setupSentinelForTest(t)

	executor := scheduler.NewSchedulerIdleTaskExecutor(scheduler.SchedulerIdleTaskExecutorOptions{
		Config: defaultConfig(),
	})

	task := &schedulerpb.SchedulerIdleTask{
		IdleTimeTotal: durationpb.New(scheduler.SentinelIdleTime),
	}
	taskAttrs := chasm.TaskAttributes{
		ScheduledTime: sentinel.Info.CreateTime.AsTime().Add(scheduler.SentinelIdleTime),
	}

	isValid, err := executor.Validate(ctx, sentinel, taskAttrs, task)
	require.NoError(t, err)
	require.True(t, isValid)
}

func TestSentinelIdleTask_Validate_InvalidAfterClosed(t *testing.T) {
	sentinel, ctx, _ := setupSentinelForTest(t)
	sentinel.Closed = true

	executor := scheduler.NewSchedulerIdleTaskExecutor(scheduler.SchedulerIdleTaskExecutorOptions{
		Config: defaultConfig(),
	})

	task := &schedulerpb.SchedulerIdleTask{
		IdleTimeTotal: durationpb.New(scheduler.SentinelIdleTime),
	}
	taskAttrs := chasm.TaskAttributes{
		ScheduledTime: sentinel.Info.CreateTime.AsTime().Add(scheduler.SentinelIdleTime),
	}

	isValid, err := executor.Validate(ctx, sentinel, taskAttrs, task)
	require.NoError(t, err)
	require.False(t, isValid)
}

func TestSentinelIdleTask_Validate_MismatchedScheduledTime(t *testing.T) {
	sentinel, ctx, _ := setupSentinelForTest(t)

	executor := scheduler.NewSchedulerIdleTaskExecutor(scheduler.SchedulerIdleTaskExecutorOptions{
		Config: defaultConfig(),
	})

	task := &schedulerpb.SchedulerIdleTask{
		IdleTimeTotal: durationpb.New(scheduler.SentinelIdleTime),
	}
	taskAttrs := chasm.TaskAttributes{
		ScheduledTime: sentinel.Info.CreateTime.AsTime().Add(99 * time.Hour),
	}

	isValid, err := executor.Validate(ctx, sentinel, taskAttrs, task)
	require.NoError(t, err)
	require.False(t, isValid)
}

func TestSentinelIdleTask_Execute(t *testing.T) {
	sentinel, ctx, _ := setupSentinelForTest(t)

	executor := scheduler.NewSchedulerIdleTaskExecutor(scheduler.SchedulerIdleTaskExecutorOptions{
		Config: defaultConfig(),
	})

	require.False(t, sentinel.Closed)
	err := executor.Execute(ctx, sentinel, chasm.TaskAttributes{}, &schedulerpb.SchedulerIdleTask{})
	require.NoError(t, err)
	require.True(t, sentinel.Closed)
}
