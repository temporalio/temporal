package scheduler_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type idleValidateTestCase struct {
	configIdleTime           time.Duration
	taskIdleTimeTotal        time.Duration
	scheduledTime            time.Time
	schedulerClosed          bool
	idleMatchesScheduledTime bool
	setupScheduler           func(*scheduler.Scheduler, chasm.Context)
	expectedValid            bool
}

func runIdleValidateTestCase(t *testing.T, env *testEnv, c *idleValidateTestCase) {
	ctx := env.MutableContext()
	sched := env.Scheduler

	sched.Closed = c.schedulerClosed

	if c.setupScheduler != nil {
		c.setupScheduler(sched, ctx)
	}

	config := &scheduler.Config{
		Tweakables: func(_ string) scheduler.Tweakables {
			tweakables := scheduler.DefaultTweakables
			tweakables.IdleTime = c.configIdleTime
			return tweakables
		},
	}

	executor := scheduler.NewSchedulerIdleTaskExecutor(scheduler.SchedulerIdleTaskExecutorOptions{
		Config: config,
	})

	task := &schedulerpb.SchedulerIdleTask{
		IdleTimeTotal: durationpb.New(c.taskIdleTimeTotal),
	}

	scheduledTime := c.scheduledTime
	if c.idleMatchesScheduledTime {
		lastEventTime := scheduledTime.Add(-c.configIdleTime)
		sched.Info.UpdateTime = timestamppb.New(lastEventTime)
		sched.Info.CreateTime = timestamppb.New(lastEventTime)
	}

	taskAttrs := chasm.TaskAttributes{
		ScheduledTime: scheduledTime,
	}

	isValid, err := executor.Validate(ctx, sched, taskAttrs, task)
	require.NoError(t, err)
	require.Equal(t, c.expectedValid, isValid)
}

func TestIdleTask_Execute(t *testing.T) {
	env := newTestEnv(t)
	ctx := env.MutableContext()
	sched := env.Scheduler

	executor := scheduler.NewSchedulerIdleTaskExecutor(scheduler.SchedulerIdleTaskExecutorOptions{
		Config: defaultConfig(),
	})

	// Verify scheduler starts open.
	require.False(t, sched.Closed)

	// Execute the idle task.
	err := executor.Execute(ctx, sched, chasm.TaskAttributes{}, &schedulerpb.SchedulerIdleTask{})
	require.NoError(t, err)

	// Verify scheduler is now closed.
	require.True(t, sched.Closed)
}

func TestIdleTask_Validate_SchedulerNotIdle(t *testing.T) {
	env := newTestEnv(t)
	now := env.TimeSource.Now()
	runIdleValidateTestCase(t, env, &idleValidateTestCase{
		configIdleTime:    10 * time.Minute,
		taskIdleTimeTotal: 10 * time.Minute,
		scheduledTime:     now,
		setupScheduler: func(sched *scheduler.Scheduler, ctx chasm.Context) {
			// Make scheduler not idle by setting it as paused.
			sched.Schedule.State.Paused = true
		},
		expectedValid: false,
	})
}

func TestIdleTask_Validate_ValidIdleTask(t *testing.T) {
	env := newTestEnv(t)
	now := env.TimeSource.Now()
	runIdleValidateTestCase(t, env, &idleValidateTestCase{
		configIdleTime:           10 * time.Minute,
		taskIdleTimeTotal:        10 * time.Minute,
		scheduledTime:            now,
		idleMatchesScheduledTime: true,
		expectedValid:            true,
	})
}

func TestIdleTask_Validate_SchedulerAlreadyClosed(t *testing.T) {
	env := newTestEnv(t)
	now := env.TimeSource.Now()
	runIdleValidateTestCase(t, env, &idleValidateTestCase{
		configIdleTime:           10 * time.Minute,
		taskIdleTimeTotal:        10 * time.Minute,
		scheduledTime:            now,
		schedulerClosed:          true,
		idleMatchesScheduledTime: true,
		expectedValid:            false, // Should return !scheduler.Closed (false when closed).
	})
}
