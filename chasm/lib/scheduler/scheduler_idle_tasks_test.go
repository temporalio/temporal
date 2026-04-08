package scheduler_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/service/history/tasks"
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

	handler := scheduler.NewSchedulerIdleTaskHandler(scheduler.SchedulerIdleTaskHandlerOptions{
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

	isValid, err := handler.Validate(ctx, sched, taskAttrs, task)
	require.NoError(t, err)
	require.Equal(t, c.expectedValid, isValid)
}

func TestIdleTask_Execute(t *testing.T) {
	env := newTestEnv(t)
	ctx := env.MutableContext()
	sched := env.Scheduler

	handler := scheduler.NewSchedulerIdleTaskHandler(scheduler.SchedulerIdleTaskHandlerOptions{
		Config: defaultConfig(),
	})

	// Verify scheduler starts open.
	require.False(t, sched.Closed)

	// Execute the idle task.
	err := handler.Execute(ctx, sched, chasm.TaskAttributes{}, &schedulerpb.SchedulerIdleTask{})
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

func TestPatch_UnpauseResetsRetentionTimer(t *testing.T) {
	env := newTestEnv(t)
	ctx := env.MutableContext()
	sched := env.Scheduler

	sched.Schedule.State.Paused = true

	idleTime := scheduler.DefaultTweakables.IdleTime
	env.TimeSource.Update(env.TimeSource.Now().Add(idleTime * 2))

	env.NodeBackend.TasksByCategory = nil

	_, err := sched.Patch(ctx, &schedulerpb.PatchScheduleRequest{
		FrontendRequest: &workflowservice.PatchScheduleRequest{
			Patch: &schedulepb.SchedulePatch{
				Unpause: "resuming after long pause",
			},
		},
	})
	require.NoError(t, err)
	require.NoError(t, env.CloseTransaction())

	require.True(t, env.HasTask(&tasks.ChasmTask{}, chasm.TaskScheduledTimeImmediate),
		"generator should be kicked on unpause to reset idle expiration")
}
