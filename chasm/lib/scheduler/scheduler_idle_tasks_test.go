package scheduler_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type idleTasksSuite struct {
	schedulerSuite
}

func TestIdleTasksSuite(t *testing.T) {
	suite.Run(t, &idleTasksSuite{})
}

type idleValidateTestCase struct {
	configIdleTime           time.Duration
	taskIdleTimeTotal        time.Duration
	scheduledTime            time.Time
	schedulerClosed          bool
	idleMatchesScheduledTime bool
	setupScheduler           func(*scheduler.Scheduler, chasm.Context)
	expectedValid            bool
}

func (s *idleTasksSuite) TestExecute() {
	ctx := s.newMutableContext()
	sched := s.scheduler

	// Create executor with default config
	executor := scheduler.NewSchedulerIdleTaskExecutor(scheduler.SchedulerIdleTaskExecutorOptions{
		Config: defaultConfig(),
	})

	// Verify scheduler starts open
	s.False(sched.Closed)

	// Execute the idle task
	err := executor.Execute(ctx, sched, chasm.TaskAttributes{}, &schedulerpb.SchedulerIdleTask{})
	s.NoError(err)

	// Verify scheduler is now closed
	s.True(sched.Closed)
}

func (s *idleTasksSuite) TestValidate_SchedulerNotIdle() {
	now := s.timeSource.Now()
	s.runValidateTestCase(&idleValidateTestCase{
		configIdleTime:    10 * time.Minute,
		taskIdleTimeTotal: 10 * time.Minute,
		scheduledTime:     now,
		setupScheduler: func(sched *scheduler.Scheduler, ctx chasm.Context) {
			// Make scheduler not idle by setting it as paused
			sched.Schedule.State.Paused = true
		},
		expectedValid: false,
	})
}

func (s *idleTasksSuite) TestValidate_ValidIdleTask() {
	now := s.timeSource.Now()
	s.runValidateTestCase(&idleValidateTestCase{
		configIdleTime:           10 * time.Minute,
		taskIdleTimeTotal:        10 * time.Minute,
		scheduledTime:            now,
		idleMatchesScheduledTime: true,
		expectedValid:            true,
	})
}

func (s *idleTasksSuite) TestValidate_SchedulerAlreadyClosed() {
	now := s.timeSource.Now()
	s.runValidateTestCase(&idleValidateTestCase{
		configIdleTime:           10 * time.Minute,
		taskIdleTimeTotal:        10 * time.Minute,
		scheduledTime:            now,
		schedulerClosed:          true,
		idleMatchesScheduledTime: true,
		expectedValid:            false, // Should return !scheduler.Closed (false when closed)
	})
}

func (s *idleTasksSuite) runValidateTestCase(c *idleValidateTestCase) {
	ctx := s.newMutableContext()
	sched := s.scheduler

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
	s.NoError(err)
	s.Equal(c.expectedValid, isValid)
}
