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
	executor *scheduler.SchedulerIdleTaskExecutor
}

func TestIdleTasksSuite(t *testing.T) {
	suite.Run(t, &idleTasksSuite{})
}

type idleValidateTestCase struct {
	ConfigIdleTime           time.Duration
	TaskIdleTimeTotal        time.Duration
	ScheduledTime            time.Time
	SchedulerClosed          bool
	IdleMatchesScheduledTime bool
	SetupScheduler           func(*scheduler.Scheduler, chasm.Context)
	ExpectedValid            bool
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

func (s *idleTasksSuite) TestValidate_IdleTimeDecreased() {
	now := s.timeSource.Now()
	s.runValidateTestCase(&idleValidateTestCase{
		ConfigIdleTime:    5 * time.Minute,
		TaskIdleTimeTotal: 10 * time.Minute,
		ScheduledTime:     now,
		ExpectedValid:     true,
	})
}

func (s *idleTasksSuite) TestValidate_SchedulerNotIdle() {
	now := s.timeSource.Now()
	s.runValidateTestCase(&idleValidateTestCase{
		ConfigIdleTime:    10 * time.Minute,
		TaskIdleTimeTotal: 10 * time.Minute,
		ScheduledTime:     now,
		SetupScheduler: func(sched *scheduler.Scheduler, ctx chasm.Context) {
			// Make scheduler not idle by setting it as paused
			sched.Schedule.State.Paused = true
		},
		ExpectedValid: false,
	})
}

func (s *idleTasksSuite) TestValidate_IdleExpirationChanged() {
	now := s.timeSource.Now()
	s.runValidateTestCase(&idleValidateTestCase{
		ConfigIdleTime:    10 * time.Minute,
		TaskIdleTimeTotal: 10 * time.Minute,
		ScheduledTime:     now.Add(-1 * time.Minute),
		ExpectedValid:     false,
	})
}

func (s *idleTasksSuite) TestValidate_ValidIdleTask() {
	now := s.timeSource.Now()
	s.runValidateTestCase(&idleValidateTestCase{
		ConfigIdleTime:           10 * time.Minute,
		TaskIdleTimeTotal:        10 * time.Minute,
		ScheduledTime:            now,
		IdleMatchesScheduledTime: true,
		ExpectedValid:            true,
	})
}

func (s *idleTasksSuite) TestValidate_SchedulerAlreadyClosed() {
	now := s.timeSource.Now()
	s.runValidateTestCase(&idleValidateTestCase{
		ConfigIdleTime:           10 * time.Minute,
		TaskIdleTimeTotal:        10 * time.Minute,
		ScheduledTime:            now,
		SchedulerClosed:          true,
		IdleMatchesScheduledTime: true,
		ExpectedValid:            false, // Should return !scheduler.Closed (false when closed)
	})
}

func (s *idleTasksSuite) runValidateTestCase(c *idleValidateTestCase) {
	ctx := s.newMutableContext()
	sched := s.scheduler

	sched.Closed = c.SchedulerClosed

	if c.SetupScheduler != nil {
		c.SetupScheduler(sched, ctx)
	}

	config := &scheduler.Config{
		Tweakables: func(_ string) scheduler.Tweakables {
			tweakables := scheduler.DefaultTweakables
			tweakables.IdleTime = c.ConfigIdleTime
			return tweakables
		},
	}

	executor := scheduler.NewSchedulerIdleTaskExecutor(scheduler.SchedulerIdleTaskExecutorOptions{
		Config: config,
	})

	task := &schedulerpb.SchedulerIdleTask{
		IdleTimeTotal: durationpb.New(c.TaskIdleTimeTotal),
	}

	scheduledTime := c.ScheduledTime
	if c.IdleMatchesScheduledTime {
		lastEventTime := scheduledTime.Add(-c.ConfigIdleTime)
		sched.Info.UpdateTime = timestamppb.New(lastEventTime)
		sched.Info.CreateTime = timestamppb.New(lastEventTime)
	}

	taskAttrs := chasm.TaskAttributes{
		ScheduledTime: scheduledTime,
	}

	isValid, err := executor.Validate(ctx, sched, taskAttrs, task)
	s.NoError(err)
	s.Equal(c.ExpectedValid, isValid)
}
