package scheduler_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func newIdleHandler(idleTime time.Duration) *scheduler.SchedulerIdleTaskHandler {
	return scheduler.NewSchedulerIdleTaskHandler(scheduler.SchedulerIdleTaskHandlerOptions{
		Config: &scheduler.Config{
			Tweakables: func(_ string) scheduler.Tweakables {
				t := scheduler.DefaultTweakables
				t.IdleTime = idleTime
				return t
			},
		},
		MetricsHandler: metrics.NoopMetricsHandler,
		BaseLogger:     log.NewTestLogger(),
	})
}

type idleValidateTestCase struct {
	configIdleTime    time.Duration
	taskIdleTimeTotal time.Duration
	scheduledTime     time.Time
	schedulerClosed   bool
	setupScheduler    func(*scheduler.Scheduler, chasm.Context)
	expectedValid     bool
}

func runIdleValidateTestCase(t *testing.T, env *testEnv, c *idleValidateTestCase) {
	ctx := env.MutableContext()
	sched := env.Scheduler

	sched.Closed = c.schedulerClosed
	if c.setupScheduler != nil {
		c.setupScheduler(sched, ctx)
	}

	handler := newIdleHandler(c.configIdleTime)
	task := &schedulerpb.SchedulerIdleTask{IdleTimeTotal: durationpb.New(c.taskIdleTimeTotal)}
	taskAttrs := chasm.TaskAttributes{ScheduledTime: c.scheduledTime}

	isValid, err := handler.Validate(ctx, sched, taskAttrs, task)
	require.NoError(t, err)
	require.Equal(t, c.expectedValid, isValid)
}

// anchorLastEventTo backdates Info.UpdateTime/CreateTime so that
// idleDeadline = anchor + idleTime; pairs with scheduledTime = anchor + idleTime
// to make Validate's expiration check resolve to "stable".
func anchorLastEventTo(sched *scheduler.Scheduler, anchor time.Time) {
	sched.Info.UpdateTime = timestamppb.New(anchor)
	sched.Info.CreateTime = timestamppb.New(anchor)
}

func TestIdleTask_Execute(t *testing.T) {
	env := newTestEnv(t)
	ctx := env.MutableContext()
	sched := env.Scheduler

	handler := newIdleHandler(10 * time.Minute)
	require.False(t, sched.Closed)
	err := handler.Execute(ctx, sched, chasm.TaskAttributes{}, &schedulerpb.SchedulerIdleTask{})
	require.NoError(t, err)
	require.True(t, sched.Closed)
}

func TestIdleTask_ExecuteInitializesMissingEventLog(t *testing.T) {
	env := newTestEnv(t)
	ctx := env.MutableContext()
	sched := env.Scheduler
	sched.EventLog = chasm.NewEmptyField[*scheduler.EventLog]()

	handler := newIdleHandler(10 * time.Minute)
	err := handler.Execute(ctx, sched, chasm.TaskAttributes{}, &schedulerpb.SchedulerIdleTask{})
	require.NoError(t, err)
	require.True(t, sched.Closed)

	eventLog := sched.EventLog.Get(ctx)
	require.Len(t, eventLog.Events, 1)
	require.Equal(t, "schedule closed from idle timer", eventLog.Events[0].Message)
}

func TestIdleTask_Validate_SchedulerNotIdle(t *testing.T) {
	env := newTestEnv(t)
	now := env.TimeSource.Now()
	runIdleValidateTestCase(t, env, &idleValidateTestCase{
		configIdleTime:    10 * time.Minute,
		taskIdleTimeTotal: 10 * time.Minute,
		scheduledTime:     now,
		setupScheduler: func(sched *scheduler.Scheduler, _ chasm.Context) {
			sched.Schedule.State.Paused = true
		},
		expectedValid: false,
	})
}

func TestIdleTask_Validate_ValidIdleTask(t *testing.T) {
	env := newTestEnv(t)
	now := env.TimeSource.Now()
	runIdleValidateTestCase(t, env, &idleValidateTestCase{
		configIdleTime:    10 * time.Minute,
		taskIdleTimeTotal: 10 * time.Minute,
		scheduledTime:     now,
		setupScheduler: func(sched *scheduler.Scheduler, _ chasm.Context) {
			anchorLastEventTo(sched, now.Add(-10*time.Minute))
		},
		expectedValid: true,
	})
}

func TestIdleTask_Validate_SchedulerAlreadyClosed(t *testing.T) {
	env := newTestEnv(t)
	now := env.TimeSource.Now()
	runIdleValidateTestCase(t, env, &idleValidateTestCase{
		configIdleTime:    10 * time.Minute,
		taskIdleTimeTotal: 10 * time.Minute,
		scheduledTime:     now,
		schedulerClosed:   true,
		setupScheduler: func(sched *scheduler.Scheduler, _ chasm.Context) {
			anchorLastEventTo(sched, now.Add(-10*time.Minute))
		},
		expectedValid: false,
	})
}

// Each Validate=false branch must emit ScheduleIdleTask{outcome=invalidated}
// with the matching reason tag; pin the reason values so a future rename of
// the constants surfaces in tests.
func TestIdleTask_Validate_MetricReasons(t *testing.T) {
	cases := []struct {
		name           string
		expectedReason string
		setup          func(sched *scheduler.Scheduler, now time.Time, taskAttrs *chasm.TaskAttributes)
	}{
		{
			name:           "closed",
			expectedReason: "closed",
			setup: func(sched *scheduler.Scheduler, _ time.Time, _ *chasm.TaskAttributes) {
				sched.Closed = true
			},
		},
		{
			name:           "held_open via paused",
			expectedReason: "held_open",
			setup: func(sched *scheduler.Scheduler, _ time.Time, _ *chasm.TaskAttributes) {
				sched.Schedule.State.Paused = true
			},
		},
		{
			name:           "expiration_shift",
			expectedReason: "expiration_shift",
			setup: func(sched *scheduler.Scheduler, now time.Time, _ *chasm.TaskAttributes) {
				sched.Info.UpdateTime = timestamppb.New(now)
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			env := newTestEnv(t)
			now := env.TimeSource.Now()
			rec := metricstest.NewCaptureHandler()
			capture := rec.StartCapture()
			defer rec.StopCapture(capture)

			handler := scheduler.NewSchedulerIdleTaskHandler(scheduler.SchedulerIdleTaskHandlerOptions{
				Config: &scheduler.Config{
					Tweakables: func(_ string) scheduler.Tweakables { return scheduler.DefaultTweakables },
				},
				MetricsHandler: rec,
				BaseLogger:     log.NewTestLogger(),
			})

			taskAttrs := chasm.TaskAttributes{ScheduledTime: now}
			c.setup(env.Scheduler, now, &taskAttrs)

			isValid, err := handler.Validate(env.MutableContext(), env.Scheduler, taskAttrs,
				&schedulerpb.SchedulerIdleTask{IdleTimeTotal: durationpb.New(10 * time.Minute)})
			require.NoError(t, err)
			require.False(t, isValid)

			recorded := capture.Snapshot()[metrics.ScheduleIdleTask.Name()]
			require.Len(t, recorded, 1, "expected exactly one idle-task counter sample")
			require.Equal(t, "invalidated", recorded[0].Tags["outcome"])
			require.Equal(t, c.expectedReason, recorded[0].Tags["reason"])
		})
	}
}
