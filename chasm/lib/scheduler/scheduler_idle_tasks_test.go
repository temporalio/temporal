package scheduler_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/clock"
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

	isValid, err := handler.Validate(ctx, sched, chasm.TaskInvocation{TaskAttributes: taskAttrs}, task)
	require.NoError(t, err)
	require.Equal(t, c.expectedValid, isValid)
}

// Pair with scheduledTime = anchor + idleTime so Validate sees a stable deadline.
func anchorLastEventTo(sched *scheduler.Scheduler, anchor time.Time) {
	sched.Info.UpdateTime = timestamppb.New(anchor)
	sched.Info.CreateTime = timestamppb.New(anchor)
	sched.LastEventTime = timestamppb.New(anchor)
}

func TestIdleTask_Execute(t *testing.T) {
	env := newTestEnv(t)
	ctx := env.MutableContext()
	sched := env.Scheduler
	eventLog := sched.EventLog.Get(ctx)

	handler := newIdleHandler(10 * time.Minute)
	require.False(t, sched.Closed)
	err := handler.Execute(ctx, sched, chasm.TaskAttributes{}, &schedulerpb.SchedulerIdleTask{})
	require.NoError(t, err)
	require.True(t, sched.Closed)
	require.Len(t, eventLog.Events, 1)
	require.Equal(t, "schedule closed from idle timer", eventLog.Events[0].Message)
}

func TestIdleTask_ExecuteInitializesEventLogMissingFromOlderTree(t *testing.T) {
	env := newTestEnv(t)
	ctx := env.MutableContext()
	sched := env.Scheduler
	sched.EventLog = chasm.NewEmptyField[*scheduler.EventLog]()
	_, ok := sched.EventLog.TryGet(ctx)
	require.False(t, ok)

	handler := newIdleHandler(10 * time.Minute)
	err := handler.Execute(ctx, sched, chasm.TaskAttributes{}, &schedulerpb.SchedulerIdleTask{})
	require.NoError(t, err)
	require.True(t, sched.Closed)

	eventLog := sched.EventLog.Get(ctx)
	require.Len(t, eventLog.Events, 1)
	require.Equal(t, "schedule closed from idle timer", eventLog.Events[0].Message)
}

func TestIdleTask_AllowAllStartRearmsIdleTimer(t *testing.T) {
	base := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(base)

	schedule := defaultSchedule()
	schedule.Spec = &schedulepb.ScheduleSpec{}
	testEngine := newSchedulerTestEngine(t, schedule, withEngineTimeSource(timeSource))

	_, err := testEngine.engine.FirePureTasks(testEngine.rootRef, base)
	require.NoError(t, err)
	oldIdleDeadline := base.Add(scheduler.DefaultTweakables.IdleTime)
	require.NoError(t, testEngine.readScheduler(func(s *scheduler.Scheduler, _ chasm.Context) error {
		require.Equal(t, oldIdleDeadline, s.IdleCloseTime.AsTime())
		return nil
	}))

	startTime := base.Add(time.Minute)
	timeSource.Update(startTime)
	require.NoError(t, testEngine.updateScheduler(func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
		invoker := s.Invoker.Get(ctx)
		invoker.BufferedStarts = append(invoker.BufferedStarts, &schedulespb.BufferedStart{
			RequestId:     "allow-all-request",
			WorkflowId:    "allow-all-workflow",
			NominalTime:   timestamppb.New(startTime),
			ActualTime:    timestamppb.New(startTime),
			DesiredTime:   timestamppb.New(startTime),
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
			Attempt:       1,
		})
		_, _, startOnlyActions := invoker.RecordExecuteResult(ctx, []*schedulespb.BufferedStart{{
			RequestId:     "allow-all-request",
			RunId:         "allow-all-run",
			StartTime:     timestamppb.New(startTime),
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		}}, nil)
		s.RecordStartOnlyActions(ctx, startOnlyActions)
		return nil
	}))

	_, err = testEngine.engine.FirePureTasks(testEngine.rootRef, oldIdleDeadline)
	require.NoError(t, err)

	newIdleDeadline := startTime.Add(scheduler.DefaultTweakables.IdleTime)
	timeSource.Update(newIdleDeadline)
	_, err = testEngine.engine.FirePureTasks(testEngine.rootRef, newIdleDeadline)
	require.NoError(t, err)
	require.NoError(t, testEngine.readScheduler(func(s *scheduler.Scheduler, _ chasm.Context) error {
		require.True(t, s.Closed)
		return nil
	}))
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

// Pins the reason tag values so a rename of the constants surfaces in tests.
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

			isValid, err := handler.Validate(env.MutableContext(), env.Scheduler, chasm.TaskInvocation{TaskAttributes: taskAttrs},
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

// Manual triggers advance lastEventTime via recentActions, so this can't close an actively used schedule.
func TestIdleTask_Validate_ManualOnlyClosesFromIdle(t *testing.T) {
	env := newTestEnv(t)
	now := env.TimeSource.Now()
	runIdleValidateTestCase(t, env, &idleValidateTestCase{
		configIdleTime:    10 * time.Minute,
		taskIdleTimeTotal: 10 * time.Minute,
		scheduledTime:     now,
		setupScheduler: func(sched *scheduler.Scheduler, _ chasm.Context) {
			anchorLastEventTo(sched, now.Add(-10*time.Minute))
			sched.Schedule.Spec = &schedulepb.ScheduleSpec{}
		},
		expectedValid: true,
	})
}

func TestIdleTask_Validate_HasBackfillHeldOpen(t *testing.T) {
	env := newTestEnv(t)
	now := env.TimeSource.Now()
	runIdleValidateTestCase(t, env, &idleValidateTestCase{
		configIdleTime:    10 * time.Minute,
		taskIdleTimeTotal: 10 * time.Minute,
		scheduledTime:     now,
		setupScheduler: func(sched *scheduler.Scheduler, _ chasm.Context) {
			anchorLastEventTo(sched, now.Add(-10*time.Minute))
			// hasMoreBackfills only checks length, so a zero-value Field stub suffices.
			sched.Backfillers = chasm.Map[string, *scheduler.Backfiller]{
				"bf-stub": chasm.Field[*scheduler.Backfiller]{},
			}
		},
		expectedValid: false,
	})
}

func TestIdleTask_Validate_ExpirationShiftedLater(t *testing.T) {
	env := newTestEnv(t)
	now := env.TimeSource.Now()
	runIdleValidateTestCase(t, env, &idleValidateTestCase{
		configIdleTime:    10 * time.Minute,
		taskIdleTimeTotal: 10 * time.Minute,
		scheduledTime:     now,
		setupScheduler: func(sched *scheduler.Scheduler, _ chasm.Context) {
			sched.Info.UpdateTime = timestamppb.New(now)
		},
		expectedValid: false,
	})
}

func TestIdleTask_Validate_ExpirationStableFires(t *testing.T) {
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

// Sentinels only reserve a schedule ID, so state that holds a real scheduler open must not hold them.
func TestIdleTask_Validate_SentinelNotHeldOpen(t *testing.T) {
	sentinel, ctx, _ := setupSentinelForTest(t)
	sentinel.Schedule = &schedulepb.Schedule{
		Spec:  &schedulepb.ScheduleSpec{},
		State: &schedulepb.ScheduleState{Paused: true},
	}
	sentinel.Backfillers = chasm.Map[string, *scheduler.Backfiller]{
		"bf-stub": chasm.Field[*scheduler.Backfiller]{},
	}

	handler := newIdleHandler(scheduler.SentinelIdleTime)
	task := &schedulerpb.SchedulerIdleTask{IdleTimeTotal: durationpb.New(scheduler.SentinelIdleTime)}
	taskAttrs := chasm.TaskAttributes{
		ScheduledTime: sentinel.Info.CreateTime.AsTime().Add(scheduler.SentinelIdleTime),
	}

	isValid, err := handler.Validate(ctx, sentinel, chasm.TaskInvocation{TaskAttributes: taskAttrs}, task)
	require.NoError(t, err)
	require.True(t, isValid, "sentinel must remain eligible to close regardless of paused/backfill/empty-spec state")
}
