package scheduler_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
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

// Manual-only schedules (empty spec) close from idle like any other: V1
// applies RetentionTime to them, and lastEventTime is advanced by manual
// triggers via recentActions, so the idle timer cannot silently kill a
// schedule that customers are actively using.
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

// A pending backfill (separate task-driven component) must drain before close.
func TestIdleTask_Validate_HasBackfillHeldOpen(t *testing.T) {
	env := newTestEnv(t)
	now := env.TimeSource.Now()
	runIdleValidateTestCase(t, env, &idleValidateTestCase{
		configIdleTime:    10 * time.Minute,
		taskIdleTimeTotal: 10 * time.Minute,
		scheduledTime:     now,
		setupScheduler: func(sched *scheduler.Scheduler, _ chasm.Context) {
			anchorLastEventTo(sched, now.Add(-10*time.Minute))
			// hasMoreBackfills only checks length, so a zero-value Field stub
			// suffices. If that ever changes, this stub will need real state.
			sched.Backfillers = chasm.Map[string, *scheduler.Backfiller]{
				"bf-stub": chasm.Field[*scheduler.Backfiller]{},
			}
		},
		expectedValid: false,
	})
}

// If lastEventTime advanced since arm (e.g., a workflow start appended to
// recentActions), the recomputed deadline is later than ScheduledTime - the
// old task is premature, the Generator will arm a fresh task at the new time.
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

// Exact-equality between recomputed deadline and ScheduledTime must fire.
// Regression guard: the original strict-equality compare was correct here too,
// but the new `.After()` semantic must preserve this stable case.
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

// Sentinels are exempt from held-open semantics, even when their state would
// otherwise hold a real scheduler open. They exist only to reserve a schedule
// ID and must auto-close after SentinelIdleTime.
func TestIdleTask_Validate_SentinelNotHeldOpen(t *testing.T) {
	sentinel, ctx, _ := setupSentinelForTest(t)
	// Force every state that would normally hold a non-sentinel open.
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

	isValid, err := handler.Validate(ctx, sentinel, taskAttrs, task)
	require.NoError(t, err)
	require.True(t, isValid, "sentinel must remain eligible to close regardless of paused/backfill/empty-spec state")
}
