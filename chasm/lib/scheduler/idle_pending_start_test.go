package scheduler_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/clock"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const idleWeek = 7 * 24 * time.Hour

var (
	pendingCreateTime  = time.Date(2026, 1, 5, 10, 0, 0, 500000000, time.UTC)
	pendingUnpauseTime = pendingCreateTime.Add(1265 * time.Millisecond)
)

func TestPendingStartIdleFix(t *testing.T) {
	t.Parallel()
	fire := pendingCreateTime.Add(idleWeek + time.Hour)
	for _, tc := range []struct {
		name                 string
		enabled              bool
		lastEventTime        time.Time
		expectedDeadline     time.Time
		expectedOldTaskValid bool
	}{
		{
			name: "enabled", enabled: true, lastEventTime: pendingCreateTime,
			expectedDeadline: fire.Add(idleWeek), expectedOldTaskValid: false,
		},
		{
			name: "disabled", enabled: false, lastEventTime: pendingCreateTime,
			expectedDeadline: pendingCreateTime.Add(idleWeek), expectedOldTaskValid: true,
		},
		{
			name: "disabled after watermark persisted", enabled: false, lastEventTime: fire,
			expectedDeadline: fire.Add(idleWeek), expectedOldTaskValid: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := newTestEnv(t)
			tweakables := scheduler.DefaultTweakables
			tweakables.EnablePendingStartIdleFix = tc.enabled
			ctx := scheduler.ContextWithTweakables(env.ReadContext(), tweakables)
			sched := env.Scheduler
			anchorLastEventTo(sched, pendingCreateTime)
			sched.LastEventTime = timestamppb.New(tc.lastEventTime)
			sched.Invoker.Get(ctx).BufferedStarts = []*schedulespb.BufferedStart{{
				ActualTime: timestamppb.New(fire),
			}}

			require.Equal(t, tc.expectedDeadline, sched.IdleDeadline(ctx, idleWeek))
			valid, err := newIdleHandler(idleWeek).Validate(ctx, sched, chasm.TaskInvocation{
				TaskAttributes: chasm.TaskAttributes{ScheduledTime: pendingCreateTime.Add(idleWeek)},
			}, &schedulerpb.SchedulerIdleTask{IdleTimeTotal: durationpb.New(idleWeek)})
			require.NoError(t, err)
			require.Equal(t, tc.expectedOldTaskValid, valid)
		})
	}
}

func oneShotSpec(fire time.Time) *schedulepb.ScheduleSpec {
	r := func(v int) []*schedulepb.Range { return []*schedulepb.Range{{Start: int32(v)}} }
	return &schedulepb.ScheduleSpec{
		StructuredCalendar: []*schedulepb.StructuredCalendarSpec{
			// Keep-alive entry past EndTime, so it never fires.
			{
				Second: r(59), Minute: r(59), Hour: r(23),
				DayOfMonth: r(31), Month: r(12), Year: r(2100),
				DayOfWeek: []*schedulepb.Range{{End: 6, Step: 1}},
			},
			{
				Second: r(fire.Second()), Minute: r(fire.Minute()), Hour: r(fire.Hour()),
				DayOfMonth: r(fire.Day()), Month: r(int(fire.Month())), Year: r(fire.Year()),
				DayOfWeek: []*schedulepb.Range{{End: 6}},
			},
		},
		StartTime:    timestamppb.New(time.Unix(0, 0)),
		EndTime:      timestamppb.New(time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)),
		TimezoneName: "Etc/GMT",
	}
}

// intervalSpecFiringAt returns an interval spec phased so that it fires at fire.
func intervalSpecFiringAt(every time.Duration, fire time.Time, end time.Time) *schedulepb.ScheduleSpec {
	phase := time.Duration(fire.Unix()%int64(every/time.Second)) * time.Second
	spec := &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{
		Interval: durationpb.New(every),
		Phase:    durationpb.New(phase),
	}}}
	if !end.IsZero() {
		spec.EndTime = timestamppb.New(end)
	}
	return spec
}

// newPausedThenUnpaused creates the schedule paused, then unpauses it ~1s later
// via UpdateSchedule.
func newPausedThenUnpaused(t *testing.T, spec *schedulepb.ScheduleSpec, limited int64) (*schedulerTestEngine, *clock.EventTimeSource) {
	t.Helper()
	ts := clock.NewEventTimeSource()
	ts.Update(pendingCreateTime)
	build := func(paused bool) *schedulepb.Schedule {
		s := defaultSchedule()
		s.Spec = spec
		s.Policies = &schedulepb.SchedulePolicies{}
		s.State = &schedulepb.ScheduleState{
			Paused:           paused,
			LimitedActions:   limited > 0,
			RemainingActions: limited,
		}
		return s
	}
	e := newSchedulerTestEngine(t, build(true), withEngineTimeSource(ts))

	ts.Update(pendingUnpauseTime)
	_, err := scheduler.NewTestHandler(e.logger).UpdateSchedule(e.engineCtx, &schedulerpb.UpdateScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.UpdateScheduleRequest{
			Namespace:  namespace,
			ScheduleId: scheduleID,
			Schedule:   build(false),
			RequestId:  "unpause",
		},
	})
	require.NoError(t, err)
	return e, ts
}

func firePureTasksAt(t *testing.T, e *schedulerTestEngine, ts *clock.EventTimeSource, at time.Time) {
	t.Helper()
	ts.Update(at)
	_, err := e.engine.FirePureTasks(e.rootRef, at)
	require.NoError(t, err)
}

// recordPendingStart simulates InvokerExecuteTask recording the single
// unstarted buffered start as started at the given time.
func recordPendingStart(t *testing.T, e *schedulerTestEngine, ts *clock.EventTimeSource, at time.Time) {
	t.Helper()
	ts.Update(at)
	require.NoError(t, e.updateScheduler(func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
		inv := s.Invoker.Get(ctx)
		var pending *schedulespb.BufferedStart
		for _, b := range inv.GetBufferedStarts() {
			if b.GetRunId() == "" {
				require.Nil(t, pending, "expected a single pending start")
				pending = b
			}
		}
		require.NotNil(t, pending)
		inv.RecordExecuteResult(ctx, []*schedulespb.BufferedStart{{
			RequestId:     pending.GetRequestId(),
			WorkflowId:    pending.GetWorkflowId(),
			RunId:         "run-" + pending.GetRequestId(),
			StartTime:     timestamppb.New(at),
			OverlapPolicy: pending.GetOverlapPolicy(),
		}}, nil)
		return nil
	}))
}

func requireSchedulerState(t *testing.T, e *schedulerTestEngine, check func(*scheduler.Scheduler, chasm.Context)) {
	t.Helper()
	require.NoError(t, e.readScheduler(func(s *scheduler.Scheduler, ctx chasm.Context) error {
		check(s, ctx)
		return nil
	}))
}

// The tick that buffers a schedule's final action must not arm the idle task
// before that action is executed. Drives the full
// lifecycle so a fix that holds the schedule open also proves it still closes.
func TestIdle_FinalBufferedStartExecutesBeforeClose(t *testing.T) {
	oneShotFire := pendingUnpauseTime.Add(idleWeek + time.Hour).Truncate(time.Second)
	intervalFire := pendingUnpauseTime.Add(8 * 24 * time.Hour).Truncate(time.Second)
	const tenDays = 10 * 24 * time.Hour

	cases := []struct {
		name    string
		spec    *schedulepb.ScheduleSpec
		limited int64
		fire    time.Time
	}{
		{"one-shot 7d+1h after unpause", oneShotSpec(oneShotFire), 0, oneShotFire},
		{"one-shot 7d+1m after unpause", oneShotSpec(pendingUnpauseTime.Add(idleWeek + time.Minute).Truncate(time.Second)), 0,
			pendingUnpauseTime.Add(idleWeek + time.Minute).Truncate(time.Second)},
		{"one-shot 6d after unpause", oneShotSpec(pendingUnpauseTime.Add(6 * 24 * time.Hour).Truncate(time.Second)), 0,
			pendingUnpauseTime.Add(6 * 24 * time.Hour).Truncate(time.Second)},
		{"limited actions=1, 10d interval", intervalSpecFiringAt(tenDays, intervalFire, time.Time{}), 1, intervalFire},
		{"10d interval, EndTime before 2nd fire", intervalSpecFiringAt(tenDays, intervalFire, intervalFire.Add(24*time.Hour)), 0, intervalFire},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e, ts := newPausedThenUnpaused(t, tc.spec, tc.limited)

			// Generator tick; processBuffer cascades. InvokerExecuteTask is a
			// side-effect task and has not run yet.
			firePureTasksAt(t, e, ts, tc.fire.Add(10*time.Millisecond))
			firePureTasksAt(t, e, ts, ts.Now())
			requireSchedulerState(t, e, func(s *scheduler.Scheduler, ctx chasm.Context) {
				require.False(t, s.Closed, "closed with the final action still buffered")
				starts := s.Invoker.Get(ctx).GetBufferedStarts()
				require.Len(t, starts, 1)
				require.Empty(t, starts[0].GetRunId())
				require.Equal(t, tc.fire.UTC(), starts[0].GetActualTime().AsTime().UTC())
				if s.IdleCloseTime != nil {
					require.False(t, s.IdleCloseTime.AsTime().Before(tc.fire.Add(idleWeek)),
						"idle deadline must not precede the pending start's time + IdleTime")
				}
			})

			startTime := tc.fire.Add(50 * time.Millisecond)
			// Recording the start calls Generate, which re-arms idle.
			recordPendingStart(t, e, ts, startTime)

			deadline := startTime.Add(idleWeek)
			requireSchedulerState(t, e, func(s *scheduler.Scheduler, _ chasm.Context) {
				require.False(t, s.Closed)
				require.Equal(t, deadline.UTC(), s.GetIdleCloseTime().AsTime().UTC())
			})

			firePureTasksAt(t, e, ts, deadline.Add(-time.Second))
			requireSchedulerState(t, e, func(s *scheduler.Scheduler, _ chasm.Context) {
				require.False(t, s.Closed)
			})
			firePureTasksAt(t, e, ts, deadline)
			requireSchedulerState(t, e, func(s *scheduler.Scheduler, _ chasm.Context) {
				require.True(t, s.Closed, "schedule must still close once idle")
			})
		})
	}
}

// A final start removed without ever getting a RunId (non-retryable start
// failure) triggers no Generate call. The idle task armed at the generator tick
// must still close the schedule, rather than leaving it stuck open.
func TestIdle_FinalStartRemovedWithoutRunID_StillCloses(t *testing.T) {
	fire := pendingUnpauseTime.Add(idleWeek + time.Hour).Truncate(time.Second)
	e, ts := newPausedThenUnpaused(t, oneShotSpec(fire), 0)

	firePureTasksAt(t, e, ts, fire.Add(10*time.Millisecond))
	require.NoError(t, e.updateScheduler(func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
		s.Invoker.Get(ctx).BufferedStarts = nil
		return nil
	}))

	deadline := fire.Add(idleWeek)
	requireSchedulerState(t, e, func(s *scheduler.Scheduler, _ chasm.Context) {
		require.False(t, s.Closed)
		require.Equal(t, deadline.UTC(), s.GetIdleCloseTime().AsTime().UTC())
	})
	firePureTasksAt(t, e, ts, deadline)
	requireSchedulerState(t, e, func(s *scheduler.Scheduler, _ chasm.Context) {
		require.True(t, s.Closed)
	})
}

// An idle task armed in the past by a pre-fix binary must be invalidated by the
// new binary while the final start is still pending.
func TestIdle_StaleIdleTaskInvalidatedByPendingStart(t *testing.T) {
	fire := pendingUnpauseTime.Add(idleWeek + time.Hour).Truncate(time.Second)
	e, ts := newPausedThenUnpaused(t, oneShotSpec(fire), 0)
	firePureTasksAt(t, e, ts, fire.Add(10*time.Millisecond))

	stale := pendingUnpauseTime.Add(idleWeek)
	require.NoError(t, e.updateScheduler(func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
		ctx.AddTask(s, chasm.TaskAttributes{ScheduledTime: stale}, &schedulerpb.SchedulerIdleTask{
			IdleTimeTotal: durationpb.New(idleWeek),
		})
		return nil
	}))
	firePureTasksAt(t, e, ts, ts.Now())
	requireSchedulerState(t, e, func(s *scheduler.Scheduler, _ chasm.Context) {
		require.False(t, s.Closed, "stale idle task closed a schedule with a pending start")
	})
}

// Weekly + EndTime: before the fix the final tick armed idle at
// prevStartTime+7d = finalNominal+prevStartLatency, racing InvokerExecuteTask.
func TestIdle_WeeklyWithEndTime_FinalRunNotRaced(t *testing.T) {
	const prevStartLatency = 50 * time.Millisecond
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	firstFire := time.Unix(base.Unix()-base.Unix()%int64(idleWeek/time.Second), 0).Add(idleWeek).UTC()
	finalFire := firstFire.Add(idleWeek)

	ts := clock.NewEventTimeSource()
	ts.Update(base)
	schedule := defaultSchedule()
	schedule.Policies = &schedulepb.SchedulePolicies{}
	schedule.Spec = &schedulepb.ScheduleSpec{
		Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(idleWeek)}},
		EndTime:  timestamppb.New(finalFire.Add(time.Hour)),
	}
	e := newSchedulerTestEngine(t, schedule, withEngineTimeSource(ts))

	firePureTasksAt(t, e, ts, firstFire)
	recordPendingStart(t, e, ts, firstFire.Add(prevStartLatency))
	// Complete it, so SKIP overlap doesn't drop the final run.
	require.NoError(t, e.updateScheduler(func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
		s.Invoker.Get(ctx).GetBufferedStarts()[0].Completed = &schedulespb.CompletedResult{
			Status:    enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			CloseTime: timestamppb.New(firstFire.Add(time.Minute)),
		}
		return nil
	}))

	firePureTasksAt(t, e, ts, finalFire)
	// Execute is slower than the previous start; the old race deadline passes.
	firePureTasksAt(t, e, ts, finalFire.Add(2*prevStartLatency))
	requireSchedulerState(t, e, func(s *scheduler.Scheduler, _ chasm.Context) {
		require.False(t, s.Closed, "final run dropped by idle race")
	})
	recordPendingStart(t, e, ts, ts.Now())
	requireSchedulerState(t, e, func(s *scheduler.Scheduler, _ chasm.Context) {
		require.Equal(t, ts.Now().Add(idleWeek).UTC(), s.GetIdleCloseTime().AsTime().UTC())
	})
}
