package scheduler_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const idleTestIdleTime = 7 * 24 * time.Hour

type bufferedStartSeed struct {
	requestID string
	startTime time.Time
	closeTime time.Time
}

// Every start carries a RunId, since recentActions() skips starts that never started.
func seedCompletedStarts(t *testing.T, sched *scheduler.Scheduler, ctx chasm.Context, seeds []bufferedStartSeed) {
	t.Helper()
	starts := make([]*schedulespb.BufferedStart, 0, len(seeds))
	for i, seed := range seeds {
		starts = append(starts, &schedulespb.BufferedStart{
			NominalTime: timestamppb.New(seed.startTime),
			ActualTime:  timestamppb.New(seed.startTime),
			DesiredTime: timestamppb.New(seed.startTime),
			RequestId:   seed.requestID,
			WorkflowId:  fmt.Sprintf("wf-%d", i),
			RunId:       fmt.Sprintf("run-%d", i),
			StartTime:   timestamppb.New(seed.startTime),
			HasCallback: true,
			Completed: &schedulespb.CompletedResult{
				Status:    enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
				CloseTime: timestamppb.New(seed.closeTime),
			},
		})
	}
	sched.Invoker.Get(ctx).BufferedStarts = starts
}

// The straggler has the latest StartTime but the oldest CloseTime, so retention evicts it first.
func burstSeeds(base time.Time, count int, skew time.Duration) []bufferedStartSeed {
	seeds := make([]bufferedStartSeed, 0, count)
	seeds = append(seeds, bufferedStartSeed{
		requestID: "req-straggler",
		startTime: base.Add(skew),
		closeTime: base.Add(time.Hour),
	})
	for i := 1; i < count; i++ {
		seeds = append(seeds, bufferedStartSeed{
			requestID: fmt.Sprintf("req-%d", i),
			startTime: base,
			closeTime: base.Add(time.Hour + time.Duration(i)*time.Second),
		})
	}
	return seeds
}

// Counts logical tasks, since CHASM materializes only one timer per (component, scheduled time).
func countIdlePureTasks(t *testing.T, env *testEnv) int {
	t.Helper()
	idleTaskID, ok := env.Registry.TaskIDFor(&schedulerpb.SchedulerIdleTask{})
	require.True(t, ok, "idle task must be registered")

	rootPath, err := chasm.DefaultPathEncoder.Encode(nil, []string{})
	require.NoError(t, err)

	root, ok := env.Node.Snapshot(nil).Nodes[rootPath]
	require.True(t, ok, "root node must be present in snapshot")

	count := 0
	for _, task := range root.GetMetadata().GetComponentAttributes().GetPureTasks() {
		if task.GetTypeId() == idleTaskID {
			count++
		}
	}
	return count
}

func idlePureTaskTimes(t *testing.T, env *testEnv) []time.Time {
	t.Helper()
	idleTaskID, ok := env.Registry.TaskIDFor(&schedulerpb.SchedulerIdleTask{})
	require.True(t, ok)

	rootPath, err := chasm.DefaultPathEncoder.Encode(nil, []string{})
	require.NoError(t, err)
	root, ok := env.Node.Snapshot(nil).Nodes[rootPath]
	require.True(t, ok)

	var times []time.Time
	for _, task := range root.GetMetadata().GetComponentAttributes().GetPureTasks() {
		if task.GetTypeId() == idleTaskID {
			times = append(times, task.GetScheduledTime().AsTime())
		}
	}
	return times
}

func runGeneratorTick(t *testing.T, env *testEnv, handler *scheduler.GeneratorTaskHandler) {
	t.Helper()
	ctx := env.MutableContext()
	sched, err := env.Node.Component(ctx, chasm.ComponentRef{})
	require.NoError(t, err)
	typed, ok := sched.(*scheduler.Scheduler)
	require.True(t, ok)

	require.NoError(t, handler.Execute(
		ctx, typed.Generator.Get(ctx), chasm.TaskAttributes{}, &schedulerpb.GeneratorTask{}))
	require.NoError(t, env.CloseTransaction())
}

func TestScheduler_LastEventTime_RegressesOnRetentionEviction(t *testing.T) {
	env := newTestEnv(t)
	ctx := env.MutableContext()
	sched := env.Scheduler

	// Actions start after creation so they, not Info, set the last-event time.
	base := env.TimeSource.Now().Add(time.Hour)
	const skew = 1500 * time.Microsecond
	// One more than retention keeps, so exactly one is evicted.
	seeds := burstSeeds(base, scheduler.RecentActionCount+1, skew)
	seedCompletedStarts(t, sched, ctx, seeds)

	before := sched.ComputeLastEventTime(ctx)
	require.Equal(t, base.Add(skew).UTC(), before.UTC(),
		"the straggler's StartTime should set the pre-eviction value")

	require.Equal(t, before.UTC(), sched.AdvanceLastEventTime(ctx).UTC())

	sched.Invoker.Get(ctx).ApplyCompletedRetention()
	require.Len(t, sched.Invoker.Get(ctx).BufferedStarts, scheduler.RecentActionCount)

	after := sched.ComputeLastEventTime(ctx)
	require.True(t, after.Before(before),
		"recomputed last-event time must be shown regressing: before=%v after=%v", before, after)
	require.Equal(t, skew, before.Sub(after),
		"the regression should be exactly the evicted start's skew")

	require.Equal(t, before.UTC(), sched.GetLastEventTimeFloored(ctx).UTC(),
		"high water mark must floor the regressed recompute")
	require.Equal(t, before.Add(idleTestIdleTime).UTC(),
		sched.IdleDeadline(ctx, idleTestIdleTime).UTC(),
		"idle deadline must not regress")
}

// With the mark unset the read path must behave as before; rollback safety depends on it.
func TestScheduler_LastEventTime_UnsetFallsBackToRecompute(t *testing.T) {
	env := newTestEnv(t)
	ctx := env.MutableContext()
	sched := env.Scheduler

	base := env.TimeSource.Now().Add(time.Hour)
	seedCompletedStarts(t, sched, ctx, burstSeeds(base, scheduler.RecentActionCount+1, 1500*time.Microsecond))

	// newTestEnv's initial Generator tick writes the mark, so clear it explicitly.
	sched.LastEventTime = nil
	require.Nil(t, sched.LastEventTime, "field must start unset")
	require.Equal(t, sched.ComputeLastEventTime(ctx).UTC(), sched.GetLastEventTimeFloored(ctx).UTC())

	// Eviction still regresses, so the equality above is not an accidental pass.
	before := sched.ComputeLastEventTime(ctx)
	sched.Invoker.Get(ctx).ApplyCompletedRetention()
	require.True(t, sched.GetLastEventTimeFloored(ctx).Before(before))
}

func TestScheduler_AdvanceLastEventTime_NeverRegresses(t *testing.T) {
	env := newTestEnv(t)
	ctx := env.MutableContext()
	sched := env.Scheduler

	base := env.TimeSource.Now().Add(time.Hour)
	seedCompletedStarts(t, sched, ctx, burstSeeds(base, scheduler.RecentActionCount+1, time.Minute))

	high := sched.AdvanceLastEventTime(ctx)

	sched.Invoker.Get(ctx).BufferedStarts = nil
	require.True(t, sched.ComputeLastEventTime(ctx).Before(high))
	require.Equal(t, high.UTC(), sched.AdvanceLastEventTime(ctx).UTC())
	require.Equal(t, high.UTC(), sched.LastEventTime.AsTime().UTC())
}

// Validate only invalidates tasks whose deadline moved later, so a no-op tick must not re-arm.
func TestGeneratorTask_IdleTask_NotReArmedWhenDeadlineUnchanged(t *testing.T) {
	env := newTestEnv(t, withSchedule(expiredSchedule(time.Now())))
	handler := newGeneratorHandler(env)

	const ticks = 20
	for range ticks {
		runGeneratorTick(t, env, handler)
	}

	require.Equal(t, 1, countIdlePureTasks(t, env),
		"repeated idle ticks must not accumulate idle tasks")
	require.NotNil(t, env.Scheduler.IdleCloseTime, "IdleCloseTime must be published")
}

func TestGeneratorTask_IdleTask_DoesNotAccumulateAcrossActionBurst(t *testing.T) {
	now := time.Now()
	env := newTestEnv(t, withSchedule(expiredSchedule(now)))
	handler := newGeneratorHandler(env)

	base := env.TimeSource.Now().Add(time.Hour)
	const skew = 1500 * time.Microsecond
	// 43 matches the production backfill that surfaced this.
	seeds := burstSeeds(base, 43, skew)

	var deadlines []time.Time
	for i := range seeds {
		ctx := env.MutableContext()
		sched, err := env.Node.Component(ctx, chasm.ComponentRef{})
		require.NoError(t, err)
		typed := sched.(*scheduler.Scheduler)

		// Mirrors HandleNexusCompletion -> recordCompletedAction: add one completion, then trim.
		seedCompletedStarts(t, typed, ctx, seeds[:i+1])
		typed.Invoker.Get(ctx).ApplyCompletedRetention()

		require.NoError(t, handler.Execute(
			ctx, typed.Generator.Get(ctx), chasm.TaskAttributes{}, &schedulerpb.GeneratorTask{}))
		require.NoError(t, env.CloseTransaction())

		require.NotNil(t, typed.IdleCloseTime)
		deadlines = append(deadlines, typed.IdleCloseTime.AsTime())
	}

	for i := 1; i < len(deadlines); i++ {
		require.False(t, deadlines[i].Before(deadlines[i-1]),
			"idle deadline regressed at action %d: %v -> %v", i, deadlines[i-1], deadlines[i])
	}

	require.Equal(t, 1, countIdlePureTasks(t, env),
		"a burst of %d actions must leave one idle task, not one per action", len(seeds))
	require.Equal(t, []time.Time{base.Add(skew).Add(idleTestIdleTime).UTC()},
		normalizeUTC(idlePureTaskTimes(t, env)),
		"the surviving task must be armed at the high water mark, not the regressed value")
}

func normalizeUTC(times []time.Time) []time.Time {
	out := make([]time.Time, 0, len(times))
	for _, ts := range times {
		out = append(out, ts.UTC())
	}
	return out
}

func TestGeneratorTask_IdleTask_SkipEmitsMetric(t *testing.T) {
	rec := metricstest.NewCaptureHandler()
	env := newTestEnv(t, withSchedule(expiredSchedule(time.Now())))
	handler := newGeneratorHandler(env, withGeneratorMetrics(rec))

	runGeneratorTick(t, env, handler)
	capture := rec.StartCapture()
	defer rec.StopCapture(capture)
	runGeneratorTick(t, env, handler)

	recorded := capture.Snapshot()[metrics.ScheduleIdleTask.Name()]
	require.Len(t, recorded, 1)
	require.Equal(t, "skipped", recorded[0].Tags["outcome"])
	require.Equal(t, "already_armed", recorded[0].Tags["reason"])
}

func TestGeneratorTask_IdleTask_ReArmedWhenDeadlineAdvances(t *testing.T) {
	env := newTestEnv(t, withSchedule(expiredSchedule(time.Now())))
	env.AllowStuck("test commits a direct state mutation before running the generator that re-arms the idle task")
	handler := newGeneratorHandler(env)

	runGeneratorTick(t, env, handler)
	require.Equal(t, 1, countIdlePureTasks(t, env))
	first := env.Scheduler.IdleCloseTime.AsTime()

	ctx := env.MutableContext()
	sched, err := env.Node.Component(ctx, chasm.ComponentRef{})
	require.NoError(t, err)
	sched.(*scheduler.Scheduler).Info.UpdateTime = timestamppb.New(env.TimeSource.Now().Add(time.Hour))
	require.NoError(t, env.CloseTransaction())

	runGeneratorTick(t, env, handler)

	second := env.Scheduler.IdleCloseTime.AsTime()
	require.True(t, second.After(first), "deadline must advance: %v -> %v", first, second)
	require.Equal(t, 1, countIdlePureTasks(t, env),
		"the stale task must be invalidated rather than left beside the new one")
	require.Equal(t, []time.Time{second.UTC()}, normalizeUTC(idlePureTaskTimes(t, env)))
}

// Pausing clears IdleCloseTime; a bad skip guard here would strand the schedule open forever.
func TestGeneratorTask_IdleTask_ReArmedAfterPauseUnpause(t *testing.T) {
	env := newTestEnv(t, withSchedule(expiredSchedule(time.Now())))
	env.AllowStuck("test commits direct pause mutations before running the generator that reconciles idle tasks")
	handler := newGeneratorHandler(env)

	runGeneratorTick(t, env, handler)
	require.Equal(t, 1, countIdlePureTasks(t, env))

	setPaused := func(paused bool) {
		ctx := env.MutableContext()
		sched, err := env.Node.Component(ctx, chasm.ComponentRef{})
		require.NoError(t, err)
		typed := sched.(*scheduler.Scheduler)
		typed.Schedule.State.Paused = paused
		typed.Info.UpdateTime = timestamppb.New(env.TimeSource.Now())
		require.NoError(t, env.CloseTransaction())
	}

	setPaused(true)
	runGeneratorTick(t, env, handler)
	require.Nil(t, env.Scheduler.IdleCloseTime, "held-open schedules must clear IdleCloseTime")

	env.TimeSource.Update(env.TimeSource.Now().Add(time.Minute))
	setPaused(false)
	runGeneratorTick(t, env, handler)

	require.NotNil(t, env.Scheduler.IdleCloseTime, "unpausing must re-arm the idle task")
	require.Equal(t, 1, countIdlePureTasks(t, env),
		"a schedule with no armed idle task can never close, and a duplicate never drains")
}

func TestGeneratorTask_IdleTask_NotArmedWhileSpecHasWork(t *testing.T) {
	env := newTestEnv(t)
	handler := newGeneratorHandler(env)

	runGeneratorTick(t, env, handler)

	require.Equal(t, 0, countIdlePureTasks(t, env))
	require.Nil(t, env.Scheduler.IdleCloseTime)
}

func TestIdleTask_Validate_RetentionEvictionDoesNotRegressDeadline(t *testing.T) {
	env := newTestEnv(t)
	ctx := env.MutableContext()
	sched := env.Scheduler

	base := env.TimeSource.Now().Add(time.Hour)
	const skew = 1500 * time.Microsecond
	seedCompletedStarts(t, sched, ctx, burstSeeds(base, scheduler.RecentActionCount+1, skew))

	armedAt := sched.AdvanceLastEventTime(ctx).Add(idleTestIdleTime)
	sched.Invoker.Get(ctx).ApplyCompletedRetention()

	handler := newIdleHandler(idleTestIdleTime)
	valid, err := handler.Validate(
		ctx, sched,
		chasm.TaskInvocation{TaskAttributes: chasm.TaskAttributes{ScheduledTime: armedAt}},
		&schedulerpb.SchedulerIdleTask{IdleTimeTotal: durationpb.New(idleTestIdleTime)},
	)
	require.NoError(t, err)
	require.True(t, valid, "task armed at the high water mark must remain valid after eviction")
	require.Equal(t, armedAt.UTC(), sched.IdleDeadline(ctx, idleTestIdleTime).UTC(),
		"recomputed deadline must match the armed time exactly, so Validate takes neither shift branch")
}

// Guards the count-based assertions above against passing vacuously.
func TestCountIdlePureTasks_SeesArmedTask(t *testing.T) {
	// newTestEnv's initial Generator tick already arms one for an exhausted spec.
	env := newTestEnv(t, withSchedule(expiredSchedule(time.Now())))
	require.Equal(t, 1, countIdlePureTasks(t, env))
	require.Len(t, idlePureTaskTimes(t, env), 1)

	runGeneratorTick(t, env, newGeneratorHandler(env))
	require.Equal(t, 1, countIdlePureTasks(t, env),
		"a tick that changes nothing must neither add nor drop the armed task")
}
