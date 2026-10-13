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

// bufferedStartSeed describes a completed action to seed into the Invoker.
type bufferedStartSeed struct {
	requestID string
	startTime time.Time
	closeTime time.Time
}

// seedCompletedStarts replaces the Invoker's buffer with completed starts built
// from seeds. Every start carries a RunId, since recentActions() skips starts
// that never started.
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

// burstSeeds reproduces the production shape: `count` actions that all started
// within the same instant, plus one that started marginally later but closed
// first, so retention evicts precisely the start holding the maximum StartTime.
//
// Returned in close-time order, oldest first, which is the order
// applyCompletedRetention evicts in.
func burstSeeds(base time.Time, count int, skew time.Duration) []bufferedStartSeed {
	seeds := make([]bufferedStartSeed, 0, count)
	// The straggler: latest StartTime, oldest CloseTime -> first to be evicted.
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

// countIdlePureTasks counts logical SchedulerIdleTask entries persisted on the
// root component. This is the number that grew to 43 in production; the physical
// timer count hides it, because CHASM materializes only the first pure task of
// each (component, scheduled time) group.
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

// idlePureTaskTimes returns the scheduled times of every persisted idle task, in
// persisted order (which CHASM keeps sorted by scheduled time).
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

// runGeneratorTick executes one GeneratorTask in its own transaction, mirroring
// the production tick that HandleNexusCompletion triggers via Generate().
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

// -----------------------------------------------------------------------------
// Root cause: the recomputed last-event time is not monotonic.
// -----------------------------------------------------------------------------

// Pins the underlying defect independently of the idle machinery: evicting the
// start that held the largest StartTime drags the *recomputed* last-event time
// backwards, while the high-water-mark read does not.
func TestScheduler_LastEventTime_RegressesOnRetentionEviction(t *testing.T) {
	env := newTestEnv(t)
	ctx := env.MutableContext()
	sched := env.Scheduler

	// Actions start after the schedule was created, so they - not Info - set the
	// last-event time. newTestEnv's initial CloseTransaction already ran a
	// Generator tick, so the mark is non-nil and sits at creation time.
	base := env.TimeSource.Now().Add(time.Hour)
	const skew = 1500 * time.Microsecond
	// One more completed start than retention keeps, so exactly one is evicted.
	seeds := burstSeeds(base, scheduler.RecentActionCount+1, skew)
	seedCompletedStarts(t, sched, ctx, seeds)

	before := sched.ComputeLastEventTime(ctx)
	require.Equal(t, base.Add(skew).UTC(), before.UTC(),
		"the straggler's StartTime should set the pre-eviction value")

	// Publish the high water mark, exactly as the Generator tick does.
	require.Equal(t, before.UTC(), sched.AdvanceLastEventTime(ctx).UTC())

	sched.Invoker.Get(ctx).ApplyCompletedRetention()
	require.Len(t, sched.Invoker.Get(ctx).BufferedStarts, scheduler.RecentActionCount)

	after := sched.ComputeLastEventTime(ctx)
	require.True(t, after.Before(before),
		"recomputed last-event time must be shown regressing: before=%v after=%v", before, after)
	require.Equal(t, skew, before.Sub(after),
		"the regression should be exactly the evicted start's skew")

	// The monotonic read path is what callers use, and it must hold the line.
	require.Equal(t, before.UTC(), sched.GetLastEventTimeFloored(ctx).UTC(),
		"high water mark must floor the regressed recompute")
	require.Equal(t, before.Add(idleTestIdleTime).UTC(),
		sched.IdleDeadline(ctx, idleTestIdleTime).UTC(),
		"idle deadline must not regress")
}

// A schedule persisted before LastEventTime existed has the field unset. The
// read path must then behave exactly as it did before, so the rollout is inert
// until the first Generator tick writes the mark. Rollback safety depends on
// this being symmetric.
func TestScheduler_LastEventTime_UnsetFallsBackToRecompute(t *testing.T) {
	env := newTestEnv(t)
	ctx := env.MutableContext()
	sched := env.Scheduler

	base := env.TimeSource.Now().Add(time.Hour)
	seedCompletedStarts(t, sched, ctx, burstSeeds(base, scheduler.RecentActionCount+1, 1500*time.Microsecond))

	// Simulate a schedule persisted before LastEventTime existed. newTestEnv's
	// initial Generator tick writes the mark, so clear it explicitly rather than
	// relying on it being absent.
	sched.LastEventTime = nil
	require.Nil(t, sched.LastEventTime, "field must start unset")
	require.Equal(t, sched.ComputeLastEventTime(ctx).UTC(), sched.GetLastEventTimeFloored(ctx).UTC())

	// And with the mark unset, eviction still regresses - i.e. this test is
	// asserting the old behaviour, not an accidental pass.
	before := sched.ComputeLastEventTime(ctx)
	sched.Invoker.Get(ctx).ApplyCompletedRetention()
	require.True(t, sched.GetLastEventTimeFloored(ctx).Before(before))
}

// The mark must never move backwards even if asked to.
func TestScheduler_AdvanceLastEventTime_NeverRegresses(t *testing.T) {
	env := newTestEnv(t)
	ctx := env.MutableContext()
	sched := env.Scheduler

	base := env.TimeSource.Now().Add(time.Hour)
	seedCompletedStarts(t, sched, ctx, burstSeeds(base, scheduler.RecentActionCount+1, time.Minute))

	high := sched.AdvanceLastEventTime(ctx)

	// Drop every action; the recomputed value collapses back to Info's anchor.
	sched.Invoker.Get(ctx).BufferedStarts = nil
	require.True(t, sched.ComputeLastEventTime(ctx).Before(high))
	require.Equal(t, high.UTC(), sched.AdvanceLastEventTime(ctx).UTC())
	require.Equal(t, high.UTC(), sched.LastEventTime.AsTime().UTC())
}

// -----------------------------------------------------------------------------
// Accumulation: the Generator must not re-arm an idle task it already armed.
// -----------------------------------------------------------------------------

// Repeated ticks with nothing changing must leave exactly one idle task. Before
// the fix this grows by one per tick, because Validate can only invalidate tasks
// whose deadline moved later and a no-op tick moves nothing.
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

// The production shape end to end: a burst of same-instant actions, then
// retention evicting the straggler that held the maximum StartTime. Asserts both
// symptoms are gone - a single idle task, and a deadline that never regresses.
func TestGeneratorTask_IdleTask_DoesNotAccumulateAcrossActionBurst(t *testing.T) {
	now := time.Now()
	env := newTestEnv(t, withSchedule(expiredSchedule(now)))
	handler := newGeneratorHandler(env)

	base := env.TimeSource.Now().Add(time.Hour)
	const skew = 1500 * time.Microsecond
	// 43 actions, matching the production backfill that surfaced this.
	seeds := burstSeeds(base, 43, skew)

	var deadlines []time.Time
	for i := range seeds {
		ctx := env.MutableContext()
		sched, err := env.Node.Component(ctx, chasm.ComponentRef{})
		require.NoError(t, err)
		typed := sched.(*scheduler.Scheduler)

		// Actions complete one at a time, each one growing the completed set and
		// then being trimmed to the retention window - the same sequence
		// HandleNexusCompletion -> recordCompletedAction drives in production.
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

// The skip must be observable, so an operator can tell "no task armed because
// one already is" apart from "no task armed at all".
func TestGeneratorTask_IdleTask_SkipEmitsMetric(t *testing.T) {
	rec := metricstest.NewCaptureHandler()
	env := newTestEnv(t, withSchedule(expiredSchedule(time.Now())))
	handler := newGeneratorHandler(env, withGeneratorMetrics(rec))

	runGeneratorTick(t, env, handler) // arms
	capture := rec.StartCapture()
	defer rec.StopCapture(capture)
	runGeneratorTick(t, env, handler) // skips

	recorded := capture.Snapshot()[metrics.ScheduleIdleTask.Name()]
	require.Len(t, recorded, 1)
	require.Equal(t, "skipped", recorded[0].Tags["outcome"])
	require.Equal(t, "already_armed", recorded[0].Tags["reason"])
}

// -----------------------------------------------------------------------------
// Stuck-open safety. Skipping the re-arm is only sound if every path that moves
// the deadline still arms a fresh task; otherwise a schedule never closes.
// -----------------------------------------------------------------------------

// When the deadline genuinely advances, a new task must be armed and the stale
// one must not survive alongside it.
func TestGeneratorTask_IdleTask_ReArmedWhenDeadlineAdvances(t *testing.T) {
	env := newTestEnv(t, withSchedule(expiredSchedule(time.Now())))
	env.AllowStuck("test commits a direct state mutation before running the generator that re-arms the idle task")
	handler := newGeneratorHandler(env)

	runGeneratorTick(t, env, handler)
	require.Equal(t, 1, countIdlePureTasks(t, env))
	first := env.Scheduler.IdleCloseTime.AsTime()

	// A real event (here: an update) advances the deadline.
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

// Pausing clears IdleCloseTime, so unpausing must arm again. This is the path
// where a bad skip guard would strand a schedule open forever.
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

// A live schedule must not arm an idle task at all - the guard must not turn the
// non-idle branch into a skip.
func TestGeneratorTask_IdleTask_NotArmedWhileSpecHasWork(t *testing.T) {
	env := newTestEnv(t)
	handler := newGeneratorHandler(env)

	runGeneratorTick(t, env, handler)

	require.Equal(t, 0, countIdlePureTasks(t, env))
	require.Nil(t, env.Scheduler.IdleCloseTime)
}

// -----------------------------------------------------------------------------
// Validate: the regressed-deadline branch.
// -----------------------------------------------------------------------------

// With the high water mark published, a retention eviction can no longer push
// Validate into its "deadline regressed" branch, so the task stays valid and
// fires at the deadline the customer's retention window earned.
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

// Guard against the snapshot helper silently returning nothing: if the root node
// stopped carrying component attributes, or the idle task's registered FQN
// changed, every count-based assertion above would pass vacuously.
func TestCountIdlePureTasks_SeesArmedTask(t *testing.T) {
	// newTestEnv's initial CloseTransaction runs the Generator's immediate task,
	// which already arms one for an exhausted spec.
	env := newTestEnv(t, withSchedule(expiredSchedule(time.Now())))
	require.Equal(t, 1, countIdlePureTasks(t, env))
	require.Len(t, idlePureTaskTimes(t, env), 1)

	runGeneratorTick(t, env, newGeneratorHandler(env))
	require.Equal(t, 1, countIdlePureTasks(t, env),
		"a tick that changes nothing must neither add nor drop the armed task")
}
