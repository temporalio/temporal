package schedulertest_test

import (
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/schedulertest"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"pgregory.net/rapid"
)

// TestSchedulerProperties drives a randomly-generated schedule through a random
// sequence of operations (advance time, pause, unpause, trigger) and asserts the
// scheduler invariants hold after every settled transition. rapid shrinks any
// failing operation sequence to a minimal reproducer — directly useful for the
// "stuck open" bug class.
//
// The outer *testing.T owns the engine scaffolding (it requires a real
// testing.TB); invariant violations are reported through the *rapid.T so rapid
// can shrink them.
func TestSchedulerProperties(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		start := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
		// A small IdleTime lets fire-time gaps cross the idle window, which the
		// 7d default never does with these intervals.
		idle := rapid.SampledFrom([]time.Duration{10 * time.Minute, time.Hour, 2 * time.Hour}).Draw(rt, "idleTime")
		sched := drawSchedule(rt, start, idle)

		d := schedulertest.NewDriver(t,
			schedulertest.WithStartTime(start),
			schedulertest.WithSchedule(sched),
			schedulertest.WithConfig(configWith(func(tw *scheduler.Tweakables) {
				tw.IdleTime = idle
			})),
			// InvokerExecuteTask may lag the timers, and workflows may run, but
			// neither for a whole idle window. Past that, V1 and V2 both close
			// with the start still buffered.
			schedulertest.WithMaxSideEffectLag(idle/2),
			schedulertest.WithMaxRunDuration(idle/2),
		)
		hook := checkInvariantsIgnoringKnownBugs(rt)
		d.AfterStep = hook

		// Check the initial settled state too.
		hook(d)

		numOps := rapid.IntRange(1, 60).Draw(rt, "numOps")
		for range numOps {
			// Bias toward stepping so the clock advances and the schedule actually
			// progresses through generator/invoker/idle transitions. "pure" fires
			// timers without dispatching side-effect tasks, so InvokerExecuteTask
			// can lag behind the generator and idle tasks.
			op := rapid.SampledFrom([]string{
				"step", "step", "pure", "pure", "sideEffect", "complete",
				"pause", "unpause", "trigger",
			}).Draw(rt, "op")

			switch op {
			case "step":
				d.Step()
			case "pure":
				d.StepPure()
			case "sideEffect":
				d.StepSideEffect()
			case "complete":
				d.CompleteWorkflow()
			case "pause":
				_ = d.Pause()
			case "unpause":
				_ = d.Unpause()
			case "trigger":
				_ = d.TriggerImmediately()
			default:
				rt.Fatalf("unhandled op %q", op)
			}
		}
	})
}

// drawSchedule generates a random but valid schedule definition. Some
// schedules end, with their last fire time placed around the idle window.
func drawSchedule(rt *rapid.T, start time.Time, idle time.Duration) *schedulepb.Schedule {
	s := schedulertest.DefaultSchedule()

	intervalSec := rapid.SampledFrom([]int64{10, 30, 60, 300, 3600, 3 * 3600}).Draw(rt, "intervalSec")
	interval := time.Duration(intervalSec) * time.Second
	s.Spec.Interval[0].Interval = durationpb.New(interval)

	if jitterSec := rapid.SampledFrom([]int64{0, 1, 5, 30}).Draw(rt, "jitterSec"); jitterSec > 0 {
		s.Spec.Jitter = durationpb.New(time.Duration(jitterSec) * time.Second)
	}

	if catchupSec := rapid.SampledFrom([]int64{60, 300, 3600}).Draw(rt, "catchupSec"); catchupSec > 0 {
		s.Policies.CatchupWindow = durationpb.New(time.Duration(catchupSec) * time.Second)
	}

	s.Policies.OverlapPolicy = rapid.SampledFrom([]enumspb.ScheduleOverlapPolicy{
		enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
		enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE,
		enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
		enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
	}).Draw(rt, "overlapPolicy")

	switch rapid.SampledFrom([]string{"forever", "endTime", "oneShot"}).Draw(rt, "shape") {
	case "endTime":
		// Last fire lands a few intervals in, so the final gap is the interval.
		n := rapid.IntRange(1, 4).Draw(rt, "firesBeforeEnd")
		s.Spec.EndTime = timestamppb.New(start.Add(time.Duration(n)*interval + time.Second))
	case "oneShot":
		// A single fire, before, at, or after the idle window since creation.
		offset := rapid.SampledFrom([]time.Duration{
			idle / 2, idle - time.Minute, idle, idle + time.Minute, 3 * idle,
		}).Draw(rt, "oneShotOffset")
		s.Spec.Interval[0].Interval = durationpb.New(time.Hour)
		s.Spec.Interval[0].Phase = durationpb.New(offset % time.Hour)
		s.Spec.StartTime = timestamppb.New(start.Add(offset))
		s.Spec.EndTime = timestamppb.New(start.Add(offset + time.Minute))
	default:
	}

	if rapid.Bool().Draw(rt, "paused") {
		s.State.Paused = true
	}
	if rapid.Bool().Draw(rt, "limited") {
		s.State.LimitedActions = true
		s.State.RemainingActions = int64(rapid.IntRange(0, 5).Draw(rt, "remainingActions"))
	}

	return s
}

// checkInvariantsIgnoringKnownBugs is CheckInvariantsHook minus one known bug
// (TestLedger_DeferredStartsStrandedByPausedCompletion): a lost start where
// every pending start is deferred and nothing is running to re-enable them.
// Delete this once that bug is fixed.
func checkInvariantsIgnoringKnownBugs(rt *rapid.T) func(*schedulertest.Driver) {
	var prev *schedulertest.Snapshot
	return func(d *schedulertest.Driver) {
		rt.Helper()
		cur := d.Snapshot()
		for _, v := range schedulertest.CheckInvariants(prev, cur) {
			if v.Name == "ledger-lost-start" && cur.RunningWorkflows == 0 &&
				cur.DeferredStarts == len(cur.PendingStarts) {
				continue
			}
			rt.Fatalf("scheduler invariant violation at step (now=%s): %s",
				cur.Now.Format("2006-01-02T15:04:05"), v)
		}
		prev = &cur
	}
}
