package schedulertest_test

import (
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm/lib/scheduler/schedulertest"
	"google.golang.org/protobuf/types/known/durationpb"
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
		sched := drawSchedule(rt)

		d := schedulertest.NewDriver(t, schedulertest.WithSchedule(sched))
		hook := schedulertest.CheckInvariantsHook(rt)
		d.AfterStep = hook

		// Check the initial settled state too.
		hook(d)

		numOps := rapid.IntRange(1, 40).Draw(rt, "numOps")
		for range numOps {
			// Bias toward "step" so the clock advances and the schedule actually
			// progresses through generator/invoker/idle transitions.
			op := rapid.SampledFrom([]string{
				"step", "step", "step", "step",
				"pause", "unpause", "trigger",
			}).Draw(rt, "op")

			switch op {
			case "step":
				d.Step()
			case "pause":
				_ = d.Pause()
			case "unpause":
				_ = d.Unpause()
			case "trigger":
				_ = d.TriggerImmediately()
			}
		}
	})
}

// drawSchedule generates a random but valid schedule definition.
func drawSchedule(rt *rapid.T) *schedulepb.Schedule {
	s := schedulertest.DefaultSchedule()

	intervalSec := rapid.SampledFrom([]int64{10, 30, 60, 300, 3600}).Draw(rt, "intervalSec")
	s.Spec.Interval[0].Interval = durationpb.New(time.Duration(intervalSec) * time.Second)

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

	if rapid.Bool().Draw(rt, "paused") {
		s.State.Paused = true
	}
	if rapid.Bool().Draw(rt, "limited") {
		s.State.LimitedActions = true
		s.State.RemainingActions = int64(rapid.IntRange(0, 5).Draw(rt, "remainingActions"))
	}

	return s
}
