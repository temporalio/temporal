package schedulertest_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/schedulertest"
)

// TestDriver_RunsIntervalScheduleForward verifies the driver fires the
// scheduler's self-scheduled tasks and advances the clock across several
// interval ticks of a normal (running) schedule.
func TestDriver_RunsIntervalScheduleForward(t *testing.T) {
	start := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	d := schedulertest.NewDriver(t, schedulertest.WithStartTime(start))

	steps, quiescent := d.RunToQuiescence(20)

	// A normal interval schedule re-arms a generator task forever, so it never
	// quiesces; it should burn the whole step budget.
	require.False(t, quiescent, "running interval schedule should not quiesce")
	require.Equal(t, 20, steps)

	// The clock must have advanced as interval ticks fire.
	require.True(t, d.Now().After(start.Add(schedulertest.DefaultInterval)),
		"clock should advance past one interval, now=%s", d.Now())

	d.ReadScheduler(func(s *scheduler.Scheduler, _ chasm.Context) {
		require.False(t, s.Closed, "schedule should still be open")
	})
}

// TestDriver_PausedScheduleKeepsTicking documents that a paused interval
// schedule is *held open* and keeps re-arming its generator task to advance the
// high-water mark — it neither quiesces nor closes — but it never buffers a
// start.
func TestDriver_PausedScheduleKeepsTicking(t *testing.T) {
	sched := schedulertest.DefaultSchedule()
	sched.State.Paused = true

	d := schedulertest.NewDriver(t, schedulertest.WithSchedule(sched))

	_, quiescent := d.RunToQuiescence(10)
	require.False(t, quiescent, "paused interval schedule keeps ticking, not quiescent")

	d.ReadScheduler(func(s *scheduler.Scheduler, _ chasm.Context) {
		require.True(t, s.Schedule.State.Paused)
		require.False(t, s.Closed, "paused schedule is held open, not closed")
	})
}

// TestDriver_LimitedActionsExhaustedClosesAndQuiesces verifies that a schedule
// with no remaining actions arms an idle task instead of generating work, and
// after the idle task fires the schedule closes and the driver reaches
// quiescence.
func TestDriver_LimitedActionsExhaustedClosesAndQuiesces(t *testing.T) {
	sched := schedulertest.DefaultSchedule()
	sched.State.LimitedActions = true
	sched.State.RemainingActions = 0

	d := schedulertest.NewDriver(t, schedulertest.WithSchedule(sched))

	_, quiescent := d.RunToQuiescence(10)
	require.True(t, quiescent, "exhausted schedule should close and quiesce")

	d.ReadScheduler(func(s *scheduler.Scheduler, _ chasm.Context) {
		require.True(t, s.Closed, "schedule should be closed after idle task fires")
	})
}
