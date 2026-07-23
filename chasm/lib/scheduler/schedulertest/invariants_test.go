package schedulertest_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm/lib/scheduler/schedulertest"
	"google.golang.org/protobuf/types/known/durationpb"
)

// TestInvariants_HoldAcrossScenarios runs a variety of schedules forward with
// the invariant hook wired in and asserts no invariant fires. Each scenario
// exercises a different legitimate-quiescence path (running, paused, exhausted,
// end-time passed).
func TestInvariants_HoldAcrossScenarios(t *testing.T) {
	cases := []struct {
		name     string
		schedule func() *schedulepb.Schedule
		maxSteps int
	}{
		{
			name:     "running interval",
			schedule: schedulertest.DefaultSchedule,
			maxSteps: 30,
		},
		{
			name: "paused interval (held open, keeps ticking)",
			schedule: func() *schedulepb.Schedule {
				s := schedulertest.DefaultSchedule()
				s.State.Paused = true
				return s
			},
			maxSteps: 15,
		},
		{
			name: "limited actions exhausted (idles then closes)",
			schedule: func() *schedulepb.Schedule {
				s := schedulertest.DefaultSchedule()
				s.State.LimitedActions = true
				s.State.RemainingActions = 0
				return s
			},
			maxSteps: 10,
		},
		{
			name: "limited actions, a few then idle",
			schedule: func() *schedulepb.Schedule {
				s := schedulertest.DefaultSchedule()
				s.State.LimitedActions = true
				s.State.RemainingActions = 3
				return s
			},
			maxSteps: 30,
		},
		{
			name: "short interval, long catchup",
			schedule: func() *schedulepb.Schedule {
				s := schedulertest.DefaultSchedule()
				s.Spec.Interval[0].Interval = durationpb.New(10 * time.Second)
				return s
			},
			maxSteps: 40,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := schedulertest.NewDriver(t, schedulertest.WithSchedule(tc.schedule()))
			d.AfterStep = schedulertest.CheckInvariantsHook(t)
			// The hook fails the test on any violation; we just drive it forward.
			d.RunToQuiescence(tc.maxSteps)
		})
	}
}

// TestCheckInvariants_DetectsStuck is a direct unit test of the invariant
// predicate: a non-closed schedule with no pending task that is not held open is
// flagged as stuck.
func TestCheckInvariants_DetectsStuck(t *testing.T) {
	stuck := schedulertest.Snapshot{
		Now:            time.Now(),
		HasPendingTask: false,
		Closed:         false,
		IsHeldOpen:     false,
	}
	violations := schedulertest.CheckInvariants(nil, stuck)
	require.NotEmpty(t, violations, "stuck state must be flagged")
	require.Equal(t, "no-stuck", violations[0].Name)
}

// TestCheckInvariants_AllowsHeldOpenWithoutTask confirms a paused schedule with
// no pending task is NOT flagged (legitimate hold-open).
func TestCheckInvariants_AllowsHeldOpenWithoutTask(t *testing.T) {
	heldOpen := schedulertest.Snapshot{
		Now:            time.Now(),
		HasPendingTask: false,
		Closed:         false,
		Paused:         true,
		IsHeldOpen:     true,
	}
	require.Empty(t, schedulertest.CheckInvariants(nil, heldOpen))
}

// TestCheckInvariants_DetectsHwmRegression confirms the high-water-mark
// monotonicity invariant fires when the generator HWM moves backward.
func TestCheckInvariants_DetectsHwmRegression(t *testing.T) {
	t0 := time.Date(2020, 1, 1, 0, 5, 0, 0, time.UTC)
	prev := schedulertest.Snapshot{HasPendingTask: true, GeneratorLPT: t0}
	cur := schedulertest.Snapshot{HasPendingTask: true, GeneratorLPT: t0.Add(-time.Minute)}
	violations := schedulertest.CheckInvariants(&prev, cur)
	require.NotEmpty(t, violations)
	require.Equal(t, "hwm-monotonic-generator", violations[0].Name)
}

// TestCheckInvariants_DetectsBufferOverrun confirms the buffer-bound invariant
// fires when buffered starts exceed MaxBufferSize, and that being exactly at the
// cap is allowed.
func TestCheckInvariants_DetectsBufferOverrun(t *testing.T) {
	over := schedulertest.Snapshot{HasPendingTask: true, MaxBufferSize: 1000, BufferedStartsCount: 1001}
	violations := schedulertest.CheckInvariants(nil, over)
	require.NotEmpty(t, violations, "buffer overrun must be flagged")
	require.Equal(t, "buffer-bound", violations[0].Name)

	atCap := schedulertest.Snapshot{HasPendingTask: true, MaxBufferSize: 1000, BufferedStartsCount: 1000}
	require.Empty(t, schedulertest.CheckInvariants(nil, atCap), "exactly at the cap is allowed")
}

// TestCheckInvariants_DetectsBudgetOverrun confirms the action-budget invariant
// fires when a single ExecuteTask takes more actions than MaxActionsPerExecution
// (the double-spend bug class), and that being exactly at the cap is allowed.
func TestCheckInvariants_DetectsBudgetOverrun(t *testing.T) {
	over := schedulertest.Snapshot{HasPendingTask: true, MaxActionsPerExecution: 5, MaxActionsObserved: 6}
	violations := schedulertest.CheckInvariants(nil, over)
	require.NotEmpty(t, violations, "budget overrun must be flagged")
	require.Equal(t, "action-budget", violations[0].Name)

	atCap := schedulertest.Snapshot{HasPendingTask: true, MaxActionsPerExecution: 5, MaxActionsObserved: 5}
	require.Empty(t, schedulertest.CheckInvariants(nil, atCap), "exactly at the cap is allowed")
}
