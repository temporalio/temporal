package schedulertest_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/schedulertest"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// configWith returns DefaultConfig with Tweakables adjusted by fn.
func configWith(fn func(*scheduler.Tweakables)) *scheduler.Config {
	cfg := schedulertest.DefaultConfig()
	tweakables := scheduler.DefaultTweakables
	fn(&tweakables)
	cfg.Tweakables = func(string) scheduler.Tweakables { return tweakables }
	return cfg
}

// oneShotSchedule fires exactly once, at start+offset.
func oneShotSchedule(start time.Time, offset time.Duration) *schedulepb.Schedule {
	s := schedulertest.DefaultSchedule()
	s.Spec.Interval[0].Interval = durationpb.New(time.Hour)
	s.Spec.Interval[0].Phase = durationpb.New(offset % time.Hour)
	s.Spec.StartTime = timestamppb.New(start.Add(offset))
	s.Spec.EndTime = timestamppb.New(start.Add(offset + time.Minute))
	return s
}

// collectViolations records every invariant violation instead of failing, so a
// test can assert that a known bug is detected.
func collectViolations(out *[]schedulertest.Violation) func(*schedulertest.Driver) {
	var prev *schedulertest.Snapshot
	return func(d *schedulertest.Driver) {
		cur := d.Snapshot()
		*out = append(*out, schedulertest.CheckInvariants(prev, cur)...)
		prev = &cur
	}
}

func violationNames(vs []schedulertest.Violation) []string {
	names := make([]string, 0, len(vs))
	for _, v := range vs {
		names = append(names, v.Name)
	}
	return names
}

// The final action of a schedule is buffered by the same generator tick that
// arms the idle task. If InvokerExecuteTask hasn't run by the time the idle task
// fires, the schedule must stay open. Pre-#13016 the idle deadline ignored the
// pending start and was already in the past.
func TestLedger_FinalStartPendingAtIdle(t *testing.T) {
	const idle = time.Hour
	start := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

	cases := []struct {
		name    string
		fixOn   bool
		offset  time.Duration
		wantBad []string
	}{
		{"fix on, final gap > IdleTime", true, 3 * idle, nil},
		{"fix on, final gap < IdleTime", true, idle / 2, nil},
		{"fix off, final gap > IdleTime", false, 3 * idle, []string{"ledger-lost-start"}},
		{"fix off, final gap < IdleTime", false, idle / 2, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var violations []schedulertest.Violation
			d := schedulertest.NewDriver(t,
				schedulertest.WithStartTime(start),
				schedulertest.WithSchedule(oneShotSchedule(start, tc.offset)),
				schedulertest.WithConfig(configWith(func(tw *scheduler.Tweakables) {
					tw.IdleTime = idle
					tw.EnablePendingStartIdleFix = tc.fixOn
				})),
				schedulertest.WithMaxSideEffectLag(idle/2),
				schedulertest.WithAfterStep(collectViolations(&violations)),
			)

			// Fire timers while InvokerExecuteTask lags, then drain.
			for d.StepPure() {
			}
			_, quiescent := d.RunToQuiescence(50)
			require.True(t, quiescent)

			require.Equal(t, tc.wantBad, uniq(violationNames(violations)))
			snap := d.Snapshot()
			require.True(t, snap.Closed)
			if tc.wantBad == nil {
				require.Len(t, snap.StartRequested, 1, "the one action must start")
			}
		})
	}
}

// A buffered start that vanishes with no RPC and no recorded discard is a
// silent drop; one with a recorded discard is not.
func TestCheckInvariants_Ledger(t *testing.T) {
	pending := func(ids ...string) map[string]struct{} {
		m := make(map[string]struct{}, len(ids))
		for _, id := range ids {
			m[id] = struct{}{}
		}
		return m
	}

	cases := []struct {
		name      string
		prev, cur schedulertest.Snapshot
		want      []string
	}{
		{
			name: "started",
			prev: schedulertest.Snapshot{HasPendingTask: true, PendingStarts: pending("a")},
			cur:  schedulertest.Snapshot{HasPendingTask: true, StartRequested: pending("a")},
		},
		{
			name: "overlap skipped",
			prev: schedulertest.Snapshot{HasPendingTask: true, PendingStarts: pending("a")},
			cur:  schedulertest.Snapshot{HasPendingTask: true, OverlapSkipped: 1},
		},
		{
			name: "dropped",
			prev: schedulertest.Snapshot{HasPendingTask: true, PendingStarts: pending("a", "b"), DroppedStarts: 3},
			cur:  schedulertest.Snapshot{HasPendingTask: true, DroppedStarts: 4, StartRequested: pending("b")},
		},
		{
			name: "silent drop",
			prev: schedulertest.Snapshot{HasPendingTask: true, PendingStarts: pending("a", "b")},
			cur:  schedulertest.Snapshot{HasPendingTask: true, OverlapSkipped: 1},
			want: []string{"ledger-silent-drop"},
		},
		{
			name: "closed with pending start",
			prev: schedulertest.Snapshot{HasPendingTask: true, PendingStarts: pending("a")},
			cur:  schedulertest.Snapshot{Closed: true, PendingStarts: pending("a")},
			want: []string{"ledger-lost-start"},
		},
		{
			name: "migration takes the buffer",
			prev: schedulertest.Snapshot{HasPendingTask: true, PendingStarts: pending("a")},
			cur:  schedulertest.Snapshot{Closed: true, Migrating: true, PendingStarts: pending("a")},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := schedulertest.CheckInvariants(&tc.prev, tc.cur)
			require.Equal(t, tc.want, uniq(violationNames(got)))
		})
	}
}

func uniq(names []string) []string {
	var out []string
	seen := map[string]bool{}
	for _, n := range names {
		if !seen[n] {
			seen[n] = true
			out = append(out, n)
		}
	}
	return out
}

// A start deferred behind a running workflow is only re-enabled by a workflow
// completion. If that completion arrives while the schedule is paused,
// ProcessBuffer drops the first start (paused) and re-defers the rest with
// nothing running, so no completion will ever re-enable them. Unpause doesn't
// re-process the buffer either; the starts (including a manual trigger, which
// pause allows) sit until the idle task closes the schedule.
func TestLedger_DeferredStartsStrandedByPausedCompletion(t *testing.T) {
	t.Skip("known bug: deferred starts stranded after a completion while paused")

	const idle = time.Hour
	start := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	s := schedulertest.DefaultSchedule()
	s.Policies.OverlapPolicy = enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL
	s.Spec.EndTime = timestamppb.New(start.Add(2*time.Minute + time.Second))

	var violations []schedulertest.Violation
	d := schedulertest.NewDriver(t,
		schedulertest.WithStartTime(start),
		schedulertest.WithSchedule(s),
		schedulertest.WithConfig(configWith(func(tw *scheduler.Tweakables) { tw.IdleTime = idle })),
		schedulertest.WithAfterStep(collectViolations(&violations)),
	)

	require.True(t, d.Step()) // 00:01 starts, keeps running
	require.True(t, d.Step()) // 00:02 final fire, deferred behind it
	require.NoError(t, d.Pause())
	require.NoError(t, d.TriggerImmediately()) // manual, deferred behind it
	require.True(t, d.CompleteWorkflow())
	require.NoError(t, d.Unpause())
	_, quiescent := d.RunToQuiescence(50)
	require.True(t, quiescent)

	require.Empty(t, violationNames(violations))
	require.Len(t, d.Snapshot().StartRequested, 2, "the manual trigger must start")
}
