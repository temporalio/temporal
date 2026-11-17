package scheduler

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/common/metrics"
	"google.golang.org/protobuf/types/known/durationpb"
)

// recoveryCase is a whole recovery from a stall: runs chained by continue-as-new until
// LastProcessedTime catches up with the current time.
type recoveryCase struct {
	gap            time.Duration // initial now - LastProcessedTime
	chunk          int           // MaxCatchupMatchesPerTask
	overlap        enumspb.ScheduleOverlapPolicy
	catchupWindow  time.Duration // 0: newCatchupArgs' 365d
	startedRunning bool          // started workflows never complete
	perRun         time.Duration // time that passes between consecutive runs
	jitter         time.Duration // the spec's jitter
}

// recoveryResult totals what the chained runs did.
type recoveryResult struct {
	runs       int
	maxWalked  int
	startedIDs []string
	first      *schedulespb.StartScheduleArgs
	final      *schedulespb.StartScheduleArgs
	finalNow   time.Time
}

func (c recoveryCase) args(t *testing.T) *schedulespb.StartScheduleArgs {
	args := newCatchupArgs(t, c.gap)
	if c.overlap != enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED {
		args.Schedule.Policies.OverlapPolicy = c.overlap
	}
	if c.catchupWindow > 0 {
		args.Schedule.Policies.CatchupWindow = durationpb.New(c.catchupWindow)
	}
	if c.jitter > 0 {
		args.Schedule.Spec.Jitter = durationpb.New(c.jitter)
	}
	return args
}

// runRecovery runs the case at ChunkedCatchup until it has caught up, checking that each
// run stays within the chunk limit and makes progress.
func runRecovery(t *testing.T, c recoveryCase) recoveryResult {
	args := c.args(t)
	res := recoveryResult{first: args}
	maxRuns := int(c.gap/time.Second)/c.chunk + 10
	now := baseStartTime
	for {
		before := args.State.LastProcessedTime.AsTime()
		r := runCatchupRepro(t, catchupCase{chunked: c.chunk, args: args, now: now, startedRunning: c.startedRunning})
		require.NoError(t, r.err, "run %d", res.runs+1)
		require.LessOrEqual(t, r.walked, c.chunk, "run %d walked more than one chunk", res.runs+1)
		res.runs++
		res.maxWalked = max(res.maxWalked, r.walked)
		res.startedIDs = append(res.startedIDs, r.startedIDs...)
		args = r.next
		after := args.State.LastProcessedTime.AsTime()
		require.True(t, after.After(before), "run %d made no progress (LastProcessedTime %s)", res.runs, after)
		// Caught up: the run didn't stop for the chunk limit. (With jitter, LastProcessedTime
		// can stay just before now, behind a match that isn't due yet.)
		if len(r.metrics[metrics.ScheduleCatchupChunked.Name()]) == 0 {
			require.False(t, after.Before(now.Add(-c.jitter)), "run %d stopped short of now-jitter", res.runs)
			break
		}
		require.Less(t, res.runs, maxRuns, "catch-up did not converge: LastProcessedTime %s, now %s", after, now)
		now = now.Add(c.perRun)
	}
	res.final, res.finalNow = args, now
	return res
}

// runPreV15 runs the same stall through the unchunked pre-ChunkedCatchup walk, in one task
// with a deadlock timeout long enough for it to finish.
func runPreV15(t *testing.T, c recoveryCase) catchupResult {
	r := runCatchupRepro(t, catchupCase{args: c.args(t), startedRunning: c.startedRunning, deadlockTimeout: 10 * time.Minute})
	require.NoError(t, r.err)
	return r
}

func bufferedNominalTimes(args *schedulespb.StartScheduleArgs) []time.Time {
	out := make([]time.Time, 0, len(args.State.BufferedStarts))
	for _, b := range args.State.BufferedStarts {
		out = append(out, b.NominalTime.AsTime())
	}
	return out
}

// TestChunkedCatchupRecoveryMatchesPreV15 checks that a chunked recovery from a stall
// longer than one chunk starts exactly what the old unchunked walk would have, had it
// finished: the same workflows, the same buffered starts and the same catchup-window
// misses.
//
// Chunking alone would not: if each chunk started from the buffer, it would empty it, so
// later chunks would refill it and start what the single walk dropped (up to a buffer's
// worth per chunk under Allow All). The buffer is left alone until the catch-up finishes,
// so it fills at the same match as in the single walk, and the
// stop-once-nothing-can-start shortcuts keep the runs short. That has to hold for chunks
// smaller than the buffer, and for chunks mostly spent on catchup-window misses, where only
// a chunk's last few matches reach the buffer.
//
// Only the OverlapSkipped/BufferDropped counters may differ: ChunkedCatchup doesn't count
// the tail it skips.
func TestChunkedCatchupRecoveryMatchesPreV15(t *testing.T) {
	policies := []enumspb.ScheduleOverlapPolicy{
		enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
		enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE,
		enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
		enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER,
		enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER,
		enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
	}
	stalls := []struct {
		name          string
		gap           time.Duration
		catchupWindow time.Duration
	}{
		{"12h-stall", 12 * time.Hour, 0}, // 43200 matches
		// 1800 misses, then 1h of matches in the window: a 2000-match chunk reaches only
		// 200 of them.
		{"miss-heavy", time.Hour + 1801*time.Second, time.Hour},
	}
	type named struct {
		name string
		c    recoveryCase
	}
	var cases []named
	for _, stall := range stalls {
		// Below, at and above the buffer size (1000).
		for _, chunk := range []int{50, 500, 2000} {
			for _, jitter := range []time.Duration{0, 500 * time.Millisecond} {
				for _, p := range policies {
					for _, running := range []bool{false, true} {
						actions := "actions-complete"
						if running {
							actions = "actions-keep-running"
						}
						cases = append(cases, named{
							name: fmt.Sprintf("%s/chunk-%d/jitter-%s/%s/%s", stall.name, chunk, jitter, p, actions),
							c: recoveryCase{
								gap: stall.gap, catchupWindow: stall.catchupWindow, chunk: chunk,
								jitter: jitter, overlap: p, startedRunning: running,
							},
						})
					}
				}
			}
		}
	}
	// Jitter longer than the interval: every match is jittered by up to 1s, but the
	// shortcuts can only skip to now-10s.
	cases = append(cases, named{"jitter-10s", recoveryCase{gap: 12 * time.Hour, chunk: 2000, overlap: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL, jitter: 10 * time.Second}})
	// Mostly catchup-window misses, which are walked in chunks before the first start.
	cases = append(cases, named{"1h-catchup-window", recoveryCase{gap: 12 * time.Hour, chunk: 2000, catchupWindow: time.Hour}})

	// The unchunked walk doesn't depend on the chunk size: run it once per scenario.
	preV15 := map[recoveryCase]catchupResult{}
	for _, nc := range cases {
		t.Run(nc.name, func(t *testing.T) {
			c := nc.c
			key := c
			key.chunk = 0
			want, ok := preV15[key]
			if !ok {
				want = runPreV15(t, c)
				preV15[key] = want
			}
			got := runRecovery(t, c)
			t.Logf("runs=%d maxWalked=%d started=%d buffered=%d", got.runs, got.maxWalked, len(got.startedIDs), len(got.final.State.BufferedStarts))

			require.Equal(t, want.startedIDs, got.startedIDs, "started workflows")
			require.Equal(t, bufferedNominalTimes(want.next), bufferedNominalTimes(got.final), "buffered starts")
			require.Equal(t, want.next.Info.ActionCount, got.final.Info.ActionCount)
			require.Equal(t, want.next.Info.MissedCatchupWindow, got.final.Info.MissedCatchupWindow)
		})
	}
}

// TestChunkedCatchupConvergesAsTimePasses recovers from a day-long stall made almost
// entirely of catchup-window misses, which no shortcut applies to, while time keeps
// passing between runs: each run is bounded by the chunk limit and the catch-up still
// converges.
func TestChunkedCatchupConvergesAsTimePasses(t *testing.T) {
	c := recoveryCase{gap: 24 * time.Hour, chunk: 5000, catchupWindow: time.Hour, perRun: 2 * time.Second}
	r := runRecovery(t, c)
	require.LessOrEqual(t, r.maxWalked, c.chunk)
	// About 82,800 misses (everything older than the 1h window, plus the time that passed)
	// at 5000 per run; then the first in-window match starts and the Skip shortcut ends the
	// range.
	require.Equal(t, 17, r.runs)
	require.Greater(t, r.final.Info.MissedCatchupWindow, int64(82800))
	require.Len(t, r.startedIDs, 1)
	require.Equal(t, r.finalNow, r.final.State.LastProcessedTime.AsTime())
}
