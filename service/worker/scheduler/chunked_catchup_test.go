package scheduler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/common/metrics"
	"google.golang.org/protobuf/types/known/durationpb"
)

// TestChunkedCatchup covers ChunkedCatchup: catch-up work per workflow task is
// bounded, and progress is committed by continue-as-new between chunks.
func TestChunkedCatchup(t *testing.T) {
	const skippedBefore = 55000000 // newCatchupArgs' Info.OverlapSkipped

	t.Run("skip/30d-gap-finishes-in-one-task", func(t *testing.T) {
		// Today this walks 2.6M matches and trips the deadlock detector.
		gap := 30 * 24 * time.Hour
		r := runCatchupRepro(t, catchupCase{gap: gap, chunked: 5000})
		require.NoError(t, r.err)
		require.Equal(t, 1, r.walked, "stops once a Skip start is buffered")
		require.Equal(t, []string{"myid-" + baseStartTime.Add(-gap+time.Second).UTC().Format(time.RFC3339)}, r.startedIDs)
		require.Equal(t, baseStartTime, r.next.State.LastProcessedTime.AsTime())
		require.EqualValues(t, skippedBefore, r.next.Info.OverlapSkipped, "the jumped tail is not counted")
		require.Zero(t, r.next.Info.BufferDropped)
	})

	t.Run("skip/running", func(t *testing.T) {
		r := runCatchupRepro(t, catchupCase{gap: 30 * 24 * time.Hour, chunked: 5000, running: true})
		require.NoError(t, r.err)
		require.Equal(t, 1, r.walked)
		require.Empty(t, r.startedIDs)
		require.EqualValues(t, skippedBefore+1, r.next.Info.OverlapSkipped)
		require.Equal(t, baseStartTime, r.next.State.LastProcessedTime.AsTime())
	})

	t.Run("buffer-all/stops-at-full-buffer", func(t *testing.T) {
		r := runCatchupRepro(t, catchupCase{gap: 30 * 24 * time.Hour, chunked: 5000, overlap: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL})
		require.NoError(t, r.err)
		require.Equal(t, 1000, r.walked)
		require.Zero(t, r.bufferDropped)
		require.Len(t, r.startedIDs, 1)
		require.Len(t, r.next.State.BufferedStarts, 999)
		require.Equal(t, baseStartTime, r.next.State.LastProcessedTime.AsTime())
	})

	t.Run("misses-are-chunked-and-counted-exactly", func(t *testing.T) {
		// Matches outside the catchup window are walked but never buffered, so neither
		// shortcut applies; only the chunk limit bounds them. Chunks 1-7 contain nothing
		// but misses, so LastProcessedTime must advance past matches that started nothing.
		const chunk = 1000
		args := newCatchupArgs(t, 3*time.Hour)
		args.Schedule.Policies.CatchupWindow = durationpb.New(time.Hour)

		var runs []catchupResult
		for {
			r := runCatchupRepro(t, catchupCase{chunked: chunk, args: args})
			require.NoError(t, r.err)
			require.LessOrEqual(t, r.walked, chunk)
			if len(runs) > 0 {
				require.True(t, r.next.State.LastProcessedTime.AsTime().After(args.State.LastProcessedTime.AsTime()),
					"run %d made no progress", len(runs)+1)
			}
			runs = append(runs, r)
			args = r.next
			if !args.State.LastProcessedTime.AsTime().Before(baseStartTime) {
				break
			}
			require.Less(t, len(runs), 20, "catch-up did not converge")
		}

		// 7199 misses + 1 start (the window is inclusive) in chunks of 1000.
		require.Len(t, runs, 8)
		require.EqualValues(t, 7199, args.Info.MissedCatchupWindow)
		last := runs[len(runs)-1]
		require.Equal(t, 200, last.walked)
		require.Equal(t, []string{"myid-" + baseStartTime.Add(-time.Hour).UTC().Format(time.RFC3339)}, last.startedIDs)
		for _, r := range runs[:len(runs)-1] {
			require.Empty(t, r.startedIDs)
		}
	})

	t.Run("instrumentation", func(t *testing.T) {
		const (
			chunkWarn = "Schedule catch-up hit the per-task match limit; continuing in a new run"
			skipWarn  = "Schedule skipped rest of catch-up range: no later match could start"
		)
		chunked := metrics.ScheduleCatchupChunked.Name()
		skipped := metrics.ScheduleCatchupRangeSkipped.Name()
		catchupRange := metrics.ScheduleCatchupRange.Name()
		reasons := func(r catchupResult) []string {
			var out []string
			for _, rec := range r.metrics[skipped] {
				out = append(out, rec.Tags[metrics.ScheduleMissedReasonTag])
			}
			return out
		}

		t.Run("chunk", func(t *testing.T) {
			args := newCatchupArgs(t, 3*time.Hour)
			args.Schedule.Policies.CatchupWindow = durationpb.New(time.Hour)
			r := runCatchupRepro(t, catchupCase{chunked: 1000, args: args})
			require.Len(t, r.metrics[chunked], 1)
			require.Equal(t, 1, r.logger.count(chunkWarn))
			require.Len(t, r.metrics[catchupRange], 1)
			require.Equal(t, 3*time.Hour, r.metrics[catchupRange][0].Value)
			require.Empty(t, r.metrics[skipped])
		})

		t.Run("skip-policy", func(t *testing.T) {
			r := runCatchupRepro(t, catchupCase{gap: 30 * 24 * time.Hour, chunked: 5000})
			require.Equal(t, []string{metrics.ScheduleCatchupSkipReasonSkipPolicy}, reasons(r))
			require.Equal(t, 1, r.logger.count(skipWarn))
			require.Len(t, r.metrics[catchupRange], 1)
			require.Empty(t, r.metrics[chunked])
		})

		t.Run("buffer-full", func(t *testing.T) {
			r := runCatchupRepro(t, catchupCase{gap: 30 * 24 * time.Hour, chunked: 5000, overlap: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL})
			require.Equal(t, []string{metrics.ScheduleCatchupSkipReasonBufferFull}, reasons(r))
			require.Equal(t, 1, r.logger.count(skipWarn))
		})

		t.Run("routine-short-skip-is-not-a-warning", func(t *testing.T) {
			// A wakeup a few seconds late for an every-second schedule.
			r := runCatchupRepro(t, catchupCase{gap: 5 * time.Second, chunked: 5000})
			require.Equal(t, []string{metrics.ScheduleCatchupSkipReasonSkipPolicy}, reasons(r))
			require.Zero(t, r.logger.count(skipWarn))
			require.Empty(t, r.metrics[catchupRange])
		})

		t.Run("single-match-is-not-a-skip", func(t *testing.T) {
			r := runCatchupRepro(t, catchupCase{gap: time.Second, chunked: 5000})
			require.Empty(t, r.metrics[skipped])
		})

		t.Run("off-without-version", func(t *testing.T) {
			r := runCatchupRepro(t, catchupCase{gap: time.Hour})
			require.Empty(t, r.metrics[skipped])
			require.Empty(t, r.metrics[chunked])
			require.Zero(t, r.logger.count(chunkWarn)+r.logger.count(skipWarn))
		})
	})

	t.Run("dynamic-config", func(t *testing.T) {
		// 3h behind with a 1h window: 7199 misses, then the first in-window match.
		missesArgs := func() *schedulespb.StartScheduleArgs {
			args := newCatchupArgs(t, 3*time.Hour)
			args.Schedule.Policies.CatchupWindow = durationpb.New(time.Hour)
			return args
		}
		dc := func(n int) *int { return &n }

		t.Run("overrides-binary-default", func(t *testing.T) {
			r := runCatchupRepro(t, catchupCase{chunked: 5000, dcMatchesPerTask: dc(1000), args: missesArgs()})
			require.NoError(t, r.err)
			require.Equal(t, 1000, r.walked)
			require.True(t, r.next.State.LastProcessedTime.AsTime().Before(baseStartTime), "should continue in a new run")
		})

		t.Run("negative-uses-binary-default", func(t *testing.T) {
			r := runCatchupRepro(t, catchupCase{chunked: 2000, dcMatchesPerTask: dc(-1), args: missesArgs()})
			require.NoError(t, r.err)
			require.Equal(t, 2000, r.walked)
		})

		t.Run("zero-disables-chunking", func(t *testing.T) {
			r := runCatchupRepro(t, catchupCase{chunked: 5000, dcMatchesPerTask: dc(0), args: missesArgs()})
			require.NoError(t, r.err)
			require.Equal(t, 7200, r.walked, "all misses, then the Skip shortcut")
			require.Equal(t, baseStartTime, r.next.State.LastProcessedTime.AsTime())
		})

		t.Run("ignored-below-ChunkedCatchup", func(t *testing.T) {
			r := runCatchupRepro(t, catchupCase{dcMatchesPerTask: dc(1000), args: missesArgs()})
			require.NoError(t, r.err)
			require.Equal(t, 10800, r.walked, "unchanged pre-v15 walk: every match")
		})
	})

	t.Run("version-gate", func(t *testing.T) {
		// Without ChunkedCatchup the walk is unchanged.
		r := runCatchupRepro(t, catchupCase{gap: time.Hour})
		require.NoError(t, r.err)
		require.Equal(t, 3600, r.walked)
		require.EqualValues(t, 2600, r.next.Info.BufferDropped)
	})
}
