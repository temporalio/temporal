package matching

import (
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"
)

func TestAddTasksRate(t *testing.T) {
	// define a fake clock and it's time for testing
	timeSource := clock.NewEventTimeSource()
	currentTime := time.Now()
	timeSource.Update(currentTime)

	tr := newTaskTracker(timeSource, 5*time.Second, 30*time.Second)

	// mini windows will have the following format : (start time, end time)
	// (0 - 4), (5 - 9), (10 - 14), (15 - 19), (20 - 24), (25 - 29), (30 - 34), ...

	// rate should be zero when no time is passed
	rate, full := tr.rateAndFull() // time: 0
	require.InDelta(t, float32(0), rate, 1e-9)
	require.False(t, full)
	tr.inc(100)
	rate, full = tr.rateAndFull() // still zero because no time is passed
	require.InDelta(t, float32(0), rate, 1e-9)
	require.False(t, full)

	// tasks should be placed in the first mini-window
	timeSource.Advance(1 * time.Second) // time: 1 second
	rate, full = tr.rateAndFull()
	require.InEpsilon(t, float32(100), rate, 0.001) // 100 tasks added in 1 second = 100 / 1 = 100
	require.False(t, full)                          // 1s < 30s total interval

	// tasks should be placed in the second mini-window with 6 total seconds elapsed
	timeSource.Advance(5 * time.Second) // time: 6 second
	tr.inc(100)
	tr.inc(100)
	rate, full = tr.rateAndFull()
	require.InEpsilon(t, float32(50), rate, 0.001) // (100 + 200) tasks added in 6 seconds = 300/6 = 50
	require.False(t, full)                         // 6s < 30s total interval

	timeSource.Advance(24 * time.Second) // time: 30 second
	tr.inc(100)
	tr.inc(100)
	tr.inc(100)
	rate, full = tr.rateAndFull()
	require.InEpsilon(t, float32(20), rate, 0.001) // (100 + 200 + 300) tasks added in (30 + 0 (current window)) seconds = 600/30 = 20
	require.True(t, full)                          // full 30s interval has now elapsed

	// this should clear out the first mini-window of 100 tasks
	timeSource.Advance(5 * time.Second) // time: 35 second
	tr.inc(10)
	require.InEpsilon(t, float32(17), tr.rate(), 0.001) // (10 + 200 + 300) tasks added in (30 + 0 (current window)) seconds = 510/30 = 17

	// this should clear out the second and third mini-windows
	timeSource.Advance(15 * time.Second) // time: 50 second
	tr.inc(10)
	require.InEpsilon(t, float32(10.666667), tr.rate(), 0.001) // (10 + 10 + 300) tasks added in (30 + 0 (current window)) seconds = 320/30 = 10.66

	// a minute passes and no tasks are added
	timeSource.Advance(60 * time.Second)
	require.Equal(t, float32(0), tr.rate()) // 0 tasks have been added in the last 30 seconds
}

func TestTaskTrackerIncOver(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		age      time.Duration // time since tracker creation when incOver is called
		n        int
		span     time.Duration // incOver covers (now-span, now]
		wantKept int
		wantRate float32
		// per-bucket counts from the current bucket backwards; nil skips the check
		wantBuckets []int32
	}{
		{
			name:        "spread over several buckets",
			age:         4 * time.Second,
			n:           400,
			span:        4 * time.Second,
			wantKept:    400,
			wantRate:    100,
			wantBuckets: []int32{0, 100, 100, 100, 100},
		},
		{
			// the buffer covers the 0.5s current bucket plus 10 full buckets: 10.5s of 21s
			name:     "older than buffer is dropped",
			age:      40500 * time.Millisecond,
			n:        2100,
			span:     21 * time.Second,
			wantKept: 1050,
			wantRate: 100,
		},
		{
			name:     "before start time is dropped",
			age:      3 * time.Second,
			n:        600,
			span:     6 * time.Second,
			wantKept: 300,
			wantRate: 100,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			timeSource := clock.NewEventTimeSource()
			timeSource.Update(time.Now())
			tr := newTaskTracker(timeSource, time.Second, 10*time.Second)
			timeSource.Advance(tc.age)

			tr.incOver(tc.n, timeSource.Now().Add(-tc.span))
			if tc.wantKept == tc.n {
				require.Equal(t, tc.wantKept, tr.tasks.totalTasks())
			} else {
				require.InDelta(t, tc.wantKept, tr.tasks.totalTasks(), 1)
			}
			for _, c := range tr.tasks.buffer {
				require.GreaterOrEqual(t, c, int32(0))
			}
			size := len(tr.tasks.buffer)
			for i, want := range tc.wantBuckets {
				require.Equal(t, want, tr.tasks.buffer[(tr.tasks.currentPos-i+size)%size])
			}
			rate, _ := tr.rateAndFull()
			require.InEpsilon(t, tc.wantRate, rate, 0.01)
		})
	}

	t.Run("no span behaves like inc", func(t *testing.T) {
		t.Parallel()
		timeSource := clock.NewEventTimeSource()
		timeSource.Update(time.Now())
		withInc := newTaskTracker(timeSource, time.Second, 10*time.Second)
		withIncOver := newTaskTracker(timeSource, time.Second, 10*time.Second)

		timeSource.Advance(2500 * time.Millisecond)
		withInc.inc(50)
		withIncOver.incOver(50, timeSource.Now())
		timeSource.Advance(time.Second)
		withInc.inc(30)
		withIncOver.incOver(30, timeSource.Now().Add(time.Second)) // since after now
		require.Equal(t, withInc.tasks, withIncOver.tasks)
		require.InEpsilon(t, withInc.rate(), withIncOver.rate(), 1e-6)
	})
}

// TestTaskTrackerIncOverRemovesBatchBias checks that batches flushed less often than the
// window is long are measured at their true rate. Attributing each batch to the instant it
// was flushed overstates the rate.
func TestTaskTrackerIncOverRemovesBatchBias(t *testing.T) {
	t.Parallel()

	const rate, batch = 150, 400
	flushEvery := time.Second * batch / rate

	readings := func(spread bool) []float32 {
		timeSource := clock.NewEventTimeSource()
		timeSource.Update(time.Now())
		tr := newTaskTracker(timeSource, 200*time.Millisecond, 2*time.Second)
		lastFlush := timeSource.Now()
		var out []float32
		for range 100 {
			timeSource.Advance(flushEvery)
			if spread {
				tr.incOver(batch, lastFlush)
			} else {
				tr.inc(batch)
			}
			lastFlush = timeSource.Now()
			r, full := tr.rateAndFull()
			require.True(t, full)
			out = append(out, r)
		}
		return out
	}

	for _, r := range readings(true) {
		require.InEpsilon(t, float32(rate), r, 0.05)
	}
	require.GreaterOrEqual(t, slices.Max(readings(false)), float32(1.2*rate))
}
