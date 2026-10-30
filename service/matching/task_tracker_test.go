package matching

import (
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
