package matching

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"
)

// addTasks is a helper which adds numberOfTasks to a taskTracker
func trackTasksHelper(tr *taskTracker, numberOfTasks int) {
	for i := 0; i < numberOfTasks; i++ {
		// adding a bunch of tasks
		tr.incrementTaskCount()
	}
}

func TestAddTasksRate(t *testing.T) {
	// define a fake clock and it's time for testing
	timeSource := clock.NewEventTimeSource()
	currentTime := time.Now()
	timeSource.Update(currentTime)

	tr := newTaskTracker(timeSource)

	// mini windows will have the following format : (start time, end time)
	// (0 - 4), (5 - 9), (10 - 14), (15 - 19), (20 - 24), (25 - 29), (30 - 34), ...

	// rate should be zero when no time is passed
	require.Equal(t, float32(0), tr.rate()) // time: 0
	trackTasksHelper(tr, 100)
	require.Equal(t, float32(0), tr.rate()) // still zero because no time is passed

	// tasks should be placed in the first mini-window
	timeSource.Advance(1 * time.Second)                  // time: 1 second
	require.InEpsilon(t, float32(100), tr.rate(), 0.001) // 100 tasks added in 1 second = 100 / 1 = 100

	// tasks should be placed in the second mini-window with 6 total seconds elapsed
	timeSource.Advance(5 * time.Second)
	trackTasksHelper(tr, 200)                           // time: 6 second
	require.InEpsilon(t, float32(50), tr.rate(), 0.001) // (100 + 200) tasks added in 6 seconds = 300/6 = 50

	timeSource.Advance(24 * time.Second) // time: 30 second
	trackTasksHelper(tr, 300)
	require.InEpsilon(t, float32(20), tr.rate(), 0.001) // (100 + 200 + 300) tasks added in (30 + 0 (current window)) seconds = 600/30 = 20

	// this should clear out the first mini-window of 100 tasks
	timeSource.Advance(5 * time.Second) // time: 35 second
	trackTasksHelper(tr, 10)
	require.InEpsilon(t, float32(17), tr.rate(), 0.001) // (10 + 200 + 300) tasks added in (30 + 0 (current window)) seconds = 510/30 = 17

	// this should clear out the second and third mini-windows
	timeSource.Advance(15 * time.Second) // time: 50 second
	trackTasksHelper(tr, 10)
	require.InEpsilon(t, float32(10.666667), tr.rate(), 0.001) // (10 + 10 + 300) tasks added in (30 + 0 (current window)) seconds = 320/30 = 10.66

	// a minute passes and no tasks are added
	timeSource.Advance(60 * time.Second)
	require.Equal(t, float32(0), tr.rate()) // 0 tasks have been added in the last 30 seconds
}
