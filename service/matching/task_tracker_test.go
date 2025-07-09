package matching

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"
)

// trackTasks is a helper which adds numberOfTasks to a taskTracker for a specific priority
func trackTasks(tr *taskTracker, numberOfTasks int, priority int32) {
	for i := 0; i < numberOfTasks; i++ {
		tr.incrementTaskCount(priority)
	}
}

func TestAddTasksRate(t *testing.T) {
	// define a fake clock and it's time for testing
	timeSource := clock.NewEventTimeSource()
	currentTime := time.Now()
	timeSource.Update(currentTime)

	const pri0 = 0
	const pri1 = 1
	tr := newTaskTracker(timeSource)

	// mini windows will have the following format : (start time, end time)
	// (0 - 4), (5 - 9), (10 - 14), (15 - 19), (20 - 24), (25 - 29), (30 - 34), ...

	// rate should be zero when no time is passed
	rates := tr.rate()
	require.Equal(t, map[int32]float32{}, rates) // no priorities tracked yet

	trackTasks(tr, 60, pri0)
	rates = tr.rate()
	require.Equal(t, map[int32]float32{}, rates) // no time passed, so no rates calculated

	// tasks should be placed in the first mini-window
	timeSource.Advance(1 * time.Second) // time: 1 second
	rates = tr.rate()
	require.Equal(t, map[int32]float32{pri0: 60}, rates) // 60 tasks added in 1 second = 60 / 1 = 60

	// tasks should be placed in the second mini-window with 6 total seconds elapsed
	timeSource.Advance(5 * time.Second) // time: 6 seconds
	trackTasks(tr, 60, pri0)
	trackTasks(tr, 60, pri1)
	rates = tr.rate()
	require.Equal(t, map[int32]float32{
		pri0: 20, // 60 tasks added in 6 seconds = 120/6 = 20
		pri1: 10, // 60 tasks added in 6 seconds = 60/6 = 10
	}, rates)

	// tasks should be placed in the third mini-window with 20 total seconds elapsed
	timeSource.Advance(14 * time.Second) // time: 20 seconds
	trackTasks(tr, 100, pri0)
	trackTasks(tr, 100, pri1)
	rates = tr.rate()
	require.Equal(t, map[int32]float32{
		pri0: 11, // (100 + 60 + 60) tasks added in 20 seconds = 220/20 = 11
		pri1: 8,  // (100 + 60) tasks added in 20 seconds = 160/20 = 8
	}, rates)

	// TODO: fix up
	// this should clear out the first mini-window
	//timeSource.Advance(10 * time.Second) // time: 30 seconds
	//rates = tr.rate()
	//trackTasks(tr, 15, pri0)
	//trackTasks(tr, 15, pri1)
	//require.Equal(t, map[int32]float32{
	//	pri0: 8.8, // (10 + 100 + 60) tasks added in 30 seconds = 170/30 = 5.67
	//	pri1: 6.4, // (100 + 60) tasks added in 30 seconds = 160/30 = 6.4
	//}, rates)

	//// this should clear out the second and third mini-windows
	//timeSource.Advance(15 * time.Second) // time: 50 seconds
	//trackTasks(tr, 200, pri0)
	//rates = tr.rate()
	//require.Equal(t, map[int32]float32{
	//	pri0: 20, // (200 + 400) tasks added in 30 seconds = 600/30 = 20
	//	pri1: 7,  // 200 tasks added in 30 seconds = 210/30 = 7
	//}, rates)
	//
	//// a minute passes and no tasks are added
	//timeSource.Advance(60 * time.Second)
	//rates = tr.rate()
	//require.Equal(t, map[int32]float32{
	//	pri0: 0, // 0 tasks have been added in the last 30 seconds
	//	pri1: 0, // 0 tasks have been added in the last 30 seconds
	//}, rates)
}
