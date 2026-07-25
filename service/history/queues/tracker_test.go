package queues

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
)

func TestExecutableTracker_PendingTaskVisibilityTime(t *testing.T) {
	controller := gomock.NewController(t)
	tracker := newExecutableTracker(GrouperNamespaceID{})

	base := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	add := func(taskID int64, visTime time.Time, state ctasks.State) {
		e := NewMockExecutable(controller)
		e.EXPECT().GetKey().Return(tasks.NewImmediateKey(taskID)).AnyTimes()
		e.EXPECT().GetVisibilityTime().Return(visTime).AnyTimes()
		e.EXPECT().State().Return(state).AnyTimes()
		e.EXPECT().GetNamespaceID().Return("ns").AnyTimes()
		e.EXPECT().GetTask().Return(e).AnyTimes()
		tracker.add(e)
	}

	_, ok := tracker.pendingTaskVisibilityTime(tasks.NewImmediateKey(20))
	require.False(t, ok, "nothing loaded at that key")

	add(20, base.Add(5*time.Minute), ctasks.TaskStatePending)
	visTime, ok := tracker.pendingTaskVisibilityTime(tasks.NewImmediateKey(20))
	require.True(t, ok)
	require.Equal(t, base.Add(5*time.Minute), visTime)

	// A key held by another task must not resolve.
	_, ok = tracker.pendingTaskVisibilityTime(tasks.NewImmediateKey(21))
	require.False(t, ok)

	// An acked-but-not-yet-shrunk task is not a pending task.
	add(30, base, ctasks.TaskStateAcked)
	_, ok = tracker.pendingTaskVisibilityTime(tasks.NewImmediateKey(30))
	require.False(t, ok, "acked task must not resolve")
}
