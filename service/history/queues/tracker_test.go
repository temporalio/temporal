package queues

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
)

func TestExecutableTracker_OldestPendingTaskVisibilityTime(t *testing.T) {
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

	require.True(t, tracker.oldestPendingTaskVisibilityTime().IsZero(),
		"empty tracker should report zero time")

	// Oldest task (by visibility time) is pending.
	add(1, base.Add(10*time.Minute), ctasks.TaskStatePending)
	add(2, base.Add(5*time.Minute), ctasks.TaskStatePending)
	add(3, base.Add(20*time.Minute), ctasks.TaskStatePending)
	require.Equal(t, base.Add(5*time.Minute), tracker.oldestPendingTaskVisibilityTime())

	// Acked tasks must be ignored even if they have older visibility times.
	add(4, base, ctasks.TaskStateAcked)
	require.Equal(t, base.Add(5*time.Minute), tracker.oldestPendingTaskVisibilityTime(),
		"acked task with older visibility time must be ignored")
}
