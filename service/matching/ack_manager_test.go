package matching

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
)

func TestAckManager_AddingTasksIncreasesBacklogCounter(t *testing.T) {
	t.Parallel()
	mgr := newAckManager(log.NewTestLogger())
	mgr.addTask(1)
	require.Equal(t, mgr.getBacklogCountHint(), int64(1))
	mgr.addTask(12)
	require.Equal(t, mgr.getBacklogCountHint(), int64(2))
}

func TestAckManager_CompleteTaskMovesAckLevelUpToGap(t *testing.T) {
	t.Parallel()
	mgr := newAckManager(log.NewTestLogger())
	mgr.addTask(1)
	require.Equal(t, int64(-1), mgr.getAckLevel(), "should only move ack level on completion")
	require.Equal(t, int64(1), mgr.completeTask(1), "should move ack level on completion")

	mgr.addTask(2)
	mgr.addTask(3)
	mgr.addTask(12)

	require.Equal(t, int64(1), mgr.completeTask(3), "task 2 is not complete, we should not move ack level")
	require.Equal(t, int64(3), mgr.completeTask(2), "both tasks 2 and 3 are complete")
}

// Add 1000 tasks in order and complete them in a random order.
// This will cause our ack level to jump as we complete them
func BenchmarkAckManager_CompleteTask(b *testing.B) {
	tasks := make([]int, 1000)
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		mgr := newAckManager(log.NewTestLogger())
		for i := 0; i < len(tasks); i++ {
			tasks[i] = i
			mgr.addTask(int64(i))
		}
		rand.Shuffle(len(tasks), func(i, j int) {
			tasks[i], tasks[j] = tasks[j], tasks[i]
		})
		b.StartTimer()

		for i := 0; i < len(tasks); i++ {
			mgr.completeTask(int64(i))
		}
	}
}
