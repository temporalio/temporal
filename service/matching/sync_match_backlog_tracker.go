package matching

import (
	"sync"
	"time"

	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

type syncMatchBacklogTracker struct {
	lock     sync.Mutex
	byPri    map[int32]int64
	ageByPri map[int32]backlogAgeTracker
}

func syncMatchBacklogPriority(task *internalTask) int32 {
	// Tasks entering taskPQ have effectivePriority initialized from the caller-provided priority
	// or the task queue default. Nexus tasks do not have a caller-provided priority on this path,
	// so they use the task queue default. effectivePriority uses a scaled internal representation
	// to leave room for intermediate matcher priorities; convert it back to the public priority key.
	return int32(task.effectivePriority / effectivePriorityFactor)
}

func isSyncMatchBacklogTask(task *internalTask) bool {
	// DB backlog tasks are already counted by the backlog manager. This tracker only accounts for
	// real in-memory sync-match tasks that would otherwise be invisible while waiting in taskPQ.
	// Excludes synthetic poll-forwarder marker tasks. Those are not real user tasks,
	// so they should never affect backlog counts.
	return !task.isPollForwarder() && task.source != enumsspb.TASK_SOURCE_DB_BACKLOG
}

func (t *syncMatchBacklogTracker) record(task *internalTask, delta int) {
	if !isSyncMatchBacklogTask(task) {
		return
	}

	pri := syncMatchBacklogPriority(task)

	t.lock.Lock()
	defer t.lock.Unlock()

	if t.byPri == nil {
		t.byPri = make(map[int32]int64)
	}
	if t.ageByPri == nil {
		t.ageByPri = make(map[int32]backlogAgeTracker)
	}

	count := max(0, t.byPri[pri]+int64(delta))
	if count == 0 {
		delete(t.byPri, pri)
		delete(t.ageByPri, pri)
		return
	}
	t.byPri[pri] = count

	ageTracker, ok := t.ageByPri[pri]
	if !ok {
		ageTracker = newBacklogAgeTracker()
	}
	ageTracker.record(task.getCreateTime(), delta)
	t.ageByPri[pri] = ageTracker
}

func (t *syncMatchBacklogTracker) statsByPriority() map[int32]*taskqueuepb.TaskQueueStats {
	t.lock.Lock()
	defer t.lock.Unlock()

	result := make(map[int32]*taskqueuepb.TaskQueueStats, len(t.byPri))
	for pri, count := range t.byPri {
		age := time.Duration(0)
		if oldest := t.ageByPri[pri].oldestTime(); !oldest.IsZero() {
			age = time.Since(oldest)
		}
		result[pri] = &taskqueuepb.TaskQueueStats{
			ApproximateBacklogCount: count,
			ApproximateBacklogAge:   durationpb.New(age),
		}
	}
	return result
}
