package queues

import (
	"fmt"

	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/tasks"
)

type (
	// TODO: make task tracking a standalone component
	// currently it's used as a implementation detail in SliceImpl
	executableTracker struct {
		pendingExecutables map[tasks.Key]Executable
		grouper            Grouper
		pendingPerKey      map[any]int
	}
)

func newExecutableTracker(grouper Grouper) *executableTracker {
	return &executableTracker{
		pendingExecutables: make(map[tasks.Key]Executable),
		grouper:            grouper,
		pendingPerKey:      make(map[any]int, 0),
	}
}

func (t *executableTracker) split(
	thisScope Scope,
	thatScope Scope,
) (*executableTracker, *executableTracker) {
	that := executableTracker{
		pendingExecutables: make(map[tasks.Key]Executable, len(t.pendingExecutables)/2),
		grouper:            t.grouper,
		pendingPerKey:      make(map[any]int, len(t.pendingPerKey)),
	}

	for key, executable := range t.pendingExecutables {
		if thisScope.Contains(executable) {
			continue
		}

		if !thatScope.Contains(executable) {
			panic(fmt.Sprintf("Queue slice encountered task doesn't belong to either scopes during split, scope: %v and %v, task: %v, task type: %v",
				thisScope, thatScope, executable.GetTask(), executable.GetType()))
		}

		delete(t.pendingExecutables, key)
		that.pendingExecutables[key] = executable

		groupKey := t.grouper.Key(executable)
		t.pendingPerKey[groupKey]--
		that.pendingPerKey[groupKey]++
	}

	return t, &that
}

func (t *executableTracker) merge(incomingTracker *executableTracker) *executableTracker {
	thisExecutables, thisPendingTasks := t.pendingExecutables, t.pendingPerKey
	thatExecutables, thatPendingTasks := incomingTracker.pendingExecutables, incomingTracker.pendingPerKey
	if len(thisExecutables) < len(thatExecutables) {
		thisExecutables, thatExecutables = thatExecutables, thisExecutables
		thisPendingTasks = thatPendingTasks
	}

	for key, executable := range thatExecutables {
		thisExecutables[key] = executable
		key := t.grouper.Key(executable)
		thisPendingTasks[key]++
	}
	t.pendingExecutables = thisExecutables
	t.pendingPerKey = thisPendingTasks
	return t
}

func (t *executableTracker) add(
	executable Executable,
) {
	t.pendingExecutables[executable.GetKey()] = executable
	key := t.grouper.Key(executable)
	t.pendingPerKey[key]++
}

func (t *executableTracker) shrink() (tasks.Key, int) {
	var tasksCompleted int
	minPendingTaskKey := tasks.MaximumKey
	for key, executable := range t.pendingExecutables {
		if executable.State() == ctasks.TaskStateAcked {
			t.pendingPerKey[t.grouper.Key(executable)]--
			delete(t.pendingExecutables, key)
			tasksCompleted++
			continue
		}

		minPendingTaskKey = tasks.MinKey(minPendingTaskKey, key)
	}

	for key, numPending := range t.pendingPerKey {
		if numPending == 0 {
			delete(t.pendingPerKey, key)
		}
	}

	return minPendingTaskKey, tasksCompleted
}

func (t *executableTracker) clear() {
	for _, executable := range t.pendingExecutables {
		executable.Cancel()
	}

	t.pendingExecutables = make(map[tasks.Key]Executable)
	t.pendingPerKey = make(map[any]int, 0)
}
