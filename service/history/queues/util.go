package queues

import (
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/service/history/tasks"
)

func IsTaskAcked(
	task tasks.Task,
	persistenceQueueState *persistencespb.QueueState,
) bool {
	queueState := FromPersistenceQueueState(persistenceQueueState)
	taskKey := task.GetKey()
	if taskKey.CompareTo(queueState.exclusiveReaderHighWatermark) >= 0 {
		return false
	}

	for _, scopes := range queueState.readerScopes {
		for _, scope := range scopes {
			if taskKey.CompareTo(scope.Range.InclusiveMin) < 0 {
				// scopes are ordered for each reader, so if task is less the current
				// range's min, it can not be contained by this or later scopes of this
				// reader.
				break
			}

			if scope.Contains(task) {
				return false
			}
		}
	}

	return true
}
