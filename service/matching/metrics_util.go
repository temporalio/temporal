package matching

import (
	"go.temporal.io/server/common/metrics"
)

// getDroppedTaskExpiryReasonTag returns the tasks_dropped reason tag for a task
// being thrown away due to in-memory expiry or validation failure.
func getDroppedTaskExpiryReasonTag(task *internalTask) metrics.Tag {
	if IsTaskExpired(task.event.AllocatedTaskInfo) {
		return metrics.DroppedTaskReasonExpiredMemoryTag
	}
	return metrics.DroppedTaskReasonInvalidTag
}

// recordDroppedTask emits the tasks_dropped metric, but only for backlog/spooled
// tasks. Sync-match tasks (including tasks forwarded from a child partition, which
// also carry a responseC) are handed straight to a poller rather than spooled, so
// dropping one is not a backlog-drain loss and is excluded here.
func recordDroppedTask(handler metrics.Handler, task *internalTask, reason metrics.Tag) {
	if task.isSyncMatchTask() {
		return
	}
	metrics.DroppedTasksCounter.With(handler).Record(1, reason)
}
