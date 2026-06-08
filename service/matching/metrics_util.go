package matching

import (
	"go.temporal.io/server/common/metrics"
)

func getInvalidTaskTag(task *internalTask) metrics.Tag {
	if IsTaskExpired(task.event.AllocatedTaskInfo) {
		return metrics.TaskExpireStageMemoryTag
	}
	return metrics.TaskInvalidTag
}

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
// also carry a responseC) are real-time dispatches whose failure is returned to the
// AddTask caller, not backlog we failed to drain, so they are intentionally excluded.
func recordDroppedTask(handler metrics.Handler, task *internalTask, reason metrics.Tag) {
	if task.isSyncMatchTask() {
		return
	}
	metrics.DroppedTasksCounter.With(handler).Record(1, reason)
}
