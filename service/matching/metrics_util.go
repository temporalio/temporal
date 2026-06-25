package matching

import (
	"go.temporal.io/server/common/metrics"
)

// dropReason is why a backlog task was dropped; it is recorded as the `reason` tag on
// tasks_dropped. The zero value (dropReasonUnspecified) means the task was not dropped.
type dropReason int

const (
	dropReasonUnspecified dropReason = iota
	dropReasonNotFound
	dropReasonInternalError
	dropReasonDataLoss
	dropReasonExpiredRead
	dropReasonExpiredMemory
	dropReasonInvalid
)

// tag maps the drop reason to its tasks_dropped `reason` metric tag.
func (r dropReason) tag() metrics.Tag {
	switch r {
	case dropReasonNotFound:
		return metrics.ReasonTag(metrics.DroppedTaskReasonNotFound)
	case dropReasonInternalError:
		return metrics.ReasonTag(metrics.DroppedTaskReasonInternalError)
	case dropReasonDataLoss:
		return metrics.ReasonTag(metrics.DroppedTaskReasonDataLoss)
	case dropReasonExpiredRead:
		return metrics.ReasonTag(metrics.DroppedTaskReasonExpiredRead)
	case dropReasonExpiredMemory:
		return metrics.ReasonTag(metrics.DroppedTaskReasonExpiredMemory)
	default:
		return metrics.ReasonTag(metrics.DroppedTaskReasonInvalid)
	}
}

// getInvalidTaskTag returns the tasks_expired stage tag for a task being dropped
// due to in-memory expiry or validation failure.
func getInvalidTaskTag(task *internalTask) metrics.Tag {
	if IsTaskExpired(task.event.AllocatedTaskInfo) {
		return metrics.TaskExpireStageMemoryTag
	}
	return metrics.TaskInvalidTag
}

// getDroppedTaskExpiryReason returns the drop reason for a task being dropped due to
// in-memory expiry or validation failure.
func getDroppedTaskExpiryReason(task *internalTask) dropReason {
	if IsTaskExpired(task.event.AllocatedTaskInfo) {
		return dropReasonExpiredMemory
	}
	return dropReasonInvalid
}

// recordDroppedTask records the tasks_dropped counter on the given physical-queue
// handler. It is a no-op when reason is dropReasonUnspecified (a non-drop completion).
func recordDroppedTask(handler metrics.Handler, reason dropReason) {
	if reason == dropReasonUnspecified {
		return
	}
	metrics.DroppedTasksCounter.With(handler).Record(1, reason.tag())
}
