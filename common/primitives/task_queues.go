package primitives

import (
	"go.temporal.io/api/serviceerror"
)

// all internal task queues shall be defined here such that we enhance security on top of them
const (
	DefaultWorkerTaskQueue = "default-worker-tq"
	PerNSWorkerTaskQueue   = "temporal-sys-per-ns-tq"

	MigrationActivityTQ           = "temporal-sys-migration-activity-tq"
	AddSearchAttributesActivityTQ = "temporal-sys-add-search-attributes-activity-tq"
	DeleteNamespaceActivityTQ     = "temporal-sys-delete-namespace-activity-tq"
	DLQActivityTQ                 = "temporal-sys-dlq-activity-tq"
)

func IsInternalPerNsTaskQueue(taskQueue string) bool {
	return taskQueue == PerNSWorkerTaskQueue
}

// CheckNotInternalPerNsTaskQueue is mainly used for workflow task queues.
func CheckNotInternalPerNsTaskQueue(taskQueue string) error {
	if IsInternalPerNsTaskQueue(taskQueue) {
		return serviceerror.NewInvalidArgumentf("cannot use internal per-ns-tq, taskQueue=%s", taskQueue)
	}
	return nil
}

// CheckInternalPerNsTaskQueueAllowed is mainly used for activities, child workflows, and other internal components.
// Only permit use of the internal task queue by activities or child workflows
// if invoked from a workflow that itself uses the internal task queue.
func CheckInternalPerNsTaskQueueAllowed(parentTaskQueue, childTaskQueue string) error {
	if parentTaskQueue == "" && childTaskQueue == "" {
		return nil // we don't handle both empty string case logic here
	}
	if !IsInternalPerNsTaskQueue(childTaskQueue) {
		return nil
	}
	if !IsInternalPerNsTaskQueue(parentTaskQueue) {
		return serviceerror.NewInvalidArgumentf(
			"cannot use internal per-ns-tq, parentTaskQueue=%s, childTaskQueue=%s",
			parentTaskQueue,
			childTaskQueue,
		)
	}
	return nil
}
