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

// CheckInternalPerNsTaskQueueAllowed tries to block the usage of internal per-namespace task queue for illegal cases.
// Parameters:
//   - targetTaskQueue: The task queue of the component.
//   - parentTaskQueue: The task queue of the parent component can be empty if the component has no parent.
//
// Returns an error if the usage is illegal, or nil if it's allowed.
func CheckInternalPerNsTaskQueueAllowed(targetTaskQueue, parentTaskQueue string) error {
	if targetTaskQueue == "" {
		return serviceerror.NewInvalidArgument("target task queue is not set")
	}
	if !IsInternalPerNsTaskQueue(targetTaskQueue) {
		return nil
	}
	if !IsInternalPerNsTaskQueue(parentTaskQueue) {
		return serviceerror.NewInvalidArgumentf(
			"cannot use internal per namespace task queue:%s (in parent component task queue: %s)",
			targetTaskQueue,
			parentTaskQueue,
		)
	}
	return nil
}
