package primitives

// all internal task queues shall be defined here such that we enhance security on top of them
const (
	DefaultWorkerTaskQueue = "default-worker-tq"
	PerNSWorkerTaskQueue   = "temporal-sys-per-ns-tq"

	MigrationActivityTQ           = "temporal-sys-migration-activity-tq"
	AddSearchAttributesActivityTQ = "temporal-sys-add-search-attributes-activity-tq"
	DeleteNamespaceActivityTQ     = "temporal-sys-delete-namespace-activity-tq"
	DLQActivityTQ                 = "temporal-sys-dlq-activity-tq"
)

func IsInternalTaskQueueForUserNs(taskQueue string) bool {
	return taskQueue == PerNSWorkerTaskQueue
}
