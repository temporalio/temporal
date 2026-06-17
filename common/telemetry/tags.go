package telemetry

const (
	ComponentPersistence     = "persistence"
	ComponentQueueArchival   = "queue.archival"
	ComponentQueueMemory     = "queue.memory"
	ComponentQueueOutbound   = "queue.outbound"
	ComponentQueueTimer      = "queue.timer"
	ComponentQueueTransfer   = "queue.transfer"
	ComponentQueueVisibility = "queue.visibility"
	ComponentUpdateRegistry  = "update.registry"

	WorkflowIDKey = "temporalWorkflowID"
	BusinessIDKey = "temporalBusinessID"
	RunIDKey      = "temporalRunID"

	WorkerTaskTypeKey = "temporal.worker_task.type"
	WorkerTaskIDKey   = "temporal.worker_task.id"

	WorkerTaskNamespaceIDKey = "temporal.worker_task.namespace_id"
	WorkerTaskWorkflowIDKey  = "temporal.worker_task.workflow_id"
	WorkerTaskRunIDKey       = "temporal.worker_task.run_id"
	WorkerTaskActivityIDKey  = "temporal.worker_task.activity_id"
	WorkerTaskTaskQueueKey   = "temporal.worker_task.task_queue"

	WorkerTaskTypeWorkflow = "workflow"
	WorkerTaskTypeActivity = "activity"
	WorkerTaskTypeNexus    = "nexus"
)
