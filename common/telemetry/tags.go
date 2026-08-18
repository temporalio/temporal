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

	NexusEndpointKey  = "nexus.endpoint"
	NexusNamespaceKey = "nexus.namespace"
	NexusOperationKey = "nexus.operation"
	NexusRequestIDKey = "nexus.request_id"
	NexusServiceKey   = "nexus.service"

	WorkerTaskTypeKey = "worker_task.type"
	WorkerTaskIDKey   = "worker_task.id"

	WorkerTaskNamespaceIDKey = "worker_task.namespace_id"
	WorkerTaskWorkflowIDKey  = "worker_task.workflow_id"
	WorkerTaskRunIDKey       = "worker_task.run_id"
	WorkerTaskActivityIDKey  = "worker_task.activity_id"
	WorkerTaskTaskQueueKey   = "worker_task.task_queue"

	WorkerTaskTypeWorkflow = "workflow"
	WorkerTaskTypeActivity = "activity"
	WorkerTaskTypeNexus    = "nexus"
)
