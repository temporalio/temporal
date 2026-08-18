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
)
