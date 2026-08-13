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

	NamespaceKey = "temporal.namespace"

	NexusEndpointKey  = "temporal.nexus.endpoint"
	NexusNamespaceKey = "temporal.nexus.namespace"
	NexusOperationKey = "temporal.nexus.operation"
	NexusRequestKey   = "temporal.nexus.request"
	NexusRequestIDKey = "temporal.nexus.request_id"
	NexusServiceKey   = "temporal.nexus.service"
)
