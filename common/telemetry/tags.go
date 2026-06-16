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

	ActivityIDKey = "temporalActivityID"
	BusinessIDKey = "temporalBusinessID"
	RunIDKey      = "temporalRunID"
	WorkflowIDKey = "temporalWorkflowID"
)
