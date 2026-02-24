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

	WorkflowIDKey    = "temporalWorkflowID"
	WorkflowRunIDKey = "temporalRunID"

	// NexusHandlerWorkflowIDKey is stamped on the DispatchNexusTask HTTP span when
	// the nexus operation starts a handler workflow asynchronously. It carries the
	// handler workflow ID so spandb can link the caller and handler workflow traces
	// even though they are in separate OTEL traces (History emits no outgoing span).
	NexusHandlerWorkflowIDKey = "temporalNexusHandlerWorkflowID"
)
