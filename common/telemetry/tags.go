package telemetry

// Attributes
const (
	ComponentPersistence     = "persistence"
	ComponentQueueArchival   = "queue.archival"
	ComponentQueueMemory     = "queue.memory"
	ComponentQueueOutbound   = "queue.outbound"
	ComponentQueueTimer      = "queue.timer"
	ComponentQueueTransfer   = "queue.transfer"
	ComponentQueueVisibility = "queue.visibility"
	ComponentUpdateRegistry  = "update.registry"

	TimeoutTypeKey      = "temporalTimeoutType"
	WorkflowIDKey       = "temporalWorkflowID"
	WorkflowRunIDKey    = "temporalRunID"
	WorkflowTaskTypeKey = "temporalWorkflowTaskType"
)

// Events
const (
	// WorkflowTaskTimeout is emitted when a workflow task times out.
	// Use the TimeoutType attribute to distinguish between timeout types
	// (e.g., TIMEOUT_TYPE_SCHEDULE_TO_START, TIMEOUT_TYPE_START_TO_CLOSE).
	WorkflowTaskTimeout = "temporalWorkflowTaskTimeout"
)
