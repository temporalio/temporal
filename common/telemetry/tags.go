package telemetry

import "go.opentelemetry.io/otel/attribute"

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

	AttrWorkflowID  attribute.Key = "workflow.id"
	AttrRunID       attribute.Key = "workflow.run_id"
	AttrNamespaceID attribute.Key = "namespace.id"
	AttrTaskQueue   attribute.Key = "task.queue"
	AttrUpdateID    attribute.Key = "update.id"
	AttrAbortReason attribute.Key = "abort.reason"

	EventSpeculativeWorkflowTaskScheduled = "SpeculativeWorkflowTaskScheduled"
	EventWorkflowTaskStored               = "WorkflowTaskStored"
	EventWorkflowTaskDiscarded            = "WorkflowTaskDiscarded"
	EventWorkflowUpdateAborted            = "WorkflowUpdateAborted"
	EventWorkflowTerminated               = "WorkflowTerminated"
)
