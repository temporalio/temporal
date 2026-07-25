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

	WorkflowIDKey = "temporalWorkflowID"
	BusinessIDKey = "temporalBusinessID"
	RunIDKey      = "temporalRunID"

	AttrWorkflowID    attribute.Key = "workflow.id"
	AttrRunID         attribute.Key = "workflow.run_id"
	AttrNamespaceID   attribute.Key = "namespace.id"
	AttrTaskQueue     attribute.Key = "task.queue"
	AttrUpdateID      attribute.Key = "update.id"
	AttrAbortReason   attribute.Key = "abort.reason"
	AttrUpdateOutcome attribute.Key = "update.outcome"

	// Generic CHASM transition telemetry (emitted by chasm.Transition.Apply under
	// TEMPORAL_OTEL_DEBUG). Carries the component identity so the umpire can observe
	// any CHASM component's lifecycle from one span-event type.
	AttrChasmComponentType         attribute.Key = "chasm.component.type"
	AttrChasmComponentPath         attribute.Key = "chasm.component.path"
	AttrChasmTransitionSource      attribute.Key = "chasm.transition.source"
	AttrChasmTransitionDestination attribute.Key = "chasm.transition.destination"

	AttrNexusEndpoint         attribute.Key = "nexus.endpoint"
	AttrNexusService          attribute.Key = "nexus.service"
	AttrNexusOperation        attribute.Key = "nexus.operation"
	AttrNexusScheduledEventID attribute.Key = "nexus.scheduled_event_id"
	AttrNexusOutcome          attribute.Key = "nexus.outcome"

	EventSpeculativeWorkflowTaskScheduled = "SpeculativeWorkflowTaskScheduled"
	EventWorkflowTaskStored               = "WorkflowTaskStored"
	EventWorkflowTaskDiscarded            = "WorkflowTaskDiscarded"
	EventWorkflowUpdateAborted            = "WorkflowUpdateAborted"
	EventWorkflowUpdateAdmitted           = "WorkflowUpdateAdmitted"
	EventWorkflowUpdateAccepted           = "WorkflowUpdateAccepted"
	EventWorkflowUpdateCompleted          = "WorkflowUpdateCompleted"
	EventWorkflowUpdateRejected           = "WorkflowUpdateRejected"
	EventWorkflowExecutionCompleted       = "WorkflowExecutionCompleted"
	EventWorkflowTerminated               = "WorkflowTerminated"

	EventNexusOperationScheduled     = "NexusOperationScheduled"
	EventNexusOperationAttemptFailed = "NexusOperationAttemptFailed" // scheduled -> backing_off
	EventNexusOperationStarted       = "NexusOperationStarted"
	EventNexusOperationSucceeded     = "NexusOperationSucceeded"
	EventNexusOperationFailed        = "NexusOperationFailed"
	EventNexusOperationCanceled      = "NexusOperationCanceled"
	EventNexusOperationTimedOut      = "NexusOperationTimedOut"

	EventChasmTransition = "chasm.transition"

	// UpdateOutcomeSuccess / UpdateOutcomeFailure are values for AttrUpdateOutcome.
	UpdateOutcomeSuccess = "success"
	UpdateOutcomeFailure = "failure"
)
