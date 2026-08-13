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

	AttrWorkflowID attribute.Key = "workflow.id"
	AttrRunID      attribute.Key = "workflow.run_id"
	// AttrFirstRunID / AttrPreviousRunID carry a run's lineage: the chain root and the immediate
	// predecessor (continue-as-new / reset / retry). Empty previous means a first run. They let an
	// observer reconstruct the run graph under a WorkflowID (see UMPIRE_IDENTITY.md).
	AttrFirstRunID    attribute.Key = "workflow.first_run_id"
	AttrPreviousRunID attribute.Key = "workflow.previous_run_id"
	// AttrRunInitiator labels how a successor run was created — the typed graph edge from its
	// predecessor. One of the RunInitiator* values below; empty for a first run.
	AttrRunInitiator           attribute.Key = "workflow.run_initiator"
	AttrWorkflowCloseOutcome   attribute.Key = "workflow.close_outcome"
	AttrWorkflowSuccessorRunID attribute.Key = "workflow.successor_run_id"
	AttrNamespaceID            attribute.Key = "namespace.id"
	AttrTaskQueue              attribute.Key = "task.queue"
	AttrUpdateID               attribute.Key = "update.id"
	AttrAbortReason            attribute.Key = "abort.reason"
	AttrUpdateOutcome          attribute.Key = "update.outcome"

	// Generic CHASM transition telemetry (emitted by chasm.Transition.Apply under
	// TEMPORAL_OTEL_DEBUG). Carries the component identity so the umpire can observe
	// any CHASM component's lifecycle from one span-event type.
	AttrChasmComponentType         attribute.Key = "chasm.component.type"
	AttrChasmComponentPath         attribute.Key = "chasm.component.path"
	AttrChasmTransitionSource      attribute.Key = "chasm.transition.source"
	AttrChasmTransitionDestination attribute.Key = "chasm.transition.destination"
	// AttrChasmTransitionEvent is the Go type of the event that triggered the transition
	// (e.g. "nexusoperation.EventStarted"), so an observer can identify the transition
	// directly instead of inferring it from the source/destination state pair.
	AttrChasmTransitionEvent attribute.Key = "chasm.transition.event"
	// AttrChasmTransitionAttempt is an optional component-contributed attribute: the
	// attempt count at the time of the transition, for components that retry.
	AttrChasmTransitionAttempt attribute.Key = "chasm.transition.attempt"

	AttrNexusEndpoint         attribute.Key = "nexus.endpoint"
	AttrNexusService          attribute.Key = "nexus.service"
	AttrNexusOperation        attribute.Key = "nexus.operation"
	AttrNexusScheduledEventID attribute.Key = "nexus.scheduled_event_id"
	AttrNexusOutcome          attribute.Key = "nexus.outcome"
	// AttrNexusRequestID is the operation's stable per-operation identity, present on
	// every transition (including the scheduling one, before the component is attached
	// to the tree and has a resolvable path).
	AttrNexusRequestID attribute.Key = "nexus.request_id"

	EventSpeculativeWorkflowTaskScheduled = "SpeculativeWorkflowTaskScheduled"
	EventWorkflowTaskStored               = "WorkflowTaskStored"
	EventWorkflowTaskDiscarded            = "WorkflowTaskDiscarded"
	EventWorkflowUpdateAborted            = "WorkflowUpdateAborted"
	EventWorkflowUpdateAdmitted           = "WorkflowUpdateAdmitted"
	EventWorkflowUpdateAccepted           = "WorkflowUpdateAccepted"
	EventWorkflowUpdateCompleted          = "WorkflowUpdateCompleted"
	EventWorkflowUpdateRejected           = "WorkflowUpdateRejected"
	EventWorkflowExecutionStarted         = "WorkflowExecutionStarted"
	EventWorkflowExecutionCompleted       = "WorkflowExecutionCompleted"
	EventWorkflowExecutionContinuedAsNew  = "WorkflowExecutionContinuedAsNew"
	EventWorkflowExecutionClosed          = "WorkflowExecutionClosed"
	EventWorkflowTerminated               = "WorkflowTerminated"

	// RunInitiator* are the values of AttrRunInitiator — the typed run-graph edge from a
	// predecessor to the successor it created.
	RunInitiatorContinuedAsNew = "continued_as_new"
	RunInitiatorRetry          = "retry"
	RunInitiatorCron           = "cron"
	RunInitiatorReset          = "reset"

	WorkflowCloseOutcomeCompleted      = "completed"
	WorkflowCloseOutcomeFailed         = "failed"
	WorkflowCloseOutcomeCanceled       = "canceled"
	WorkflowCloseOutcomeTerminated     = "terminated"
	WorkflowCloseOutcomeTimedOut       = "timed_out"
	WorkflowCloseOutcomeContinuedAsNew = "continued_as_new"

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
