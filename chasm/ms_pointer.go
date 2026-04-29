package chasm

import (
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/common/nexus/nexusrpc"
)

// MSPointer is a special CHASM type which components can use to access their Node's underlying backend (i.e. mutable
// state). It is used to expose methods needed from the mutable state without polluting the chasm.Context interface.
// When deserializing components with fields of this type, the CHASM engine will set the value to its NodeBackend.
// This should only be used by the Workflow component.
type MSPointer struct {
	backend NodeBackend
}

// NewMSPointer creates a new MSPointer instance.
func NewMSPointer(backend NodeBackend) MSPointer {
	return MSPointer{
		backend: backend,
	}
}

// WorkflowRunTimeout returns the workflow run timeout duration. Returns 0 if no timeout is set.
func (m MSPointer) WorkflowRunTimeout() time.Duration {
	return m.backend.GetExecutionInfo().GetWorkflowRunTimeout().AsDuration()
}

// AddHistoryEvent adds a history event via the underlying mutable state.
func (m MSPointer) AddHistoryEvent(t enumspb.EventType, setAttributes func(*historypb.HistoryEvent)) *historypb.HistoryEvent {
	return m.backend.AddHistoryEvent(t, setAttributes)
}

// HasAnyBufferedEvent returns true if there is at least one buffered event that matches the provided filter.
func (m MSPointer) HasAnyBufferedEvent(filter func(*historypb.HistoryEvent) bool) bool {
	return m.backend.HasAnyBufferedEvent(filter)
}

// LoadHistoryEvent loads a history event from the underlying mutable state using the given token.
func (m MSPointer) LoadHistoryEvent(ctx Context, token []byte) (*historypb.HistoryEvent, error) {
	return m.backend.LoadHistoryEvent(ctx.goContext(), token)
}

// GetNexusCompletion retrieves the Nexus operation completion data for the given request ID from the underlying mutable state.
func (m MSPointer) GetNexusCompletion(ctx Context, requestID string) (nexusrpc.CompleteOperationOptions, error) {
	return m.backend.GetNexusCompletion(ctx.goContext(), requestID)
}

// ScheduleWorkflowTask schedules a new workflow task if one is not already pending.
// This is called by embedded activity components when they complete, to resume the workflow.
func (m MSPointer) ScheduleWorkflowTask() error {
	return m.backend.ScheduleWorkflowTask()
}

// WriteActivityTaskStartedHistoryEvent writes an ActivityTaskStarted history event for a
// workflow-embedded CHASM activity (buffered — the event ID is assigned during flush).
func (m MSPointer) WriteActivityTaskStartedHistoryEvent(
	scheduledEventID int64,
	attempt int32,
	requestID string,
	identity string,
	stamp *commonpb.WorkerVersionStamp,
) error {
	return m.backend.WriteActivityTaskStartedHistoryEvent(scheduledEventID, attempt, requestID, identity, stamp)
}

// WriteActivityTaskCompletedHistoryEvent writes an ActivityTaskCompleted history event for a
// workflow-embedded CHASM activity. Pass startedEventID=0 when both started and completed events
// are written in the same transaction — the history builder wires the event ID automatically.
func (m MSPointer) WriteActivityTaskCompletedHistoryEvent(
	scheduledEventID int64,
	startedEventID int64,
	identity string,
	result *commonpb.Payloads,
) error {
	return m.backend.WriteActivityTaskCompletedHistoryEvent(scheduledEventID, startedEventID, identity, result)
}

// WriteActivityTaskFailedHistoryEvent writes an ActivityTaskFailed history event for a
// workflow-embedded CHASM activity. Pass startedEventID=0 when both started and failed events
// are written in the same transaction — the history builder wires the event ID automatically.
func (m MSPointer) WriteActivityTaskFailedHistoryEvent(
	scheduledEventID int64,
	startedEventID int64,
	failure *failurepb.Failure,
	retryState enumspb.RetryState,
	identity string,
) error {
	return m.backend.WriteActivityTaskFailedHistoryEvent(scheduledEventID, startedEventID, failure, retryState, identity)
}

// WriteActivityTaskTimedOutHistoryEvent writes an ActivityTaskTimedOut history event for a
// workflow-embedded CHASM activity. Pass startedEventID=0 when both started and timed-out events
// are written in the same transaction — the history builder wires the event ID automatically.
func (m MSPointer) WriteActivityTaskTimedOutHistoryEvent(
	scheduledEventID int64,
	startedEventID int64,
	timeoutFailure *failurepb.Failure,
	retryState enumspb.RetryState,
) error {
	return m.backend.WriteActivityTaskTimedOutHistoryEvent(scheduledEventID, startedEventID, timeoutFailure, retryState)
}
