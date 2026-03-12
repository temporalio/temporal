package chasm

import (
	"time"

	enumspb "go.temporal.io/api/enums/v1"
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

// GetNexusCompletion retrieves the Nexus operation completion data for the given request ID from the underlying mutable state.
func (m MSPointer) GetNexusCompletion(ctx Context, requestID string) (nexusrpc.CompleteOperationOptions, error) {
	return m.backend.GetNexusCompletion(ctx.goContext(), requestID)
}
