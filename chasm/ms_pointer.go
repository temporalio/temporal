package chasm

import (
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"google.golang.org/protobuf/proto"
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

func (m MSPointer) GenerateEventLoadToken(event *historypb.HistoryEvent) ([]byte, error) {
	return m.backend.GenerateEventLoadToken(event)
}

// LoadHistoryEvent loads a history event from the underlying mutable state using the given token.
func (m MSPointer) LoadHistoryEvent(ctx Context, token []byte) (*historypb.HistoryEvent, error) {
	return m.backend.LoadHistoryEvent(ctx.goContext(), token)
}

// GetNexusCompletion retrieves the Nexus operation completion data for the given request ID from the underlying mutable state.
func (m MSPointer) GetNexusCompletion(ctx Context, requestID string) (nexusrpc.CompleteOperationOptions, error) {
	return m.backend.GetNexusCompletion(ctx.goContext(), requestID)
}

// GetWorkflowTypeName retrieves the workflow type name from the underlying mutable state.
func (m MSPointer) GetWorkflowTypeName() string {
	return m.backend.GetExecutionInfo().GetWorkflowTypeName()
}

// GetNexusUpdateCompletion retrieves the Nexus operation completion data for the given update ID and request ID from the underlying mutable state.
func (m MSPointer) GetNexusUpdateCompletion(ctx Context, updateID string, requestID string) (nexusrpc.CompleteOperationOptions, error) {
	return m.backend.GetNexusUpdateCompletion(ctx.goContext(), updateID, requestID)
}

// GetPredefinedSearchAttributes retrieves the predefined search attributes from the underlying mutable state.
func (m MSPointer) GetPredefinedSearchAttributes() (map[string]VisibilityValue, error) {
	msSearchAttributes := m.backend.GetExecutionInfo().GetSearchAttributes()
	predefinedWithType := make(map[string]*commonpb.Payload)
	for saName, saPayload := range msSearchAttributes {
		if saType, ok := predefinedSearchAttributes[saName]; ok {
			if sadefs.GetMetadataType(saPayload) == enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED {
				saPayload = proto.CloneOf(saPayload)
				sadefs.SetMetadataType(saPayload, saType)
			}
			predefinedWithType[saName] = saPayload
		}
	}
	saMap, err := newSearchAttributesMapFromProto(
		&commonpb.SearchAttributes{
			IndexedFields: predefinedWithType,
		},
	)
	return saMap.values, err
}
