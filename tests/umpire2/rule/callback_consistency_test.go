package rule

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire2/fact"
	"go.temporal.io/server/tests/umpire2/model"
	"go.temporal.io/server/tests/umpire2/protocol"
)

func TestCallbackReferenceConsistencyAcceptsMatchingEventReference(t *testing.T) {
	state, store := callbackReferenceState(t, 0, nil)
	require.Empty(t, umpire.CheckSafetyRule(context.Background(), &CallbackReferenceConsistency{}, state, log.NewNoopLogger(), umpire.RuleConfig{Relations: store}))
}

func TestCallbackReferenceConsistencyAcceptsMatchingRequestReference(t *testing.T) {
	state, store := callbackReferenceState(t, 0, func(attachment *fact.WorkflowCallbackAttachment, started *fact.NexusOperationStartedHistory) {
		attachment.AttachmentEventID = 7
		attachment.AttachmentEventTime = attachment.HandlerWorkflowStartTime.Add(time.Minute)
		attachment.ReferenceKind = "request"
		attachment.ReferenceValue = "attach-request-id"
		attachment.ReferencedEventType = enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED
		started.ReferenceKind = attachment.ReferenceKind
		started.ReferenceValue = attachment.ReferenceValue
		started.ReferencedEventType = attachment.ReferencedEventType
		started.SetEventTime(attachment.AttachmentEventTime.Add(time.Second))
	})

	require.Empty(t, umpire.CheckSafetyRule(context.Background(), &CallbackReferenceConsistency{}, state, log.NewNoopLogger(), umpire.RuleConfig{Relations: store}))
}

func TestCallbackReferenceConsistencyRejectsStartBeforeReferencedEvent(t *testing.T) {
	state, store := callbackReferenceState(t, -time.Second, nil)
	violations := umpire.CheckSafetyRule(context.Background(), &CallbackReferenceConsistency{}, state, log.NewNoopLogger(), umpire.RuleConfig{Relations: store})
	require.Len(t, violations, 1)
	require.Contains(t, violations[0].Message, "callback reference")
}

func TestCallbackReferenceConsistencyRejectsWrongHandlerRun(t *testing.T) {
	state, store := callbackReferenceState(t, 0, func(_ *fact.WorkflowCallbackAttachment, started *fact.NexusOperationStartedHistory) {
		started.HandlerRunID = "wrong-run-id"
	})

	violations := umpire.CheckSafetyRule(context.Background(), &CallbackReferenceConsistency{}, state, log.NewNoopLogger(), umpire.RuleConfig{Relations: store})
	require.Len(t, violations, 1)
}

func TestCallbackReferenceConsistencyRejectsMalformedReference(t *testing.T) {
	state, store := callbackReferenceState(t, 0, func(attachment *fact.WorkflowCallbackAttachment, _ *fact.NexusOperationStartedHistory) {
		attachment.Malformed = true
		attachment.ErrorClass = "invalid-attachment-reference"
		attachment.ReferenceKind = ""
		attachment.ReferenceValue = ""
	})

	violations := umpire.CheckSafetyRule(context.Background(), &CallbackReferenceConsistency{}, state, log.NewNoopLogger(), umpire.RuleConfig{Relations: store})
	require.Len(t, violations, 1)
}

func TestCallbackReferenceConsistencyWaitsForRelations(t *testing.T) {
	state := newTestModelState()
	attachment := callbackAttachmentFact()
	routeFact(t, state, attachment)
	compiled, err := protocol.Default()
	require.NoError(t, err)
	store, err := compiled.NewRelationStore()
	require.NoError(t, err)

	require.Empty(t, umpire.CheckSafetyRule(context.Background(), &CallbackReferenceConsistency{}, state, log.NewNoopLogger(), umpire.RuleConfig{Relations: store}))
}

func TestCallbackResponseConsistencyAcceptsLateDuplicateAndRejectsConflict(t *testing.T) {
	state, store := callbackReferenceState(t, 0, nil)
	routeFact(t, state, makeNexusSucceeded("caller-id", "5"))
	firstAt := time.Date(2026, time.August, 12, 20, 0, 0, 0, time.UTC)
	response := callbackResponseFact("async_success", "fingerprint", firstAt)
	routeFact(t, state, response)
	routeFact(t, state, response)
	require.Empty(t, umpire.CheckSafetyRule(context.Background(), &CallbackResponseConsistency{}, state, log.NewNoopLogger(), umpire.RuleConfig{Relations: store}))

	conflict := callbackResponseFact("failure", "conflict", firstAt.Add(time.Second))
	routeFact(t, state, conflict)
	violations := umpire.CheckSafetyRule(context.Background(), &CallbackResponseConsistency{}, state, log.NewNoopLogger(), umpire.RuleConfig{Relations: store})
	require.Len(t, violations, 1)
	require.Contains(t, violations[0].Message, "conflicting Nexus start responses")
}

func callbackReferenceState(
	t *testing.T,
	startOffset time.Duration,
	mutate func(*fact.WorkflowCallbackAttachment, *fact.NexusOperationStartedHistory),
) (*umpire.ModelState, *umpire.RelationStore) {
	t.Helper()
	state := newTestModelState()
	attachment := callbackAttachmentFact()
	observation := &fact.NexusCallbackObservation{
		NamespaceID: "namespace-id",
		CallbackID:  "callback-id",
		OperationID: "caller-id:5",
		EntityPath:  callbackPath(),
	}
	started := &fact.NexusOperationStartedHistory{
		NamespaceID:         "namespace-id",
		WorkflowID:          "caller-id",
		ScheduledEventID:    "5",
		HandlerWorkflowID:   "handler-id",
		HandlerRunID:        "handler-run-id",
		ReferenceKind:       "event",
		ReferenceValue:      "1",
		ReferencedEventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		EntityPath:          nexusPath("caller-id", "5"),
	}
	started.SetEventTime(attachment.HandlerWorkflowStartTime.Add(startOffset))
	if mutate != nil {
		mutate(attachment, started)
	}
	for _, observed := range []umpire.Fact{attachment, observation, makeNexusScheduled("caller-id", "5"), started} {
		routeFact(t, state, observed)
	}
	compiled, err := protocol.Default()
	require.NoError(t, err)
	store, err := compiled.NewRelationStore()
	require.NoError(t, err)
	require.Empty(t, compiled.ApplyRelations(store, []umpire.Fact{attachment, observation}))
	return state, store
}

func callbackAttachmentFact() *fact.WorkflowCallbackAttachment {
	startedAt := time.Date(2026, time.August, 12, 19, 0, 0, 0, time.UTC)
	return &fact.WorkflowCallbackAttachment{
		NamespaceID:              "namespace-id",
		CallbackID:               "callback-id",
		HandlerWorkflowID:        "handler-id",
		HandlerRunID:             "handler-run-id",
		HandlerWorkflowStartTime: startedAt,
		AttachmentEventTime:      startedAt,
		AttachmentEventID:        1,
		ReferenceKind:            "event",
		ReferenceValue:           "1",
		ReferencedEventType:      enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		EntityPath:               callbackPath(),
	}
}

func callbackResponseFact(kind, fingerprint string, observedAt time.Time) *fact.NexusStartResponse {
	return &fact.NexusStartResponse{
		NamespaceID:         "namespace-id",
		CallbackID:          "callback-id",
		DeliveryID:          "delivery-id",
		ResponseKind:        kind,
		ResponseFingerprint: fingerprint,
		ObservedAt:          observedAt,
		EntityPath:          callbackPath(),
	}
}

func callbackPath() *umpire.EntityPath {
	return &umpire.EntityPath{
		EntityID:  umpire.NewEntityID(model.CallbackType, "callback-id"),
		Ancestors: []umpire.EntityID{umpire.NewEntityID(model.NamespaceType, "namespace-id")},
	}
}
