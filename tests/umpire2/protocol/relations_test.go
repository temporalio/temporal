package protocol

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire2/fact"
	"go.temporal.io/server/tests/umpire2/model"
)

func TestDefaultRelationsDeriveWorkflowRunMembershipAndLineage(t *testing.T) {
	compiled, err := Default()
	require.NoError(t, err)
	store, err := compiled.NewRelationStore()
	require.NoError(t, err)

	errs := compiled.ApplyRelations(store, []umpire.Fact{&fact.WorkflowRunStarted{
		NamespaceID:   "namespace",
		WorkflowID:    "workflow",
		RunID:         "run-2",
		FirstRunID:    "run-1",
		PreviousRunID: "run-1",
		Initiator:     "continued_as_new",
	}})

	require.Empty(t, errs)
	require.Equal(t, []umpire.EntityID{
		umpire.NewEntityID(model.WorkflowRunType, "namespace\x00run-2"),
	}, store.Targets(
		WorkflowRunsRelation,
		umpire.NewEntityID(model.WorkflowType, "namespace\x00workflow"),
	))
	require.Equal(t, []umpire.EntityID{
		umpire.NewEntityID(model.WorkflowRunType, "namespace\x00run-2"),
	}, store.Targets(
		WorkflowRunSuccessorRelation,
		umpire.NewEntityID(model.WorkflowRunType, "namespace\x00run-1"),
	))
}

func TestDefaultRelationsDeriveNexusActivityLinksFromEitherSide(t *testing.T) {
	compiled, err := Default()
	require.NoError(t, err)
	store, err := compiled.NewRelationStore()
	require.NoError(t, err)
	operation := umpire.NewEntityID(model.NexusOperationType, "namespace\x00operation")
	activity := umpire.NewEntityID(model.ActivityType, "namespace\x00activity")
	activityLink := &commonpb.Link{Variant: &commonpb.Link_Activity_{Activity: &commonpb.Link_Activity{ActivityId: "activity"}}}
	operationLink := &commonpb.Link{Variant: &commonpb.Link_NexusOperation_{NexusOperation: &commonpb.Link_NexusOperation{OperationId: "operation"}}}

	errs := compiled.ApplyRelations(store, []umpire.Fact{
		fact.NewNexusOperationExecutionSnapshot("namespace", "operation", []*commonpb.Link{activityLink}),
		fact.NewActivityExecutionSnapshot("namespace", "activity", 0, []*commonpb.Link{operationLink}),
	})

	require.Empty(t, errs)
	require.Equal(t, []umpire.EntityID{activity}, store.Targets(NexusActivityRelation, operation))
	require.Equal(t, []umpire.EntityID{operation}, store.Targets(ActivityNexusRelation, activity))
	require.Len(t, store.Snapshot(), 2)
}

func TestDefaultRelationsDeriveIdempotentCallbackTargets(t *testing.T) {
	compiled, err := Default()
	require.NoError(t, err)
	store, err := compiled.NewRelationStore()
	require.NoError(t, err)
	callback := umpire.NewEntityID(model.CallbackType, "namespace\x00callback")
	operation := umpire.NewEntityID(model.NexusOperationType, "namespace\x00operation")
	handlerRun := umpire.NewEntityID(model.WorkflowRunType, "namespace\x00handler-run")
	facts := []umpire.Fact{
		&fact.NexusCallbackObservation{NamespaceID: "namespace", CallbackID: "callback", OperationID: "operation"},
		&fact.NexusCallbackObservation{NamespaceID: "namespace", CallbackID: "callback", OperationID: "operation"},
		&fact.WorkflowCallbackAttachment{NamespaceID: "namespace", CallbackID: "callback", HandlerRunID: "handler-run"},
	}

	require.Empty(t, compiled.ApplyRelations(store, facts))
	require.Equal(t, []umpire.EntityID{operation}, store.Targets(CallbackOperationRelation, callback))
	require.Equal(t, []umpire.EntityID{handlerRun}, store.Targets(CallbackHandlerRunRelation, callback))
	require.Equal(t, []umpire.EntityID{callback}, store.Sources(CallbackHandlerRunRelation, handlerRun))
	require.Len(t, store.Snapshot(), 2)
}

func TestDefaultRelationsRejectCallbackConflictsWithoutReplacingAcceptedEdge(t *testing.T) {
	compiled, err := Default()
	require.NoError(t, err)
	store, err := compiled.NewRelationStore()
	require.NoError(t, err)
	callbackFact := func(operation string) umpire.Fact {
		return &fact.NexusCallbackObservation{NamespaceID: "namespace", CallbackID: "callback", OperationID: operation}
	}
	require.Empty(t, compiled.ApplyRelations(store, []umpire.Fact{callbackFact("accepted")}))
	errs := compiled.ApplyRelations(store, []umpire.Fact{callbackFact("conflict")})
	require.Len(t, errs, 1)
	require.ErrorIs(t, errs[0], umpire.ErrRelationCardinality)
	require.Equal(t, []umpire.EntityID{
		umpire.NewEntityID(model.NexusOperationType, "namespace\x00accepted"),
	}, store.Targets(CallbackOperationRelation, umpire.NewEntityID(model.CallbackType, "namespace\x00callback")))

	require.Equal(t, 1, store.PurgeScope(umpire.NewEntityID(model.NamespaceType, "namespace")))
	require.Empty(t, store.Snapshot())
}
