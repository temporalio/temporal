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
