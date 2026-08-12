package umpire

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewRelationStoreRejectsInvalidSchemas(t *testing.T) {
	tests := []struct {
		name    string
		schemas []RelationSchema
	}{
		{name: "empty type", schemas: []RelationSchema{{Source: "Workflow", Target: "WorkflowRun"}}},
		{name: "empty source", schemas: []RelationSchema{{Type: "workflow-run", Target: "WorkflowRun"}}},
		{name: "empty target", schemas: []RelationSchema{{Type: "workflow-run", Source: "Workflow"}}},
		{name: "invalid source cardinality", schemas: []RelationSchema{{Type: "workflow-run", Source: "Workflow", Target: "WorkflowRun", SourceCardinality: RelationCardinality(10)}}},
		{name: "invalid target cardinality", schemas: []RelationSchema{{Type: "workflow-run", Source: "Workflow", Target: "WorkflowRun", TargetCardinality: RelationCardinality(10)}}},
		{name: "duplicate type", schemas: []RelationSchema{
			{Type: "workflow-run", Source: "Workflow", Target: "WorkflowRun"},
			{Type: "workflow-run", Source: "WorkflowRun", Target: "Workflow"},
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, err := NewRelationStore(test.schemas...)

			require.Nil(t, store)
			require.ErrorIs(t, err, ErrRelationSchema)
		})
	}
}

func TestRelationStoreAddQueryRemoveAndSnapshot(t *testing.T) {
	store, err := NewRelationStore(
		RelationSchema{Type: "workflow-run", Source: "Workflow", Target: "WorkflowRun", SourceCardinality: RelationMany, TargetCardinality: RelationOne},
		RelationSchema{Type: "run-successor", Source: "WorkflowRun", Target: "WorkflowRun", SourceCardinality: RelationOne, TargetCardinality: RelationOne},
	)
	require.NoError(t, err)
	wf := NewEntityID("Workflow", "wf")
	run1 := NewEntityID("WorkflowRun", "run-1")
	run2 := NewEntityID("WorkflowRun", "run-2")

	added, err := store.Add(RelationEdge{Type: "workflow-run", Source: wf, Target: run2})
	require.NoError(t, err)
	require.True(t, added)
	added, err = store.Add(RelationEdge{Type: "workflow-run", Source: wf, Target: run1})
	require.NoError(t, err)
	require.True(t, added)
	added, err = store.Add(RelationEdge{Type: "workflow-run", Source: wf, Target: run1})
	require.NoError(t, err)
	require.False(t, added)

	require.Equal(t, []EntityID{run1, run2}, store.Targets("workflow-run", wf))
	require.Equal(t, []EntityID{wf}, store.Sources("workflow-run", run1))
	require.Equal(t, []RelationEdge{
		{Type: "workflow-run", Source: wf, Target: run1},
		{Type: "workflow-run", Source: wf, Target: run2},
	}, store.Snapshot())

	snapshot := store.Snapshot()
	snapshot[0].Source.ID = "changed"
	require.Equal(t, wf, store.Snapshot()[0].Source)

	removed, err := store.Remove(RelationEdge{Type: "workflow-run", Source: wf, Target: run1})
	require.NoError(t, err)
	require.True(t, removed)
	removed, err = store.Remove(RelationEdge{Type: "workflow-run", Source: wf, Target: run1})
	require.NoError(t, err)
	require.False(t, removed)
	require.Empty(t, store.Sources("workflow-run", run1))
}

func TestRelationStoreRejectsInvalidEndpointsAndCardinalityAtomically(t *testing.T) {
	store, err := NewRelationStore(RelationSchema{
		Type:              "operation-handler",
		Source:            "NexusOperation",
		Target:            "NexusHandler",
		SourceCardinality: RelationOne,
		TargetCardinality: RelationMany,
	})
	require.NoError(t, err)
	op := NewEntityID("NexusOperation", "op")
	handler1 := NewEntityID("NexusHandler", "handler-1")
	handler2 := NewEntityID("NexusHandler", "handler-2")
	_, err = store.Add(RelationEdge{Type: "operation-handler", Source: op, Target: handler1})
	require.NoError(t, err)

	_, err = store.Add(RelationEdge{Type: "unknown", Source: op, Target: handler2})
	require.ErrorIs(t, err, ErrRelationSchema)
	_, err = store.Add(RelationEdge{Type: "operation-handler", Source: NewEntityID("Workflow", "wf"), Target: handler2})
	require.ErrorIs(t, err, ErrRelationEndpoint)
	_, err = store.Add(RelationEdge{Type: "operation-handler", Source: op, Target: NewEntityID("Workflow", "wf")})
	require.ErrorIs(t, err, ErrRelationEndpoint)
	_, err = store.Add(RelationEdge{Type: "operation-handler", Source: op, Target: handler2})
	require.ErrorIs(t, err, ErrRelationCardinality)

	require.Equal(t, []EntityID{handler1}, store.Targets("operation-handler", op))
	require.Empty(t, store.Sources("operation-handler", handler2))
}

func TestRelationStoreConcurrentQueries(t *testing.T) {
	store, err := NewRelationStore(RelationSchema{
		Type:              "workflow-run",
		Source:            "Workflow",
		Target:            "WorkflowRun",
		SourceCardinality: RelationMany,
		TargetCardinality: RelationOne,
	})
	require.NoError(t, err)
	wf := NewEntityID("Workflow", "wf")

	var writers sync.WaitGroup
	for i := range 32 {
		writers.Go(func() {
			_, addErr := store.Add(RelationEdge{
				Type:   "workflow-run",
				Source: wf,
				Target: NewEntityID("WorkflowRun", string(rune('a'+i))),
			})
			require.NoError(t, addErr)
			_ = store.Targets("workflow-run", wf)
			_ = store.Snapshot()
		})
	}
	writers.Wait()
	require.Len(t, store.Targets("workflow-run", wf), 32)
}

func TestRelationStoreErrorsAreStructured(t *testing.T) {
	store, err := NewRelationStore(RelationSchema{Type: "workflow-run", Source: "Workflow", Target: "WorkflowRun"})
	require.NoError(t, err)

	_, err = store.Add(RelationEdge{Type: "workflow-run", Source: NewEntityID("Workflow", ""), Target: NewEntityID("WorkflowRun", "run")})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrRelationEndpoint)
	var relationErr *RelationError
	require.ErrorAs(t, err, &relationErr)
	require.Equal(t, RelationType("workflow-run"), relationErr.Type)
}
