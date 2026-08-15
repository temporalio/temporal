package umpire

import (
	"context"
	"iter"
	"testing"

	"github.com/stretchr/testify/require"
)

type RuntimeTestFact struct {
	Path *EntityPath
}

func (*RuntimeTestFact) Name() string { return "RuntimeTestFact" }
func (f *RuntimeTestFact) TargetEntity() *EntityPath {
	return f.Path
}

type RuntimeTestEntity struct {
	seen int
}

func (*RuntimeTestEntity) Type() EntityType { return "RuntimeTestEntity" }
func (e *RuntimeTestEntity) OnFact(context.Context, *EntityPath, iter.Seq[Fact]) error {
	e.seen++
	return nil
}

type RuntimeHealthy struct{}

func (*RuntimeHealthy) Name() string { return "RuntimeHealthyRule" }
func (*RuntimeHealthy) CheckSafety(c *SafetyContext) {
	for entry := range c.ChangedEntities() {
		c.Pass(entry.Key)
	}
}

func TestRuntimeOwnsIngestionChecksSnapshotsAndPurge(t *testing.T) {
	const relationType RelationType = "self"
	runtime, err := NewRuntime(RuntimeDeclaration{
		Facts: []Fact{&RuntimeTestFact{}},
		Entities: []RuntimeEntityDeclaration{
			{Type: "RuntimeTestEntity", New: func() Entity { return &RuntimeTestEntity{} }},
		},
		Relations: []RelationSchema{
			{Type: relationType, Source: "RuntimeTestEntity", Target: "RuntimeTestEntity", SourceCardinality: RelationMany, TargetCardinality: RelationMany},
		},
		RelationDerivers: []RelationDeriver{
			func(observed Fact) []RelationMutation {
				path := observed.TargetEntity()
				return []RelationMutation{{Edge: RelationEdge{Type: relationType, Scope: path.Root(), Source: path.EntityID, Target: path.EntityID}}}
			},
		},
		Rules: []RuntimeRuleDeclaration{
			{Safety: func() SafetyRule { return &RuntimeHealthy{} }},
		},
	})
	require.NoError(t, err)
	scope := NewEntityID("Namespace", "namespace")
	entityID := NewEntityID("RuntimeTestEntity", "entity")
	require.NoError(t, runtime.Ingest(t.Context(), &RuntimeTestFact{Path: &EntityPath{EntityID: entityID, Ancestors: []EntityID{scope}}}))
	require.Empty(t, runtime.Check(t.Context(), scope, false))

	snapshot := runtime.Snapshot(scope)
	require.Positive(t, snapshot.Generation)
	require.Equal(t, []FactSnapshot{{Name: "RuntimeTestFact"}}, snapshot.Facts)
	require.Equal(t, []RelationEdge{{Type: relationType, Scope: scope, Source: entityID, Target: entityID}}, snapshot.Relations)
	require.Equal(t, []string{"Namespace:namespace@RuntimeTestEntity:entity"}, runtime.PassedKeys("RuntimeHealthyRule"))

	runtime.Purge(scope)
	require.Empty(t, runtime.Snapshot(scope).Entities)
	require.Empty(t, runtime.Snapshot(scope).Facts)
	require.Empty(t, runtime.Snapshot(scope).Relations)
}

func TestRuntimeRejectsInvalidDeclarationBeforeAllocation(t *testing.T) {
	_, err := NewRuntime(RuntimeDeclaration{
		Facts: []Fact{&RuntimeTestFact{}, &RuntimeTestFact{}},
	})
	require.Error(t, err)

	_, err = NewRuntime(RuntimeDeclaration{
		Entities: []RuntimeEntityDeclaration{{Type: "RuntimeTestEntity", New: func() Entity { return nil }}},
	})
	require.EqualError(t, err, "runtime: entity 0 factory returned nil")
}
