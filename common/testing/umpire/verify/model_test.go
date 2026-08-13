package verify

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateRejectsActionWithUnknownState(t *testing.T) {
	model := Model{
		Version: "test/v1",
		Entities: []EntityType{{
			Name:    "Workflow",
			IDs:     []string{"workflow-0"},
			Initial: "created",
			States:  []State{{Name: "created"}, {Name: "started"}},
		}},
		Actions: []Action{{
			Name: "start",
			Parameters: []Parameter{{
				Name:    "workflow",
				Type:    "Workflow",
				Binding: InputBinding,
			}},
			Effects: []Effect{{Kind: SetStateEffect, Entity: "Workflow", Ref: "workflow", State: "missing"}},
		}},
	}

	err := Validate(model)
	require.ErrorContains(t, err, `action "start" effect 0 references unknown Workflow state "missing"`)
}

func TestValidateRejectsConflictingRegressionRefinement(t *testing.T) {
	model := nexusModel()
	second := model.Actions[0]
	second.Name = "schedule-again"
	model.Actions = append(model.Actions, second)
	model.Refinements = []Refinement{
		{Name: "one", Action: "schedule", RegressionActions: []string{"nexus.schedule"}},
		{Name: "two", Action: "schedule-again", RegressionActions: []string{"nexus.schedule"}},
	}

	err := Validate(model)
	require.ErrorContains(t, err, `regression action "nexus.schedule" is refined more than once`)
}

func TestInterpreterAppliesEntityAndRelationEffectsAtomically(t *testing.T) {
	model := nexusModel()
	interpreter, err := NewInterpreter(model)
	require.NoError(t, err)

	initial := interpreter.InitialState()
	successors, err := interpreter.Step(initial, "schedule", Bindings{
		"caller":    "workflow-0",
		"operation": "operation-0",
	})
	require.NoError(t, err)
	require.Len(t, successors, 1)

	next := successors[0]
	require.Equal(t, "started", next.Entities["Workflow"]["workflow-0"])
	require.Equal(t, "scheduled", next.Entities["NexusOperation"]["operation-0"])
	require.Equal(t, []RelationTuple{{Source: "operation-0", Target: "workflow-0"}}, next.Relations["nexus-child-of"])
	require.Empty(t, initial.Entities["NexusOperation"])
	require.Empty(t, initial.Relations["nexus-child-of"])
}

func TestInterpreterEnumeratesOnlyUnusedFreshBindings(t *testing.T) {
	model := nexusModel()
	interpreter, err := NewInterpreter(model)
	require.NoError(t, err)

	initial := interpreter.InitialState()
	enabled := interpreter.Enabled(initial)
	require.Equal(t, []EnabledAction{
		{Name: "schedule", Bindings: Bindings{"caller": "workflow-0", "operation": "operation-0"}},
		{Name: "schedule", Bindings: Bindings{"caller": "workflow-0", "operation": "operation-1"}},
	}, enabled)

	used, err := interpreter.Step(initial, "schedule", enabled[0].Bindings)
	require.NoError(t, err)
	require.Equal(t, []EnabledAction{
		{Name: "schedule", Bindings: Bindings{"caller": "workflow-0", "operation": "operation-1"}},
	}, interpreter.Enabled(used[0]))
}

func TestInterpreterExploresTheCompleteFiniteGraph(t *testing.T) {
	interpreter, err := NewInterpreter(nexusModel())
	require.NoError(t, err)

	exploration, err := interpreter.Explore(2)
	require.NoError(t, err)
	require.True(t, exploration.Complete)
	require.Len(t, exploration.States, 4)
	require.Len(t, exploration.Transitions, 4)
	require.Empty(t, exploration.Violations)
}

func TestInterpreterReportsStructuralInvariantViolations(t *testing.T) {
	model := nexusModel()
	model.Relations[0].TargetCardinality = One
	interpreter, err := NewInterpreter(model)
	require.NoError(t, err)

	exploration, err := interpreter.Explore(2)
	require.NoError(t, err)
	require.Contains(t, exploration.Violations, PropertyViolation{
		State:    3,
		Property: "relation nexus-child-of target cardinality",
	})
}

func TestInterpreterReplaysNormalizedTrace(t *testing.T) {
	interpreter, err := NewInterpreter(nexusModel())
	require.NoError(t, err)

	states, err := interpreter.Replay([]TraceStep{
		{Action: "schedule", Bindings: Bindings{"caller": "workflow-0", "operation": "operation-0"}},
		{Action: "schedule", Bindings: Bindings{"caller": "workflow-0", "operation": "operation-1"}},
	})
	require.NoError(t, err)
	require.Len(t, states, 1)
	require.Len(t, states[0].Entities["NexusOperation"], 2)
}

func TestEvaluateExprChecksQuantifiedCrossEntityProperty(t *testing.T) {
	model := Model{
		Version: "test/v1",
		Entities: []EntityType{
			{Name: "operation", IDs: []string{"op"}},
			{Name: "activity", IDs: []string{"activity"}},
		},
		Relations: []Relation{
			{Name: "forward", Source: "operation", Target: "activity", SourceCardinality: Many, TargetCardinality: Many},
			{Name: "reverse", Source: "activity", Target: "operation", SourceCardinality: Many, TargetCardinality: Many},
		},
	}
	property := Expr{Op: ForAllExpr, Entity: "operation", Var: "op", Args: []Expr{{
		Op: ForAllExpr, Entity: "activity", Var: "activity", Args: []Expr{{
			Op: ImpliesExpr,
			Args: []Expr{
				{Op: RelationHasExpr, Relation: "forward", Source: "op", Target: "activity"},
				{Op: RelationHasExpr, Relation: "reverse", Source: "activity", Target: "op"},
			},
		}},
	}}}
	state := ModelState{
		Entities:  map[string]map[string]string{"operation": {"op": ""}, "activity": {"activity": ""}},
		Relations: map[string][]RelationTuple{"forward": {{Source: "op", Target: "activity"}}},
	}

	holds, err := EvaluateExpr(model, state, property, nil)
	require.NoError(t, err)
	require.False(t, holds)
	state.Relations["reverse"] = []RelationTuple{{Source: "activity", Target: "op"}}
	holds, err = EvaluateExpr(model, state, property, nil)
	require.NoError(t, err)
	require.True(t, holds)
}

func TestMarshalModelIsIndependentOfDeclarationOrder(t *testing.T) {
	left := nexusModel()
	right := nexusModel()
	right.Entities[0], right.Entities[1] = right.Entities[1], right.Entities[0]
	right.Entities[0].States[0], right.Entities[0].States[1] = right.Entities[0].States[1], right.Entities[0].States[0]

	leftJSON, err := MarshalModel(left)
	require.NoError(t, err)
	rightJSON, err := MarshalModel(right)
	require.NoError(t, err)
	require.JSONEq(t, string(leftJSON), string(rightJSON))

	leftHash, err := HashModel(left)
	require.NoError(t, err)
	rightHash, err := HashModel(right)
	require.NoError(t, err)
	require.Equal(t, leftHash, rightHash)
}

func nexusModel() Model {
	return Model{
		Version: "test/v1",
		Entities: []EntityType{
			{
				Name:            "Workflow",
				IDs:             []string{"workflow-0"},
				InitiallyExists: []string{"workflow-0"},
				Initial:         "started",
				States:          []State{{Name: "created"}, {Name: "started"}},
			},
			{
				Name:    "NexusOperation",
				IDs:     []string{"operation-0", "operation-1"},
				Initial: "unspecified",
				States:  []State{{Name: "unspecified"}, {Name: "scheduled", MustProgress: true}},
			},
		},
		Relations: []Relation{{
			Name:              "nexus-child-of",
			Source:            "NexusOperation",
			Target:            "Workflow",
			SourceCardinality: One,
			TargetCardinality: Many,
		}},
		Actions: []Action{{
			Name: "schedule",
			Parameters: []Parameter{
				{Name: "operation", Type: "NexusOperation", Binding: FreshBinding},
				{Name: "caller", Type: "Workflow", Binding: InputBinding},
			},
			Guard: StateIs("Workflow", "caller", "started"),
			Effects: []Effect{
				{Kind: CreateEffect, Entity: "NexusOperation", Ref: "operation", State: "scheduled"},
				{Kind: AddRelationEffect, Relation: "nexus-child-of", Source: "operation", Target: "caller"},
			},
		}},
	}
}
