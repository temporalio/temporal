package verify

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeCounterexampleReplaysBoundActionAndDerivesStateDelta(t *testing.T) {
	model := counterexampleModel()

	trace, err := NormalizeCounterexample(model, "job-remains-ready", TraceEvidence{
		Steps: []ObservedTraceStep{{
			Action:   "complete",
			Bindings: Bindings{"job": "job#0"},
		}},
	})

	require.NoError(t, err)
	require.Equal(t, []TraceStep{{
		Action:   "complete",
		Bindings: Bindings{"job": "job#0"},
		Deltas: []StateDelta{{
			Entity: "job", ID: "job#0", FromState: "ready", ToState: "done",
		}},
	}}, trace)
}

func TestNormalizeCounterexampleInfersUniqueActionFromObservedState(t *testing.T) {
	trace, err := NormalizeCounterexample(counterexampleModel(), "job-remains-ready", TraceEvidence{
		Steps: []ObservedTraceStep{{
			After: &ModelState{
				Entities:  map[string]map[string]string{"job": {"job#0": "done"}},
				Relations: map[string][]RelationTuple{},
			},
		}},
	})

	require.NoError(t, err)
	require.Equal(t, "complete", trace[0].Action)
	require.Equal(t, Bindings{"job": "job#0"}, trace[0].Bindings)
}

func TestNormalizeCounterexampleRejectsReplayThatDoesNotViolateProperty(t *testing.T) {
	_, err := NormalizeCounterexample(counterexampleModel(), "job-remains-ready", TraceEvidence{})

	require.ErrorContains(t, err, "property-not-violated")
}

func TestNormalizeCounterexampleRejectsDifferentNativeInitialState(t *testing.T) {
	_, err := NormalizeCounterexample(counterexampleModel(), "job-remains-ready", TraceEvidence{
		Initial: &ModelState{
			Entities:  map[string]map[string]string{"job": {"job#0": "done"}},
			Relations: map[string][]RelationTuple{},
		},
	})

	require.ErrorContains(t, err, "initial-state-mismatch")
}

func TestNormalizeCounterexampleDerivesCreationAndRelationAdditionDeltas(t *testing.T) {
	model := relationCounterexampleModel()

	trace, err := NormalizeCounterexample(model, "no-links", TraceEvidence{
		Steps: []ObservedTraceStep{{
			Action:   "attach",
			Bindings: Bindings{"source": "source#0", "target": "target#0"},
		}},
	})

	require.NoError(t, err)
	require.Equal(t, []StateDelta{
		{Entity: "target", ID: "target#0", ToState: "created"},
		{Relation: "link", Source: "source#0", Target: "target#0", Added: true},
	}, trace[0].Deltas)
}

func TestNormalizeCounterexampleDerivesRelationRemovalDelta(t *testing.T) {
	model := relationCounterexampleModel()
	model.Entities[1].InitiallyExists = []string{"target#0"}
	model.Actions[0].Parameters[1].Binding = InputBinding
	model.Actions[0].Effects = model.Actions[0].Effects[1:]
	model.Actions = append(model.Actions, Action{
		Name: "detach",
		Parameters: []Parameter{
			{Name: "source", Type: "source", Binding: InputBinding},
			{Name: "target", Type: "target", Binding: InputBinding},
		},
		Guard: Expr{Op: RelationHasExpr, Relation: "link", Source: "source", Target: "target"},
		Effects: []Effect{{
			Kind: RemoveRelationEffect, Relation: "link", Source: "source", Target: "target",
		}},
	})
	model.Properties = []Property{{
		Name: "link-required", Kind: SafetyProperty,
		Expr: Expr{Op: ForAllExpr, Entity: "source", Var: "source", Args: []Expr{{
			Op: ForAllExpr, Entity: "target", Var: "target", Args: []Expr{{
				Op: RelationHasExpr, Relation: "link", Source: "source", Target: "target",
			}},
		}}},
	}}
	bindings := Bindings{"source": "source#0", "target": "target#0"}

	trace, err := NormalizeCounterexample(model, "link-required", TraceEvidence{
		Steps: []ObservedTraceStep{
			{Action: "attach", Bindings: bindings},
			{
				Action: "detach", Bindings: bindings,
				After: &ModelState{
					Entities: map[string]map[string]string{
						"source": {"source#0": "ready"},
						"target": {"target#0": "unused"},
					},
					Relations: map[string][]RelationTuple{"link": nil},
				},
			},
		},
	})

	require.NoError(t, err)
	require.Equal(t, []StateDelta{{
		Relation: "link", Source: "source#0", Target: "target#0",
	}}, trace[1].Deltas)
}

func TestStateDeltasAreDeterministicallyOrdered(t *testing.T) {
	before := ModelState{
		Entities: map[string]map[string]string{
			"b": {"b#0": "old"},
			"a": {"a#0": "old"},
		},
		Relations: map[string][]RelationTuple{"z-rel": nil, "a-rel": nil},
	}
	after := ModelState{
		Entities: map[string]map[string]string{
			"b": {"b#0": "new"},
			"a": {"a#0": "new"},
		},
		Relations: map[string][]RelationTuple{
			"z-rel": {{Source: "b#0", Target: "a#0"}},
			"a-rel": {{Source: "a#0", Target: "b#0"}},
		},
	}
	want := []StateDelta{
		{Entity: "a", ID: "a#0", FromState: "old", ToState: "new"},
		{Entity: "b", ID: "b#0", FromState: "old", ToState: "new"},
		{Relation: "a-rel", Source: "a#0", Target: "b#0", Added: true},
		{Relation: "z-rel", Source: "b#0", Target: "a#0", Added: true},
	}

	for range 20 {
		require.Equal(t, want, stateDeltas(before, after))
	}
}

func TestNormalizeCounterexampleRejectsNativeDeltaMismatch(t *testing.T) {
	_, err := NormalizeCounterexample(counterexampleModel(), "job-remains-ready", TraceEvidence{
		Steps: []ObservedTraceStep{{
			Action:   "complete",
			Bindings: Bindings{"job": "job#0"},
			Deltas: []StateDelta{{
				Entity: "job", ID: "job#0", FromState: "ready", ToState: "missing",
			}},
		}},
	})

	require.ErrorContains(t, err, "delta-mismatch")
}

func TestNormalizeCounterexampleRejectsAmbiguousBranch(t *testing.T) {
	model := counterexampleModel()
	model.Entities[0].States = append(model.Entities[0].States, State{Name: "failed", Terminal: true})
	model.Actions[0].Effects = nil
	model.Actions[0].Branches = []Branch{
		{Name: "done", Effects: []Effect{{Kind: SetStateEffect, Entity: "job", Ref: "job", State: "done"}}},
		{Name: "failed", Effects: []Effect{{Kind: SetStateEffect, Entity: "job", Ref: "job", State: "failed"}}},
	}

	_, err := NormalizeCounterexample(model, "job-remains-ready", TraceEvidence{
		Steps: []ObservedTraceStep{{Action: "complete", Bindings: Bindings{"job": "job#0"}}},
	})

	require.ErrorContains(t, err, "transition-ambiguous")
}

func TestNormalizeCounterexampleAcceptsNoOpWithObservedEmptyDeltas(t *testing.T) {
	model := Model{
		Version: "no-op-counterexample-test/v1",
		Actions: []Action{{Name: "observe"}},
		Properties: []Property{{
			Name: "always-false", Kind: SafetyProperty, Expr: Expr{Op: FalseExpr},
		}},
	}

	trace, err := NormalizeCounterexample(model, "always-false", TraceEvidence{
		Steps: []ObservedTraceStep{{Action: "observe", Bindings: Bindings{}, Deltas: []StateDelta{}}},
	})
	require.NoError(t, err)
	require.Len(t, trace, 1)
	require.Empty(t, trace[0].Deltas)
}

func TestNormalizeCounterexampleRejectsAmbiguousMissingBindings(t *testing.T) {
	model := counterexampleModel()
	model.Entities[0].IDs = append(model.Entities[0].IDs, "job#1")
	model.Entities[0].InitiallyExists = append(model.Entities[0].InitiallyExists, "job#1")

	_, err := NormalizeCounterexample(model, "job-remains-ready", TraceEvidence{
		Steps: []ObservedTraceStep{{Action: "complete"}},
	})
	require.ErrorContains(t, err, "transition-ambiguous")
}

func TestNormalizeCounterexampleRejectsUnreplayableLaterStep(t *testing.T) {
	bindings := Bindings{"job": "job#0"}

	_, err := NormalizeCounterexample(counterexampleModel(), "job-remains-ready", TraceEvidence{
		Steps: []ObservedTraceStep{
			{Action: "complete", Bindings: bindings},
			{Action: "complete", Bindings: bindings},
		},
	})
	require.ErrorContains(t, err, "transition-unreplayable: step 1")
}

func counterexampleModel() Model {
	return Model{
		Version: "counterexample-test/v1",
		Entities: []EntityType{{
			Name: "job", IDs: []string{"job#0"}, InitiallyExists: []string{"job#0"}, Initial: "ready",
			States: []State{{Name: "ready"}, {Name: "done", Terminal: true}},
		}},
		Actions: []Action{{
			Name:       "complete",
			Parameters: []Parameter{{Name: "job", Type: "job", Binding: InputBinding}},
			Guard:      StateIs("job", "job", "ready"),
			Effects:    []Effect{{Kind: SetStateEffect, Entity: "job", Ref: "job", State: "done"}},
		}},
		Properties: []Property{{
			Name: "job-remains-ready", Kind: SafetyProperty,
			Expr: Expr{Op: ForAllExpr, Entity: "job", Var: "job", Args: []Expr{StateIs("job", "job", "ready")}},
		}},
	}
}

func relationCounterexampleModel() Model {
	return Model{
		Version: "relation-counterexample-test/v1",
		Entities: []EntityType{
			{Name: "source", IDs: []string{"source#0"}, InitiallyExists: []string{"source#0"}, Initial: "ready", States: []State{{Name: "ready"}}},
			{Name: "target", IDs: []string{"target#0"}, Initial: "unused", States: []State{{Name: "unused"}, {Name: "created"}}},
		},
		Relations: []Relation{{
			Name: "link", Source: "source", Target: "target", SourceCardinality: One, TargetCardinality: One,
		}},
		Actions: []Action{{
			Name: "attach",
			Parameters: []Parameter{
				{Name: "source", Type: "source", Binding: InputBinding},
				{Name: "target", Type: "target", Binding: FreshBinding},
			},
			Effects: []Effect{
				{Kind: CreateEffect, Entity: "target", Ref: "target", State: "created"},
				{Kind: AddRelationEffect, Relation: "link", Source: "source", Target: "target"},
			},
		}},
		Properties: []Property{{
			Name: "no-links", Kind: SafetyProperty,
			Expr: Expr{Op: ForAllExpr, Entity: "source", Var: "source", Args: []Expr{{
				Op: ForAllExpr, Entity: "target", Var: "target", Args: []Expr{{
					Op: NotExpr, Args: []Expr{{Op: RelationHasExpr, Relation: "link", Source: "source", Target: "target"}},
				}},
			}}},
		}},
	}
}
