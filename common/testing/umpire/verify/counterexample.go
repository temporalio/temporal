package verify

import (
	"encoding/json"
	"fmt"
	"slices"
)

type TraceEvidence struct {
	Initial *ModelState
	Steps   []ObservedTraceStep
}

type ObservedTraceStep struct {
	Action   string
	Bindings Bindings
	After    *ModelState
	Deltas   []StateDelta
}

func NormalizeCounterexample(model Model, property string, evidence TraceEvidence) ([]TraceStep, error) {
	interpreter, err := NewInterpreter(model)
	if err != nil {
		return nil, err
	}
	state := interpreter.InitialState()
	if evidence.Initial != nil && !equalModelState(state, *evidence.Initial) {
		return nil, fmt.Errorf("initial-state-mismatch: native initial state differs from the canonical initial state")
	}

	trace := make([]TraceStep, 0, len(evidence.Steps))
	for index, observed := range evidence.Steps {
		candidates, err := matchingTransitions(interpreter, state, observed)
		if err != nil {
			return nil, fmt.Errorf("transition-unreplayable: step %d: %w", index, err)
		}
		if len(candidates) == 0 {
			return nil, fmt.Errorf("transition-unreplayable: step %d matched no canonical transition", index)
		}
		if len(candidates) != 1 {
			return nil, fmt.Errorf("transition-ambiguous: step %d matched %d canonical transitions", index, len(candidates))
		}
		matched := candidates[0]
		deltas := stateDeltas(state, matched.after)
		if observed.Deltas != nil && !slices.Equal(observed.Deltas, deltas) {
			return nil, fmt.Errorf("delta-mismatch: step %d native deltas differ from canonical replay", index)
		}
		trace = append(trace, TraceStep{
			Action:   matched.action,
			Bindings: cloneBindings(matched.bindings),
			Deltas:   deltas,
		})
		state = matched.after
	}

	quiescent := len(interpreter.Enabled(state)) == 0
	for _, violation := range interpreter.violations(0, state, quiescent) {
		if violation.Property == property {
			return trace, nil
		}
	}
	return nil, fmt.Errorf("property-not-violated: canonical replay does not violate %q", property)
}

type matchedTransition struct {
	action   string
	bindings Bindings
	after    ModelState
}

func matchingTransitions(interpreter *Interpreter, state ModelState, observed ObservedTraceStep) ([]matchedTransition, error) {
	var enabled []EnabledAction
	if observed.Action != "" && observed.Bindings != nil {
		enabled = []EnabledAction{{Name: observed.Action, Bindings: observed.Bindings}}
	} else {
		for _, action := range interpreter.Enabled(state) {
			if observed.Action == "" || action.Name == observed.Action {
				enabled = append(enabled, action)
			}
		}
	}

	seen := map[string]struct{}{}
	var result []matchedTransition
	for _, action := range enabled {
		successors, err := interpreter.Step(state, action.Name, action.Bindings)
		if err != nil {
			if observed.Action != "" && observed.Bindings != nil {
				return nil, err
			}
			continue
		}
		for _, successor := range successors {
			if observed.After != nil && !equalModelState(successor, *observed.After) {
				continue
			}
			key, err := transitionKey(action, successor)
			if err != nil {
				return nil, err
			}
			if _, duplicate := seen[key]; duplicate {
				continue
			}
			seen[key] = struct{}{}
			result = append(result, matchedTransition{action: action.Name, bindings: action.Bindings, after: successor})
		}
	}
	return result, nil
}

func transitionKey(action EnabledAction, state ModelState) (string, error) {
	encoded, err := json.Marshal(struct {
		Action EnabledAction `json:"action"`
		State  ModelState    `json:"state"`
	}{Action: action, State: state})
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func equalModelState(left, right ModelState) bool {
	leftKey, leftErr := stateKey(left)
	rightKey, rightErr := stateKey(right)
	return leftErr == nil && rightErr == nil && leftKey == rightKey
}

func stateDeltas(before, after ModelState) []StateDelta {
	var result []StateDelta
	for entity, instances := range after.Entities {
		for id, toState := range instances {
			fromState, existed := before.Entities[entity][id]
			if !existed || fromState != toState {
				result = append(result, StateDelta{Entity: entity, ID: id, FromState: fromState, ToState: toState})
			}
		}
	}
	for relation, tuples := range after.Relations {
		for _, tuple := range tuples {
			if !slices.Contains(before.Relations[relation], tuple) {
				result = append(result, StateDelta{Relation: relation, Source: tuple.Source, Target: tuple.Target, Added: true})
			}
		}
	}
	for relation, tuples := range before.Relations {
		for _, tuple := range tuples {
			if !slices.Contains(after.Relations[relation], tuple) {
				result = append(result, StateDelta{Relation: relation, Source: tuple.Source, Target: tuple.Target})
			}
		}
	}
	slices.SortFunc(result, compareStateDelta)
	return result
}

func compareStateDelta(left, right StateDelta) int {
	if left.Entity != "" && right.Entity == "" {
		return -1
	}
	if left.Entity == "" && right.Entity != "" {
		return 1
	}
	for _, values := range [][2]string{
		{left.Entity, right.Entity},
		{left.ID, right.ID},
		{left.Relation, right.Relation},
		{left.Source, right.Source},
		{left.Target, right.Target},
	} {
		if comparison := compareString(values[0], values[1]); comparison != 0 {
			return comparison
		}
	}
	return 0
}
