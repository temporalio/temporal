package verify

import (
	"encoding/json"
	"fmt"
	"slices"
)

type Bindings map[string]string

type RelationTuple struct {
	Source string `json:"source"`
	Target string `json:"target"`
}

type ModelState struct {
	Entities  map[string]map[string]string `json:"entities"`
	Relations map[string][]RelationTuple   `json:"relations"`
}

type EnabledAction struct {
	Name     string   `json:"name"`
	Bindings Bindings `json:"bindings,omitempty"`
}

type ReachableState struct {
	State ModelState `json:"state"`
	Depth uint64     `json:"depth"`
}

type Transition struct {
	From   int           `json:"from"`
	To     int           `json:"to"`
	Action EnabledAction `json:"action"`
	Branch int           `json:"branch,omitempty"`
}

type PropertyViolation struct {
	State    int    `json:"state"`
	Property string `json:"property"`
}

type Exploration struct {
	States      []ReachableState    `json:"states"`
	Transitions []Transition        `json:"transitions"`
	Violations  []PropertyViolation `json:"violations,omitempty"`
	Complete    bool                `json:"complete"`
}

type Interpreter struct {
	model     Model
	entities  map[string]EntityType
	relations map[string]Relation
	actions   map[string]Action
}

func NewInterpreter(model Model) (*Interpreter, error) {
	if err := Validate(model); err != nil {
		return nil, err
	}
	normalized := normalizeModel(model)
	interpreter := &Interpreter{
		model:     normalized,
		entities:  make(map[string]EntityType, len(normalized.Entities)),
		relations: make(map[string]Relation, len(normalized.Relations)),
		actions:   make(map[string]Action, len(normalized.Actions)),
	}
	for _, entity := range normalized.Entities {
		interpreter.entities[entity.Name] = entity
	}
	for _, relation := range normalized.Relations {
		interpreter.relations[relation.Name] = relation
	}
	for _, action := range normalized.Actions {
		interpreter.actions[action.Name] = action
	}
	return interpreter, nil
}

// EvaluateExpr evaluates the shared property algebra against a supplied model state.
func EvaluateExpr(model Model, state ModelState, expression Expr, bindings Bindings) (bool, error) {
	interpreter, err := NewInterpreter(model)
	if err != nil {
		return false, err
	}
	return interpreter.eval(state, expression, cloneBindings(bindings)), nil
}

func CheckState(model Model, state ModelState, quiescent bool) ([]PropertyViolation, error) {
	interpreter, err := NewInterpreter(model)
	if err != nil {
		return nil, err
	}
	return interpreter.violations(0, state, quiescent), nil
}

func (i *Interpreter) InitialState() ModelState {
	state := ModelState{
		Entities:  make(map[string]map[string]string, len(i.model.Entities)),
		Relations: make(map[string][]RelationTuple, len(i.model.Relations)),
	}
	for _, entity := range i.model.Entities {
		state.Entities[entity.Name] = map[string]string{}
		for _, id := range entity.InitiallyExists {
			state.Entities[entity.Name][id] = entity.Initial
		}
	}
	for _, relation := range i.model.Relations {
		state.Relations[relation.Name] = nil
	}
	return state
}

func (i *Interpreter) Enabled(state ModelState) []EnabledAction {
	var result []EnabledAction
	for _, action := range i.model.Actions {
		i.enumerateBindings(state, action, 0, Bindings{}, map[string]map[string]struct{}{}, func(bindings Bindings) {
			if i.eval(state, action.Guard, bindings) {
				result = append(result, EnabledAction{Name: action.Name, Bindings: bindings})
			}
		})
	}
	return result
}

func (i *Interpreter) enumerateBindings(
	state ModelState,
	action Action,
	index int,
	bindings Bindings,
	fresh map[string]map[string]struct{},
	yield func(Bindings),
) {
	if index == len(action.Parameters) {
		yield(cloneBindings(bindings))
		return
	}
	parameter := action.Parameters[index]
	entity := i.entities[parameter.Type]
	for _, id := range entity.IDs {
		_, exists := state.Entities[parameter.Type][id]
		if parameter.Binding == FreshBinding {
			if exists || containsID(fresh[parameter.Type], id) {
				continue
			}
			if fresh[parameter.Type] == nil {
				fresh[parameter.Type] = map[string]struct{}{}
			}
			fresh[parameter.Type][id] = struct{}{}
		} else if !exists {
			continue
		}
		bindings[parameter.Name] = id
		i.enumerateBindings(state, action, index+1, bindings, fresh, yield)
		delete(bindings, parameter.Name)
		if parameter.Binding == FreshBinding {
			delete(fresh[parameter.Type], id)
		}
	}
}

func (i *Interpreter) Step(state ModelState, actionName string, bindings Bindings) ([]ModelState, error) {
	action, found := i.actions[actionName]
	if !found {
		return nil, fmt.Errorf("unknown verification action %q", actionName)
	}
	if err := i.validateBindings(state, action, bindings); err != nil {
		return nil, fmt.Errorf("action %q: %w", actionName, err)
	}
	if !i.eval(state, action.Guard, bindings) {
		return nil, fmt.Errorf("action %q is not enabled", actionName)
	}
	branches := action.Branches
	if len(branches) == 0 {
		branches = []Branch{{Effects: nil}}
	}
	result := make([]ModelState, 0, len(branches))
	for _, branch := range branches {
		next := cloneState(state)
		effects := append(slices.Clone(action.Effects), branch.Effects...)
		for _, effect := range effects {
			if err := i.apply(&next, effect, bindings); err != nil {
				return nil, fmt.Errorf("action %q: %w", actionName, err)
			}
		}
		result = append(result, next)
	}
	return result, nil
}

func (i *Interpreter) Explore(maxDepth uint64) (Exploration, error) {
	initial := i.InitialState()
	key, err := stateKey(initial)
	if err != nil {
		return Exploration{}, err
	}
	result := Exploration{
		States:   []ReachableState{{State: initial}},
		Complete: true,
	}
	seen := map[string]int{key: 0}
	for current := 0; current < len(result.States); current++ {
		reachable := result.States[current]
		enabled := i.Enabled(reachable.State)
		result.Violations = append(result.Violations, i.violations(current, reachable.State, len(enabled) == 0)...)
		for _, action := range enabled {
			successors, err := i.Step(reachable.State, action.Name, action.Bindings)
			if err != nil {
				return Exploration{}, err
			}
			for branch, successor := range successors {
				key, err := stateKey(successor)
				if err != nil {
					return Exploration{}, err
				}
				target, found := seen[key]
				if !found {
					if reachable.Depth >= maxDepth {
						result.Complete = false
						continue
					}
					target = len(result.States)
					seen[key] = target
					result.States = append(result.States, ReachableState{State: successor, Depth: reachable.Depth + 1})
				}
				result.Transitions = append(result.Transitions, Transition{From: current, To: target, Action: action, Branch: branch})
			}
		}
	}
	return result, nil
}

func (i *Interpreter) Replay(trace []TraceStep) ([]ModelState, error) {
	states := []ModelState{i.InitialState()}
	for index, step := range trace {
		var successors []ModelState
		seen := map[string]struct{}{}
		for _, state := range states {
			current, err := i.Step(state, step.Action, step.Bindings)
			if err != nil {
				continue
			}
			for _, successor := range current {
				key, err := stateKey(successor)
				if err != nil {
					return nil, err
				}
				if _, duplicate := seen[key]; duplicate {
					continue
				}
				seen[key] = struct{}{}
				successors = append(successors, successor)
			}
		}
		if len(successors) == 0 {
			return nil, fmt.Errorf("trace step %d action %q is not enabled in any successor", index, step.Action)
		}
		states = successors
	}
	return states, nil
}

func (i *Interpreter) violations(stateIndex int, state ModelState, quiescent bool) []PropertyViolation {
	var result []PropertyViolation
	for _, relation := range i.model.Relations {
		tuples := state.Relations[relation.Name]
		for _, tuple := range tuples {
			_, sourceExists := state.Entities[relation.Source][tuple.Source]
			_, targetExists := state.Entities[relation.Target][tuple.Target]
			if !sourceExists || !targetExists {
				result = append(result, PropertyViolation{State: stateIndex, Property: "relation " + relation.Name + " endpoints"})
				break
			}
		}
		if relation.SourceCardinality == One && exceedsCardinality(tuples, true) {
			result = append(result, PropertyViolation{State: stateIndex, Property: "relation " + relation.Name + " source cardinality"})
		}
		if relation.TargetCardinality == One && exceedsCardinality(tuples, false) {
			result = append(result, PropertyViolation{State: stateIndex, Property: "relation " + relation.Name + " target cardinality"})
		}
	}
	for _, property := range i.model.Properties {
		if property.Kind == SafetyProperty || property.Kind == QuiescentProperty && quiescent {
			if !i.eval(state, property.Expr, Bindings{}) {
				result = append(result, PropertyViolation{State: stateIndex, Property: property.Name})
			}
		}
	}
	return result
}

func exceedsCardinality(tuples []RelationTuple, source bool) bool {
	seen := map[string]struct{}{}
	for _, tuple := range tuples {
		value := tuple.Target
		if source {
			value = tuple.Source
		}
		if _, duplicate := seen[value]; duplicate {
			return true
		}
		seen[value] = struct{}{}
	}
	return false
}

func stateKey(state ModelState) (string, error) {
	encoded, err := json.Marshal(state)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func (i *Interpreter) validateBindings(state ModelState, action Action, bindings Bindings) error {
	if len(bindings) != len(action.Parameters) {
		return fmt.Errorf("got %d bindings, expected %d", len(bindings), len(action.Parameters))
	}
	fresh := map[string]map[string]struct{}{}
	for _, parameter := range action.Parameters {
		id, found := bindings[parameter.Name]
		if !found {
			return fmt.Errorf("binding %q is missing", parameter.Name)
		}
		if !slices.Contains(i.entities[parameter.Type].IDs, id) {
			return fmt.Errorf("binding %q uses unknown %s identity %q", parameter.Name, parameter.Type, id)
		}
		_, exists := state.Entities[parameter.Type][id]
		if parameter.Binding == FreshBinding {
			if exists || containsID(fresh[parameter.Type], id) {
				return fmt.Errorf("fresh binding %q reuses %s identity %q", parameter.Name, parameter.Type, id)
			}
			if fresh[parameter.Type] == nil {
				fresh[parameter.Type] = map[string]struct{}{}
			}
			fresh[parameter.Type][id] = struct{}{}
		} else if !exists {
			return fmt.Errorf("binding %q references absent %s identity %q", parameter.Name, parameter.Type, id)
		}
	}
	return nil
}

func (i *Interpreter) eval(state ModelState, expr Expr, bindings Bindings) bool {
	switch expr.Op {
	case "", TrueExpr:
		return true
	case NotExpr:
		return !i.eval(state, expr.Args[0], bindings)
	case AndExpr:
		for _, argument := range expr.Args {
			if !i.eval(state, argument, bindings) {
				return false
			}
		}
		return true
	case OrExpr:
		for _, argument := range expr.Args {
			if i.eval(state, argument, bindings) {
				return true
			}
		}
		return false
	case ImpliesExpr:
		return !i.eval(state, expr.Args[0], bindings) || i.eval(state, expr.Args[1], bindings)
	case EntityExistsExpr:
		_, found := state.Entities[expr.Entity][bindings[expr.Ref]]
		return found
	case StateIsExpr:
		current, exists := state.Entities[expr.Entity][bindings[expr.Ref]]
		if !exists {
			current = i.entities[expr.Entity].Initial
		}
		return current == expr.State
	case RelationHasExpr:
		tuple := RelationTuple{Source: bindings[expr.Source], Target: bindings[expr.Target]}
		return slices.Contains(state.Relations[expr.Relation], tuple)
	case ForAllExpr, ExistsExpr:
		entity := i.entities[expr.Entity]
		for _, id := range entity.IDs {
			if _, exists := state.Entities[expr.Entity][id]; !exists {
				continue
			}
			bindings := cloneBindings(bindings)
			bindings[expr.Var] = id
			current := i.eval(state, expr.Args[0], bindings)
			if expr.Op == ExistsExpr && current {
				return true
			}
			if expr.Op == ForAllExpr && !current {
				return false
			}
		}
		return expr.Op == ForAllExpr
	default:
		return false
	}
}

func (i *Interpreter) apply(state *ModelState, effect Effect, bindings Bindings) error {
	switch effect.Kind {
	case CreateEffect:
		id := bindings[effect.Ref]
		if _, exists := state.Entities[effect.Entity][id]; exists {
			return fmt.Errorf("create effect reuses %s identity %q", effect.Entity, id)
		}
		state.Entities[effect.Entity][id] = effect.State
	case SetStateEffect:
		id := bindings[effect.Ref]
		if _, exists := state.Entities[effect.Entity][id]; !exists {
			return fmt.Errorf("state effect references absent %s identity %q", effect.Entity, id)
		}
		state.Entities[effect.Entity][id] = effect.State
	case AddRelationEffect:
		relation := i.relations[effect.Relation]
		tuple := RelationTuple{Source: bindings[effect.Source], Target: bindings[effect.Target]}
		if _, exists := state.Entities[relation.Source][tuple.Source]; !exists {
			return fmt.Errorf("relation %q source %q is absent", effect.Relation, tuple.Source)
		}
		if _, exists := state.Entities[relation.Target][tuple.Target]; !exists {
			return fmt.Errorf("relation %q target %q is absent", effect.Relation, tuple.Target)
		}
		if err := addRelation(state, relation, tuple); err != nil {
			return err
		}
	case RemoveRelationEffect:
		tuple := RelationTuple{Source: bindings[effect.Source], Target: bindings[effect.Target]}
		state.Relations[effect.Relation] = deleteTuple(state.Relations[effect.Relation], tuple)
	default:
		return fmt.Errorf("unknown effect kind %q", effect.Kind)
	}
	return nil
}

func addRelation(state *ModelState, relation Relation, tuple RelationTuple) error {
	tuples := state.Relations[relation.Name]
	if slices.Contains(tuples, tuple) {
		return nil
	}
	tuples = append(tuples, tuple)
	slices.SortFunc(tuples, func(left, right RelationTuple) int {
		if result := compareString(left.Source, right.Source); result != 0 {
			return result
		}
		return compareString(left.Target, right.Target)
	})
	state.Relations[relation.Name] = tuples
	return nil
}

func deleteTuple(tuples []RelationTuple, target RelationTuple) []RelationTuple {
	for index, tuple := range tuples {
		if tuple == target {
			return append(slices.Clone(tuples[:index]), tuples[index+1:]...)
		}
	}
	return tuples
}

func cloneState(state ModelState) ModelState {
	result := ModelState{
		Entities:  make(map[string]map[string]string, len(state.Entities)),
		Relations: make(map[string][]RelationTuple, len(state.Relations)),
	}
	for entity, instances := range state.Entities {
		result.Entities[entity] = make(map[string]string, len(instances))
		for id, current := range instances {
			result.Entities[entity][id] = current
		}
	}
	for relation, tuples := range state.Relations {
		result.Relations[relation] = slices.Clone(tuples)
	}
	return result
}

func cloneBindings(bindings Bindings) Bindings {
	result := make(Bindings, len(bindings))
	for name, id := range bindings {
		result[name] = id
	}
	return result
}

func containsID(values map[string]struct{}, id string) bool {
	_, found := values[id]
	return found
}
