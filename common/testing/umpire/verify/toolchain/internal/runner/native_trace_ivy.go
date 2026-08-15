package runner

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"go.temporal.io/server/common/testing/umpire/verify"
)

func decodeIvyTrace(request Request, payload string) (verify.TraceEvidence, error) {
	marker := strings.Index(payload, "Trace follows...")
	if marker < 0 {
		marker = strings.Index(payload, "searching for a small model... done")
		if marker < 0 {
			return verify.TraceEvidence{}, fmt.Errorf("native-trace-missing: Ivy output has no textual counterexample trace")
		}
		return decodeIvySmallModelTrace(request, payload[marker:])
	}
	lines := strings.Split(payload[marker+len("Trace follows..."):], "\n")
	values := newIvyValues(request.TraceVocabulary.Identities)
	if err := values.learnIdentityAliases(request.TraceVocabulary, lines); err != nil {
		return verify.TraceEvidence{}, fmt.Errorf("native-trace-malformed: Ivy trace identities: %w", err)
	}
	var states []verify.ModelState
	var actions []verify.ObservedTraceStep
	var equations []string
	inState := false
	for _, line := range lines {
		switch strings.TrimSpace(line) {
		case "[":
			if inState {
				return verify.TraceEvidence{}, fmt.Errorf("native-trace-malformed: Ivy trace has a nested state block")
			}
			inState = true
			equations = nil
		case "]":
			if !inState {
				return verify.TraceEvidence{}, fmt.Errorf("native-trace-malformed: Ivy trace closes a state block that was not opened")
			}
			for _, equation := range equations {
				if err := values.apply(request, equation); err != nil {
					return verify.TraceEvidence{}, fmt.Errorf("native-trace-malformed: Ivy state %d: %w", len(states), err)
				}
			}
			state, err := values.modelState(request.Model)
			if err != nil {
				return verify.TraceEvidence{}, fmt.Errorf("native-trace-malformed: Ivy state %d: %w", len(states), err)
			}
			states = append(states, state)
			inState = false
		case "":
		default:
			if inState {
				equations = append(equations, line)
				continue
			}
			if match := ivyTraceActionPattern.FindStringSubmatch(line); len(match) != 0 {
				action, err := decodeIvyAction(request, values, match[1], match[2])
				if err != nil {
					return verify.TraceEvidence{}, fmt.Errorf("native-trace-malformed: Ivy step %d: %w", len(actions), err)
				}
				actions = append(actions, action)
				continue
			}
			if match := ivyTraceCallPattern.FindStringSubmatch(line); len(match) != 0 {
				action, err := decodeIvyAction(request, values, match[1], "")
				if err != nil {
					return verify.TraceEvidence{}, fmt.Errorf("native-trace-malformed: Ivy step %d: %w", len(actions), err)
				}
				actions = append(actions, action)
			}
		}
	}
	if inState {
		return verify.TraceEvidence{}, fmt.Errorf("native-trace-malformed: Ivy trace has an unterminated state block")
	}
	if len(states) == 0 {
		return verify.TraceEvidence{}, fmt.Errorf("native-trace-missing: Ivy counterexample trace has no states")
	}
	if len(states) != len(actions)+1 {
		return verify.TraceEvidence{}, fmt.Errorf("native-trace-malformed: Ivy trace has %d states and %d actions", len(states), len(actions))
	}
	evidence := verify.TraceEvidence{Initial: &states[0], Steps: actions}
	for index := range evidence.Steps {
		evidence.Steps[index].After = &states[index+1]
	}
	return evidence, nil
}

func decodeIvySmallModelTrace(request Request, payload string) (verify.TraceEvidence, error) {
	values := newIvyValues(request.TraceVocabulary.Identities)
	bindings := map[string]string{}
	nativeAction := ""
	for _, line := range strings.Split(payload, "\n") {
		if match := ivyTraceCallPattern.FindStringSubmatch(line); len(match) != 0 {
			if nativeAction != "" {
				return verify.TraceEvidence{}, fmt.Errorf("native-trace-malformed: Ivy counterexample contains multiple calls")
			}
			nativeAction = match[1]
			continue
		}
		if match := ivyFormalBinding.FindStringSubmatch(line); len(match) != 0 {
			if existing, duplicate := bindings[match[1]]; duplicate && existing != match[2] {
				return verify.TraceEvidence{}, fmt.Errorf("native-trace-malformed: Ivy action repeats binding %q", match[1])
			}
			bindings[match[1]] = match[2]
		}
	}
	if nativeAction == "" {
		return verify.TraceEvidence{}, fmt.Errorf("native-trace-missing: Ivy counterexample has no action call")
	}
	arguments := make([]string, 0, len(bindings))
	for native, value := range bindings {
		arguments = append(arguments, native+"="+value)
	}
	slices.Sort(arguments)
	action, err := decodeIvyAction(request, values, nativeAction, strings.Join(arguments, ","))
	if err != nil {
		return verify.TraceEvidence{}, fmt.Errorf("native-trace-malformed: Ivy step 0: %w", err)
	}
	return verify.TraceEvidence{Steps: []verify.ObservedTraceStep{action}}, nil
}

type ivyValues struct {
	exists     map[string]map[string]bool
	states     map[string]map[string]string
	relations  map[string]map[verify.RelationTuple]bool
	scalars    map[string]string
	identities map[string]string
}

func newIvyValues(identities map[string]string) ivyValues {
	return ivyValues{
		exists:     map[string]map[string]bool{},
		states:     map[string]map[string]string{},
		relations:  map[string]map[verify.RelationTuple]bool{},
		scalars:    map[string]string{},
		identities: cloneNames(identities),
	}
}

func (values ivyValues) apply(request Request, line string) error {
	match := ivyTraceEquation.FindStringSubmatch(line)
	if len(match) == 0 {
		scalar := ivyTraceScalar.FindStringSubmatch(line)
		if len(scalar) == 0 {
			return fmt.Errorf("unrecognized valuation %q", strings.TrimSpace(line))
		}
		if request.TraceVocabulary.Identities[scalar[1]] != "" {
			return nil
		}
		if !knownIvyBinding(request.TraceVocabulary, scalar[1]) {
			return fmt.Errorf("unrecognized valuation %q", strings.TrimSpace(line))
		}
		values.scalars[scalar[1]] = values.canonicalIdentity(scalar[2])
		return nil
	}
	name := match[1]
	arguments := splitCommaSeparated(match[2])
	value := match[3]
	if entity := request.TraceVocabulary.EntityExists[name]; entity != "" {
		if len(arguments) != 1 {
			return fmt.Errorf("existence valuation %q has %d arguments", name, len(arguments))
		}
		boolean, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("existence valuation %q: %w", name, err)
		}
		if values.exists[entity] == nil {
			values.exists[entity] = map[string]bool{}
		}
		values.exists[entity][values.canonicalIdentity(arguments[0])] = boolean
		return nil
	}
	if entity := request.TraceVocabulary.EntityStates[name]; entity != "" {
		if len(arguments) != 1 {
			return fmt.Errorf("state valuation %q has %d arguments", name, len(arguments))
		}
		if values.states[entity] == nil {
			values.states[entity] = map[string]string{}
		}
		values.states[entity][values.canonicalIdentity(arguments[0])] = canonicalValue(value, request.TraceVocabulary.States)
		return nil
	}
	if relation := request.TraceVocabulary.Relations[name]; relation != "" {
		if len(arguments) != 2 {
			return fmt.Errorf("relation valuation %q has %d arguments", name, len(arguments))
		}
		boolean, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("relation valuation %q: %w", name, err)
		}
		if values.relations[relation] == nil {
			values.relations[relation] = map[verify.RelationTuple]bool{}
		}
		values.relations[relation][verify.RelationTuple{
			Source: values.canonicalIdentity(arguments[0]),
			Target: values.canonicalIdentity(arguments[1]),
		}] = boolean
		return nil
	}
	return fmt.Errorf("unmapped state valuation %q", name)
}

func (values ivyValues) modelState(model verify.Model) (verify.ModelState, error) {
	state := verify.ModelState{
		Entities:  make(map[string]map[string]string, len(model.Entities)),
		Relations: make(map[string][]verify.RelationTuple, len(model.Relations)),
	}
	entities := make(map[string]verify.EntityType, len(model.Entities))
	for _, entity := range model.Entities {
		entities[entity.Name] = entity
		state.Entities[entity.Name] = map[string]string{}
		for _, id := range entity.IDs {
			exists, found := values.exists[entity.Name][id]
			if !found {
				return verify.ModelState{}, fmt.Errorf("entity %q has no existence valuation for %q", entity.Name, id)
			}
			entityState, found := values.states[entity.Name][id]
			if len(entity.States) != 0 && !found {
				return verify.ModelState{}, fmt.Errorf("entity %q has no state valuation for %q", entity.Name, id)
			}
			if exists {
				state.Entities[entity.Name][id] = entityState
			}
		}
	}
	for _, relation := range model.Relations {
		for _, source := range entities[relation.Source].IDs {
			for _, target := range entities[relation.Target].IDs {
				tuple := verify.RelationTuple{Source: source, Target: target}
				present, found := values.relations[relation.Name][tuple]
				if !found {
					return verify.ModelState{}, fmt.Errorf("relation %q has no valuation for %q to %q", relation.Name, source, target)
				}
				if present {
					state.Relations[relation.Name] = append(state.Relations[relation.Name], tuple)
				}
			}
		}
	}
	return state, nil
}

func decodeIvyAction(request Request, values ivyValues, nativeAction, rawArguments string) (verify.ObservedTraceStep, error) {
	actionName := request.TraceVocabulary.Actions[nativeAction]
	if actionName == "" {
		actionName = request.ActionNames[nativeAction]
	}
	if actionName == "" {
		return verify.ObservedTraceStep{}, fmt.Errorf("unmapped action %q", nativeAction)
	}
	var action verify.Action
	found := false
	for _, candidate := range request.Model.Actions {
		if candidate.Name == actionName {
			action = candidate
			found = true
			break
		}
	}
	if !found {
		return verify.ObservedTraceStep{}, fmt.Errorf("canonical action %q is absent from the model", actionName)
	}
	arguments := splitCommaSeparated(rawArguments)
	if len(arguments) == 0 && len(action.Parameters) != 0 {
		arguments = make([]string, len(action.Parameters))
		for index, parameter := range action.Parameters {
			for native, canonical := range request.TraceVocabulary.Bindings[nativeAction] {
				if canonical != parameter.Name {
					continue
				}
				arguments[index] = values.scalars[native]
				break
			}
		}
	}
	if len(arguments) != 0 && strings.Contains(arguments[0], "=") {
		ordered := make([]string, len(action.Parameters))
		for _, argument := range arguments {
			name, value, named := strings.Cut(argument, "=")
			if !named {
				return verify.ObservedTraceStep{}, fmt.Errorf("action %q mixes named and positional bindings", nativeAction)
			}
			name = strings.TrimSpace(name)
			canonicalName := request.TraceVocabulary.Bindings[nativeAction][name]
			if canonicalName == "" {
				canonicalName = name
			}
			parameterIndex := slices.IndexFunc(action.Parameters, func(parameter verify.Parameter) bool {
				return parameter.Name == canonicalName
			})
			if parameterIndex < 0 {
				return verify.ObservedTraceStep{}, fmt.Errorf("action %q has unmapped binding %q", nativeAction, name)
			}
			if ordered[parameterIndex] != "" {
				return verify.ObservedTraceStep{}, fmt.Errorf("action %q repeats binding %q", nativeAction, canonicalName)
			}
			ordered[parameterIndex] = strings.TrimSpace(value)
		}
		arguments = ordered
	}
	if len(arguments) != len(action.Parameters) {
		return verify.ObservedTraceStep{}, fmt.Errorf("action %q has %d bindings, expected %d", nativeAction, len(arguments), len(action.Parameters))
	}
	bindings := make(verify.Bindings, len(arguments))
	for index, argument := range arguments {
		argument = strings.TrimSpace(argument)
		if argument == "" {
			return verify.ObservedTraceStep{}, fmt.Errorf("action %q is missing binding %q", nativeAction, action.Parameters[index].Name)
		}
		bindings[action.Parameters[index].Name] = canonicalIvyIdentity(request, values, action.Parameters[index].Type, argument)
	}
	return verify.ObservedTraceStep{Action: actionName, Bindings: bindings}, nil
}

func canonicalIvyIdentity(request Request, values ivyValues, entityName, value string) string {
	canonical := values.canonicalIdentity(value)
	if canonical != value {
		return canonical
	}
	index, err := strconv.Atoi(value)
	if err != nil {
		return value
	}
	for _, entity := range request.Model.Entities {
		if entity.Name == entityName && index >= 0 && index < len(entity.IDs) {
			return entity.IDs[index]
		}
	}
	return value
}

func (values ivyValues) canonicalIdentity(value string) string {
	return canonicalValue(value, values.identities)
}

func (values ivyValues) learnIdentityAliases(vocabulary verify.TraceVocabulary, lines []string) error {
	for _, line := range lines {
		match := ivyTraceScalar.FindStringSubmatch(line)
		if len(match) == 0 {
			continue
		}
		canonical := vocabulary.Identities[match[1]]
		if canonical == "" {
			continue
		}
		if existing := values.identities[match[2]]; existing != "" && existing != canonical {
			return fmt.Errorf("solver identity %q maps to both %q and %q", match[2], existing, canonical)
		}
		values.identities[match[2]] = canonical
	}
	return nil
}

func knownIvyBinding(vocabulary verify.TraceVocabulary, name string) bool {
	for _, bindings := range vocabulary.Bindings {
		if bindings[name] != "" {
			return true
		}
	}
	return false
}

func splitCommaSeparated(value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	for index := range parts {
		parts[index] = strings.TrimSpace(parts[index])
	}
	return parts
}
