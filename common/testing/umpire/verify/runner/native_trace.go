package runner

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"go.temporal.io/server/common/testing/umpire/verify"
)

var (
	tlcStateHeaderPattern  = regexp.MustCompile(`(?m)^State\s+\d+:\s+<([^>\r\n]*)>`)
	tlcActionLabelPattern  = regexp.MustCompile(`^([A-Za-z0-9_]+)(?:\(([^)]*)\))?`)
	tlcFunctionPairPattern = regexp.MustCompile(`("(?:\\.|[^"])*")\s*(?::>|\|->)\s*("(?:\\.|[^"])*")`)
	tlcRelationPairPattern = regexp.MustCompile(`<<\s*("(?:\\.|[^"])*")\s*,\s*("(?:\\.|[^"])*")\s*>>`)
	tlcStringPattern       = regexp.MustCompile(`"(?:\\.|[^"])*"`)
	ivyTraceActionPattern  = regexp.MustCompile(`^\s*>\s*([A-Za-z0-9_]+)(?:\(([^)]*)\))?\s*$`)
	ivyTraceCallPattern    = regexp.MustCompile(`^\s*call\s+([A-Za-z0-9_]+)\s*$`)
	ivyTraceEquation       = regexp.MustCompile(`^\s*([A-Za-z0-9_]+)\(([^)]*)\)\s*=\s*([^\s]+)\s*$`)
	ivyTraceScalar         = regexp.MustCompile(`^\s*([A-Za-z0-9_]+)\s*=\s*([^\s]+)\s*$`)
)

func decodeTLCTrace(request Request, payload string) (verify.TraceEvidence, error) {
	headers := tlcStateHeaderPattern.FindAllStringSubmatchIndex(payload, -1)
	if len(headers) == 0 {
		return verify.TraceEvidence{}, fmt.Errorf("native-trace-missing: TLC output has no state trace")
	}
	states := make([]verify.ModelState, len(headers))
	labels := make([]string, len(headers))
	for index, header := range headers {
		end := len(payload)
		if index+1 < len(headers) {
			end = headers[index+1][0]
		}
		assignments, err := parseTLCAssignments(payload[header[1]:end])
		if err != nil {
			return verify.TraceEvidence{}, err
		}
		states[index], err = decodeTLCState(request, assignments)
		if err != nil {
			return verify.TraceEvidence{}, fmt.Errorf("native-trace-malformed: TLC state %d: %w", index, err)
		}
		labels[index] = payload[header[2]:header[3]]
	}

	evidence := verify.TraceEvidence{Initial: &states[0]}
	for index := 1; index < len(states); index++ {
		action, bindings, err := decodeTLCAction(request, labels[index])
		if err != nil {
			return verify.TraceEvidence{}, fmt.Errorf("native-trace-malformed: TLC step %d: %w", index-1, err)
		}
		evidence.Steps = append(evidence.Steps, verify.ObservedTraceStep{
			Action: action, Bindings: bindings, After: &states[index],
		})
	}
	return evidence, nil
}

func parseTLCAssignments(block string) (map[string]string, error) {
	assignments := map[string]string{}
	currentName := ""
	var currentValue strings.Builder
	flush := func() error {
		if currentName == "" {
			return nil
		}
		if _, duplicate := assignments[currentName]; duplicate {
			return fmt.Errorf("native-trace-malformed: TLC state repeats variable %q", currentName)
		}
		assignments[currentName] = strings.TrimSpace(currentValue.String())
		currentName = ""
		currentValue.Reset()
		return nil
	}
	for _, line := range strings.Split(block, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, `/\`) {
			if err := flush(); err != nil {
				return nil, err
			}
			name, value, found := strings.Cut(strings.TrimSpace(strings.TrimPrefix(trimmed, `/\`)), "=")
			if !found || strings.TrimSpace(name) == "" {
				return nil, fmt.Errorf("native-trace-malformed: TLC state has malformed assignment %q", trimmed)
			}
			currentName = strings.TrimSpace(name)
			currentValue.WriteString(strings.TrimSpace(value))
			continue
		}
		if currentName == "" || trimmed == "" {
			continue
		}
		if len(line) == 0 || line[0] != ' ' && line[0] != '\t' {
			break
		}
		currentValue.WriteByte(' ')
		currentValue.WriteString(trimmed)
	}
	if err := flush(); err != nil {
		return nil, err
	}
	return assignments, nil
}

func decodeTLCState(request Request, assignments map[string]string) (verify.ModelState, error) {
	decoded := newNativeState(request.Model)
	known := map[string]struct{}{}
	for native, entity := range request.TraceVocabulary.EntityExists {
		known[native] = struct{}{}
		value, found := assignments[native]
		if !found {
			return verify.ModelState{}, fmt.Errorf("missing entity-existence variable %q", native)
		}
		ids, err := decodeTLAStringSet(value)
		if err != nil {
			return verify.ModelState{}, fmt.Errorf("variable %q: %w", native, err)
		}
		decoded.exists[entity] = canonicalValues(ids, request.TraceVocabulary.Identities)
		decoded.seenExists[entity] = true
	}
	for native, entity := range request.TraceVocabulary.EntityStates {
		known[native] = struct{}{}
		value, found := assignments[native]
		if !found {
			return verify.ModelState{}, fmt.Errorf("missing entity-state variable %q", native)
		}
		states, err := decodeTLAFunction(value, request.TraceVocabulary)
		if err != nil {
			return verify.ModelState{}, fmt.Errorf("variable %q: %w", native, err)
		}
		decoded.states[entity] = states
		decoded.seenStates[entity] = true
	}
	for native, relation := range request.TraceVocabulary.Relations {
		known[native] = struct{}{}
		value, found := assignments[native]
		if !found {
			return verify.ModelState{}, fmt.Errorf("missing relation variable %q", native)
		}
		tuples, err := decodeTLARelation(value, request.TraceVocabulary.Identities)
		if err != nil {
			return verify.ModelState{}, fmt.Errorf("variable %q: %w", native, err)
		}
		decoded.relations[relation] = tuples
		decoded.seenRelations[relation] = true
	}
	for name := range assignments {
		if _, found := known[name]; !found {
			return verify.ModelState{}, fmt.Errorf("unmapped state variable %q", name)
		}
	}
	return decoded.modelState(request.Model)
}

func decodeTLCAction(request Request, label string) (string, verify.Bindings, error) {
	match := tlcActionLabelPattern.FindStringSubmatch(label)
	if len(match) == 0 {
		return "", nil, fmt.Errorf("unrecognized action label %q", label)
	}
	nativeAction := match[1]
	action := request.TraceVocabulary.Actions[nativeAction]
	if action == "" {
		action = request.ActionNames[nativeAction]
	}
	if action == "" {
		return "", nil, fmt.Errorf("unmapped action %q", nativeAction)
	}
	bindings := verify.Bindings{}
	for _, binding := range tlaBindingPattern.FindAllStringSubmatch(match[2], -1) {
		name := request.TraceVocabulary.Bindings[nativeAction][binding[1]]
		if name == "" {
			name = binding[1]
		}
		bindings[name] = canonicalValue(binding[2], request.TraceVocabulary.Identities)
	}
	return action, bindings, nil
}

func decodeITFTrace(request Request, payload string) (verify.TraceEvidence, error) {
	var document struct {
		Meta struct {
			Format string `json:"format"`
		} `json:"#meta"`
		Vars   []string                     `json:"vars"`
		States []map[string]json.RawMessage `json:"states"`
	}
	if err := json.Unmarshal([]byte(payload), &document); err != nil {
		return verify.TraceEvidence{}, fmt.Errorf("native-trace-malformed: Apalache ITF JSON: %w", err)
	}
	if document.Meta.Format != "ITF" {
		return verify.TraceEvidence{}, fmt.Errorf("native-trace-malformed: Apalache trace format is %q, expected ITF", document.Meta.Format)
	}
	if len(document.States) == 0 {
		return verify.TraceEvidence{}, fmt.Errorf("native-trace-missing: Apalache ITF trace has no states")
	}
	for _, variable := range document.Vars {
		if !knownStateVariable(request.TraceVocabulary, variable) {
			return verify.TraceEvidence{}, fmt.Errorf("native-trace-malformed: Apalache ITF has unmapped state variable %q", variable)
		}
	}
	states := make([]verify.ModelState, len(document.States))
	for index, native := range document.States {
		state, err := decodeITFState(request, native)
		if err != nil {
			return verify.TraceEvidence{}, fmt.Errorf("native-trace-malformed: Apalache ITF state %d: %w", index, err)
		}
		states[index] = state
	}
	evidence := verify.TraceEvidence{Initial: &states[0]}
	for index := 1; index < len(states); index++ {
		evidence.Steps = append(evidence.Steps, verify.ObservedTraceStep{After: &states[index]})
	}
	return evidence, nil
}

func decodeITFState(request Request, values map[string]json.RawMessage) (verify.ModelState, error) {
	decoded := newNativeState(request.Model)
	for native, entity := range request.TraceVocabulary.EntityExists {
		raw, found := values[native]
		if !found {
			return verify.ModelState{}, fmt.Errorf("missing entity-existence variable %q", native)
		}
		ids, err := decodeITFStringSet(raw)
		if err != nil {
			return verify.ModelState{}, fmt.Errorf("variable %q: %w", native, err)
		}
		decoded.exists[entity] = canonicalValues(ids, request.TraceVocabulary.Identities)
		decoded.seenExists[entity] = true
	}
	for native, entity := range request.TraceVocabulary.EntityStates {
		raw, found := values[native]
		if !found {
			return verify.ModelState{}, fmt.Errorf("missing entity-state variable %q", native)
		}
		states, err := decodeITFFunction(raw, request.TraceVocabulary)
		if err != nil {
			return verify.ModelState{}, fmt.Errorf("variable %q: %w", native, err)
		}
		decoded.states[entity] = states
		decoded.seenStates[entity] = true
	}
	for native, relation := range request.TraceVocabulary.Relations {
		raw, found := values[native]
		if !found {
			return verify.ModelState{}, fmt.Errorf("missing relation variable %q", native)
		}
		tuples, err := decodeITFRelation(raw, request.TraceVocabulary.Identities)
		if err != nil {
			return verify.ModelState{}, fmt.Errorf("variable %q: %w", native, err)
		}
		decoded.relations[relation] = tuples
		decoded.seenRelations[relation] = true
	}
	for name := range values {
		if !knownStateVariable(request.TraceVocabulary, name) {
			return verify.ModelState{}, fmt.Errorf("unmapped state variable %q", name)
		}
	}
	return decoded.modelState(request.Model)
}

func decodeIvyTrace(request Request, payload string) (verify.TraceEvidence, error) {
	marker := strings.Index(payload, "Trace follows...")
	if marker < 0 {
		return verify.TraceEvidence{}, fmt.Errorf("native-trace-missing: Ivy output has no textual counterexample trace")
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
	if len(arguments) != len(action.Parameters) {
		return verify.ObservedTraceStep{}, fmt.Errorf("action %q has %d bindings, expected %d", nativeAction, len(arguments), len(action.Parameters))
	}
	bindings := make(verify.Bindings, len(arguments))
	for index, argument := range arguments {
		if _, value, named := strings.Cut(argument, "="); named {
			argument = strings.TrimSpace(value)
		}
		argument = strings.TrimSpace(argument)
		if argument == "" {
			return verify.ObservedTraceStep{}, fmt.Errorf("action %q is missing binding %q", nativeAction, action.Parameters[index].Name)
		}
		bindings[action.Parameters[index].Name] = values.canonicalIdentity(argument)
	}
	return verify.ObservedTraceStep{Action: actionName, Bindings: bindings}, nil
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

type nativeState struct {
	exists        map[string][]string
	states        map[string]map[string]string
	relations     map[string][]verify.RelationTuple
	seenExists    map[string]bool
	seenStates    map[string]bool
	seenRelations map[string]bool
}

func newNativeState(model verify.Model) nativeState {
	return nativeState{
		exists:        make(map[string][]string, len(model.Entities)),
		states:        make(map[string]map[string]string, len(model.Entities)),
		relations:     make(map[string][]verify.RelationTuple, len(model.Relations)),
		seenExists:    make(map[string]bool, len(model.Entities)),
		seenStates:    make(map[string]bool, len(model.Entities)),
		seenRelations: make(map[string]bool, len(model.Relations)),
	}
}

func (decoded nativeState) modelState(model verify.Model) (verify.ModelState, error) {
	state := verify.ModelState{
		Entities:  make(map[string]map[string]string, len(model.Entities)),
		Relations: make(map[string][]verify.RelationTuple, len(model.Relations)),
	}
	for _, entity := range model.Entities {
		if !decoded.seenExists[entity.Name] {
			return verify.ModelState{}, fmt.Errorf("missing canonical entity existence for %q", entity.Name)
		}
		if len(entity.States) != 0 && !decoded.seenStates[entity.Name] {
			return verify.ModelState{}, fmt.Errorf("missing canonical entity state for %q", entity.Name)
		}
		state.Entities[entity.Name] = make(map[string]string, len(decoded.exists[entity.Name]))
		for _, id := range decoded.exists[entity.Name] {
			value := entity.Initial
			if len(entity.States) != 0 {
				var found bool
				value, found = decoded.states[entity.Name][id]
				if !found {
					return verify.ModelState{}, fmt.Errorf("entity %q has no state for identity %q", entity.Name, id)
				}
			}
			state.Entities[entity.Name][id] = value
		}
	}
	for _, relation := range model.Relations {
		if !decoded.seenRelations[relation.Name] {
			return verify.ModelState{}, fmt.Errorf("missing canonical relation %q", relation.Name)
		}
		tuples := slices.Clone(decoded.relations[relation.Name])
		slices.SortFunc(tuples, func(left, right verify.RelationTuple) int {
			if comparison := strings.Compare(left.Source, right.Source); comparison != 0 {
				return comparison
			}
			return strings.Compare(left.Target, right.Target)
		})
		state.Relations[relation.Name] = tuples
	}
	return state, nil
}

func decodeTLAStringSet(value string) ([]string, error) {
	value = strings.TrimSpace(value)
	if err := validateTLASequence(value, "{", "}", ",", tlcStringPattern); err != nil {
		return nil, fmt.Errorf("expected a set of strings, got %q", value)
	}
	return decodeQuotedStrings(tlcStringPattern.FindAllString(value, -1))
}

func decodeTLAFunction(value string, vocabulary verify.TraceVocabulary) (map[string]string, error) {
	value = strings.TrimSpace(value)
	if value != "[]" {
		if err := validateTLASequence(value, "(", ")", "@@", tlcFunctionPairPattern); err != nil {
			return nil, fmt.Errorf("expected a finite string function, got %q", value)
		}
	}
	result := map[string]string{}
	for _, pair := range tlcFunctionPairPattern.FindAllStringSubmatch(value, -1) {
		id, err := strconv.Unquote(pair[1])
		if err != nil {
			return nil, err
		}
		state, err := strconv.Unquote(pair[2])
		if err != nil {
			return nil, err
		}
		result[canonicalValue(id, vocabulary.Identities)] = canonicalValue(state, vocabulary.States)
	}
	if len(result) == 0 && strings.TrimSpace(value) != "[]" {
		return nil, fmt.Errorf("expected a finite string function, got %q", value)
	}
	return result, nil
}

func decodeTLARelation(value string, identities map[string]string) ([]verify.RelationTuple, error) {
	value = strings.TrimSpace(value)
	if err := validateTLASequence(value, "{", "}", ",", tlcRelationPairPattern); err != nil {
		return nil, fmt.Errorf("expected a set of string pairs, got %q", value)
	}
	var result []verify.RelationTuple
	for _, pair := range tlcRelationPairPattern.FindAllStringSubmatch(value, -1) {
		source, err := strconv.Unquote(pair[1])
		if err != nil {
			return nil, err
		}
		target, err := strconv.Unquote(pair[2])
		if err != nil {
			return nil, err
		}
		result = append(result, verify.RelationTuple{
			Source: canonicalValue(source, identities), Target: canonicalValue(target, identities),
		})
	}
	if len(result) == 0 && value != "{}" {
		return nil, fmt.Errorf("expected a set of string pairs, got %q", value)
	}
	return result, nil
}

func validateTLASequence(value, opening, closing, separator string, pattern *regexp.Regexp) error {
	if !strings.HasPrefix(value, opening) || !strings.HasSuffix(value, closing) {
		return errors.New("invalid delimiters")
	}
	body := value[len(opening) : len(value)-len(closing)]
	matches := pattern.FindAllStringIndex(body, -1)
	if len(matches) == 0 {
		if strings.TrimSpace(body) == "" {
			return nil
		}
		return errors.New("missing values")
	}
	cursor := 0
	for index, match := range matches {
		want := ""
		if index != 0 {
			want = separator
		}
		if strings.TrimSpace(body[cursor:match[0]]) != want {
			return errors.New("invalid separator")
		}
		cursor = match[1]
	}
	if strings.TrimSpace(body[cursor:]) != "" {
		return errors.New("trailing input")
	}
	return nil
}

func decodeITFStringSet(raw json.RawMessage) ([]string, error) {
	var value struct {
		Set []string `json:"#set"`
	}
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, err
	}
	if value.Set == nil && !strings.Contains(string(raw), `"#set"`) {
		return nil, fmt.Errorf("expected an ITF set")
	}
	return value.Set, nil
}

func decodeITFFunction(raw json.RawMessage, vocabulary verify.TraceVocabulary) (map[string]string, error) {
	var value struct {
		Map [][]json.RawMessage `json:"#map"`
	}
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, err
	}
	if value.Map == nil && !strings.Contains(string(raw), `"#map"`) {
		return nil, fmt.Errorf("expected an ITF map")
	}
	result := make(map[string]string, len(value.Map))
	for _, pair := range value.Map {
		if len(pair) != 2 {
			return nil, fmt.Errorf("ITF map entry has %d values", len(pair))
		}
		var id string
		var state string
		if err := json.Unmarshal(pair[0], &id); err != nil {
			return nil, fmt.Errorf("ITF map key: %w", err)
		}
		if err := json.Unmarshal(pair[1], &state); err != nil {
			return nil, fmt.Errorf("ITF map value: %w", err)
		}
		result[canonicalValue(id, vocabulary.Identities)] = canonicalValue(state, vocabulary.States)
	}
	return result, nil
}

func decodeITFRelation(raw json.RawMessage, identities map[string]string) ([]verify.RelationTuple, error) {
	var value struct {
		Set []json.RawMessage `json:"#set"`
	}
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, err
	}
	if value.Set == nil && !strings.Contains(string(raw), `"#set"`) {
		return nil, fmt.Errorf("expected an ITF set")
	}
	result := make([]verify.RelationTuple, 0, len(value.Set))
	for _, rawTuple := range value.Set {
		var tuple struct {
			Values []string `json:"#tup"`
		}
		if err := json.Unmarshal(rawTuple, &tuple); err != nil {
			return nil, err
		}
		if len(tuple.Values) != 2 {
			return nil, fmt.Errorf("ITF relation tuple has %d values", len(tuple.Values))
		}
		result = append(result, verify.RelationTuple{
			Source: canonicalValue(tuple.Values[0], identities),
			Target: canonicalValue(tuple.Values[1], identities),
		})
	}
	return result, nil
}

func decodeQuotedStrings(values []string) ([]string, error) {
	result := make([]string, len(values))
	for index, value := range values {
		decoded, err := strconv.Unquote(value)
		if err != nil {
			return nil, err
		}
		result[index] = decoded
	}
	return result, nil
}

func knownStateVariable(vocabulary verify.TraceVocabulary, name string) bool {
	return vocabulary.EntityExists[name] != "" || vocabulary.EntityStates[name] != "" || vocabulary.Relations[name] != ""
}

func canonicalValues(values []string, vocabulary map[string]string) []string {
	result := make([]string, len(values))
	for index, value := range values {
		result[index] = canonicalValue(value, vocabulary)
	}
	return result
}

func canonicalValue(value string, vocabulary map[string]string) string {
	if canonical := vocabulary[value]; canonical != "" {
		return canonical
	}
	return value
}

func normalizeEvidence(model verify.Model, properties []string, evidence verify.TraceEvidence) (string, []verify.TraceStep, error) {
	properties = uniqueStrings(properties)
	if len(properties) == 0 {
		return "", nil, fmt.Errorf("property-unmapped: counterexample has no recognized failed property")
	}
	type match struct {
		property string
		trace    []verify.TraceStep
	}
	var matches []match
	var firstErr error
	for _, property := range properties {
		trace, err := verify.NormalizeCounterexample(model, property, evidence)
		if err == nil {
			matches = append(matches, match{property: property, trace: trace})
			continue
		}
		if firstErr == nil {
			firstErr = err
		}
	}
	if len(matches) == 1 {
		return matches[0].property, matches[0].trace, nil
	}
	if len(matches) > 1 {
		return "", nil, fmt.Errorf("property-unmapped: native property maps to %d violated canonical properties", len(matches))
	}
	return "", nil, firstErr
}

func uniqueStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		if _, duplicate := seen[value]; duplicate {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}
