package runner

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"go.temporal.io/server/common/testing/umpire/verify"
)

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

func decodeFizzTrace(request Request, payload string) (verify.TraceEvidence, error) {
	var links []struct {
		Name string `json:"Name"`
		Node struct {
			State map[string]json.RawMessage `json:"state"`
		} `json:"Node"`
	}
	if err := json.Unmarshal([]byte(payload), &links); err != nil {
		return verify.TraceEvidence{}, fmt.Errorf("native-trace-malformed: FizzBee error graph JSON: %w", err)
	}
	if len(links) == 0 || links[0].Name != "Init" {
		return verify.TraceEvidence{}, errors.New("native-trace-missing: FizzBee error graph has no initial state")
	}
	states := make([]verify.ModelState, len(links))
	for index, link := range links {
		state, err := decodeFizzState(request, link.Node.State)
		if err != nil {
			return verify.TraceEvidence{}, fmt.Errorf("native-trace-malformed: FizzBee state %d: %w", index, err)
		}
		states[index] = state
	}
	evidence := verify.TraceEvidence{Initial: &states[0]}
	var current *verify.ObservedTraceStep
	currentNativeAction := ""
	flush := func() {
		if current != nil {
			evidence.Steps = append(evidence.Steps, *current)
			current = nil
			currentNativeAction = ""
		}
	}
	for index := 1; index < len(links); index++ {
		nativeAction := links[index].Name
		action := request.TraceVocabulary.Actions[nativeAction]
		if action == "" {
			action = request.ActionNames[nativeAction]
		}
		if action != "" {
			flush()
			currentNativeAction = nativeAction
			current = &verify.ObservedTraceStep{Action: action, Bindings: verify.Bindings{}, After: &states[index]}
			continue
		}
		choice := fizzChoicePattern.FindStringSubmatch(nativeAction)
		if len(choice) != 3 || current == nil {
			return verify.TraceEvidence{}, fmt.Errorf("native-trace-malformed: FizzBee step %d has unmapped action %q", index-1, nativeAction)
		}
		current.After = &states[index]
		binding := request.TraceVocabulary.Bindings[currentNativeAction][choice[1]]
		if binding == "" {
			if choice[1] == "branch" {
				continue
			}
			return verify.TraceEvidence{}, fmt.Errorf("native-trace-malformed: FizzBee step %d has unmapped choice %q", index-1, choice[1])
		}
		var value string
		if err := json.Unmarshal([]byte(choice[2]), &value); err != nil {
			return verify.TraceEvidence{}, fmt.Errorf("native-trace-malformed: FizzBee step %d binding %q: %w", index-1, binding, err)
		}
		current.Bindings[binding] = canonicalValue(value, request.TraceVocabulary.Identities)
	}
	flush()
	return evidence, nil
}

func decodeFizzState(request Request, values map[string]json.RawMessage) (verify.ModelState, error) {
	decoded := newNativeState(request.Model)
	for native, entity := range request.TraceVocabulary.EntityExists {
		raw, found := values[native]
		if !found {
			return verify.ModelState{}, fmt.Errorf("missing entity-existence variable %q", native)
		}
		var ids []string
		if err := json.Unmarshal(raw, &ids); err != nil {
			return verify.ModelState{}, fmt.Errorf("variable %q: expected a string array: %w", native, err)
		}
		decoded.exists[entity] = canonicalValues(ids, request.TraceVocabulary.Identities)
		decoded.seenExists[entity] = true
	}
	for native, entity := range request.TraceVocabulary.EntityStates {
		raw, found := values[native]
		if !found {
			return verify.ModelState{}, fmt.Errorf("missing entity-state variable %q", native)
		}
		var states map[string]string
		if err := json.Unmarshal(raw, &states); err != nil {
			return verify.ModelState{}, fmt.Errorf("variable %q: expected a string map: %w", native, err)
		}
		canonical := make(map[string]string, len(states))
		for id, state := range states {
			canonical[canonicalValue(id, request.TraceVocabulary.Identities)] = canonicalValue(state, request.TraceVocabulary.States)
		}
		decoded.states[entity] = canonical
		decoded.seenStates[entity] = true
	}
	for native, relation := range request.TraceVocabulary.Relations {
		raw, found := values[native]
		if !found {
			return verify.ModelState{}, fmt.Errorf("missing relation variable %q", native)
		}
		var tuples [][]string
		if err := json.Unmarshal(raw, &tuples); err != nil {
			return verify.ModelState{}, fmt.Errorf("variable %q: expected an array of relation tuples: %w", native, err)
		}
		var canonicalTuples []verify.RelationTuple
		for _, tuple := range tuples {
			if len(tuple) != 2 {
				return verify.ModelState{}, fmt.Errorf("variable %q: relation tuple has %d values", native, len(tuple))
			}
			canonicalTuples = append(canonicalTuples, verify.RelationTuple{
				Source: canonicalValue(tuple[0], request.TraceVocabulary.Identities),
				Target: canonicalValue(tuple[1], request.TraceVocabulary.Identities),
			})
		}
		decoded.relations[relation] = canonicalTuples
		decoded.seenRelations[relation] = true
	}
	for name := range values {
		if !knownStateVariable(request.TraceVocabulary, name) {
			return verify.ModelState{}, fmt.Errorf("unmapped state variable %q", name)
		}
	}
	return decoded.modelState(request.Model)
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
		if name != "#meta" && !knownStateVariable(request.TraceVocabulary, name) && !knownITFConstant(request.TraceVocabulary, name) {
			return verify.ModelState{}, fmt.Errorf("unmapped state variable %q", name)
		}
	}
	return decoded.modelState(request.Model)
}

func knownITFConstant(vocabulary verify.TraceVocabulary, name string) bool {
	for native := range vocabulary.EntityExists {
		if strings.TrimPrefix(native, "exists_")+"IDs" == name {
			return true
		}
	}
	return false
}
