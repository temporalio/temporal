package runner

import (
	"errors"
	"fmt"
	"regexp"
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
	ivyFormalBinding       = regexp.MustCompile(`^\s*fml:([A-Za-z0-9_]+)\s*=\s*([^\s]+)\s*$`)
	ivyTraceEquation       = regexp.MustCompile(`^\s*([A-Za-z0-9_]+)\(([^)]*)\)\s*=\s*([^\s]+)\s*$`)
	ivyTraceScalar         = regexp.MustCompile(`^\s*([A-Za-z0-9_]+)\s*=\s*([^\s]+)\s*$`)
	fizzChoicePattern      = regexp.MustCompile(`^Any:([^=]+)=(.*)$`)
)

func decodeTLCTrace(request Request, payload string) (verify.TraceEvidence, error) {
	headers := tlcStateHeaderPattern.FindAllStringSubmatchIndex(payload, -1)
	if len(headers) == 0 {
		return verify.TraceEvidence{}, errors.New("native-trace-missing: TLC output has no state trace")
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
	var bindings verify.Bindings
	if match[2] != "" {
		bindings = verify.Bindings{}
	}
	for _, binding := range tlaBindingPattern.FindAllStringSubmatch(match[2], -1) {
		name := request.TraceVocabulary.Bindings[nativeAction][binding[1]]
		if name == "" {
			name = binding[1]
		}
		bindings[name] = canonicalValue(binding[2], request.TraceVocabulary.Identities)
	}
	return action, bindings, nil
}
