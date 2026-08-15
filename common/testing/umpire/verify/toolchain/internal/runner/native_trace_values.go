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
