package dynamicconfig

import (
	"encoding/json"
	"errors"
	"io"
	"slices"
	"strings"
)

// ParseConstraintsJSON parses a JSON-encoded Constraints value. An empty string represents no
// constraints.
func ParseConstraintsJSON(input string) (Constraints, error) {
	constraints, _, err := ParseConstraintsJSONWithFields(input)
	return constraints, err
}

// ParseConstraintsJSONWithFields also returns the JSON fields that were explicitly supplied.
func ParseConstraintsJSONWithFields(input string) (Constraints, []string, error) {
	if strings.TrimSpace(input) == "" {
		return Constraints{}, []string{}, nil
	}

	decoder := json.NewDecoder(strings.NewReader(input))
	decoder.DisallowUnknownFields()
	var constraints *Constraints
	if err := decoder.Decode(&constraints); err != nil {
		return Constraints{}, nil, err
	}
	if constraints == nil {
		return Constraints{}, nil, errors.New("constraints must be a JSON object")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return Constraints{}, nil, errors.New("constraints must contain exactly one JSON value")
	}

	var encodedFields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(input), &encodedFields); err != nil {
		return Constraints{}, nil, err
	}
	fields := make([]string, 0, len(encodedFields))
	for field := range encodedFields {
		fields = append(fields, field)
	}
	slices.Sort(fields)
	return *constraints, fields, nil
}
