package dynamicconfig

import (
	"encoding/json"
	"errors"
	"io"
	"strings"
)

// ParseConstraintsJSON parses a JSON-encoded Constraints value. An empty string represents no
// constraints.
func ParseConstraintsJSON(input string) (Constraints, error) {
	if strings.TrimSpace(input) == "" {
		return Constraints{}, nil
	}

	decoder := json.NewDecoder(strings.NewReader(input))
	decoder.DisallowUnknownFields()
	var constraints *Constraints
	if err := decoder.Decode(&constraints); err != nil {
		return Constraints{}, err
	}
	if constraints == nil {
		return Constraints{}, errors.New("constraints must be a JSON object")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return Constraints{}, errors.New("constraints must contain exactly one JSON value")
	}
	return *constraints, nil
}
