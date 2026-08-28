package dynamicconfig

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
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

type constrainedValueJSON struct {
	Constraints Constraints     `json:"constraints"`
	Value       json.RawMessage `json:"value"`
}

// MarshalValue encodes a dynamic config value as JSON, representing durations as strings.
func MarshalValue(value any) ([]byte, error) {
	if duration, ok := value.(time.Duration); ok {
		value = duration.String()
	}
	return json.Marshal(value)
}

// MarshalConstrainedValues encodes the configured values for one dynamic config key as JSON.
func MarshalConstrainedValues(key Key, values []ConstrainedValue) ([]byte, error) {
	valuesForJSON := make([]constrainedValueJSON, len(values))
	for i, value := range values {
		encodedValue, err := MarshalValue(value.Value)
		if err != nil {
			return nil, fmt.Errorf("dynamic config key %q constrained value at index %d: %w", key.String(), i, err)
		}
		valuesForJSON[i] = constrainedValueJSON{
			Constraints: value.Constraints,
			Value:       encodedValue,
		}
	}
	return json.Marshal(valuesForJSON)
}

// MarshalConfigValueMap encodes configured values for all dynamic config keys as JSON.
func MarshalConfigValueMap(values ConfigValueMap) ([]byte, error) {
	valuesByKey := make(map[string]json.RawMessage, len(values))
	for key, constrainedValues := range values {
		encodedValues, err := MarshalConstrainedValues(key, constrainedValues)
		if err != nil {
			return nil, err
		}
		valuesByKey[key.String()] = encodedValues
	}
	return json.Marshal(valuesByKey)
}
