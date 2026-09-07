package dynamicconfig

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"gopkg.in/yaml.v3"
)

type (
	diagnosticConstrainedValue struct {
		Constraints diagnosticConstraints `yaml:"constraints"`
		Value       any                   `yaml:"value"`
	}

	diagnosticConstraints struct {
		Namespace     string `yaml:"namespace,omitempty"`
		NamespaceID   string `yaml:"namespaceId,omitempty"`
		TaskQueueName string `yaml:"taskQueueName,omitempty"`
		Destination   string `yaml:"destination,omitempty"`
		ChasmTaskType string `yaml:"chasmTaskType,omitempty"`
		// TaskType is the YAML alias for Constraints.TaskQueueType.
		TaskType enumspb.TaskQueueType `yaml:"taskType,omitempty"`
		ShardID  int32                 `yaml:"shardId,omitempty"`
		// HistoryTaskType is the YAML alias for Constraints.TaskType.
		HistoryTaskType enumsspb.TaskType `yaml:"historyTaskType,omitempty"`
	}
)

// ParseAliasedConstraintsJSON parses JSON-encoded Constraints using file-based client field aliases.
// An empty string represents no constraints.
func ParseAliasedConstraintsJSON(input string) (Constraints, error) {
	if strings.TrimSpace(input) == "" {
		return Constraints{}, nil
	}

	decoder := json.NewDecoder(strings.NewReader(input))
	decoder.UseNumber()
	var constraints map[string]any
	if err := decoder.Decode(&constraints); err != nil {
		return Constraints{}, err
	}
	if constraints == nil {
		return Constraints{}, errors.New("constraints must be a JSON object")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return Constraints{}, errors.New("constraints must contain exactly one JSON object")
	}
	for key, value := range constraints {
		if number, ok := value.(json.Number); ok {
			if integer, err := strconv.Atoi(number.String()); err == nil {
				constraints[key] = integer
			}
		}
	}

	loader := &YamlLoader{}
	parsed := convertYamlConstraints(MakeKey(""), constraints, PrecedenceUnknown, loader)
	if err := loader.Err(); err != nil {
		return Constraints{}, err
	}
	return parsed, nil
}

// MarshalValueYAML encodes a dynamic config value as YAML. It applies only to diagnostic
// serialization.
func MarshalValueYAML(value any) ([]byte, error) {
	return marshalYAML(value)
}

// MarshalConstrainedValuesYAML encodes configured values for one dynamic config key as YAML. It
// applies only to diagnostic serialization.
func MarshalConstrainedValuesYAML(key Key, values []ConstrainedValue) ([]byte, error) {
	valuesForYAML, err := newDiagnosticConstrainedValues(key, values)
	if err != nil {
		return nil, err
	}
	return marshalYAML(valuesForYAML)
}

// MarshalConfigValueMapYAML encodes configured values using the format accepted by YamlLoader.
// It applies only to diagnostic serialization.
func MarshalConfigValueMapYAML(values ConfigValueMap) ([]byte, error) {
	valuesForYAML := make(map[string][]diagnosticConstrainedValue, len(values))
	for key, constrainedValues := range values {
		encodedValues, err := newDiagnosticConstrainedValues(key, constrainedValues)
		if err != nil {
			return nil, err
		}
		valuesForYAML[key.String()] = encodedValues
	}
	return marshalYAML(valuesForYAML)
}

func newDiagnosticConstrainedValues(key Key, values []ConstrainedValue) ([]diagnosticConstrainedValue, error) {
	valuesForYAML := make([]diagnosticConstrainedValue, len(values))
	for i, constrainedValue := range values {
		value := diagnosticConstrainedValue{
			Constraints: newDiagnosticConstraints(constrainedValue.Constraints),
			Value:       constrainedValue.Value,
		}
		if _, err := marshalYAML(value); err != nil {
			return nil, fmt.Errorf("dynamic config key %q constrained value at index %d: %w", key.String(), i, err)
		}
		valuesForYAML[i] = value
	}
	return valuesForYAML, nil
}

func newDiagnosticConstraints(constraints Constraints) diagnosticConstraints {
	return diagnosticConstraints{
		Namespace:       constraints.Namespace,
		NamespaceID:     constraints.NamespaceID,
		TaskQueueName:   constraints.TaskQueueName,
		Destination:     constraints.Destination,
		ChasmTaskType:   constraints.ChasmTaskType,
		TaskType:        constraints.TaskQueueType,
		ShardID:         constraints.ShardID,
		HistoryTaskType: constraints.TaskType,
	}
}

func marshalYAML(value any) (data []byte, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			if recoveredErr, ok := recovered.(error); ok {
				err = recoveredErr
			} else {
				err = fmt.Errorf("%v", recovered)
			}
		}
	}()
	return yaml.Marshal(value)
}
