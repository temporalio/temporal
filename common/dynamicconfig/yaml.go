package dynamicconfig

import (
	"errors"
	"fmt"
	"io"
	"strings"

	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"gopkg.in/yaml.v3"
)

type (
	dumpConstrainedValueYAML struct {
		Constraints dumpConstraintsYAML `yaml:"constraints"`
		Value       any                 `yaml:"value"`
	}

	dumpConstraintsYAML struct {
		Namespace     string                `yaml:"namespace,omitempty"`
		NamespaceID   string                `yaml:"namespaceId,omitempty"`
		TaskQueueName string                `yaml:"taskQueueName,omitempty"`
		Destination   string                `yaml:"destination,omitempty"`
		ChasmTaskType string                `yaml:"chasmTaskType,omitempty"`
		TaskQueueType enumspb.TaskQueueType `yaml:"taskType,omitempty"`
		ShardID       int32                 `yaml:"shardId,omitempty"`
		TaskType      enumsspb.TaskType     `yaml:"historyTaskType,omitempty"`
	}
)

// ParseConstraintsYAML parses YAML-encoded Constraints. An empty string represents no constraints.
func ParseConstraintsYAML(input string) (Constraints, error) {
	if strings.TrimSpace(input) == "" {
		return Constraints{}, nil
	}

	decoder := yaml.NewDecoder(strings.NewReader(input))
	var constraints map[string]any
	if err := decoder.Decode(&constraints); err != nil {
		return Constraints{}, err
	}
	if constraints == nil {
		return Constraints{}, errors.New("constraints must be a YAML mapping")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return Constraints{}, errors.New("constraints must contain exactly one YAML document")
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
	valuesForYAML, err := newDumpConstrainedValuesYAML(key, values)
	if err != nil {
		return nil, err
	}
	return marshalYAML(valuesForYAML)
}

// MarshalConfigValueMapYAML encodes configured values using the format accepted by YamlLoader.
// It applies only to diagnostic serialization.
func MarshalConfigValueMapYAML(values ConfigValueMap) ([]byte, error) {
	valuesForYAML := make(map[string][]dumpConstrainedValueYAML, len(values))
	for key, constrainedValues := range values {
		encodedValues, err := newDumpConstrainedValuesYAML(key, constrainedValues)
		if err != nil {
			return nil, err
		}
		valuesForYAML[key.String()] = encodedValues
	}
	return marshalYAML(valuesForYAML)
}

func newDumpConstrainedValuesYAML(key Key, values []ConstrainedValue) ([]dumpConstrainedValueYAML, error) {
	valuesForYAML := make([]dumpConstrainedValueYAML, len(values))
	for i, constrainedValue := range values {
		value := dumpConstrainedValueYAML{
			Constraints: newDumpConstraintsYAML(constrainedValue.Constraints),
			Value:       constrainedValue.Value,
		}
		if _, err := marshalYAML(value); err != nil {
			return nil, fmt.Errorf("dynamic config key %q constrained value at index %d: %w", key.String(), i, err)
		}
		valuesForYAML[i] = value
	}
	return valuesForYAML, nil
}

func newDumpConstraintsYAML(constraints Constraints) dumpConstraintsYAML {
	return dumpConstraintsYAML(constraints)
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
