package dynamicconfig

import (
	"errors"
	"fmt"
	"strings"

	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"gopkg.in/yaml.v3"
)

type (
	ConfigValueMap map[Key][]ConstrainedValue

	// YamlLoader loads files and individual values from yaml files.
	// Intended usage is to create a YamlLoader and call LoadFile or LoadValue.
	// The parsed values will be placed in Map (initialized if nil).
	// You can place your own map in Map before calling LoadFile/LoadValue if desired.
	//
	// Any warnings or errors encountered during parsing will be added to Warnings or Errors.
	// Warnings should be reported but not block using the new values.
	// Errors should abort the loading process.
	YamlLoader struct {
		Map      ConfigValueMap
		Warnings []error
		Errors   []error
	}

	yamlConstrainedValue []struct {
		Constraints map[string]any
		Value       any
	}
)

// LoadYamlFile is a convenience function to create a YamlLoader and call LoadFile.
func LoadYamlFile(contents []byte) *YamlLoader {
	var lr YamlLoader
	lr.LoadFile(contents)
	return &lr
}

// Err returns a joined error of lr.Errors.
func (lr *YamlLoader) Err() error {
	return errors.Join(lr.Errors...)
}

// LoadFile parses and processes the given file contents into lr.Map (initialized if nil).
func (lr *YamlLoader) LoadFile(contents []byte) {
	var yamlValues map[string]yamlConstrainedValue
	if err := yaml.Unmarshal(contents, &yamlValues); err != nil {
		lr.errorf("decode error: %w", err)
		return
	}

	if lr.Map == nil {
		lr.Map = make(ConfigValueMap, len(yamlValues))
	}

	for key, yamlCV := range yamlValues {
		lr.add(MakeKey(key), yamlCV)
	}
}

// LoadValue parses and processes the given single value into lr.Map (initialized if nil).
func (lr *YamlLoader) LoadValue(key Key, contents []byte) {
	var yamlCV yamlConstrainedValue
	if err := yaml.Unmarshal(contents, &yamlCV); err != nil {
		lr.errorf("decode error: %w", err)
		return
	}

	if lr.Map == nil {
		lr.Map = make(ConfigValueMap)
	}
	lr.add(key, yamlCV)
}

func (lr *YamlLoader) add(key Key, yamlCV yamlConstrainedValue) {
	precedence := PrecedenceUnknown
	setting := queryRegistry(key)
	if setting == nil {
		lr.warnf("unregistered key %q", key)
	} else {
		precedence = setting.Precedence()
	}

	cvs := make([]ConstrainedValue, len(yamlCV))
	for i, cv := range yamlCV {
		// yaml will unmarshal map into map[interface{}]interface{} instead of map[string]interface{}
		// manually convert key type to string for all values here
		val, err := convertKeyTypeToString(cv.Value)
		if err != nil {
			lr.error(err)
			continue
		}

		// try validating if known setting
		if setting != nil {
			if valErr := setting.Validate(val); valErr != nil {
				// TODO: raise this to error level
				lr.warnf("validation failed: key %q value %v: %w", key, cv.Value, valErr)
			}
		}

		cvs[i].Value = val
		cvs[i].Constraints = convertYamlConstraints(key, cv.Constraints, precedence, lr)
	}
	lr.Map[key] = cvs
}

func (lr *YamlLoader) warn(err error) {
	lr.Warnings = append(lr.Warnings, err)
}

func (lr *YamlLoader) warnf(format string, args ...any) {
	lr.warn(fmt.Errorf(format, args...))
}

func (lr *YamlLoader) error(err error) {
	lr.Errors = append(lr.Errors, err)
}

func (lr *YamlLoader) errorf(format string, args ...any) {
	lr.error(fmt.Errorf(format, args...))
}

func convertKeyTypeToString(v interface{}) (interface{}, error) {
	switch v := v.(type) {
	case map[interface{}]interface{}:
		return convertKeyTypeToStringMap(v)
	case []interface{}:
		return convertKeyTypeToStringSlice(v)
	default:
		return v, nil
	}
}

func convertKeyTypeToStringMap(m map[interface{}]interface{}) (map[string]interface{}, error) {
	stringKeyMap := make(map[string]interface{})
	for key, value := range m {
		stringKey, ok := key.(string)
		if !ok {
			return nil, fmt.Errorf("type of key %v is not string", key)
		}
		convertedValue, err := convertKeyTypeToString(value)
		if err != nil {
			return nil, err
		}
		stringKeyMap[stringKey] = convertedValue
	}
	return stringKeyMap, nil
}

func convertKeyTypeToStringSlice(s []interface{}) ([]interface{}, error) {
	stringKeySlice := make([]interface{}, len(s))
	for idx, value := range s {
		convertedValue, err := convertKeyTypeToString(value)
		if err != nil {
			return nil, err
		}
		stringKeySlice[idx] = convertedValue
	}
	return stringKeySlice, nil
}

// nolint:revive // cognitive-complexity, it's just a big switch
func convertYamlConstraints(key Key, m map[string]any, precedence Precedence, lr *YamlLoader) Constraints {
	var cs Constraints
	for k, v := range m {
		validConstraint := true
		switch strings.ToLower(k) {
		case "namespace":
			if v, ok := v.(string); ok {
				cs.Namespace = v
			} else {
				lr.errorf("namespace constraint must be string")
			}
			validConstraint = precedence == PrecedenceNamespace || precedence == PrecedenceTaskQueue || precedence == PrecedenceDestination
		case "namespaceid":
			if v, ok := v.(string); ok {
				cs.NamespaceID = v
			} else {
				lr.errorf("namespaceID constraint must be string")
			}
			validConstraint = precedence == PrecedenceNamespaceID
		case "taskqueuename":
			if v, ok := v.(string); ok {
				cs.TaskQueueName = v
			} else {
				lr.errorf("taskQueueName constraint must be string")
			}
			validConstraint = precedence == PrecedenceTaskQueue
		case "tasktype":
			switch v := v.(type) {
			case string:
				i, err := enumspb.TaskQueueTypeFromString(v)
				if err != nil {
					lr.errorf("invalid value for taskType: %w", err)
				} else if i <= enumspb.TASK_QUEUE_TYPE_UNSPECIFIED {
					lr.errorf("taskType constraint must be Workflow/Activity")
				}
				cs.TaskQueueType = i
			case int:
				if v > int(enumspb.TASK_QUEUE_TYPE_UNSPECIFIED) {
					cs.TaskQueueType = enumspb.TaskQueueType(v)
				} else {
					lr.errorf("taskType constraint must be Workflow/Activity")
				}
			default:
				lr.errorf("taskType constraint must be Workflow/Activity")
			}
			validConstraint = precedence == PrecedenceTaskQueue
		case "historytasktype":
			switch v := v.(type) {
			case string:
				tt, err := enumsspb.TaskTypeFromString(v)
				if err != nil {
					lr.errorf("invalid value for historytasktype constraint: %w", err)
				} else if tt <= enumsspb.TASK_TYPE_UNSPECIFIED {
					lr.errorf("historytasktype %s constraint is not supported", v)
				}
				cs.TaskType = tt
			case int:
				cs.TaskType = enumsspb.TaskType(v)
			default:
				lr.errorf("historytasktype %T constraint is not supported", v)
			}
			validConstraint = precedence == PrecedenceTaskType
		case "shardid":
			if v, ok := v.(int); ok {
				cs.ShardID = int32(v)
			} else {
				lr.errorf("shardID constraint must be integer")
			}
			validConstraint = precedence == PrecedenceShardID
		case "destination":
			if v, ok := v.(string); ok {
				cs.Destination = v
			} else {
				lr.errorf("destination constraint must be string")
			}
			validConstraint = precedence == PrecedenceDestination
		default:
			lr.errorf("unknown constraint type %q", k)
		}

		// don't log error for PrecedenceUnknown, we would already have logged for an
		// unregistered key above
		// TODO: raise this to error level
		if !validConstraint && precedence != PrecedenceUnknown {
			lr.warnf("constraint %q isn't valid for dynamic config key %q", k, key)
		}
	}
	return cs
}
