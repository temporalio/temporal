package dynamicconfig

import (
	"fmt"
	"reflect"
	"strings"

	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/log"
)

// DiffAndLogConfigs computes the difference between two ConfigValueMaps. The result is
// returned as a ConfigValueMap that can be merged with old to produce new, except with deleted
// keys mapped to nil. It also logs the differences to a logger.
func DiffAndLogConfigs(logger log.Logger, old ConfigValueMap, new ConfigValueMap) ConfigValueMap {
	changedMap := make(map[Key][]ConstrainedValue)

	for key, newValues := range new {
		oldValues, ok := old[key]
		if !ok {
			for _, newValue := range newValues {
				// new key added
				diffAndLogValue(logger, key, nil, &newValue)
			}
			changedMap[Key(key)] = newValues
		} else {
			// compare existing keys
			changed := diffAndLogConstraints(logger, key, oldValues, newValues)
			if changed {
				changedMap[Key(key)] = newValues
			}
		}
	}

	// check for removed values
	for key, oldValues := range old {
		if _, ok := new[key]; !ok {
			for _, oldValue := range oldValues {
				diffAndLogValue(logger, key, &oldValue, nil)
			}
			changedMap[Key(key)] = nil
		}
	}

	return changedMap
}

func diffAndLogConstraints(logger log.Logger, key Key, oldValues []ConstrainedValue, newValues []ConstrainedValue) bool {
	changed := false
	for _, oldValue := range oldValues {
		matchFound := false
		for _, newValue := range newValues {
			if oldValue.Constraints == newValue.Constraints {
				matchFound = true
				if !reflect.DeepEqual(oldValue.Value, newValue.Value) {
					diffAndLogValue(logger, key, &oldValue, &newValue)
					changed = true
				}
			}
		}
		if !matchFound {
			diffAndLogValue(logger, key, &oldValue, nil)
			changed = true
		}
	}

	for _, newValue := range newValues {
		matchFound := false
		for _, oldValue := range oldValues {
			if oldValue.Constraints == newValue.Constraints {
				matchFound = true
			}
		}
		if !matchFound {
			diffAndLogValue(logger, key, nil, &newValue)
			changed = true
		}
	}
	return changed
}

func diffAndLogValue(logger log.Logger, key Key, oldValue *ConstrainedValue, newValue *ConstrainedValue) {
	logLine := &strings.Builder{}
	logLine.Grow(128)
	logLine.WriteString("dynamic config changed for the key: ")
	logLine.WriteString(string(key))
	logLine.WriteString(" oldValue: ")
	appendConstrainedValue(logLine, oldValue)
	logLine.WriteString(" newValue: ")
	appendConstrainedValue(logLine, newValue)
	logger.Info(logLine.String())
}

func appendConstrainedValue(logLine *strings.Builder, value *ConstrainedValue) {
	if value == nil {
		logLine.WriteString("nil")
	} else {
		logLine.WriteString("{ constraints: {")
		if value.Constraints.Namespace != "" {
			logLine.WriteString(fmt.Sprintf("{Namespace:%s}", value.Constraints.Namespace))
		}
		if value.Constraints.NamespaceID != "" {
			logLine.WriteString(fmt.Sprintf("{NamespaceID:%s}", value.Constraints.NamespaceID))
		}
		if value.Constraints.TaskQueueName != "" {
			logLine.WriteString(fmt.Sprintf("{TaskQueueName:%s}", value.Constraints.TaskQueueName))
		}
		if value.Constraints.TaskQueueType != enumspb.TASK_QUEUE_TYPE_UNSPECIFIED {
			logLine.WriteString(fmt.Sprintf("{TaskQueueType:%s}", value.Constraints.TaskQueueType))
		}
		if value.Constraints.ShardID != 0 {
			logLine.WriteString(fmt.Sprintf("{ShardID:%d}", value.Constraints.ShardID))
		}
		if value.Constraints.TaskType != enumsspb.TASK_TYPE_UNSPECIFIED {
			logLine.WriteString(fmt.Sprintf("{HistoryTaskType:%s}", value.Constraints.TaskType))
		}
		if value.Constraints.Destination != "" {
			logLine.WriteString(fmt.Sprintf("{Destination:%s}", value.Constraints.Destination))
		}
		logLine.WriteString(fmt.Sprint("} value: ", value.Value, " }"))
	}
}
