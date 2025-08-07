package configs

import (
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/tasks"
)

var (
	DefaultPriorityWeight = 1

	DefaultActiveTaskPriorityWeight = map[tasks.Priority]int{
		tasks.PriorityHigh:        10,
		tasks.PriorityLow:         9,
		tasks.PriorityPreemptable: 1,
	}

	DefaultStandbyTaskPriorityWeight = map[tasks.Priority]int{
		// we basically treat standby tasks as low priority tasks
		tasks.PriorityHigh:        1,
		tasks.PriorityLow:         1,
		tasks.PriorityPreemptable: 1,
	}
)

func ConvertWeightsToDynamicConfigValue(
	weights map[tasks.Priority]int,
) map[string]interface{} {
	weightsForDC := make(map[string]interface{})
	for priority, weight := range weights {
		weightsForDC[priority.String()] = weight
	}
	return weightsForDC
}

func ConvertDynamicConfigValueToWeights(
	weightsFromDC map[string]interface{},
	logger log.Logger,
) map[tasks.Priority]int {
	weights := make(map[tasks.Priority]int)
	for key, value := range weightsFromDC {
		priority, ok := tasks.PriorityValue[key]
		if !ok {
			logger.Error("Unknown key for task priority name, fallback to default weights", tag.Key(key), tag.Value(value))
			return DefaultActiveTaskPriorityWeight
		}

		var intValue int
		switch value := value.(type) {
		case float64:
			intValue = int(value)
		case int:
			intValue = value
		case int32:
			intValue = int(value)
		case int64:
			intValue = int(value)
		default:
			logger.Error("Unknown type for task priority weight, fallback to default weights", tag.Key(key), tag.Value(value))
			return DefaultActiveTaskPriorityWeight
		}
		weights[priority] = intValue
	}
	return weights
}
