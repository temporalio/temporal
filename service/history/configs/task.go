// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package configs

import (
	"strconv"
	"strings"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

const (
	numBitsPerLevel = 3
)

const (
	TaskHighPriorityClass = iota << numBitsPerLevel
	TaskDefaultPriorityClass
	TaskLowPriorityClass
)

const (
	TaskHighPrioritySubclass = iota
	TaskDefaultPrioritySubclass
	TaskLowPrioritySubclass
)

var (
	TaskPriorityHigh    = GetTaskPriority(TaskHighPriorityClass, TaskDefaultPrioritySubclass)
	TaskPriorityDefault = GetTaskPriority(TaskDefaultPriorityClass, TaskDefaultPrioritySubclass)
	TaskPriorityLow     = GetTaskPriority(TaskLowPriorityClass, TaskDefaultPrioritySubclass)
)

var DefaultTaskPriorityWeight = map[int]int{
	TaskPriorityHigh:    200,
	TaskPriorityDefault: 100,
	TaskPriorityLow:     50,
}

func ConvertWeightsToDynamicConfigValue(
	weights map[int]int,
) map[string]interface{} {
	weightsForDC := make(map[string]interface{})
	for priority, weight := range weights {
		weightsForDC[strconv.Itoa(priority)] = weight
	}
	return weightsForDC
}

func ConvertDynamicConfigValueToWeights(
	weightsFromDC map[string]interface{},
	logger log.Logger,
) map[int]int {
	weights := make(map[int]int)
	for key, value := range weightsFromDC {
		intKey, err := strconv.Atoi(strings.TrimSpace(key))
		if err != nil {
			logger.Error("Failed to parse dynamic config map value to int map, fallback to default weights", tag.Error(err))
			return DefaultTaskPriorityWeight
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
			logger.Error("Failed to parse dynamic config map value to int map, fallback to default weights", tag.Error(err))
			return DefaultTaskPriorityWeight
		}
		weights[intKey] = intValue
	}
	return weights
}

func GetTaskPriority(
	class, subClass int,
) int {
	return class | subClass
}
