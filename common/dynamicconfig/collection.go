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

package dynamicconfig

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	enumspb "go.temporal.io/api/enums/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/primitives/timestamp"
)

type (
	// Collection implements lookup and constraint logic on top of a Client.
	// The rest of the server code should use Collection as the interface to dynamic config,
	// instead of the low-level Client.
	Collection struct {
		client   Client
		logger   log.Logger
		errCount int64
	}

	// These function types follow a similar pattern:
	//   {X}PropertyFn - returns a value of type X that is global (no filters)
	//   {X}PropertyFnWith{Y}Filter - returns a value of type X with the given filters
	// Available value types:
	//   Bool: bool
	//   Duration: time.Duration
	//   Float: float64
	//   Int: int
	//   Map: map[string]any
	//   String: string
	// Available filters:
	//   Namespace func(namespace string)
	//   NamespaceID func(namespaceID string)
	//   TaskQueue func(namespace string, taskQueue string, taskType enumspb.TaskQueueType)  (matching task queue)
	//   TaskType func(taskType enumspsb.TaskType)  (history task type)
	//   ShardID func(shardID int32)
)

const (
	errCountLogThreshold = 1000
)

var (
	errKeyNotPresent        = errors.New("key not present")
	errNoMatchingConstraint = errors.New("no matching constraint in key")
)

// NewCollection creates a new collection
func NewCollection(client Client, logger log.Logger) *Collection {
	return &Collection{
		client:   client,
		logger:   logger,
		errCount: -1,
	}
}

func (c *Collection) throttleLog() bool {
	// TODO: This is a lot of unnecessary contention with little benefit. Consider using
	// https://github.com/cespare/percpu here.
	errCount := atomic.AddInt64(&c.errCount, 1)
	// log only the first x errors and then one every x after that to reduce log noise
	return errCount < errCountLogThreshold || errCount%errCountLogThreshold == 0
}

func (c *Collection) HasKey(key Key) bool {
	cvs := c.client.GetValue(key)
	return len(cvs) > 0
}

func findMatch[T any](cvs []ConstrainedValue, defaultCVs []TypedConstrainedValue[T], precedence []Constraints) (any, error) {
	if len(cvs)+len(defaultCVs) == 0 {
		return nil, errKeyNotPresent
	}
	for _, m := range precedence {
		for _, cv := range cvs {
			if m == cv.Constraints {
				return cv.Value, nil
			}
		}
		for _, cv := range defaultCVs {
			if m == cv.Constraints {
				return cv.Value, nil
			}
		}
	}
	// key is present but no constraint section matches
	return nil, errNoMatchingConstraint
}

// matchAndConvert can't be a method of Collection because methods can't be generic, but we can
// take a *Collection as an argument.
func matchAndConvert[T any, P any](
	c *Collection,
	s setting[T, P],
	precedence []Constraints,
	converter func(value any) (T, error),
) T {
	cvs := c.client.GetValue(s.key)

	defaultCVs := s.cdef
	if defaultCVs == nil {
		defaultCVs = []TypedConstrainedValue[T]{{Value: s.def}}
	}

	val, matchErr := findMatch(cvs, defaultCVs, precedence)
	if matchErr != nil {
		if c.throttleLog() {
			c.logger.Debug("No such key in dynamic config, using default", tag.Key(s.key.String()), tag.Error(matchErr))
		}
		// couldn't find a constrained match, use default
		val = s.def
	}

	typedVal, convertErr := converter(val)
	if convertErr != nil && matchErr == nil {
		// We failed to convert the value to the desired type. Try converting the default. note
		// that if matchErr != nil then val _is_ defaultValue and we don't have to try this again.
		if c.throttleLog() {
			c.logger.Warn("Failed to convert value, using default", tag.Key(s.key.String()), tag.IgnoredValue(val), tag.Error(convertErr))
		}
		typedVal, convertErr = converter(s.def)
	}
	if convertErr != nil {
		// If we can't convert the default, that's a bug in our code, use Warn level.
		c.logger.Warn("Can't convert default value (this is a bug; fix server code)", tag.Key(s.key.String()), tag.IgnoredValue(s.def), tag.Error(convertErr))
		// Return typedVal anyway since we have to return something.
	}
	return typedVal
}

func precedenceGlobal() []Constraints {
	return []Constraints{
		{},
	}
}

func precedenceNamespace(namespace string) []Constraints {
	return []Constraints{
		{Namespace: namespace},
		{},
	}
}

func precedenceNamespaceID(namespaceID string) []Constraints {
	return []Constraints{
		{NamespaceID: namespaceID},
		{},
	}
}

func precedenceTaskQueue(namespace string, taskQueue string, taskType enumspb.TaskQueueType) []Constraints {
	return []Constraints{
		{Namespace: namespace, TaskQueueName: taskQueue, TaskQueueType: taskType},
		{Namespace: namespace, TaskQueueName: taskQueue},
		// A task-queue-name-only filter applies to a single task queue name across all
		// namespaces, with higher precedence than a namespace-only filter. This is intended to
		// be used by defaultNumTaskQueuePartitions and is probably not useful otherwise.
		{TaskQueueName: taskQueue},
		{Namespace: namespace},
		{},
	}
}

func precedenceDestination(namespace string, destination string) []Constraints {
	return []Constraints{
		{Namespace: namespace, Destination: destination},
		{Destination: destination},
		{Namespace: namespace},
		{},
	}
}

func precedenceShardID(shardID int32) []Constraints {
	return []Constraints{
		{ShardID: shardID},
		{},
	}
}

func precedenceTaskType(taskType enumsspb.TaskType) []Constraints {
	return []Constraints{
		{TaskType: taskType},
		{},
	}
}

func convertInt(val any) (int, error) {
	if intVal, ok := val.(int); ok {
		return intVal, nil
	}
	return 0, errors.New("value type is not int")
}

func convertFloat(val any) (float64, error) {
	if floatVal, ok := val.(float64); ok {
		return floatVal, nil
	} else if intVal, ok := val.(int); ok {
		return float64(intVal), nil
	}
	return 0, errors.New("value type is not float64")
}

func convertDuration(val any) (time.Duration, error) {
	switch v := val.(type) {
	case time.Duration:
		return v, nil
	case int:
		// treat plain int as seconds
		return time.Duration(v) * time.Second, nil
	case string:
		d, err := timestamp.ParseDurationDefaultSeconds(v)
		if err != nil {
			return 0, fmt.Errorf("failed to parse duration: %v", err)
		}
		return d, nil
	}
	return 0, errors.New("value not convertible to Duration")
}

func convertString(val any) (string, error) {
	if stringVal, ok := val.(string); ok {
		return stringVal, nil
	}
	return "", errors.New("value type is not string")
}

func convertBool(val any) (bool, error) {
	if boolVal, ok := val.(bool); ok {
		return boolVal, nil
	}
	return false, errors.New("value type is not bool")
}

func convertMap(val any) (map[string]any, error) {
	if mapVal, ok := val.(map[string]any); ok {
		return mapVal, nil
	}
	return nil, errors.New("value type is not map")
}
