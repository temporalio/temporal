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
	//   TaskQueueInfo func(namespace string, taskQueue string, taskType enumspb.TaskQueueType)
	//   ShardID func(shardID int32)
	BoolPropertyFn                             func() bool
	BoolPropertyFnWithNamespaceFilter          func(namespace string) bool
	BoolPropertyFnWithNamespaceIDFilter        func(namespaceID string) bool
	BoolPropertyFnWithTaskQueueInfoFilters     func(namespace string, taskQueue string, taskType enumspb.TaskQueueType) bool
	DurationPropertyFn                         func() time.Duration
	DurationPropertyFnWithNamespaceFilter      func(namespace string) time.Duration
	DurationPropertyFnWithNamespaceIDFilter    func(namespaceID string) time.Duration
	DurationPropertyFnWithShardIDFilter        func(shardID int32) time.Duration
	DurationPropertyFnWithTaskQueueInfoFilters func(namespace string, taskQueue string, taskType enumspb.TaskQueueType) time.Duration
	DurationPropertyFnWithTaskTypeFilter       func(task enumsspb.TaskType) time.Duration
	FloatPropertyFn                            func() float64
	FloatPropertyFnWithNamespaceFilter         func(namespace string) float64
	FloatPropertyFnWithShardIDFilter           func(shardID int32) float64
	FloatPropertyFnWithTaskQueueInfoFilters    func(namespace string, taskQueue string, taskType enumspb.TaskQueueType) float64
	IntPropertyFn                              func() int
	IntPropertyFnWithNamespaceFilter           func(namespace string) int
	IntPropertyFnWithShardIDFilter             func(shardID int32) int
	IntPropertyFnWithTaskQueueInfoFilters      func(namespace string, taskQueue string, taskType enumspb.TaskQueueType) int
	MapPropertyFn                              func() map[string]any
	MapPropertyFnWithNamespaceFilter           func(namespace string) map[string]any
	StringPropertyFn                           func() string
	StringPropertyFnWithNamespaceFilter        func(namespace string) string
	StringPropertyFnWithNamespaceIDFilter      func(namespaceID string) string
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

// GetIntProperty gets property and asserts that it's an integer
func (c *Collection) GetIntProperty(key Key, defaultValue any) IntPropertyFn {
	return func() int {
		return matchAndConvert(
			c,
			key,
			defaultValue,
			globalPrecedence(),
			convertInt,
		)
	}
}

// GetIntPropertyFilteredByNamespace gets property with namespace filter and asserts that it's an integer
func (c *Collection) GetIntPropertyFilteredByNamespace(key Key, defaultValue any) IntPropertyFnWithNamespaceFilter {
	return func(namespace string) int {
		return matchAndConvert(
			c,
			key,
			defaultValue,
			namespacePrecedence(namespace),
			convertInt,
		)
	}
}

// GetIntPropertyFilteredByTaskQueueInfo gets property with taskQueueInfo as filters and asserts that it's an integer
func (c *Collection) GetIntPropertyFilteredByTaskQueueInfo(key Key, defaultValue any) IntPropertyFnWithTaskQueueInfoFilters {
	return func(namespace string, taskQueue string, taskType enumspb.TaskQueueType) int {
		return matchAndConvert(
			c,
			key,
			defaultValue,
			taskQueuePrecedence(namespace, taskQueue, taskType),
			convertInt,
		)
	}
}

// GetIntPropertyFilteredByShardID gets property with shardID as filter and asserts that it's an integer
func (c *Collection) GetIntPropertyFilteredByShardID(key Key, defaultValue any) IntPropertyFnWithShardIDFilter {
	return func(shardID int32) int {
		return matchAndConvert(
			c,
			key,
			defaultValue,
			shardIDPrecedence(shardID),
			convertInt,
		)
	}
}

// GetFloat64Property gets property and asserts that it's a float64
func (c *Collection) GetFloat64Property(key Key, defaultValue any) FloatPropertyFn {
	return func() float64 {
		return matchAndConvert(
			c,
			key,
			defaultValue,
			globalPrecedence(),
			convertFloat,
		)
	}
}

// GetFloat64PropertyFilteredByShardID gets property with shardID filter and asserts that it's a float64
func (c *Collection) GetFloat64PropertyFilteredByShardID(key Key, defaultValue any) FloatPropertyFnWithShardIDFilter {
	return func(shardID int32) float64 {
		return matchAndConvert(
			c,
			key,
			defaultValue,
			shardIDPrecedence(shardID),
			convertFloat,
		)
	}
}

// GetFloatPropertyFilteredByNamespace gets property with namespace filter and asserts that it's a float64
func (c *Collection) GetFloatPropertyFilteredByNamespace(key Key, defaultValue any) FloatPropertyFnWithNamespaceFilter {
	return func(namespace string) float64 {
		return matchAndConvert(
			c,
			key,
			defaultValue,
			namespacePrecedence(namespace),
			convertFloat,
		)
	}
}

// GetFloatPropertyFilteredByTaskQueueInfo gets property with taskQueueInfo as filters and asserts that it's a float64
func (c *Collection) GetFloatPropertyFilteredByTaskQueueInfo(key Key, defaultValue any) FloatPropertyFnWithTaskQueueInfoFilters {
	return func(namespace string, taskQueue string, taskType enumspb.TaskQueueType) float64 {
		return matchAndConvert(
			c,
			key,
			defaultValue,
			taskQueuePrecedence(namespace, taskQueue, taskType),
			convertFloat,
		)
	}
}

// GetDurationProperty gets property and asserts that it's a duration
func (c *Collection) GetDurationProperty(key Key, defaultValue any) DurationPropertyFn {
	return func() time.Duration {
		return matchAndConvert(
			c,
			key,
			defaultValue,
			globalPrecedence(),
			convertDuration,
		)
	}
}

// GetDurationPropertyFilteredByNamespace gets property with namespace filter and asserts that it's a duration
func (c *Collection) GetDurationPropertyFilteredByNamespace(key Key, defaultValue any) DurationPropertyFnWithNamespaceFilter {
	return func(namespace string) time.Duration {
		return matchAndConvert(
			c,
			key,
			defaultValue,
			namespacePrecedence(namespace),
			convertDuration,
		)
	}
}

// GetDurationPropertyFilteredByNamespaceID gets property with namespaceID filter and asserts that it's a duration
func (c *Collection) GetDurationPropertyFilteredByNamespaceID(key Key, defaultValue any) DurationPropertyFnWithNamespaceIDFilter {
	return func(namespaceID string) time.Duration {
		return matchAndConvert(
			c,
			key,
			defaultValue,
			namespaceIDPrecedence(namespaceID),
			convertDuration,
		)
	}
}

// GetDurationPropertyFilteredByTaskQueueInfo gets property with taskQueueInfo as filters and asserts that it's a duration
func (c *Collection) GetDurationPropertyFilteredByTaskQueueInfo(key Key, defaultValue any) DurationPropertyFnWithTaskQueueInfoFilters {
	return func(namespace string, taskQueue string, taskType enumspb.TaskQueueType) time.Duration {
		return matchAndConvert(
			c,
			key,
			defaultValue,
			taskQueuePrecedence(namespace, taskQueue, taskType),
			convertDuration,
		)
	}
}

// GetDurationPropertyFilteredByShardID gets property with shardID id as filter and asserts that it's a duration
func (c *Collection) GetDurationPropertyFilteredByShardID(key Key, defaultValue any) DurationPropertyFnWithShardIDFilter {
	return func(shardID int32) time.Duration {
		return matchAndConvert(
			c,
			key,
			defaultValue,
			shardIDPrecedence(shardID),
			convertDuration,
		)
	}
}

// GetDurationPropertyFilteredByTaskType gets property with task type as filters and asserts that it's a duration
func (c *Collection) GetDurationPropertyFilteredByTaskType(key Key, defaultValue any) DurationPropertyFnWithTaskTypeFilter {
	return func(taskType enumsspb.TaskType) time.Duration {
		return matchAndConvert(
			c,
			key,
			defaultValue,
			taskTypePrecedence(taskType),
			convertDuration,
		)
	}
}

// GetBoolProperty gets property and asserts that it's a bool
func (c *Collection) GetBoolProperty(key Key, defaultValue any) BoolPropertyFn {
	return func() bool {
		return matchAndConvert(
			c,
			key,
			defaultValue,
			globalPrecedence(),
			convertBool,
		)
	}
}

// GetStringProperty gets property and asserts that it's a string
func (c *Collection) GetStringProperty(key Key, defaultValue any) StringPropertyFn {
	return func() string {
		return matchAndConvert(
			c,
			key,
			defaultValue,
			globalPrecedence(),
			convertString,
		)
	}
}

// GetMapProperty gets property and asserts that it's a map
func (c *Collection) GetMapProperty(key Key, defaultValue any) MapPropertyFn {
	return func() map[string]interface{} {
		return matchAndConvert(
			c,
			key,
			defaultValue,
			globalPrecedence(),
			convertMap,
		)
	}
}

// GetStringPropertyFnWithNamespaceFilter gets property with namespace filter and asserts that it's a string
func (c *Collection) GetStringPropertyFnWithNamespaceFilter(key Key, defaultValue any) StringPropertyFnWithNamespaceFilter {
	return func(namespace string) string {
		return matchAndConvert(
			c,
			key,
			defaultValue,
			namespacePrecedence(namespace),
			convertString,
		)
	}
}

// GetStringPropertyFnWithNamespaceIDFilter gets property with namespace ID filter and asserts that it's a string
func (c *Collection) GetStringPropertyFnWithNamespaceIDFilter(key Key, defaultValue any) StringPropertyFnWithNamespaceIDFilter {
	return func(namespaceID string) string {
		return matchAndConvert(
			c,
			key,
			defaultValue,
			namespaceIDPrecedence(namespaceID),
			convertString,
		)
	}
}

// GetMapPropertyFnWithNamespaceFilter gets property and asserts that it's a map
func (c *Collection) GetMapPropertyFnWithNamespaceFilter(key Key, defaultValue any) MapPropertyFnWithNamespaceFilter {
	return func(namespace string) map[string]interface{} {
		return matchAndConvert(
			c,
			key,
			defaultValue,
			namespacePrecedence(namespace),
			convertMap,
		)
	}
}

// GetBoolPropertyFnWithNamespaceFilter gets property with namespace filter and asserts that it's a bool
func (c *Collection) GetBoolPropertyFnWithNamespaceFilter(key Key, defaultValue any) BoolPropertyFnWithNamespaceFilter {
	return func(namespace string) bool {
		return matchAndConvert(
			c,
			key,
			defaultValue,
			namespacePrecedence(namespace),
			convertBool,
		)
	}
}

// GetBoolPropertyFnWithNamespaceIDFilter gets property with namespaceID filter and asserts that it's a bool
func (c *Collection) GetBoolPropertyFnWithNamespaceIDFilter(key Key, defaultValue any) BoolPropertyFnWithNamespaceIDFilter {
	return func(namespaceID string) bool {
		return matchAndConvert(
			c,
			key,
			defaultValue,
			namespaceIDPrecedence(namespaceID),
			convertBool,
		)
	}
}

// GetBoolPropertyFilteredByTaskQueueInfo gets property with taskQueueInfo as filters and asserts that it's a bool
func (c *Collection) GetBoolPropertyFilteredByTaskQueueInfo(key Key, defaultValue any) BoolPropertyFnWithTaskQueueInfoFilters {
	return func(namespace string, taskQueue string, taskType enumspb.TaskQueueType) bool {
		return matchAndConvert(
			c,
			key,
			defaultValue,
			taskQueuePrecedence(namespace, taskQueue, taskType),
			convertBool,
		)
	}
}

// Task queue partitions use a dedicated function to handle defaults.
func (c *Collection) GetTaskQueuePartitionsProperty(key Key) IntPropertyFnWithTaskQueueInfoFilters {
	return c.GetIntPropertyFilteredByTaskQueueInfo(key, defaultNumTaskQueuePartitions)
}

func (c *Collection) HasKey(key Key) bool {
	cvs := c.client.GetValue(key)
	return len(cvs) > 0
}

func findMatch(cvs, defaultCVs []ConstrainedValue, precedence []Constraints) (any, error) {
	if len(cvs)+len(defaultCVs) == 0 {
		return nil, errKeyNotPresent
	}
	for _, m := range precedence {
		// duplicate the code so that we don't have to allocate a new slice to hold the
		// concatenation of cvs and defaultCVs
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
func matchAndConvert[T any](
	c *Collection,
	key Key,
	defaultValue any,
	precedence []Constraints,
	converter func(value any) (T, error),
) T {
	cvs := c.client.GetValue(key)

	// defaultValue may be a list of constrained values. In that case, one of them must have an
	// empty constraint set to be the fallback default. Otherwise we'll return the zero value
	// and log an error (since []ConstrainedValue can't be converted to the desired type).
	defaultCVs, _ := defaultValue.([]ConstrainedValue)

	val, matchErr := findMatch(cvs, defaultCVs, precedence)
	if matchErr != nil {
		if c.throttleLog() {
			c.logger.Debug("No such key in dynamic config, using default", tag.Key(key.String()), tag.Error(matchErr))
		}
		// couldn't find a constrained match, use default
		val = defaultValue
	}

	typedVal, convertErr := converter(val)
	if convertErr != nil && matchErr == nil {
		// We failed to convert the value to the desired type. Try converting the default. note
		// that if matchErr != nil then val _is_ defaultValue and we don't have to try this again.
		if c.throttleLog() {
			c.logger.Warn("Failed to convert value, using default", tag.Key(key.String()), tag.IgnoredValue(val), tag.Error(convertErr))
		}
		typedVal, convertErr = converter(defaultValue)
	}
	if convertErr != nil {
		// If we can't convert the default, that's a bug in our code, use Warn level.
		c.logger.Warn("Can't convert default value (this is a bug; fix server code)", tag.Key(key.String()), tag.IgnoredValue(defaultValue), tag.Error(convertErr))
		// Return typedVal anyway since we have to return something.
	}
	return typedVal
}

func globalPrecedence() []Constraints {
	return []Constraints{
		{},
	}
}

func namespacePrecedence(namespace string) []Constraints {
	return []Constraints{
		{Namespace: namespace},
		{},
	}
}

func namespaceIDPrecedence(namespaceID string) []Constraints {
	return []Constraints{
		{NamespaceID: namespaceID},
		{},
	}
}

func taskQueuePrecedence(namespace string, taskQueue string, taskType enumspb.TaskQueueType) []Constraints {
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

func shardIDPrecedence(shardID int32) []Constraints {
	return []Constraints{
		{ShardID: shardID},
		{},
	}
}

func taskTypePrecedence(taskType enumsspb.TaskType) []Constraints {
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
