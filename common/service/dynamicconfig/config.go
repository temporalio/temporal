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
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	enumspb "go.temporal.io/api/enums/v1"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

const (
	errCountLogThreshold = 1000
)

// NewCollection creates a new collection
func NewCollection(client Client, logger log.Logger) *Collection {
	return &Collection{
		client:   client,
		logger:   logger,
		keys:     &sync.Map{},
		errCount: -1,
	}
}

// Collection wraps dynamic config client with a closure so that across the code, the config values
// can be directly accessed by calling the function without propagating the client everywhere in
// code
type Collection struct {
	client   Client
	logger   log.Logger
	keys     *sync.Map // map of config Key to strongly typed value
	errCount int64
}

func (c *Collection) logError(key Key, err error) {
	errCount := atomic.AddInt64(&c.errCount, 1)
	if errCount%errCountLogThreshold == 0 {
		// log only every 'x' errors to reduce mem allocs and to avoid log noise
		c.logger.Debug("Failed to fetch key from dynamic config", tag.Key(key.String()), tag.Error(err))
	}
}

func (c *Collection) logValue(
	key Key,
	value, defaultValue interface{},
	cmpValueEquals func(interface{}, interface{}) bool,
) {
	cachedValue, isInCache := c.keys.Load(key)
	if !isInCache || !cmpValueEquals(cachedValue, value) {
		c.keys.Store(key, value)
		c.logger.Info("Get dynamic config", tag.Name(key.String()), tag.Value(value), tag.DefaultValue(defaultValue))
	}
}

// PropertyFn is a wrapper to get property from dynamic config
type PropertyFn func() interface{}

// IntPropertyFn is a wrapper to get int property from dynamic config
type IntPropertyFn func(opts ...FilterOption) int

// IntPropertyFnWithNamespaceFilter is a wrapper to get int property from dynamic config with namespace as filter
type IntPropertyFnWithNamespaceFilter func(namespace string) int

// IntPropertyFnWithTaskQueueInfoFilters is a wrapper to get int property from dynamic config with three filters: namespace, taskQueue, taskType
type IntPropertyFnWithTaskQueueInfoFilters func(namespace string, taskQueue string, taskType enumspb.TaskQueueType) int

// IntPropertyFnWithShardIDFilter is a wrapper to get int property from dynamic config with shardID as filter
type IntPropertyFnWithShardIDFilter func(shardID int32) int

// FloatPropertyFn is a wrapper to get float property from dynamic config
type FloatPropertyFn func(opts ...FilterOption) float64

// FloatPropertyFnWithShardIDFilter is a wrapper to get float property from dynamic config with shardID as filter
type FloatPropertyFnWithShardIDFilter func(shardID int32) float64

// FloatPropertyFnWithNamespaceFilter is a wrapper to get float property from dynamic config with namespace as filter
type FloatPropertyFnWithNamespaceFilter func(namespace string) float64

// FloatPropertyFnWithTaskQueueInfoFilters is a wrapper to get float property from dynamic config with three filters: namespace, taskQueue, taskType
type FloatPropertyFnWithTaskQueueInfoFilters func(namespace string, taskQueue string, taskType enumspb.TaskQueueType) float64

// DurationPropertyFn is a wrapper to get duration property from dynamic config
type DurationPropertyFn func(opts ...FilterOption) time.Duration

// DurationPropertyFnWithNamespaceFilter is a wrapper to get duration property from dynamic config with namespace as filter
type DurationPropertyFnWithNamespaceFilter func(namespace string) time.Duration

// DurationPropertyFnWithNamespaceIDFilter is a wrapper to get duration property from dynamic config with namespaceID as filter
type DurationPropertyFnWithNamespaceIDFilter func(namespaceID string) time.Duration

// DurationPropertyFnWithTaskQueueInfoFilters is a wrapper to get duration property from dynamic config  with three filters: namespace, taskQueue, taskType
type DurationPropertyFnWithTaskQueueInfoFilters func(namespace string, taskQueue string, taskType enumspb.TaskQueueType) time.Duration

// DurationPropertyFnWithShardIDFilter is a wrapper to get duration property from dynamic config with shardID as filter
type DurationPropertyFnWithShardIDFilter func(shardID int32) time.Duration

// BoolPropertyFn is a wrapper to get bool property from dynamic config
type BoolPropertyFn func(opts ...FilterOption) bool

// StringPropertyFn is a wrapper to get string property from dynamic config
type StringPropertyFn func(opts ...FilterOption) string

// MapPropertyFn is a wrapper to get map property from dynamic config
type MapPropertyFn func(opts ...FilterOption) map[string]interface{}

// StringPropertyFnWithNamespaceFilter is a wrapper to get string property from dynamic config
type StringPropertyFnWithNamespaceFilter func(namespace string) string

// MapPropertyFnWithNamespaceFilter is a wrapper to get map property from dynamic config
type MapPropertyFnWithNamespaceFilter func(namespace string) map[string]interface{}

// BoolPropertyFnWithNamespaceFilter is a wrapper to get bool property from dynamic config
type BoolPropertyFnWithNamespaceFilter func(namespace string) bool

// BoolPropertyFnWithNamespaceIDFilter is a wrapper to get bool property from dynamic config
type BoolPropertyFnWithNamespaceIDFilter func(id string) bool

// BoolPropertyFnWithTaskQueueInfoFilters is a wrapper to get bool property from dynamic config with three filters: namespace, taskQueue, taskType
type BoolPropertyFnWithTaskQueueInfoFilters func(namespace string, taskQueue string, taskType enumspb.TaskQueueType) bool

// GetProperty gets a interface property and returns defaultValue if property is not found
func (c *Collection) GetProperty(key Key, defaultValue interface{}) PropertyFn {
	return func() interface{} {
		val, err := c.client.GetValue(key, defaultValue)
		if err != nil {
			c.logError(key, err)
		}
		c.logValue(key, val, defaultValue, reflect.DeepEqual)
		return val
	}
}

func getFilterMap(opts ...FilterOption) map[Filter]interface{} {
	l := len(opts)
	m := make(map[Filter]interface{}, l)
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// GetIntProperty gets property and asserts that it's an integer
func (c *Collection) GetIntProperty(key Key, defaultValue int) IntPropertyFn {
	return func(opts ...FilterOption) int {
		val, err := c.client.GetIntValue(key, getFilterMap(opts...), defaultValue)
		if err != nil {
			c.logError(key, err)
		}
		c.logValue(key, val, defaultValue, intCompareEquals)
		return val
	}
}

// GetIntPropertyFilteredByNamespace gets property with namespace filter and asserts that it's an integer
func (c *Collection) GetIntPropertyFilteredByNamespace(key Key, defaultValue int) IntPropertyFnWithNamespaceFilter {
	return func(namespace string) int {
		val, err := c.client.GetIntValue(key, getFilterMap(NamespaceFilter(namespace)), defaultValue)
		if err != nil {
			c.logError(key, err)
		}
		c.logValue(key, val, defaultValue, intCompareEquals)
		return val
	}
}

// GetIntPropertyFilteredByTaskQueueInfo gets property with taskQueueInfo as filters and asserts that it's an integer
func (c *Collection) GetIntPropertyFilteredByTaskQueueInfo(key Key, defaultValue int) IntPropertyFnWithTaskQueueInfoFilters {
	return func(namespace string, taskQueue string, taskType enumspb.TaskQueueType) int {
		val := defaultValue
		var err error

		filterMaps := []map[Filter]interface{}{
			getFilterMap(NamespaceFilter(namespace), TaskQueueFilter(taskQueue), TaskTypeFilter(taskType)),
			getFilterMap(NamespaceFilter(namespace), TaskQueueFilter(taskQueue)),
		}

		for _, filterMap := range filterMaps {
			val, err = c.client.GetIntValue(
				key,
				filterMap,
				defaultValue,
			)
			if err != nil {
				c.logError(key, err)
			}

			if val != defaultValue {
				break
			}
		}

		c.logValue(key, val, defaultValue, intCompareEquals)
		return val
	}
}

// GetIntPropertyFilteredByShardID gets property with shardID as filter and asserts that it's an integer
func (c *Collection) GetIntPropertyFilteredByShardID(key Key, defaultValue int) IntPropertyFnWithShardIDFilter {
	return func(shardID int32) int {
		val, err := c.client.GetIntValue(
			key,
			getFilterMap(ShardIDFilter(shardID)),
			defaultValue,
		)
		if err != nil {
			c.logError(key, err)
		}
		c.logValue(key, val, defaultValue, intCompareEquals)
		return val
	}
}

// GetFloat64Property gets property and asserts that it's a float64
func (c *Collection) GetFloat64Property(key Key, defaultValue float64) FloatPropertyFn {
	return func(opts ...FilterOption) float64 {
		val, err := c.client.GetFloatValue(key, getFilterMap(opts...), defaultValue)
		if err != nil {
			c.logError(key, err)
		}
		c.logValue(key, val, defaultValue, float64CompareEquals)
		return val
	}
}

// GetFloat64PropertyFilteredByShardID gets property with shardID filter and asserts that it's a float64
func (c *Collection) GetFloat64PropertyFilteredByShardID(key Key, defaultValue float64) FloatPropertyFnWithShardIDFilter {
	return func(shardID int32) float64 {
		val, err := c.client.GetFloatValue(
			key,
			getFilterMap(ShardIDFilter(shardID)),
			defaultValue,
		)
		if err != nil {
			c.logError(key, err)
		}
		c.logValue(key, val, defaultValue, float64CompareEquals)
		return val
	}
}

// GetFloatPropertyFilteredByNamespace gets property with namespace filter and asserts that it's a float
func (c *Collection) GetFloatPropertyFilteredByNamespace(key Key, defaultValue float64) FloatPropertyFnWithNamespaceFilter {
	return func(namespace string) float64 {
		val, err := c.client.GetFloatValue(key, getFilterMap(NamespaceFilter(namespace)), defaultValue)
		if err != nil {
			c.logError(key, err)
		}
		c.logValue(key, val, defaultValue, float64CompareEquals)
		return val
	}
}

// GetFloatPropertyFilteredByTaskQueueInfo gets property with taskQueueInfo as filters and asserts that it's an integer
func (c *Collection) GetFloatPropertyFilteredByTaskQueueInfo(key Key, defaultValue float64) FloatPropertyFnWithTaskQueueInfoFilters {
	return func(namespace string, taskQueue string, taskType enumspb.TaskQueueType) float64 {
		val := defaultValue
		var err error

		filterMaps := []map[Filter]interface{}{
			getFilterMap(NamespaceFilter(namespace), TaskQueueFilter(taskQueue), TaskTypeFilter(taskType)),
			getFilterMap(NamespaceFilter(namespace), TaskQueueFilter(taskQueue)),
		}

		for _, filterMap := range filterMaps {
			val, err = c.client.GetFloatValue(
				key,
				filterMap,
				defaultValue,
			)
			if err != nil {
				c.logError(key, err)
			}

			if val != defaultValue {
				break
			}
		}

		c.logValue(key, val, defaultValue, float64CompareEquals)
		return val
	}
}

// GetDurationProperty gets property and asserts that it's a duration
func (c *Collection) GetDurationProperty(key Key, defaultValue time.Duration) DurationPropertyFn {
	return func(opts ...FilterOption) time.Duration {
		val, err := c.client.GetDurationValue(key, getFilterMap(opts...), defaultValue)
		if err != nil {
			c.logError(key, err)
		}
		c.logValue(key, val, defaultValue, durationCompareEquals)
		return val
	}
}

// GetDurationPropertyFilteredByNamespace gets property with namespace filter and asserts that it's a duration
func (c *Collection) GetDurationPropertyFilteredByNamespace(key Key, defaultValue time.Duration) DurationPropertyFnWithNamespaceFilter {
	return func(namespace string) time.Duration {
		val, err := c.client.GetDurationValue(key, getFilterMap(NamespaceFilter(namespace)), defaultValue)
		if err != nil {
			c.logError(key, err)
		}
		c.logValue(key, val, defaultValue, durationCompareEquals)
		return val
	}
}

// GetDurationPropertyFilteredByNamespaceID gets property with namespaceID filter and asserts that it's a duration
func (c *Collection) GetDurationPropertyFilteredByNamespaceID(key Key, defaultValue time.Duration) DurationPropertyFnWithNamespaceIDFilter {
	return func(namespaceID string) time.Duration {
		val, err := c.client.GetDurationValue(key, getFilterMap(NamespaceIDFilter(namespaceID)), defaultValue)
		if err != nil {
			c.logError(key, err)
		}
		c.logValue(key, val, defaultValue, durationCompareEquals)
		return val
	}
}

// GetDurationPropertyFilteredByTaskQueueInfo gets property with taskQueueInfo as filters and asserts that it's a duration
func (c *Collection) GetDurationPropertyFilteredByTaskQueueInfo(key Key, defaultValue time.Duration) DurationPropertyFnWithTaskQueueInfoFilters {
	return func(namespace string, taskQueue string, taskType enumspb.TaskQueueType) time.Duration {
		val := defaultValue
		var err error

		filterMaps := []map[Filter]interface{}{
			getFilterMap(NamespaceFilter(namespace), TaskQueueFilter(taskQueue), TaskTypeFilter(taskType)),
			getFilterMap(NamespaceFilter(namespace), TaskQueueFilter(taskQueue)),
		}

		for _, filterMap := range filterMaps {
			val, err = c.client.GetDurationValue(
				key,
				filterMap,
				defaultValue,
			)
			if err != nil {
				c.logError(key, err)
			}

			if val != defaultValue {
				break
			}
		}

		c.logValue(key, val, defaultValue, durationCompareEquals)
		return val
	}
}

// GetDurationPropertyFilteredByShardID gets property with shardID id as filter and asserts that it's a duration
func (c *Collection) GetDurationPropertyFilteredByShardID(key Key, defaultValue time.Duration) DurationPropertyFnWithShardIDFilter {
	return func(shardID int32) time.Duration {
		val, err := c.client.GetDurationValue(
			key,
			getFilterMap(ShardIDFilter(shardID)),
			defaultValue,
		)
		if err != nil {
			c.logError(key, err)
		}
		c.logValue(key, val, defaultValue, durationCompareEquals)
		return val
	}
}

// GetBoolProperty gets property and asserts that it's an bool
func (c *Collection) GetBoolProperty(key Key, defaultValue bool) BoolPropertyFn {
	return func(opts ...FilterOption) bool {
		val, err := c.client.GetBoolValue(key, getFilterMap(opts...), defaultValue)
		if err != nil {
			c.logError(key, err)
		}
		c.logValue(key, val, defaultValue, boolCompareEquals)
		return val
	}
}

// GetStringProperty gets property and asserts that it's an string
func (c *Collection) GetStringProperty(key Key, defaultValue string) StringPropertyFn {
	return func(opts ...FilterOption) string {
		val, err := c.client.GetStringValue(key, getFilterMap(opts...), defaultValue)
		if err != nil {
			c.logError(key, err)
		}
		c.logValue(key, val, defaultValue, stringCompareEquals)
		return val
	}
}

// GetMapProperty gets property and asserts that it's a map
func (c *Collection) GetMapProperty(key Key, defaultValue map[string]interface{}) MapPropertyFn {
	return func(opts ...FilterOption) map[string]interface{} {
		val, err := c.client.GetMapValue(key, getFilterMap(opts...), defaultValue)
		if err != nil {
			c.logError(key, err)
		}
		c.logValue(key, val, defaultValue, reflect.DeepEqual)
		return val
	}
}

// GetStringPropertyFnWithNamespaceFilter gets property with namespace filter and asserts that its namespace
func (c *Collection) GetStringPropertyFnWithNamespaceFilter(key Key, defaultValue string) StringPropertyFnWithNamespaceFilter {
	return func(namespace string) string {
		val, err := c.client.GetStringValue(key, getFilterMap(NamespaceFilter(namespace)), defaultValue)
		if err != nil {
			c.logError(key, err)
		}
		c.logValue(key, val, defaultValue, stringCompareEquals)
		return val
	}
}

// GetMapPropertyFnWithNamespaceFilter gets property and asserts that it's a map
func (c *Collection) GetMapPropertyFnWithNamespaceFilter(key Key, defaultValue map[string]interface{}) MapPropertyFnWithNamespaceFilter {
	return func(namespace string) map[string]interface{} {
		val, err := c.client.GetMapValue(key, getFilterMap(NamespaceFilter(namespace)), defaultValue)
		if err != nil {
			c.logError(key, err)
		}
		c.logValue(key, val, defaultValue, reflect.DeepEqual)
		return val
	}
}

// GetBoolPropertyFnWithNamespaceFilter gets property with namespace filter and asserts that its namespace
func (c *Collection) GetBoolPropertyFnWithNamespaceFilter(key Key, defaultValue bool) BoolPropertyFnWithNamespaceFilter {
	return func(namespace string) bool {
		val, err := c.client.GetBoolValue(key, getFilterMap(NamespaceFilter(namespace)), defaultValue)
		if err != nil {
			c.logError(key, err)
		}
		c.logValue(key, val, defaultValue, boolCompareEquals)
		return val
	}
}

// GetBoolPropertyFnWithNamespaceIDFilter gets property with namespaceID filter and asserts that it's a bool
func (c *Collection) GetBoolPropertyFnWithNamespaceIDFilter(key Key, defaultValue bool) BoolPropertyFnWithNamespaceIDFilter {
	return func(id string) bool {
		val, err := c.client.GetBoolValue(key, getFilterMap(NamespaceIDFilter(id)), defaultValue)
		if err != nil {
			c.logError(key, err)
		}
		c.logValue(key, val, defaultValue, boolCompareEquals)
		return val
	}
}

// GetBoolPropertyFilteredByTaskQueueInfo gets property with taskQueueInfo as filters and asserts that it's an bool
func (c *Collection) GetBoolPropertyFilteredByTaskQueueInfo(key Key, defaultValue bool) BoolPropertyFnWithTaskQueueInfoFilters {
	return func(namespace string, taskQueue string, taskType enumspb.TaskQueueType) bool {
		val := defaultValue
		var err error

		filterMaps := []map[Filter]interface{}{
			getFilterMap(NamespaceFilter(namespace), TaskQueueFilter(taskQueue), TaskTypeFilter(taskType)),
			getFilterMap(NamespaceFilter(namespace), TaskQueueFilter(taskQueue)),
		}

		for _, filterMap := range filterMaps {
			val, err = c.client.GetBoolValue(
				key,
				filterMap,
				defaultValue,
			)
			if err != nil {
				c.logError(key, err)
			}

			if val != defaultValue {
				break
			}
		}

		c.logValue(key, val, defaultValue, boolCompareEquals)
		return val
	}
}
