// Copyright (c) 2017 Uber Technologies, Inc.
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

	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
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
		c.logger.Warn("Failed to fetch key from dynamic config", tag.Key(key.String()), tag.Error(err))
	}
}

func (c *Collection) logValue(
	key Key,
	value, defaultValue interface{},
	cmpValueEquals func(interface{}, interface{}) bool,
) {
	loadedValue, loaded := c.keys.LoadOrStore(key, value)
	if !loaded || !cmpValueEquals(loadedValue, value) {
		c.logger.Info("Get dynamic config",
			tag.Name(key.String()), tag.Value(value), tag.DefaultValue(defaultValue))
	}
}

// PropertyFn is a wrapper to get property from dynamic config
type PropertyFn func() interface{}

// IntPropertyFn is a wrapper to get int property from dynamic config
type IntPropertyFn func(opts ...FilterOption) int

// IntPropertyFnWithDomainFilter is a wrapper to get int property from dynamic config with domain as filter
type IntPropertyFnWithDomainFilter func(domain string) int

// IntPropertyFnWithTaskListInfoFilters is a wrapper to get int property from dynamic config with three filters: domain, taskList, taskType
type IntPropertyFnWithTaskListInfoFilters func(domain string, taskList string, taskType int) int

// FloatPropertyFn is a wrapper to get float property from dynamic config
type FloatPropertyFn func(opts ...FilterOption) float64

// DurationPropertyFn is a wrapper to get duration property from dynamic config
type DurationPropertyFn func(opts ...FilterOption) time.Duration

// DurationPropertyFnWithDomainFilter is a wrapper to get duration property from dynamic config with domain as filter
type DurationPropertyFnWithDomainFilter func(domain string) time.Duration

// DurationPropertyFnWithTaskListInfoFilters is a wrapper to get duration property from dynamic config  with three filters: domain, taskList, taskType
type DurationPropertyFnWithTaskListInfoFilters func(domain string, taskList string, taskType int) time.Duration

// BoolPropertyFn is a wrapper to get bool property from dynamic config
type BoolPropertyFn func(opts ...FilterOption) bool

// StringPropertyFn is a wrapper to get string property from dynamic config
type StringPropertyFn func(opts ...FilterOption) string

// MapPropertyFn is a wrapper to get map property from dynamic config
type MapPropertyFn func(opts ...FilterOption) map[string]interface{}

// StringPropertyFnWithDomainFilter is a wrapper to get string property from dynamic config
type StringPropertyFnWithDomainFilter func(domain string) string

// BoolPropertyFnWithDomainFilter is a wrapper to get string property from dynamic config
type BoolPropertyFnWithDomainFilter func(domain string) bool

// BoolPropertyFnWithTaskListInfoFilters is a wrapper to get bool property from dynamic config with three filters: domain, taskList, taskType
type BoolPropertyFnWithTaskListInfoFilters func(domain string, taskList string, taskType int) bool

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

// GetIntPropertyFilteredByDomain gets property with domain filter and asserts that it's an integer
func (c *Collection) GetIntPropertyFilteredByDomain(key Key, defaultValue int) IntPropertyFnWithDomainFilter {
	return func(domain string) int {
		val, err := c.client.GetIntValue(key, getFilterMap(DomainFilter(domain)), defaultValue)
		if err != nil {
			c.logError(key, err)
		}
		c.logValue(key, val, defaultValue, intCompareEquals)
		return val
	}
}

// GetIntPropertyFilteredByTaskListInfo gets property with taskListInfo as filters and asserts that it's an integer
func (c *Collection) GetIntPropertyFilteredByTaskListInfo(key Key, defaultValue int) IntPropertyFnWithTaskListInfoFilters {
	return func(domain string, taskList string, taskType int) int {
		val, err := c.client.GetIntValue(
			key,
			getFilterMap(DomainFilter(domain), TaskListFilter(taskList), TaskTypeFilter(taskType)),
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

// GetDurationPropertyFilteredByDomain gets property with domain filter and asserts that it's a duration
func (c *Collection) GetDurationPropertyFilteredByDomain(key Key, defaultValue time.Duration) DurationPropertyFnWithDomainFilter {
	return func(domain string) time.Duration {
		val, err := c.client.GetDurationValue(key, getFilterMap(DomainFilter(domain)), defaultValue)
		if err != nil {
			c.logError(key, err)
		}
		c.logValue(key, val, defaultValue, durationCompareEquals)
		return val
	}
}

// GetDurationPropertyFilteredByTaskListInfo gets property with taskListInfo as filters and asserts that it's a duration
func (c *Collection) GetDurationPropertyFilteredByTaskListInfo(key Key, defaultValue time.Duration) DurationPropertyFnWithTaskListInfoFilters {
	return func(domain string, taskList string, taskType int) time.Duration {
		val, err := c.client.GetDurationValue(
			key,
			getFilterMap(DomainFilter(domain), TaskListFilter(taskList), TaskTypeFilter(taskType)),
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

// GetStringPropertyFnWithDomainFilter gets property with domain filter and asserts that its domain
func (c *Collection) GetStringPropertyFnWithDomainFilter(key Key, defaultValue string) StringPropertyFnWithDomainFilter {
	return func(domain string) string {
		val, err := c.client.GetStringValue(key, getFilterMap(DomainFilter(domain)), defaultValue)
		if err != nil {
			c.logError(key, err)
		}
		c.logValue(key, val, defaultValue, stringCompareEquals)
		return val
	}
}

// GetBoolPropertyFnWithDomainFilter gets property with domain filter and asserts that its domain
func (c *Collection) GetBoolPropertyFnWithDomainFilter(key Key, defaultValue bool) BoolPropertyFnWithDomainFilter {
	return func(domain string) bool {
		val, err := c.client.GetBoolValue(key, getFilterMap(DomainFilter(domain)), defaultValue)
		if err != nil {
			c.logError(key, err)
		}
		c.logValue(key, val, defaultValue, boolCompareEquals)
		return val
	}
}

// GetBoolPropertyFilteredByTaskListInfo gets property with taskListInfo as filters and asserts that it's an bool
func (c *Collection) GetBoolPropertyFilteredByTaskListInfo(key Key, defaultValue bool) BoolPropertyFnWithTaskListInfoFilters {
	return func(domain string, taskList string, taskType int) bool {
		val, err := c.client.GetBoolValue(
			key,
			getFilterMap(DomainFilter(domain), TaskListFilter(taskList), TaskTypeFilter(taskType)),
			defaultValue,
		)
		if err != nil {
			c.logError(key, err)
		}
		c.logValue(key, val, defaultValue, boolCompareEquals)
		return val
	}
}
