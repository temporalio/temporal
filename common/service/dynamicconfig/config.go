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
	"sync"
	"time"

	"github.com/uber-common/bark"
)

// NewCollection creates a new collection
func NewCollection(client Client, logger bark.Logger) *Collection {
	return &Collection{client, logger, &sync.Map{}}
}

// Collection wraps dynamic config client with a closure so that across the code, the config values
// can be directly accessed by calling the function without propagating the client everywhere in
// code
type Collection struct {
	client Client
	logger bark.Logger
	keys   *sync.Map
}

func (c *Collection) logNoValue(key Key, err error) {
	_, loaded := c.keys.LoadOrStore(key, key)
	if !loaded {
		c.logger.Debugf("Failed to fetch key: %s from dynamic config with err: %s", key.String(), err.Error())
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

// BoolPropertyFnWithTaskListInfoFilters is a wrapper to get bool property from dynamic config with three filters: domain, taskList, taskType
type BoolPropertyFnWithTaskListInfoFilters func(domain string, taskList string, taskType int) bool

// GetProperty gets a eface property and returns defaultValue if property is not found
func (c *Collection) GetProperty(key Key, defaultValue interface{}) PropertyFn {
	return func() interface{} {
		val, err := c.client.GetValue(key, defaultValue)
		if err != nil {
			c.logNoValue(key, err)
		}
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
			c.logNoValue(key, err)
		}
		return val
	}
}

// GetIntPropertyFilteredByDomain gets property with domain filter and asserts that it's an integer
func (c *Collection) GetIntPropertyFilteredByDomain(key Key, defaultValue int) IntPropertyFnWithDomainFilter {
	return func(domain string) int {
		val, err := c.client.GetIntValue(key, getFilterMap(DomainFilter(domain)), defaultValue)
		if err != nil {
			c.logNoValue(key, err)
		}
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
			c.logNoValue(key, err)
		}
		return val
	}
}

// GetFloat64Property gets property and asserts that it's a float64
func (c *Collection) GetFloat64Property(key Key, defaultValue float64) FloatPropertyFn {
	return func(opts ...FilterOption) float64 {
		val, err := c.client.GetFloatValue(key, getFilterMap(opts...), defaultValue)
		if err != nil {
			c.logNoValue(key, err)
		}
		return val
	}
}

// GetDurationProperty gets property and asserts that it's a duration
func (c *Collection) GetDurationProperty(key Key, defaultValue time.Duration) DurationPropertyFn {
	return func(opts ...FilterOption) time.Duration {
		val, err := c.client.GetDurationValue(key, getFilterMap(opts...), defaultValue)
		if err != nil {
			c.logNoValue(key, err)
		}
		return val
	}
}

// GetDurationPropertyFilteredByDomain gets property with domain filter and asserts that it's a duration
func (c *Collection) GetDurationPropertyFilteredByDomain(key Key, defaultValue time.Duration) DurationPropertyFnWithDomainFilter {
	return func(domain string) time.Duration {
		val, err := c.client.GetDurationValue(key, getFilterMap(DomainFilter(domain)), defaultValue)
		if err != nil {
			c.logNoValue(key, err)
		}
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
			c.logNoValue(key, err)
		}
		return val
	}
}

// GetBoolProperty gets property and asserts that it's an bool
func (c *Collection) GetBoolProperty(key Key, defaultValue bool) BoolPropertyFn {
	return func(opts ...FilterOption) bool {
		val, err := c.client.GetBoolValue(key, getFilterMap(opts...), defaultValue)
		if err != nil {
			c.logNoValue(key, err)
		}
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
			c.logNoValue(key, err)
		}
		return val
	}
}
