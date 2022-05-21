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
	"reflect"
	"strings"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/primitives/timestamp"
)

var _ Client = (*basicClient)(nil)

var (
	errKeyNotPresent        = errors.New("key not present")
	errNoMatchingConstraint = errors.New("no matching constraint in key")
)

type configValueMap map[string][]*constrainedValue

type constrainedValue struct {
	Value       interface{}
	Constraints map[string]interface{}
}

type basicClient struct {
	logger log.Logger
	values atomic.Value // configValueMap
}

func newBasicClient(logger log.Logger) *basicClient {
	bc := &basicClient{
		logger: logger,
	}
	bc.values.Store(configValueMap{})
	return bc
}

func (bc *basicClient) getValues() configValueMap {
	return bc.values.Load().(configValueMap)
}

func (bc *basicClient) updateValues(newValues configValueMap) {
	oldValues := bc.getValues()
	bc.values.Store(newValues)
	bc.logDiff(oldValues, newValues)
}

func (bc *basicClient) GetValue(
	name Key,
	defaultValue interface{},
) (interface{}, error) {
	return bc.getValueWithFilters(name, nil, defaultValue)
}

func (bc *basicClient) GetValueWithFilters(
	name Key,
	filters []map[Filter]interface{},
	defaultValue interface{},
) (interface{}, error) {
	return bc.getValueWithFilters(name, filters, defaultValue)
}

func (bc *basicClient) GetIntValue(
	name Key,
	filters []map[Filter]interface{},
	defaultValue int,
) (int, error) {
	val, err := bc.getValueWithFilters(name, filters, defaultValue)
	if err != nil {
		return defaultValue, err
	}

	if intVal, ok := val.(int); ok {
		return intVal, nil
	}
	return defaultValue, errors.New("value type is not int")
}

func (bc *basicClient) GetFloatValue(
	name Key,
	filters []map[Filter]interface{},
	defaultValue float64,
) (float64, error) {
	val, err := bc.getValueWithFilters(name, filters, defaultValue)
	if err != nil {
		return defaultValue, err
	}

	if floatVal, ok := val.(float64); ok {
		return floatVal, nil
	} else if intVal, ok := val.(int); ok {
		return float64(intVal), nil
	}
	return defaultValue, errors.New("value type is not float64")
}

func (bc *basicClient) GetBoolValue(
	name Key,
	filters []map[Filter]interface{},
	defaultValue bool,
) (bool, error) {
	val, err := bc.getValueWithFilters(name, filters, defaultValue)
	if err != nil {
		return defaultValue, err
	}

	if boolVal, ok := val.(bool); ok {
		return boolVal, nil
	}
	return defaultValue, errors.New("value type is not bool")
}

func (bc *basicClient) GetStringValue(
	name Key,
	filters []map[Filter]interface{},
	defaultValue string,
) (string, error) {
	val, err := bc.getValueWithFilters(name, filters, defaultValue)
	if err != nil {
		return defaultValue, err
	}

	if stringVal, ok := val.(string); ok {
		return stringVal, nil
	}
	return defaultValue, errors.New("value type is not string")
}

func (bc *basicClient) GetMapValue(
	name Key,
	filters []map[Filter]interface{},
	defaultValue map[string]interface{},
) (map[string]interface{}, error) {
	val, err := bc.getValueWithFilters(name, filters, defaultValue)
	if err != nil {
		return defaultValue, err
	}
	if mapVal, ok := val.(map[string]interface{}); ok {
		return mapVal, nil
	}
	return defaultValue, errors.New("value type is not map")
}

func (bc *basicClient) GetDurationValue(
	name Key, filters []map[Filter]interface{}, defaultValue time.Duration,
) (time.Duration, error) {
	val, err := bc.getValueWithFilters(name, filters, defaultValue)
	if err != nil {
		return defaultValue, err
	}

	switch v := val.(type) {
	case time.Duration:
		return v, nil
	case string:
		{
			d, err := timestamp.ParseDurationDefaultDays(v)
			if err != nil {
				return defaultValue, fmt.Errorf("failed to parse duration: %v", err)
			}
			return d, nil
		}
	}
	return defaultValue, errors.New("value not convertible to Duration")
}

func (bc *basicClient) getValueWithFilters(
	key Key,
	filters []map[Filter]interface{},
	defaultValue interface{},
) (interface{}, error) {
	keyName := strings.ToLower(key.String())
	values := bc.values.Load().(configValueMap)
	constrainedValues := values[keyName]
	if constrainedValues == nil {
		return defaultValue, errKeyNotPresent
	}
	for _, filter := range filters {
		for _, constrainedValue := range constrainedValues {
			if match(constrainedValue, filter) {
				return constrainedValue.Value, nil
			}
		}
	}
	// if not found yet, try empty filter
	for _, constrainedValue := range constrainedValues {
		if len(constrainedValue.Constraints) == 0 {
			return constrainedValue.Value, nil
		}
	}
	// key is present but no constraint section matches
	return defaultValue, errNoMatchingConstraint
}

// match will return true if the constraints matches the filters exactly
func match(v *constrainedValue, filters map[Filter]interface{}) bool {
	if len(v.Constraints) != len(filters) {
		return false
	}

	for filter, filterValue := range filters {
		if v.Constraints[filter.String()] != filterValue {
			return false
		}
	}
	return true
}

func (fc *basicClient) logDiff(old configValueMap, new configValueMap) {
	for key, newValues := range new {
		oldValues, ok := old[key]
		if !ok {
			for _, newValue := range newValues {
				// new key added
				fc.logValueDiff(key, nil, newValue)
			}
		} else {
			// compare existing keys
			fc.logConstraintsDiff(key, oldValues, newValues)
		}
	}

	// check for removed values
	for key, oldValues := range old {
		if _, ok := new[key]; !ok {
			for _, oldValue := range oldValues {
				fc.logValueDiff(key, oldValue, nil)
			}
		}
	}
}

func (bc *basicClient) logConstraintsDiff(key string, oldValues []*constrainedValue, newValues []*constrainedValue) {
	for _, oldValue := range oldValues {
		matchFound := false
		for _, newValue := range newValues {
			if reflect.DeepEqual(oldValue.Constraints, newValue.Constraints) {
				matchFound = true
				if !reflect.DeepEqual(oldValue.Value, newValue.Value) {
					bc.logValueDiff(key, oldValue, newValue)
				}
			}
		}
		if !matchFound {
			bc.logValueDiff(key, oldValue, nil)
		}
	}

	for _, newValue := range newValues {
		matchFound := false
		for _, oldValue := range oldValues {
			if reflect.DeepEqual(oldValue.Constraints, newValue.Constraints) {
				matchFound = true
			}
		}
		if !matchFound {
			bc.logValueDiff(key, nil, newValue)
		}
	}
}

func (bc *basicClient) logValueDiff(key string, oldValue *constrainedValue, newValue *constrainedValue) {
	logLine := &strings.Builder{}
	logLine.Grow(128)
	logLine.WriteString("dynamic config changed for the key: ")
	logLine.WriteString(key)
	logLine.WriteString(" oldValue: ")
	bc.appendConstrainedValue(logLine, oldValue)
	logLine.WriteString(" newValue: ")
	bc.appendConstrainedValue(logLine, newValue)
	bc.logger.Info(logLine.String())
}

func (bc *basicClient) appendConstrainedValue(logLine *strings.Builder, value *constrainedValue) {
	if value == nil {
		logLine.WriteString("nil")
	} else {
		logLine.WriteString("{ constraints: {")
		for constraintKey, constraintValue := range value.Constraints {
			logLine.WriteString("{")
			logLine.WriteString(constraintKey)
			logLine.WriteString(":")
			logLine.WriteString(fmt.Sprintf("%v", constraintValue))
			logLine.WriteString("}")
		}
		logLine.WriteString(fmt.Sprint("} value: ", value.Value, " }"))
	}
}
