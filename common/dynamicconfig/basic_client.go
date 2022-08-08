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

	"golang.org/x/exp/slices"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/primitives/timestamp"
)

var _ Client = (*basicClient)(nil)

var (
	errKeyNotPresent        = errors.New("key not present")
	errNoMatchingConstraint = errors.New("no matching constraint in key")
)

type configValueMap map[string][]ConstrainedValue

type ConstrainedValue struct {
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
	oldValues := bc.values.Swap(newValues).(configValueMap)
	bc.logDiff(oldValues, newValues)
}

func (bc *basicClient) GetValue(
	name Key,
	defaultValue any,
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
	defaultValue any,
) (int, error) {
	val, err := bc.getValueWithFilters(name, filters, defaultValue)
	if intVal, ok := val.(int); ok {
		return intVal, err
	}
	intVal, _ := defaultValue.(int)
	return intVal, errors.New("value type is not int")
}

func (bc *basicClient) GetFloatValue(
	name Key,
	filters []map[Filter]interface{},
	defaultValue any,
) (float64, error) {
	val, err := bc.getValueWithFilters(name, filters, defaultValue)
	if floatVal, ok := val.(float64); ok {
		return floatVal, err
	} else if intVal, ok := val.(int); ok {
		return float64(intVal), err
	}
	floatVal, _ := defaultValue.(float64)
	return floatVal, errors.New("value type is not float64")
}

func (bc *basicClient) GetBoolValue(
	name Key,
	filters []map[Filter]interface{},
	defaultValue any,
) (bool, error) {
	val, err := bc.getValueWithFilters(name, filters, defaultValue)
	if boolVal, ok := val.(bool); ok {
		return boolVal, err
	}
	boolVal, _ := defaultValue.(bool)
	return boolVal, errors.New("value type is not bool")
}

func (bc *basicClient) GetStringValue(
	name Key,
	filters []map[Filter]interface{},
	defaultValue any,
) (string, error) {
	val, err := bc.getValueWithFilters(name, filters, defaultValue)
	if stringVal, ok := val.(string); ok {
		return stringVal, err
	}
	stringVal, _ := defaultValue.(string)
	return stringVal, errors.New("value type is not string")
}

func (bc *basicClient) GetMapValue(
	name Key,
	filters []map[Filter]interface{},
	defaultValue any,
) (map[string]any, error) {
	val, err := bc.getValueWithFilters(name, filters, defaultValue)
	if mapVal, ok := val.(map[string]any); ok {
		return mapVal, err
	}
	mapVal, _ := defaultValue.(map[string]any)
	return mapVal, errors.New("value type is not map")
}

func (bc *basicClient) GetDurationValue(
	name Key, filters []map[Filter]interface{}, defaultValue any,
) (time.Duration, error) {
	val, err := bc.getValueWithFilters(name, filters, defaultValue)
	switch v := val.(type) {
	case time.Duration:
		return v, err
	case string:
		d, parseErr := timestamp.ParseDurationDefaultDays(v)
		if parseErr != nil {
			durationVal, _ := defaultValue.(time.Duration)
			return durationVal, fmt.Errorf("failed to parse duration: %v", parseErr)
		}
		return d, err
	}
	durationVal, _ := defaultValue.(time.Duration)
	return durationVal, errors.New("value not convertible to Duration")
}

func (bc *basicClient) getValueWithFilters(
	key Key,
	filters []map[Filter]interface{},
	defaultValue interface{},
) (interface{}, error) {
	keyName := strings.ToLower(key.String())
	values := bc.getValues()
	constrainedValues := values[keyName]

	if defaultConstraints, ok := defaultValue.([]ConstrainedValue); ok {
		// if defaultValue is a list of constrained values, then one of them must have an empty
		// constraint set
		constrainedValues = append(slices.Clone(constrainedValues), defaultConstraints...)
	}

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
func match(v ConstrainedValue, filters map[Filter]interface{}) bool {
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
				fc.logValueDiff(key, nil, &newValue)
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
				fc.logValueDiff(key, &oldValue, nil)
			}
		}
	}
}

func (bc *basicClient) logConstraintsDiff(key string, oldValues []ConstrainedValue, newValues []ConstrainedValue) {
	for _, oldValue := range oldValues {
		matchFound := false
		for _, newValue := range newValues {
			if reflect.DeepEqual(oldValue.Constraints, newValue.Constraints) {
				matchFound = true
				if !reflect.DeepEqual(oldValue.Value, newValue.Value) {
					bc.logValueDiff(key, &oldValue, &newValue)
				}
			}
		}
		if !matchFound {
			bc.logValueDiff(key, &oldValue, nil)
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
			bc.logValueDiff(key, nil, &newValue)
		}
	}
}

func (bc *basicClient) logValueDiff(key string, oldValue *ConstrainedValue, newValue *ConstrainedValue) {
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

func (bc *basicClient) appendConstrainedValue(logLine *strings.Builder, value *ConstrainedValue) {
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
