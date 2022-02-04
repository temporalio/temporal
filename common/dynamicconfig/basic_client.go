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
	"strings"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common/primitives/timestamp"
)

var _ Client = (*basicClient)(nil)

type configValueMap map[string][]*constrainedValue

type constrainedValue struct {
	Value       interface{}
	Constraints map[string]interface{}
}

type basicClient struct {
	values atomic.Value // configValueMap
}

func newBasicClient() *basicClient {
	bc := &basicClient{}
	bc.values.Store(configValueMap{})
	return bc
}

func (bc *basicClient) GetValue(
	name Key,
	defaultValue interface{},
) (interface{}, error) {
	return bc.getValueWithFilters(name, nil, defaultValue)
}

func (bc *basicClient) GetValueWithFilters(
	name Key,
	filters map[Filter]interface{},
	defaultValue interface{},
) (interface{}, error) {
	return bc.getValueWithFilters(name, filters, defaultValue)
}

func (bc *basicClient) GetIntValue(
	name Key,
	filters map[Filter]interface{},
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
	filters map[Filter]interface{},
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
	filters map[Filter]interface{},
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
	filters map[Filter]interface{},
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
	filters map[Filter]interface{},
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
	name Key, filters map[Filter]interface{}, defaultValue time.Duration,
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
	filters map[Filter]interface{},
	defaultValue interface{},
) (interface{}, error) {
	keyName := strings.ToLower(key.String())
	values := bc.values.Load().(configValueMap)
	found := false
	for _, constrainedValue := range values[keyName] {
		if len(constrainedValue.Constraints) == 0 {
			// special handling for default value (value without any constraints)
			defaultValue = constrainedValue.Value
			found = true
			continue
		}
		if match(constrainedValue, filters) {
			return constrainedValue.Value, nil
		}
	}
	if !found {
		return defaultValue, errors.New("unable to find key")
	}
	return defaultValue, nil
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
