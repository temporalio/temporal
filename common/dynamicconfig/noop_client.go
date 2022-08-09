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
	"time"

	"go.temporal.io/server/common/log"
)

type (
	// noopClient is a dummy implements of dynamicconfig Client interface, all operations will always return default values.
	noopClient struct{}
)

// NewNoopClient creates a nop client
func NewNoopClient() *noopClient {
	return &noopClient{}
}

// NewNoopCollection creates a new nop collection
func NewNoopCollection() *Collection {
	return NewCollection(&noopClient{}, log.NewNoopLogger())
}

func (mc *noopClient) GetValue(name Key, defaultValue interface{}) (interface{}, error) {
	return nil, errors.New("noop key search")
}

func (mc *noopClient) GetValueWithFilters(name Key, filters []map[Filter]interface{}, defaultValue interface{}) (interface{}, error) {
	return nil, errors.New("noop key search")
}

func (mc *noopClient) GetIntValue(name Key, filters []map[Filter]interface{}, defaultValue int) (int, error) {
	return defaultValue, errors.New("noop key search")
}

func (mc *noopClient) GetFloatValue(name Key, filters []map[Filter]interface{}, defaultValue float64) (float64, error) {
	return defaultValue, errors.New("noop key search")
}

func (mc *noopClient) GetBoolValue(name Key, filters []map[Filter]interface{}, defaultValue bool) (bool, error) {
	if len(filters) > 0 && filters[0][Namespace] == "TestRawHistoryNamespace" {
		return true, errors.New("noop key search")
	}
	return defaultValue, errors.New("noop key search")
}

func (mc *noopClient) GetStringValue(name Key, filters []map[Filter]interface{}, defaultValue string) (string, error) {
	return defaultValue, errors.New("noop key search")
}

func (mc *noopClient) GetMapValue(name Key, filters []map[Filter]interface{}, defaultValue map[string]interface{}) (map[string]interface{}, error) {
	return defaultValue, errors.New("noop key search")
}

func (mc *noopClient) GetDurationValue(name Key, filters []map[Filter]interface{}, defaultValue time.Duration) (time.Duration, error) {
	return defaultValue, errors.New("noop key search")
}
