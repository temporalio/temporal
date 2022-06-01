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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/log"
)

type inMemoryClient struct {
	globalValues atomic.Value
}

func newInMemoryClient() *inMemoryClient {
	var globalValues atomic.Value
	globalValues.Store(make(map[Key]interface{}))
	return &inMemoryClient{globalValues: globalValues}
}

func (mc *inMemoryClient) SetValue(key Key, value interface{}) {
	v := mc.globalValues.Load().(map[Key]interface{})
	v[key] = value
	mc.globalValues.Store(v)
}

func (mc *inMemoryClient) GetValue(key Key, defaultValue interface{}) (interface{}, error) {
	v := mc.globalValues.Load().(map[Key]interface{})
	if val, ok := v[key]; ok {
		return val, nil
	}
	return defaultValue, errors.New("unable to find key")
}

func (mc *inMemoryClient) GetValueWithFilters(
	name Key, filters []map[Filter]interface{}, defaultValue interface{},
) (interface{}, error) {
	return mc.GetValue(name, defaultValue)
}

func (mc *inMemoryClient) GetIntValue(name Key, filters []map[Filter]interface{}, defaultValue int) (int, error) {
	v := mc.globalValues.Load().(map[Key]interface{})
	if val, ok := v[name]; ok {
		return val.(int), nil
	}
	return defaultValue, errors.New("unable to find key")
}

func (mc *inMemoryClient) GetFloatValue(name Key, filters []map[Filter]interface{}, defaultValue float64) (float64, error) {
	v := mc.globalValues.Load().(map[Key]interface{})
	if val, ok := v[name]; ok {
		return val.(float64), nil
	}
	return defaultValue, errors.New("unable to find key")
}

func (mc *inMemoryClient) GetBoolValue(name Key, filters []map[Filter]interface{}, defaultValue bool) (bool, error) {
	v := mc.globalValues.Load().(map[Key]interface{})
	if val, ok := v[name]; ok {
		return val.(bool), nil
	}
	return defaultValue, errors.New("unable to find key")
}

func (mc *inMemoryClient) GetStringValue(name Key, filters []map[Filter]interface{}, defaultValue string) (string, error) {
	v := mc.globalValues.Load().(map[Key]interface{})
	if val, ok := v[name]; ok {
		return val.(string), nil
	}
	return defaultValue, errors.New("unable to find key")
}

func (mc *inMemoryClient) GetMapValue(
	name Key, filters []map[Filter]interface{}, defaultValue map[string]interface{},
) (map[string]interface{}, error) {
	v := mc.globalValues.Load().(map[Key]interface{})
	if val, ok := v[name]; ok {
		return val.(map[string]interface{}), nil
	}
	return defaultValue, errors.New("unable to find key")
}

func (mc *inMemoryClient) GetDurationValue(
	name Key, filters []map[Filter]interface{}, defaultValue time.Duration,
) (time.Duration, error) {
	v := mc.globalValues.Load().(map[Key]interface{})
	if val, ok := v[name]; ok {
		return val.(time.Duration), nil
	}
	return defaultValue, errors.New("unable to find key")
}

type configSuite struct {
	suite.Suite
	client *inMemoryClient
	cln    *Collection
}

func TestConfigSuite(t *testing.T) {
	s := new(configSuite)
	suite.Run(t, s)
}

func (s *configSuite) SetupSuite() {
	s.client = newInMemoryClient()
	logger := log.NewNoopLogger()
	s.cln = NewCollection(s.client, logger)
}

func (s *configSuite) TestGetProperty() {
	value := s.cln.GetProperty(testGetPropertyKey, "a")
	s.Equal("a", value())
	s.client.SetValue(testGetPropertyKey, "b")
	s.Equal("b", value())
}

func (s *configSuite) TestGetIntProperty() {
	value := s.cln.GetIntProperty(testGetIntPropertyKey, 10)
	s.Equal(10, value())
	s.client.SetValue(testGetIntPropertyKey, 50)
	s.Equal(50, value())
}

func (s *configSuite) TestGetIntPropertyFilteredByNamespace() {
	namespace := "testNamespace"
	value := s.cln.GetIntPropertyFilteredByNamespace(testGetIntPropertyFilteredByNamespaceKey, 10)
	s.Equal(10, value(namespace))
	s.client.SetValue(testGetIntPropertyFilteredByNamespaceKey, 50)
	s.Equal(50, value(namespace))
}

func (s *configSuite) TestGetStringPropertyFnWithNamespaceFilter() {
	namespace := "testNamespace"
	value := s.cln.GetStringPropertyFnWithNamespaceFilter(DefaultEventEncoding, "abc")
	s.Equal("abc", value(namespace))
	s.client.SetValue(DefaultEventEncoding, "efg")
	s.Equal("efg", value(namespace))
}

func (s *configSuite) TestGetIntPropertyFilteredByTaskQueueInfo() {
	namespace := "testNamespace"
	taskQueue := "testTaskQueue"
	value := s.cln.GetIntPropertyFilteredByTaskQueueInfo(testGetIntPropertyFilteredByTaskQueueInfoKey, 10)
	s.Equal(10, value(namespace, taskQueue, 0))
	s.client.SetValue(testGetIntPropertyFilteredByTaskQueueInfoKey, 50)
	s.Equal(50, value(namespace, taskQueue, 0))
}

func (s *configSuite) TestGetFloat64Property() {
	value := s.cln.GetFloat64Property(testGetFloat64PropertyKey, 0.1)
	s.Equal(0.1, value())
	s.client.SetValue(testGetFloat64PropertyKey, 0.01)
	s.Equal(0.01, value())
}

func (s *configSuite) TestGetBoolProperty() {
	value := s.cln.GetBoolProperty(testGetBoolPropertyKey, true)
	s.Equal(true, value())
	s.client.SetValue(testGetBoolPropertyKey, false)
	s.Equal(false, value())
}

func (s *configSuite) TestGetBoolPropertyFilteredByNamespaceID() {
	namespaceID := "testNamespaceID"
	value := s.cln.GetBoolPropertyFnWithNamespaceIDFilter(testGetBoolPropertyFilteredByNamespaceIDKey, true)
	s.Equal(true, value(namespaceID))
	s.client.SetValue(testGetBoolPropertyFilteredByNamespaceIDKey, false)
	s.Equal(false, value(namespaceID))
}

func (s *configSuite) TestGetBoolPropertyFilteredByTaskQueueInfo() {
	namespace := "testNamespace"
	taskQueue := "testTaskQueue"
	value := s.cln.GetBoolPropertyFilteredByTaskQueueInfo(testGetBoolPropertyFilteredByTaskQueueInfoKey, false)
	s.Equal(false, value(namespace, taskQueue, 0))
	s.client.SetValue(testGetBoolPropertyFilteredByTaskQueueInfoKey, true)
	s.Equal(true, value(namespace, taskQueue, 0))
}

func (s *configSuite) TestGetDurationProperty() {
	value := s.cln.GetDurationProperty(testGetDurationPropertyKey, time.Second)
	s.Equal(time.Second, value())
	s.client.SetValue(testGetDurationPropertyKey, time.Minute)
	s.Equal(time.Minute, value())
}

func (s *configSuite) TestGetDurationPropertyFilteredByNamespace() {
	namespace := "testNamespace"
	value := s.cln.GetDurationPropertyFilteredByNamespace(testGetDurationPropertyFilteredByNamespaceKey, time.Second)
	s.Equal(time.Second, value(namespace))
	s.client.SetValue(testGetDurationPropertyFilteredByNamespaceKey, time.Minute)
	s.Equal(time.Minute, value(namespace))
}

func (s *configSuite) TestGetDurationPropertyFilteredByTaskQueueInfo() {
	namespace := "testNamespace"
	taskQueue := "testTaskQueue"
	value := s.cln.GetDurationPropertyFilteredByTaskQueueInfo(testGetDurationPropertyFilteredByTaskQueueInfoKey, time.Second)
	s.Equal(time.Second, value(namespace, taskQueue, 0))
	s.client.SetValue(testGetDurationPropertyFilteredByTaskQueueInfoKey, time.Minute)
	s.Equal(time.Minute, value(namespace, taskQueue, 0))
}

func (s *configSuite) TestGetMapProperty() {
	val := map[string]interface{}{
		"testKey": 123,
	}
	value := s.cln.GetMapProperty(testGetMapPropertyKey, val)
	s.Equal(val, value())
	val["testKey"] = "321"
	s.client.SetValue(testGetMapPropertyKey, val)
	s.Equal(val, value())
	s.Equal("321", value()["testKey"])
}

func TestDynamicConfigFilterTypeIsMapped(t *testing.T) {
	require.Equal(t, int(lastFilterTypeForTest), len(filters))
	for i := unknownFilter; i < lastFilterTypeForTest; i++ {
		require.NotEmpty(t, filters[i])
	}
}
