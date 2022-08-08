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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"golang.org/x/exp/maps"

	"go.temporal.io/server/common/log"
)

type inMemoryClient struct {
	*basicClient
}

func newInMemoryClient() *inMemoryClient {
	return &inMemoryClient{
		basicClient: newBasicClient(log.NewNoopLogger()),
	}
}

func (mc *inMemoryClient) SetValue(key Key, value interface{}) {
	values := maps.Clone(mc.getValues())
	values[strings.ToLower(key.String())] = []ConstrainedValue{{Value: value}}
	mc.updateValues(values)
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

func (s *configSuite) TestGetDurationPropertyStructuredDefaults() {
	defaults := []ConstrainedValue{
		{
			Constraints: map[string]any{
				Namespace.String():     "ns2",
				TaskQueueName.String(): "tq2",
			},
			Value: 2 * time.Minute,
		},
		{
			Constraints: map[string]any{
				TaskQueueName.String(): "tq2",
			},
			Value: 5 * time.Minute,
		},
		{
			Value: 7 * time.Minute,
		},
	}
	value := s.cln.GetDurationPropertyFilteredByTaskQueueInfo(testGetDurationPropertyStructuredDefaults, defaults)
	s.Equal(7*time.Minute, value("ns1", "tq1", 0))
	s.Equal(7*time.Minute, value("ns2", "tq1", 0))
	s.Equal(5*time.Minute, value("ns1", "tq2", 0))
	s.Equal(2*time.Minute, value("ns2", "tq2", 0))

	// user-set values should take precedence. defaults are included below in the interleaved
	// precedence order to make the test easier to read
	values := maps.Clone(s.client.getValues())
	values[strings.ToLower(testGetDurationPropertyStructuredDefaults)] = []ConstrainedValue{
		{
			Constraints: map[string]any{
				Namespace.String():     "ns2",
				TaskQueueName.String(): "tq2",
			},
			Value: 2 * time.Second,
		},
		// {
		//   Constraints: map[string]any{
		//     Namespace.String():     "ns2",
		//     TaskQueueName.String(): "tq2",
		//   },
		//   Value: 2 * time.Minute,
		// },
		// {
		//   Constraints: map[string]any{
		//     TaskQueueName.String(): "tq2",
		//   },
		//   Value: 5 * time.Minute,
		// },
		{
			Constraints: map[string]any{
				Namespace.String(): "ns1",
			},
			Value: 5 * time.Second,
		},
		{
			Value: 7 * time.Second,
		},
		// {
		//   Value: 7 * time.Minute,
		// },
	}
	s.client.updateValues(values)

	s.Equal(5*time.Second, value("ns1", "tq1", 0))
	s.Equal(7*time.Second, value("ns2", "tq1", 0))
	s.Equal(5*time.Minute, value("ns1", "tq2", 0))
	s.Equal(2*time.Second, value("ns2", "tq2", 0))
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
