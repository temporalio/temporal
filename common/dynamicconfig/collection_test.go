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
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/log"
)

const (
	// dynamic config for tests
	unknownKey                                        = "unknownKey"
	testGetPropertyKey                                = "testGetPropertyKey"
	testCaseInsensitivePropertyKey                    = "testCaseInsensitivePropertyKey"
	testGetIntPropertyKey                             = "testGetIntPropertyKey"
	testGetFloat64PropertyKey                         = "testGetFloat64PropertyKey"
	testGetDurationPropertyKey                        = "testGetDurationPropertyKey"
	testGetBoolPropertyKey                            = "testGetBoolPropertyKey"
	testGetStringPropertyKey                          = "testGetStringPropertyKey"
	testGetMapPropertyKey                             = "testGetMapPropertyKey"
	testGetIntPropertyFilteredByNamespaceKey          = "testGetIntPropertyFilteredByNamespaceKey"
	testGetDurationPropertyFilteredByNamespaceKey     = "testGetDurationPropertyFilteredByNamespaceKey"
	testGetIntPropertyFilteredByTaskQueueInfoKey      = "testGetIntPropertyFilteredByTaskQueueInfoKey"
	testGetDurationPropertyFilteredByTaskQueueInfoKey = "testGetDurationPropertyFilteredByTaskQueueInfoKey"
	testGetDurationPropertyStructuredDefaults         = "testGetDurationPropertyStructuredDefaults"
	testGetBoolPropertyFilteredByNamespaceIDKey       = "testGetBoolPropertyFilteredByNamespaceIDKey"
	testGetBoolPropertyFilteredByTaskQueueInfoKey     = "testGetBoolPropertyFilteredByTaskQueueInfoKey"
)

// Note: fileSourceSuite also heavily tests Collection, since some tests are easier with data
// provided from a file.
type collectionSuite struct {
	suite.Suite
	source StaticSource
	cln    *Collection
}

func TestCollectionSuite(t *testing.T) {
	s := new(collectionSuite)
	suite.Run(t, s)
}

func (s *collectionSuite) SetupSuite() {
	s.source = make(StaticSource)
	logger := log.NewNoopLogger()
	s.cln = NewCollection(s.source, logger)
}

func (s *collectionSuite) TestGetIntProperty() {
	value := s.cln.GetIntProperty(testGetIntPropertyKey, 10)
	s.Equal(10, value())
	s.source[testGetIntPropertyKey] = 50
	s.Equal(50, value())
}

func (s *collectionSuite) TestGetIntPropertyFilteredByNamespace() {
	namespace := "testNamespace"
	value := s.cln.GetIntPropertyFilteredByNamespace(testGetIntPropertyFilteredByNamespaceKey, 10)
	s.Equal(10, value(namespace))
	s.source[testGetIntPropertyFilteredByNamespaceKey] = 50
	s.Equal(50, value(namespace))
}

func (s *collectionSuite) TestGetStringPropertyFnWithNamespaceFilter() {
	namespace := "testNamespace"
	value := s.cln.GetStringPropertyFnWithNamespaceFilter(DefaultEventEncoding, "abc")
	s.Equal("abc", value(namespace))
	s.source[DefaultEventEncoding] = "efg"
	s.Equal("efg", value(namespace))
}

func (s *collectionSuite) TestGetIntPropertyFilteredByTaskQueueInfo() {
	namespace := "testNamespace"
	taskQueue := "testTaskQueue"
	value := s.cln.GetIntPropertyFilteredByTaskQueueInfo(testGetIntPropertyFilteredByTaskQueueInfoKey, 10)
	s.Equal(10, value(namespace, taskQueue, 0))
	s.source[testGetIntPropertyFilteredByTaskQueueInfoKey] = 50
	s.Equal(50, value(namespace, taskQueue, 0))
}

func (s *collectionSuite) TestGetFloat64Property() {
	value := s.cln.GetFloat64Property(testGetFloat64PropertyKey, 0.1)
	s.Equal(0.1, value())
	s.source[testGetFloat64PropertyKey] = 0.01
	s.Equal(0.01, value())
}

func (s *collectionSuite) TestGetBoolProperty() {
	value := s.cln.GetBoolProperty(testGetBoolPropertyKey, true)
	s.Equal(true, value())
	s.source[testGetBoolPropertyKey] = false
	s.Equal(false, value())
}

func (s *collectionSuite) TestGetBoolPropertyFilteredByNamespaceID() {
	namespaceID := "testNamespaceID"
	value := s.cln.GetBoolPropertyFnWithNamespaceIDFilter(testGetBoolPropertyFilteredByNamespaceIDKey, true)
	s.Equal(true, value(namespaceID))
	s.source[testGetBoolPropertyFilteredByNamespaceIDKey] = false
	s.Equal(false, value(namespaceID))
}

func (s *collectionSuite) TestGetBoolPropertyFilteredByTaskQueueInfo() {
	namespace := "testNamespace"
	taskQueue := "testTaskQueue"
	value := s.cln.GetBoolPropertyFilteredByTaskQueueInfo(testGetBoolPropertyFilteredByTaskQueueInfoKey, false)
	s.Equal(false, value(namespace, taskQueue, 0))
	s.source[testGetBoolPropertyFilteredByTaskQueueInfoKey] = true
	s.Equal(true, value(namespace, taskQueue, 0))
}

func (s *collectionSuite) TestGetDurationProperty() {
	value := s.cln.GetDurationProperty(testGetDurationPropertyKey, time.Second)
	s.Equal(time.Second, value())
	s.source[testGetDurationPropertyKey] = time.Minute
	s.Equal(time.Minute, value())
}

func (s *collectionSuite) TestGetDurationPropertyFilteredByNamespace() {
	namespace := "testNamespace"
	value := s.cln.GetDurationPropertyFilteredByNamespace(testGetDurationPropertyFilteredByNamespaceKey, time.Second)
	s.Equal(time.Second, value(namespace))
	s.source[testGetDurationPropertyFilteredByNamespaceKey] = time.Minute
	s.Equal(time.Minute, value(namespace))
}

func (s *collectionSuite) TestGetDurationPropertyFilteredByTaskQueueInfo() {
	namespace := "testNamespace"
	taskQueue := "testTaskQueue"
	value := s.cln.GetDurationPropertyFilteredByTaskQueueInfo(testGetDurationPropertyFilteredByTaskQueueInfoKey, time.Second)
	s.Equal(time.Second, value(namespace, taskQueue, 0))
	s.source[testGetDurationPropertyFilteredByTaskQueueInfoKey] = time.Minute
	s.Equal(time.Minute, value(namespace, taskQueue, 0))
}

func (s *collectionSuite) TestGetDurationPropertyStructuredDefaults() {
	defaults := []ConstrainedValue{
		{
			Constraints: Constraints{
				Namespace:     "ns2",
				TaskQueueName: "tq2",
			},
			Value: 2 * time.Minute,
		},
		{
			Constraints: Constraints{
				TaskQueueName: "tq2",
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
	s.source[testGetDurationPropertyStructuredDefaults] = []ConstrainedValue{
		{
			Constraints: Constraints{
				Namespace:     "ns2",
				TaskQueueName: "tq2",
			},
			Value: 2 * time.Second,
		},
		// {
		//   Constraints: Constraints{
		//     Namespace:     "ns2",
		//     TaskQueueName: "tq2",
		//   },
		//   Value: 2 * time.Minute,
		// },
		// {
		//   Constraints: Constraints{
		//     TaskQueueName: "tq2",
		//   },
		//   Value: 5 * time.Minute,
		// },
		{
			Constraints: Constraints{
				Namespace: "ns1",
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

	s.Equal(5*time.Second, value("ns1", "tq1", 0))
	s.Equal(7*time.Second, value("ns2", "tq1", 0))
	s.Equal(5*time.Minute, value("ns1", "tq2", 0))
	s.Equal(2*time.Second, value("ns2", "tq2", 0))
}

func (s *collectionSuite) TestGetMapProperty() {
	val := map[string]interface{}{
		"testKey": 123,
	}
	value := s.cln.GetMapProperty(testGetMapPropertyKey, val)
	s.Equal(val, value())
	val["testKey"] = "321"
	s.source[testGetMapPropertyKey] = val
	s.Equal(val, value())
	s.Equal("321", value()["testKey"])
}

func (s *collectionSuite) TestFindMatch() {
	testCases := []struct {
		v       []ConstrainedValue
		filters []Constraints
		matched bool
	}{
		{
			v: []ConstrainedValue{
				{Constraints: Constraints{}},
			},
			filters: []Constraints{
				{Namespace: "some random namespace"},
			},
			matched: false,
		},
		{
			v: []ConstrainedValue{
				{Constraints: Constraints{Namespace: "samples-namespace"}},
			},
			filters: []Constraints{
				{Namespace: "some random namespace"},
			},
			matched: false,
		},
		{
			v: []ConstrainedValue{
				{Constraints: Constraints{Namespace: "samples-namespace", TaskQueueName: "sample-task-queue"}},
			},
			filters: []Constraints{
				{Namespace: "samples-namespace", TaskQueueName: "sample-task-queue"},
			},
			matched: true,
		},
		{
			v: []ConstrainedValue{
				{Constraints: Constraints{Namespace: "samples-namespace"}},
			},
			filters: []Constraints{
				{TaskQueueName: "sample-task-queue"},
			},
			matched: false,
		},
	}

	for _, tc := range testCases {
		_, err := findMatch(tc.v, nil, tc.filters)
		s.Equal(tc.matched, err == nil)
		_, err = findMatch(nil, tc.v, tc.filters)
		s.Equal(tc.matched, err == nil)
	}
}
