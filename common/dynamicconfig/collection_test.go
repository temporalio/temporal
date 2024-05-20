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
	"maps"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	enumsspb "go.temporal.io/server/api/enums/v1"
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
	testGetTypedPropertyKey                           = "testGetTypedPropertyKey"
	testGetIntPropertyFilteredByNamespaceKey          = "testGetIntPropertyFilteredByNamespaceKey"
	testGetDurationPropertyFilteredByNamespaceKey     = "testGetDurationPropertyFilteredByNamespaceKey"
	testGetIntPropertyFilteredByTaskQueueInfoKey      = "testGetIntPropertyFilteredByTaskQueueInfoKey"
	testGetDurationPropertyFilteredByTaskQueueInfoKey = "testGetDurationPropertyFilteredByTaskQueueInfoKey"
	testGetDurationPropertyFilteredByTaskTypeKey      = "testGetDurationPropertyFilteredByTaskTypeKey"
	testGetDurationPropertyStructuredDefaults         = "testGetDurationPropertyStructuredDefaults"
	testGetBoolPropertyFilteredByNamespaceIDKey       = "testGetBoolPropertyFilteredByNamespaceIDKey"
	testGetBoolPropertyFilteredByTaskQueueInfoKey     = "testGetBoolPropertyFilteredByTaskQueueInfoKey"
	testGetStringPropertyFilteredByNamespaceIDKey     = "testGetStringPropertyFilteredByNamespaceIDKey"
	testGetIntPropertyFilteredByDestinationKey        = "testGetIntPropertyFilteredByDestinationKey"
)

// Note: fileBasedClientSuite also heavily tests Collection, since some tests are easier with data
// provided from a file.
type collectionSuite struct {
	suite.Suite
	client StaticClient
	cln    *Collection
}

func TestCollectionSuite(t *testing.T) {
	s := new(collectionSuite)
	suite.Run(t, s)
}

func (s *collectionSuite) SetupSuite() {
	s.client = make(StaticClient)
	logger := log.NewNoopLogger()
	s.cln = NewCollection(s.client, logger)
}

func (s *collectionSuite) TestGetIntProperty() {
	setting := NewGlobalIntSetting(testGetIntPropertyKey, 10, "")
	value := setting.Get(s.cln)
	s.Equal(10, value())
	s.client[testGetIntPropertyKey] = 50
	s.Equal(50, value())
}

func (s *collectionSuite) TestGetIntPropertyFilteredByNamespace() {
	setting := NewNamespaceIntSetting(testGetIntPropertyFilteredByNamespaceKey, 10, "")
	namespace := "testNamespace"
	value := setting.Get(s.cln)
	s.Equal(10, value(namespace))
	s.client[testGetIntPropertyFilteredByNamespaceKey] = 50
	s.Equal(50, value(namespace))
}

func (s *collectionSuite) TestGetStringPropertyFnFilteredByNamespace() {
	namespace := "testNamespace"
	value := DefaultEventEncoding.Get(s.cln)
	s.Equal(DefaultEventEncoding.def, value(namespace))
	s.client[DefaultEventEncoding.key] = "efg"
	s.Equal("efg", value(namespace))
}

func (s *collectionSuite) TestGetStringPropertyFnFilteredByNamespaceID() {
	setting := NewNamespaceIDStringSetting(testGetStringPropertyFilteredByNamespaceIDKey, "abc", "")
	namespaceID := "testNamespaceID"
	value := setting.Get(s.cln)
	s.Equal("abc", value(namespaceID))
	s.client[testGetStringPropertyFilteredByNamespaceIDKey] = "efg"
	s.Equal("efg", value(namespaceID))
}

func (s *collectionSuite) TestGetIntPropertyFilteredByTaskQueueInfo() {
	setting := NewTaskQueueIntSetting(testGetIntPropertyFilteredByTaskQueueInfoKey, 10, "")
	namespace := "testNamespace"
	taskQueue := "testTaskQueue"
	value := setting.Get(s.cln)
	s.Equal(10, value(namespace, taskQueue, 0))
	s.client[testGetIntPropertyFilteredByTaskQueueInfoKey] = 50
	s.Equal(50, value(namespace, taskQueue, 0))
}

func (s *collectionSuite) TestGetFloat64Property() {
	setting := NewGlobalFloatSetting(testGetFloat64PropertyKey, 0.1, "")
	value := setting.Get(s.cln)
	s.Equal(0.1, value())
	s.client[testGetFloat64PropertyKey] = 0.01
	s.Equal(0.01, value())
}

func (s *collectionSuite) TestGetBoolProperty() {
	setting := NewGlobalBoolSetting(testGetBoolPropertyKey, true, "")
	value := setting.Get(s.cln)
	s.Equal(true, value())
	s.client[testGetBoolPropertyKey] = false
	s.Equal(false, value())
}

func (s *collectionSuite) TestGetBoolPropertyFilteredByNamespaceID() {
	setting := NewNamespaceIDBoolSetting(testGetBoolPropertyFilteredByNamespaceIDKey, true, "")
	namespaceID := "testNamespaceID"
	value := setting.Get(s.cln)
	s.Equal(true, value(namespaceID))
	s.client[testGetBoolPropertyFilteredByNamespaceIDKey] = false
	s.Equal(false, value(namespaceID))
}

func (s *collectionSuite) TestGetBoolPropertyFilteredByTaskQueueInfo() {
	setting := NewTaskQueueBoolSetting(testGetBoolPropertyFilteredByTaskQueueInfoKey, false, "")
	namespace := "testNamespace"
	taskQueue := "testTaskQueue"
	value := setting.Get(s.cln)
	s.Equal(false, value(namespace, taskQueue, 0))
	s.client[testGetBoolPropertyFilteredByTaskQueueInfoKey] = true
	s.Equal(true, value(namespace, taskQueue, 0))
}

func (s *collectionSuite) TestGetDurationProperty() {
	setting := NewGlobalDurationSetting(testGetDurationPropertyKey, 1*time.Second, "")
	value := setting.Get(s.cln)
	s.Equal(time.Second, value())
	s.client[testGetDurationPropertyKey] = time.Minute
	s.Equal(time.Minute, value())
	s.client[testGetDurationPropertyKey] = 33
	s.Equal(33*time.Second, value())
	s.client[testGetDurationPropertyKey] = "33"
	s.Equal(33*time.Second, value())
}

func (s *collectionSuite) TestGetDurationPropertyFilteredByNamespace() {
	setting := NewNamespaceDurationSetting(testGetDurationPropertyFilteredByNamespaceKey, time.Second, "")
	namespace := "testNamespace"
	value := setting.Get(s.cln)
	s.Equal(time.Second, value(namespace))
	s.client[testGetDurationPropertyFilteredByNamespaceKey] = time.Minute
	s.Equal(time.Minute, value(namespace))
}

func (s *collectionSuite) TestGetDurationPropertyFilteredByTaskQueueInfo() {
	setting := NewTaskQueueDurationSetting(testGetDurationPropertyFilteredByTaskQueueInfoKey, time.Second, "")
	namespace := "testNamespace"
	taskQueue := "testTaskQueue"
	value := setting.Get(s.cln)
	s.Equal(time.Second, value(namespace, taskQueue, 0))
	s.client[testGetDurationPropertyFilteredByTaskQueueInfoKey] = time.Minute
	s.Equal(time.Minute, value(namespace, taskQueue, 0))
}

func (s *collectionSuite) TestGetDurationPropertyFilteredByTaskType() {
	setting := NewTaskTypeDurationSetting(testGetDurationPropertyFilteredByTaskTypeKey, time.Second, "")
	taskType := enumsspb.TASK_TYPE_UNSPECIFIED
	value := setting.Get(s.cln)
	s.Equal(time.Second, value(taskType))
	s.client[testGetDurationPropertyFilteredByTaskTypeKey] = time.Minute
	s.Equal(time.Minute, value(taskType))
}

func (s *collectionSuite) TestGetDurationPropertyStructuredDefaults() {
	setting := NewTaskQueueDurationSettingWithConstrainedDefault(
		testGetDurationPropertyStructuredDefaults,
		[]TypedConstrainedValue[time.Duration]{
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
		},
		"",
	)
	value := setting.Get(s.cln)
	s.Equal(7*time.Minute, value("ns1", "tq1", 0))
	s.Equal(7*time.Minute, value("ns2", "tq1", 0))
	s.Equal(5*time.Minute, value("ns1", "tq2", 0))
	s.Equal(2*time.Minute, value("ns2", "tq2", 0))

	// user-set values should take precedence. defaults are included below in the interleaved
	// precedence order to make the test easier to read
	s.client[testGetDurationPropertyStructuredDefaults] = []ConstrainedValue{
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
	setting := NewGlobalMapSetting(
		testGetMapPropertyKey,
		map[string]interface{}{"testKey": 123},
		"",
	)
	value := setting.Get(s.cln)
	s.Equal(setting.def, value())
	val := maps.Clone(setting.def)
	val["testKey"] = "321"
	s.client[testGetMapPropertyKey] = val
	s.Equal(val, value())
	s.Equal("321", value()["testKey"])
}

func (s *collectionSuite) TestGetTyped() {
	type myFancyType struct {
		Number int
		Names  []string
	}
	setting := NewGlobalTypedSettingWithConverter(
		testGetTypedPropertyKey,
		ConvertStructure(myFancyType{-3, nil}), // used if convert is called
		myFancyType{28, []string{"global", "typed", "setting"}},
		"",
	)
	get := setting.Get(s.cln)

	s.Run("Default", func() {
		s.Equal(setting.def, get())
	})

	s.Run("Basic", func() {
		// map[string]any is what the yaml library decodes arbitrary data into
		s.client[testGetTypedPropertyKey] = map[string]any{
			"Number": 39,
			"Names":  []string{"new", "names"},
		}
		s.Equal(myFancyType{
			Number: 39,
			Names:  []string{"new", "names"},
		}, get())
	})

	s.Run("CaseInsensitive", func() {
		s.client[testGetTypedPropertyKey] = map[string]any{
			"naMES": []string{"case", "insensitive"},
		}
		s.Equal(-3, get().Number) // note the convert default is used here
		s.Equal([]string{"case", "insensitive"}, get().Names)
	})

	s.Run("WrongType", func() {
		s.client[testGetTypedPropertyKey] = 200
		s.Equal(setting.def, get())
	})
}

func (s *collectionSuite) TestGetTypedSimpleList() {
	setting := NewGlobalTypedSettingWithConverter(
		testGetTypedPropertyKey,
		ConvertStructure([]float64(nil)),
		[]float64{1.5, 1.1, 2.6, 3.7, 6.3},
		"",
	)
	get := setting.Get(s.cln)

	s.Run("Default", func() {
		s.Equal(setting.def, get())
	})

	s.Run("Basic", func() {
		s.client[testGetTypedPropertyKey] = []any{19.0, -2.0}
		s.Equal([]float64{19.0, -2.0}, get())
	})

	s.Run("WrongType", func() {
		s.client[testGetTypedPropertyKey] = []any{88.8, false, -5, "oops"}
		s.Equal(setting.def, get())
	})
}

func (s *collectionSuite) TestGetTypedListOfStruct() {
	type simple struct{ A, B int }
	setting := NewGlobalTypedSettingWithConverter(
		testGetTypedPropertyKey,
		ConvertStructure([]simple(nil)),
		[]simple{{1, 5}, {2, 9}},
		"",
	)
	get := setting.Get(s.cln)

	s.Run("Default", func() {
		s.Equal(setting.def, get())
	})

	s.Run("Basic", func() {
		s.client[testGetTypedPropertyKey] = []any{
			map[string]any{"A": 12, "B": 6},
			map[string]any{"A": -23, "B": 0},
			map[string]any{"B": 555, "C": "ignored"},
		}
		s.Equal([]simple{{12, 6}, {-23, 0}, {0, 555}}, get())
	})

	s.Run("WrongType", func() {
		s.client[testGetTypedPropertyKey] = []any{
			map[string]any{"A": false, "B": true},
		}
		s.Equal(setting.def, get())
	})
}

func (s *collectionSuite) TestGetIntPropertyFilteredByDestination() {
	setting := NewDestinationIntSetting(testGetIntPropertyFilteredByDestinationKey, 10, "")
	namespaceName := "testNamespace"
	destination1 := "testDestination1"
	destination2 := "testDestination2"
	value := setting.Get(s.cln)
	s.Equal(10, value(namespaceName, destination1))
	s.client[testGetIntPropertyFilteredByDestinationKey] = []ConstrainedValue{
		{
			Constraints: Constraints{
				Namespace:   namespaceName,
				Destination: destination1,
			},
			Value: 50,
		},
		{
			Constraints: Constraints{
				Namespace: namespaceName,
			},
			Value: 75,
		},
		{
			Constraints: Constraints{
				Destination: destination1,
			},
			Value: 90,
		},
		{
			Constraints: Constraints{
				Destination: destination2,
			},
			Value: 100,
		},
	}
	s.Equal(50, value(namespaceName, destination1))
	s.Equal(75, value(namespaceName, "testAnotherDestination"))
	s.Equal(90, value("testAnotherNamespace", destination1))
	s.Equal(100, value(namespaceName, destination2)) // priority: destination >>> namespace
	s.Equal(10, value("testAnotherNamespace", "testAnotherDestination"))
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
		_, err := findMatch[struct{}](tc.v, nil, tc.filters)
		s.Equal(tc.matched, err == nil)
	}
}

func (s *collectionSuite) TestFindMatchWithTyped() {
	testCases := []struct {
		tv      []TypedConstrainedValue[struct{}]
		filters []Constraints
		matched bool
	}{
		{
			tv: []TypedConstrainedValue[struct{}]{
				{Constraints: Constraints{}},
			},
			filters: []Constraints{
				{Namespace: "some random namespace"},
			},
			matched: false,
		},
		{
			tv: []TypedConstrainedValue[struct{}]{
				{Constraints: Constraints{Namespace: "samples-namespace"}},
			},
			filters: []Constraints{
				{Namespace: "some random namespace"},
			},
			matched: false,
		},
		{
			tv: []TypedConstrainedValue[struct{}]{
				{Constraints: Constraints{Namespace: "samples-namespace", TaskQueueName: "sample-task-queue"}},
			},
			filters: []Constraints{
				{Namespace: "samples-namespace", TaskQueueName: "sample-task-queue"},
			},
			matched: true,
		},
		{
			tv: []TypedConstrainedValue[struct{}]{
				{Constraints: Constraints{Namespace: "samples-namespace"}},
			},
			filters: []Constraints{
				{TaskQueueName: "sample-task-queue"},
			},
			matched: false,
		},
	}

	for _, tc := range testCases {
		_, err := findMatch(nil, tc.tv, tc.filters)
		s.Equal(tc.matched, err == nil)
	}
}

func BenchmarkCollection(b *testing.B) {
	// client with just one value
	client1 := StaticClient(map[Key]any{
		MatchingMaxTaskBatchSize.Key(): []ConstrainedValue{{Value: 12}},
	})
	cln1 := NewCollection(client1, log.NewNoopLogger())
	b.Run("global int", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N/2; i++ {
			size := MatchingThrottledLogRPS.Get(cln1)
			_ = size()
			size = MatchingThrottledLogRPS.Get(cln1)
			_ = size()
		}
	})
	b.Run("namespace int", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N/2; i++ {
			size := HistoryMaxPageSize.Get(cln1)
			_ = size("my-namespace")
			size = WorkflowExecutionMaxInFlightUpdates.Get(cln1)
			_ = size("my-namespace")
		}
	})
	b.Run("taskqueue int", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N/2; i++ {
			size := MatchingMaxTaskBatchSize.Get(cln1)
			_ = size("my-namespace", "my-task-queue", 1)
			size = MatchingGetTasksBatchSize.Get(cln1)
			_ = size("my-namespace", "my-task-queue", 1)
		}
	})

	// client with more constrained values
	client2 := StaticClient(map[Key]any{
		MatchingMaxTaskBatchSize.Key(): []ConstrainedValue{
			{
				Constraints: Constraints{
					TaskQueueName: "other-tq",
				},
				Value: 18,
			},
			{
				Constraints: Constraints{
					Namespace: "other-ns",
				},
				Value: 15,
			},
		},
	})
	cln2 := NewCollection(client2, log.NewNoopLogger())
	b.Run("single default", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N/4; i++ {
			size := MatchingMaxTaskBatchSize.Get(cln2)
			_ = size("my-namespace", "my-task-queue", 1)
			size = MatchingMaxTaskBatchSize.Get(cln2)
			_ = size("my-namespace", "other-tq", 1)
			size = MatchingMaxTaskBatchSize.Get(cln2)
			_ = size("other-ns", "my-task-queue", 1)
			size = MatchingMaxTaskBatchSize.Get(cln2)
			_ = size("other-ns", "other-tq", 1)
		}
	})
	b.Run("structured default", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N/4; i++ {
			size := MatchingNumTaskqueueWritePartitions.Get(cln2)
			_ = size("my-namespace", "my-task-queue", 1)
			size = MatchingNumTaskqueueWritePartitions.Get(cln2)
			_ = size("my-namespace", "other-tq", 1)
			size = MatchingNumTaskqueueWritePartitions.Get(cln2)
			_ = size("other-ns", "my-task-queue", 1)
			size = MatchingNumTaskqueueWritePartitions.Get(cln2)
			_ = size("other-ns", "other-tq", 1)
		}
	})
}
