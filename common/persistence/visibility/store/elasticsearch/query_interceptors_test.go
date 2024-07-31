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

package elasticsearch

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/searchattribute"
)

type (
	QueryInterceptorSuite struct {
		suite.Suite
		*require.Assertions
		controller *gomock.Controller
	}
)

func TestQueryInterceptorSuite(t *testing.T) {
	suite.Run(t, &QueryInterceptorSuite{})
}

func (s *QueryInterceptorSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())
}

func (s *QueryInterceptorSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *QueryInterceptorSuite) TestTimeProcessFunc() {
	mockNameInterceptor := s.createMockNameInterceptor()
	vi := NewValuesInterceptor(
		"test-namespace",
		searchattribute.TestNameTypeMap,
		searchattribute.NewTestMapperProvider(nil),
		mockNameInterceptor,
	)

	cases := []struct {
		key   string
		value interface{}
	}{
		{key: searchattribute.StartTime, value: int64(1528358645123456789)},
		{key: searchattribute.CloseTime, value: "2018-06-07T15:04:05+07:00"},
		{key: searchattribute.ExecutionTime, value: "some invalid time string"},
		{key: searchattribute.WorkflowID, value: "should not be modified"},
	}
	expected := []struct {
		value     string
		returnErr bool
	}{
		{value: "2018-06-07T08:04:05.123456789Z", returnErr: false},
		{value: "2018-06-07T15:04:05+07:00", returnErr: false},
		{value: "", returnErr: true},
		{value: "should not be modified", returnErr: false},
	}

	for i, testCase := range cases {
		v, err := vi.Values(testCase.key, testCase.value)
		if expected[i].returnErr {
			s.Error(err)
			continue
		}
		s.NoError(err)
		s.Len(v, 1)
		s.Equal(expected[i].value, v[0])
	}
}

func (s *QueryInterceptorSuite) TestStatusProcessFunc() {
	mockNameInterceptor := s.createMockNameInterceptor()
	vi := NewValuesInterceptor(
		"test-namespace",
		searchattribute.TestNameTypeMap,
		searchattribute.NewTestMapperProvider(nil),
		mockNameInterceptor,
	)

	cases := []struct {
		key   string
		value interface{}
	}{
		{key: searchattribute.ExecutionStatus, value: "Completed"},
		{key: searchattribute.ExecutionStatus, value: int64(1)},
		{key: searchattribute.ExecutionStatus, value: "1"},
		{key: searchattribute.ExecutionStatus, value: int64(100)},
		{key: searchattribute.ExecutionStatus, value: "100"},
		{key: searchattribute.ExecutionStatus, value: "BadStatus"},
		{key: searchattribute.WorkflowID, value: "should not be modified"},
	}
	expected := []struct {
		value     string
		returnErr bool
	}{
		{value: "Completed", returnErr: false},
		{value: "Running", returnErr: false},
		{value: "1", returnErr: false},
		{value: "", returnErr: true},
		{value: "100", returnErr: false},
		{value: "BadStatus", returnErr: false},
		{value: "should not be modified", returnErr: false},
	}

	for i, testCase := range cases {
		v, err := vi.Values(testCase.key, testCase.value)
		if expected[i].returnErr {
			s.Error(err)
			continue
		}
		s.NoError(err)
		s.Len(v, 1)
		s.Equal(expected[i].value, v[0])
	}
}

func (s *QueryInterceptorSuite) TestDurationProcessFunc() {
	mockNameInterceptor := s.createMockNameInterceptor()
	vi := NewValuesInterceptor(
		"test-namespace",
		searchattribute.TestNameTypeMap,
		searchattribute.NewTestMapperProvider(nil),
		mockNameInterceptor,
	)

	cases := []struct {
		key   string
		value interface{}
	}{
		{key: searchattribute.ExecutionDuration, value: "1"},
		{key: searchattribute.ExecutionDuration, value: int64(1)},
		{key: searchattribute.ExecutionDuration, value: "5h3m"},
		{key: searchattribute.ExecutionDuration, value: "00:00:01"},
		{key: searchattribute.ExecutionDuration, value: "00:00:61"},
		{key: searchattribute.ExecutionDuration, value: "bad value"},
		{key: searchattribute.WorkflowID, value: "should not be modified"},
	}
	expected := []struct {
		value     interface{}
		returnErr bool
	}{
		{value: int64(1), returnErr: false},
		{value: int64(1), returnErr: false},
		{value: int64(18180000000000), returnErr: false},
		{value: int64(1000000000), returnErr: false},
		{value: nil, returnErr: true},
		{value: nil, returnErr: true},
		{value: "should not be modified", returnErr: false},
	}

	for i, testCase := range cases {
		v, err := vi.Values(testCase.key, testCase.value)
		if expected[i].returnErr {
			s.Error(err)
			var converterErr *query.ConverterError
			s.ErrorAs(err, &converterErr)
			continue
		}
		s.NoError(err)
		s.Len(v, 1)
		s.Equal(expected[i].value, v[0])
	}
}

// Verifies the nameInterceptor correctly transforms ScheduleID to WorkflowID and stores the transformation.
func (s *QueryInterceptorSuite) TestNameInterceptor_ScheduleIDToWorkflowID() {
	ni := s.createMockNameInterceptor()

	fieldName, err := ni.Name(searchattribute.ScheduleID, query.FieldNameFilter)
	s.NoError(err)
	s.Equal(searchattribute.WorkflowID, fieldName)

	s.Len(ni.fieldTransformations, 1)
	transformation, exists := ni.fieldTransformations[searchattribute.WorkflowID]
	s.True(exists)
	s.Equal(searchattribute.ScheduleID, transformation.originalField)
	s.Equal(searchattribute.WorkflowID, transformation.newField)
}

// Ensures the valuesInterceptor applies the ScheduleID to WorkflowID transformation,
// including prepending the WorkflowIDPrefix.
func (s *QueryInterceptorSuite) TestValuesInterceptor_ScheduleIDToWorkflowID() {
	mockNameInterceptor := s.createMockNameInterceptor()
	mockNameInterceptor.fieldTransformations[searchattribute.WorkflowID] = fieldTransformation{
		originalField: searchattribute.ScheduleID,
		newField:      searchattribute.WorkflowID,
	}

	vi := NewValuesInterceptor(
		"test-namespace",
		searchattribute.TestNameTypeMap,
		searchattribute.NewTestMapperProvider(nil),
		mockNameInterceptor,
	)

	values, err := vi.Values(searchattribute.WorkflowID, "test-schedule-id")
	s.NoError(err)
	s.Len(values, 1)
	s.Equal(searchattribute.ScheduleWorkflowIDPrefix+"test-schedule-id", values[0])
}

// Ensures the valuesInterceptor doesn't modify values when no transformation is needed.
func (s *QueryInterceptorSuite) TestValuesInterceptor_NoTransformation() {
	mockNameInterceptor := s.createMockNameInterceptor()
	vi := NewValuesInterceptor(
		"test-namespace",
		searchattribute.TestNameTypeMap,
		searchattribute.NewTestMapperProvider(nil),
		mockNameInterceptor,
	)

	values, err := vi.Values(searchattribute.WorkflowID, "test-workflow-id")
	s.NoError(err)
	s.Len(values, 1)
	s.Equal("test-workflow-id", values[0])
}

func (s *QueryInterceptorSuite) createMockNameInterceptor() *nameInterceptor {
	return &nameInterceptor{
		namespace:                      "test-namespace",
		searchAttributesTypeMap:        searchattribute.TestNameTypeMap,
		searchAttributesMapperProvider: searchattribute.NewTestMapperProvider(nil),
		fieldTransformations:           make(map[string]fieldTransformation),
	}
}
