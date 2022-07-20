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

package configs

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/workflowservice/v1"
)

type (
	quotasSuite struct {
		suite.Suite
		*require.Assertions
	}
)

func TestQuotasSuite(t *testing.T) {
	s := new(quotasSuite)
	suite.Run(t, s)
}

func (s *quotasSuite) SetupSuite() {
}

func (s *quotasSuite) TearDownSuite() {
}

func (s *quotasSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *quotasSuite) TearDownTest() {
}

func (s *quotasSuite) TestExecutionAPIToPriorityMapping() {
	mapping := make(map[int]struct{})
	for _, priority := range ExecutionAPIToPriority {
		mapping[priority] = struct{}{}
	}
	s.Equal(mapping, ExecutionAPIPriorities)
}

func (s *quotasSuite) TestVisibilityAPIToPriorityMapping() {
	mapping := make(map[int]struct{})
	for _, priority := range VisibilityAPIToPriority {
		mapping[priority] = struct{}{}
	}
	s.Equal(mapping, VisibilityAPIPriorities)
}

func (s *quotasSuite) TestOtherAPIToPriorityMapping() {
	mapping := make(map[int]struct{})
	for _, priority := range OtherAPIToPriority {
		mapping[priority] = struct{}{}
	}
	s.Equal(mapping, OtherAPIPriorities)
}

func (s *quotasSuite) TestExecutionAPIs() {
	apis := map[string]struct{}{
		"StartWorkflowExecution":             {},
		"SignalWithStartWorkflowExecution":   {},
		"SignalWorkflowExecution":            {},
		"RequestCancelWorkflowExecution":     {},
		"TerminateWorkflowExecution":         {},
		"GetWorkflowExecutionHistory":        {},
		"GetWorkflowExecutionHistoryReverse": {},
		"UpdateWorkflow":                     {},

		"RecordActivityTaskHeartbeat":      {},
		"RecordActivityTaskHeartbeatById":  {},
		"RespondActivityTaskCanceled":      {},
		"RespondActivityTaskCanceledById":  {},
		"RespondActivityTaskFailed":        {},
		"RespondActivityTaskFailedById":    {},
		"RespondActivityTaskCompleted":     {},
		"RespondActivityTaskCompletedById": {},
		"RespondWorkflowTaskCompleted":     {},

		"ResetWorkflowExecution":      {},
		"DescribeWorkflowExecution":   {},
		"RespondWorkflowTaskFailed":   {},
		"QueryWorkflow":               {},
		"RespondQueryTaskCompleted":   {},
		"PollWorkflowTaskQueue":       {},
		"PollActivityTaskQueue":       {},
		"GetWorkerBuildIdOrdering":    {},
		"UpdateWorkerBuildIdOrdering": {},

		"ResetStickyTaskQueue":    {},
		"DescribeTaskQueue":       {},
		"ListTaskQueuePartitions": {},
	}

	var service workflowservice.WorkflowServiceServer
	t := reflect.TypeOf(&service).Elem()
	apiToPriority := make(map[string]int, t.NumMethod())
	for i := 0; i < t.NumMethod(); i++ {
		apiName := t.Method(i).Name
		if _, ok := apis[apiName]; ok {
			apiToPriority[apiName] = ExecutionAPIToPriority[apiName]
		}
	}
	s.Equal(apiToPriority, ExecutionAPIToPriority)
}

func (s *quotasSuite) TestVisibilityAPIs() {
	apis := map[string]struct{}{
		"CountWorkflowExecutions":        {},
		"ScanWorkflowExecutions":         {},
		"ListOpenWorkflowExecutions":     {},
		"ListClosedWorkflowExecutions":   {},
		"ListWorkflowExecutions":         {},
		"ListArchivedWorkflowExecutions": {},
	}

	var service workflowservice.WorkflowServiceServer
	t := reflect.TypeOf(&service).Elem()
	apiToPriority := make(map[string]int, t.NumMethod())
	for i := 0; i < t.NumMethod(); i++ {
		apiName := t.Method(i).Name
		if _, ok := apis[apiName]; ok {
			apiToPriority[apiName] = VisibilityAPIToPriority[apiName]
		}
	}
	s.Equal(apiToPriority, VisibilityAPIToPriority)
}

func (s *quotasSuite) TestOtherAPIs() {
	apis := map[string]struct{}{
		"GetClusterInfo":      {},
		"GetSystemInfo":       {},
		"GetSearchAttributes": {},

		"RegisterNamespace":  {},
		"UpdateNamespace":    {},
		"DescribeNamespace":  {},
		"ListNamespaces":     {},
		"DeprecateNamespace": {},

		"CreateSchedule":            {},
		"DescribeSchedule":          {},
		"UpdateSchedule":            {},
		"PatchSchedule":             {},
		"ListScheduleMatchingTimes": {},
		"DeleteSchedule":            {},
		"ListSchedules":             {},
	}

	var service workflowservice.WorkflowServiceServer
	t := reflect.TypeOf(&service).Elem()
	apiToPriority := make(map[string]int, t.NumMethod())
	for i := 0; i < t.NumMethod(); i++ {
		apiName := t.Method(i).Name
		if _, ok := apis[apiName]; ok {
			apiToPriority[apiName] = OtherAPIToPriority[apiName]
		}
	}
	s.Equal(apiToPriority, OtherAPIToPriority)
}

func (s *quotasSuite) TestAllAPIs() {
	var service workflowservice.WorkflowServiceServer
	t := reflect.TypeOf(&service).Elem()
	expectedAPIs := make(map[string]struct{}, t.NumMethod())
	for i := 0; i < t.NumMethod(); i++ {
		expectedAPIs[t.Method(i).Name] = struct{}{}
	}

	actualAPIs := make(map[string]struct{})
	for api := range ExecutionAPIToPriority {
		actualAPIs[api] = struct{}{}
	}
	for api := range VisibilityAPIToPriority {
		actualAPIs[api] = struct{}{}
	}
	for api := range OtherAPIToPriority {
		actualAPIs[api] = struct{}{}
	}
	s.Equal(expectedAPIs, actualAPIs)
}
