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
	"golang.org/x/exp/slices"
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
	for _, priority := range ExecutionAPIToPriority {
		index := slices.Index(ExecutionAPIPrioritiesOrdered, priority)
		s.NotEqual(-1, index)
	}
}

func (s *quotasSuite) TestVisibilityAPIToPriorityMapping() {
	for _, priority := range VisibilityAPIToPriority {
		index := slices.Index(VisibilityAPIPrioritiesOrdered, priority)
		s.NotEqual(-1, index)
	}
}

func (s *quotasSuite) TestOtherAPIToPriorityMapping() {
	for _, priority := range OtherAPIToPriority {
		index := slices.Index(OtherAPIPrioritiesOrdered, priority)
		s.NotEqual(-1, index)
	}
}

func (s *quotasSuite) TestExecutionAPIPrioritiesOrdered() {
	for idx := range ExecutionAPIPrioritiesOrdered[1:] {
		s.True(ExecutionAPIPrioritiesOrdered[idx] < ExecutionAPIPrioritiesOrdered[idx+1])
	}
}

func (s *quotasSuite) TestVisibilityAPIPrioritiesOrdered() {
	for idx := range VisibilityAPIPrioritiesOrdered[1:] {
		s.True(VisibilityAPIPrioritiesOrdered[idx] < VisibilityAPIPrioritiesOrdered[idx+1])
	}
}

func (s *quotasSuite) TestOtherAPIPrioritiesOrdered() {
	for idx := range OtherAPIPrioritiesOrdered[1:] {
		s.True(OtherAPIPrioritiesOrdered[idx] < OtherAPIPrioritiesOrdered[idx+1])
	}
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
		"DeleteWorkflowExecution":     {},

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
		"GetWorkflowExecution":           {},
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

		"DescribeBatchOperation": {},
		"ListBatchOperations":    {},
		"StartBatchOperation":    {},
		"StopBatchOperation":     {},
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
