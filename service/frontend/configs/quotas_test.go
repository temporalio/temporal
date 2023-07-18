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
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/quotas"
	"golang.org/x/exp/slices"
)

var (
	testRateBurstFn        = quotas.NewDefaultIncomingRateBurst(func() float64 { return 5 })
	testOperatorRPSRatioFn = func() float64 { return 0.2 }
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

func (s *quotasSuite) TestNamespaceReplicationInducingAPIToPriorityMapping() {
	for _, priority := range NamespaceReplicationInducingAPIToPriority {
		index := slices.Index(NamespaceReplicationInducingAPIPrioritiesOrdered, priority)
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

func (s *quotasSuite) TestNamespaceReplicationInducingAPIPrioritiesOrdered() {
	for idx := range NamespaceReplicationInducingAPIPrioritiesOrdered[1:] {
		s.True(NamespaceReplicationInducingAPIPrioritiesOrdered[idx] < NamespaceReplicationInducingAPIPrioritiesOrdered[idx+1])
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
		"UpdateWorkflowExecution":            {},
		"PollWorkflowExecutionUpdate":        {},

		"RecordActivityTaskHeartbeat":      {},
		"RecordActivityTaskHeartbeatById":  {},
		"RespondActivityTaskCanceled":      {},
		"RespondActivityTaskCanceledById":  {},
		"RespondActivityTaskFailed":        {},
		"RespondActivityTaskFailedById":    {},
		"RespondActivityTaskCompleted":     {},
		"RespondActivityTaskCompletedById": {},
		"RespondWorkflowTaskCompleted":     {},

		"ResetWorkflowExecution":        {},
		"DescribeWorkflowExecution":     {},
		"RespondWorkflowTaskFailed":     {},
		"QueryWorkflow":                 {},
		"RespondQueryTaskCompleted":     {},
		"PollWorkflowTaskQueue":         {},
		"PollActivityTaskQueue":         {},
		"GetWorkerBuildIdCompatibility": {},
		"GetWorkerTaskReachability":     {},
		"DeleteWorkflowExecution":       {},

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

func (s *quotasSuite) TestNamespaceReplicationInducingAPIs() {
	apis := map[string]struct{}{
		"RegisterNamespace":                {},
		"UpdateNamespace":                  {},
		"UpdateWorkerBuildIdCompatibility": {},
	}

	var service workflowservice.WorkflowServiceServer
	t := reflect.TypeOf(&service).Elem()
	apiToPriority := make(map[string]int, t.NumMethod())
	for i := 0; i < t.NumMethod(); i++ {
		apiName := t.Method(i).Name
		if _, ok := apis[apiName]; ok {
			apiToPriority[apiName] = NamespaceReplicationInducingAPIToPriority[apiName]
		}
	}
	s.Equal(apiToPriority, NamespaceReplicationInducingAPIToPriority)
}

func (s *quotasSuite) TestOtherAPIs() {
	apis := map[string]struct{}{
		"GetClusterInfo":      {},
		"GetSystemInfo":       {},
		"GetSearchAttributes": {},

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
	for api := range NamespaceReplicationInducingAPIToPriority {
		actualAPIs[api] = struct{}{}
	}
	for api := range OtherAPIToPriority {
		actualAPIs[api] = struct{}{}
	}
	s.Equal(expectedAPIs, actualAPIs)
}

func (s *quotasSuite) TestOperatorPriority_Execution() {
	limiter := NewExecutionPriorityRateLimiter(testRateBurstFn, testOperatorRPSRatioFn)
	s.testOperatorPrioritized(limiter, "DescribeWorkflowExecution")
}

func (s *quotasSuite) TestOperatorPriority_Visibility() {
	limiter := NewVisibilityPriorityRateLimiter(testRateBurstFn, testOperatorRPSRatioFn)
	s.testOperatorPrioritized(limiter, "ListOpenWorkflowExecutions")
}

func (s *quotasSuite) TestOperatorPriority_NamespaceReplicationInducing() {
	limiter := NewNamespaceReplicationInducingAPIPriorityRateLimiter(testRateBurstFn, testOperatorRPSRatioFn)
	s.testOperatorPrioritized(limiter, "RegisterNamespace")
}

func (s *quotasSuite) TestOperatorPriority_Other() {
	limiter := NewOtherAPIPriorityRateLimiter(testRateBurstFn, testOperatorRPSRatioFn)
	s.testOperatorPrioritized(limiter, "DescribeNamespace")
}

func (s *quotasSuite) testOperatorPrioritized(limiter quotas.RequestRateLimiter, api string) {
	operatorRequest := quotas.NewRequest(
		api,
		1,
		"test-namespace",
		headers.CallerTypeOperator,
		-1,
		"")

	apiRequest := quotas.NewRequest(
		api,
		1,
		"test-namespace",
		headers.CallerTypeAPI,
		-1,
		"")

	requestTime := time.Now()
	limitCount := 0

	for i := 0; i < 12; i++ {
		if !limiter.Allow(requestTime, apiRequest) {
			limitCount++
			s.True(limiter.Allow(requestTime, operatorRequest))
		}
	}
	s.Equal(2, limitCount)
}
