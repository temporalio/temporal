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
	"golang.org/x/exp/slices"

	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/testing/temporalapi"
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
		"/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution":             {},
		"/temporal.api.workflowservice.v1.WorkflowService/SignalWithStartWorkflowExecution":   {},
		"/temporal.api.workflowservice.v1.WorkflowService/SignalWorkflowExecution":            {},
		"/temporal.api.workflowservice.v1.WorkflowService/RequestCancelWorkflowExecution":     {},
		"/temporal.api.workflowservice.v1.WorkflowService/TerminateWorkflowExecution":         {},
		"/temporal.api.workflowservice.v1.WorkflowService/GetWorkflowExecutionHistory":        {},
		"/temporal.api.workflowservice.v1.WorkflowService/GetWorkflowExecutionHistoryReverse": {},
		"/temporal.api.workflowservice.v1.WorkflowService/UpdateWorkflowExecution":            {},
		"/temporal.api.workflowservice.v1.WorkflowService/PollWorkflowExecutionUpdate":        {},

		"/temporal.api.workflowservice.v1.WorkflowService/RecordActivityTaskHeartbeat":      {},
		"/temporal.api.workflowservice.v1.WorkflowService/RecordActivityTaskHeartbeatById":  {},
		"/temporal.api.workflowservice.v1.WorkflowService/RespondActivityTaskCanceled":      {},
		"/temporal.api.workflowservice.v1.WorkflowService/RespondActivityTaskCanceledById":  {},
		"/temporal.api.workflowservice.v1.WorkflowService/RespondActivityTaskFailed":        {},
		"/temporal.api.workflowservice.v1.WorkflowService/RespondActivityTaskFailedById":    {},
		"/temporal.api.workflowservice.v1.WorkflowService/RespondActivityTaskCompleted":     {},
		"/temporal.api.workflowservice.v1.WorkflowService/RespondActivityTaskCompletedById": {},
		"/temporal.api.workflowservice.v1.WorkflowService/RespondWorkflowTaskCompleted":     {},
		"/temporal.api.workflowservice.v1.WorkflowService/RespondNexusTaskCompleted":        {},
		"/temporal.api.workflowservice.v1.WorkflowService/RespondNexusTaskFailed":           {},

		"/temporal.api.workflowservice.v1.WorkflowService/ResetWorkflowExecution":        {},
		"/temporal.api.workflowservice.v1.WorkflowService/DescribeWorkflowExecution":     {},
		"/temporal.api.workflowservice.v1.WorkflowService/RespondWorkflowTaskFailed":     {},
		"/temporal.api.workflowservice.v1.WorkflowService/QueryWorkflow":                 {},
		"/temporal.api.workflowservice.v1.WorkflowService/RespondQueryTaskCompleted":     {},
		"/temporal.api.workflowservice.v1.WorkflowService/PollWorkflowTaskQueue":         {},
		"/temporal.api.workflowservice.v1.WorkflowService/PollActivityTaskQueue":         {},
		"/temporal.api.workflowservice.v1.WorkflowService/PollNexusTaskQueue":            {},
		"/temporal.api.workflowservice.v1.WorkflowService/GetWorkerBuildIdCompatibility": {},
		"/temporal.api.workflowservice.v1.WorkflowService/GetWorkerTaskReachability":     {},
		"/temporal.api.workflowservice.v1.WorkflowService/DeleteWorkflowExecution":       {},

		"/temporal.api.workflowservice.v1.WorkflowService/ResetStickyTaskQueue":    {},
		"/temporal.api.workflowservice.v1.WorkflowService/DescribeTaskQueue":       {},
		"/temporal.api.workflowservice.v1.WorkflowService/ListTaskQueuePartitions": {},
	}

	var service workflowservice.WorkflowServiceServer
	t := reflect.TypeOf(&service).Elem()
	apiToPriority := make(map[string]int, t.NumMethod())
	for i := 0; i < t.NumMethod(); i++ {
		apiName := "/temporal.api.workflowservice.v1.WorkflowService/" + t.Method(i).Name
		if _, ok := apis[apiName]; ok {
			apiToPriority[apiName] = ExecutionAPIToPriority[apiName]
		}
	}
	apiToPriority["/temporal.api.nexusservice.v1.NexusService/DispatchNexusTask"] = ExecutionAPIToPriority["/temporal.api.nexusservice.v1.NexusService/DispatchNexusTask"]
	s.Equal(apiToPriority, ExecutionAPIToPriority)
}

func (s *quotasSuite) TestVisibilityAPIs() {
	apis := map[string]struct{}{
		"/temporal.api.workflowservice.v1.WorkflowService/GetWorkflowExecution":           {},
		"/temporal.api.workflowservice.v1.WorkflowService/CountWorkflowExecutions":        {},
		"/temporal.api.workflowservice.v1.WorkflowService/ScanWorkflowExecutions":         {},
		"/temporal.api.workflowservice.v1.WorkflowService/ListOpenWorkflowExecutions":     {},
		"/temporal.api.workflowservice.v1.WorkflowService/ListClosedWorkflowExecutions":   {},
		"/temporal.api.workflowservice.v1.WorkflowService/ListWorkflowExecutions":         {},
		"/temporal.api.workflowservice.v1.WorkflowService/ListArchivedWorkflowExecutions": {},
	}

	var service workflowservice.WorkflowServiceServer
	t := reflect.TypeOf(&service).Elem()
	apiToPriority := make(map[string]int, t.NumMethod())
	for i := 0; i < t.NumMethod(); i++ {
		apiName := "/temporal.api.workflowservice.v1.WorkflowService/" + t.Method(i).Name
		if _, ok := apis[apiName]; ok {
			apiToPriority[apiName] = VisibilityAPIToPriority[apiName]
		}
	}
	s.Equal(apiToPriority, VisibilityAPIToPriority)
}

func (s *quotasSuite) TestNamespaceReplicationInducingAPIs() {
	apis := map[string]struct{}{
		"/temporal.api.workflowservice.v1.WorkflowService/RegisterNamespace":                {},
		"/temporal.api.workflowservice.v1.WorkflowService/UpdateNamespace":                  {},
		"/temporal.api.workflowservice.v1.WorkflowService/UpdateWorkerBuildIdCompatibility": {},
	}

	var service workflowservice.WorkflowServiceServer
	t := reflect.TypeOf(&service).Elem()
	apiToPriority := make(map[string]int, t.NumMethod())
	for i := 0; i < t.NumMethod(); i++ {
		apiName := "/temporal.api.workflowservice.v1.WorkflowService/" + t.Method(i).Name
		if _, ok := apis[apiName]; ok {
			apiToPriority[apiName] = NamespaceReplicationInducingAPIToPriority[apiName]
		}
	}
	s.Equal(apiToPriority, NamespaceReplicationInducingAPIToPriority)
}

func (s *quotasSuite) TestOtherAPIs() {
	apis := map[string]struct{}{
		"/temporal.api.workflowservice.v1.WorkflowService/GetClusterInfo":      {},
		"/temporal.api.workflowservice.v1.WorkflowService/GetSystemInfo":       {},
		"/temporal.api.workflowservice.v1.WorkflowService/GetSearchAttributes": {},

		"/temporal.api.workflowservice.v1.WorkflowService/DescribeNamespace":  {},
		"/temporal.api.workflowservice.v1.WorkflowService/ListNamespaces":     {},
		"/temporal.api.workflowservice.v1.WorkflowService/DeprecateNamespace": {},

		"/temporal.api.workflowservice.v1.WorkflowService/CreateSchedule":            {},
		"/temporal.api.workflowservice.v1.WorkflowService/DescribeSchedule":          {},
		"/temporal.api.workflowservice.v1.WorkflowService/UpdateSchedule":            {},
		"/temporal.api.workflowservice.v1.WorkflowService/PatchSchedule":             {},
		"/temporal.api.workflowservice.v1.WorkflowService/ListScheduleMatchingTimes": {},
		"/temporal.api.workflowservice.v1.WorkflowService/DeleteSchedule":            {},
		"/temporal.api.workflowservice.v1.WorkflowService/ListSchedules":             {},

		"/temporal.api.workflowservice.v1.WorkflowService/DescribeBatchOperation": {},
		"/temporal.api.workflowservice.v1.WorkflowService/ListBatchOperations":    {},
		"/temporal.api.workflowservice.v1.WorkflowService/StartBatchOperation":    {},
		"/temporal.api.workflowservice.v1.WorkflowService/StopBatchOperation":     {},
	}

	var service workflowservice.WorkflowServiceServer
	t := reflect.TypeOf(&service).Elem()
	apiToPriority := make(map[string]int, t.NumMethod())
	for i := 0; i < t.NumMethod(); i++ {
		apiName := "/temporal.api.workflowservice.v1.WorkflowService/" + t.Method(i).Name
		if _, ok := apis[apiName]; ok {
			apiToPriority[apiName] = OtherAPIToPriority[apiName]
		}
	}
	s.Equal(apiToPriority, OtherAPIToPriority)
}

func (s *quotasSuite) TestAllAPIs() {
	var service workflowservice.WorkflowServiceServer
	expectedAPIs := make(map[string]struct{})
	temporalapi.WalkExportedMethods(&service, func(m reflect.Method) {
		expectedAPIs["/temporal.api.workflowservice.v1.WorkflowService/"+m.Name] = struct{}{}
	})
	expectedAPIs["/temporal.api.nexusservice.v1.NexusService/DispatchNexusTask"] = struct{}{}

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
