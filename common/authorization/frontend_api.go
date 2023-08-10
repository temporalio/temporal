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

package authorization

var readOnlyNamespaceAPI = map[string]struct{}{
	"DescribeNamespace":                  {},
	"GetWorkflowExecutionHistory":        {},
	"GetWorkflowExecutionHistoryReverse": {},
	"ListOpenWorkflowExecutions":         {},
	"ListClosedWorkflowExecutions":       {},
	"ListWorkflowExecutions":             {},
	"ListArchivedWorkflowExecutions":     {},
	"ScanWorkflowExecutions":             {},
	"CountWorkflowExecutions":            {},
	"QueryWorkflow":                      {},
	"DescribeWorkflowExecution":          {},
	"DescribeTaskQueue":                  {},
	"ListTaskQueuePartitions":            {},
	"DescribeSchedule":                   {},
	"ListSchedules":                      {},
	"ListScheduleMatchingTimes":          {},
	"DescribeBatchOperation":             {},
	"ListBatchOperations":                {},
	"GetWorkerBuildIdCompatibility":      {},
	"GetWorkerTaskReachability":          {},
}

var readOnlyGlobalAPI = map[string]struct{}{
	"ListNamespaces":      {},
	"GetSearchAttributes": {},
	"GetClusterInfo":      {},
	"GetSystemInfo":       {},
}

// note that these use the fully-qualified name
var healthCheckAPI = map[string]struct{}{
	"/grpc.health.v1.Health/Check":                                   {},
	"/temporal.api.workflowservice.v1.WorkflowService/GetSystemInfo": {},
}

func IsReadOnlyNamespaceAPI(api string) bool {
	_, found := readOnlyNamespaceAPI[api]
	return found
}

func IsReadOnlyGlobalAPI(api string) bool {
	_, found := readOnlyGlobalAPI[api]
	return found
}

func IsHealthCheckAPI(fullApi string) bool {
	_, found := healthCheckAPI[fullApi]
	return found
}
