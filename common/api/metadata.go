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

package api

import "strings"

type (
	// Describes the scope of a method (whole cluster or inividual namespace).
	Scope int32

	// Describes what level of access is needed for a method. Note that this field is
	// completely advisory. Any authorizer implementation may implement whatever logic it
	// chooses, including ignoring this field. It is used by the "default" authorizer to check
	// against roles in claims.
	Access int32

	MethodMetadata struct {
		// Describes the scope of a method (whole cluster or inividual namespace).
		Scope Scope
		// Describes what level of access is needed for a method (advisory).
		Access Access
	}
)

const (
	// Represents a missing Scope value.
	ScopeUnknown Scope = iota
	// Method affects a single namespace. The request message must contain a string field named "Namespace".
	ScopeNamespace
	// Method affects the whole cluster. The request message must _not_ contain any field named "Namespace".
	ScopeCluster
)

const (
	// Represents a missing Access value.
	AccessUnknown Access = iota
	// Method is read-only and should be accessible to readers.
	AccessReadOnly
	// Method is a normal write method.
	AccessWrite
	// Method is an administrative operation.
	AccessAdmin
)

const (
	WorkflowServicePrefix = "/temporal.api.workflowservice.v1.WorkflowService/"
	OperatorServicePrefix = "/temporal.api.operatorservice.v1.OperatorService/"
	AdminServicePrefix    = "/temporal.server.api.adminservice.v1.AdminService/"
)

var (
	workflowServiceMetadata = map[string]MethodMetadata{
		"RegisterNamespace":                  MethodMetadata{Scope: ScopeNamespace, Access: AccessAdmin},
		"DescribeNamespace":                  MethodMetadata{Scope: ScopeNamespace, Access: AccessReadOnly},
		"ListNamespaces":                     MethodMetadata{Scope: ScopeCluster, Access: AccessReadOnly},
		"UpdateNamespace":                    MethodMetadata{Scope: ScopeNamespace, Access: AccessAdmin},
		"DeprecateNamespace":                 MethodMetadata{Scope: ScopeNamespace, Access: AccessAdmin},
		"StartWorkflowExecution":             MethodMetadata{Scope: ScopeNamespace, Access: AccessWrite},
		"GetWorkflowExecutionHistory":        MethodMetadata{Scope: ScopeNamespace, Access: AccessReadOnly},
		"GetWorkflowExecutionHistoryReverse": MethodMetadata{Scope: ScopeNamespace, Access: AccessReadOnly},
		"PollWorkflowTaskQueue":              MethodMetadata{Scope: ScopeNamespace, Access: AccessWrite},
		"RespondWorkflowTaskCompleted":       MethodMetadata{Scope: ScopeNamespace, Access: AccessWrite},
		"RespondWorkflowTaskFailed":          MethodMetadata{Scope: ScopeNamespace, Access: AccessWrite},
		"PollActivityTaskQueue":              MethodMetadata{Scope: ScopeNamespace, Access: AccessWrite},
		"RecordActivityTaskHeartbeat":        MethodMetadata{Scope: ScopeNamespace, Access: AccessWrite},
		"RecordActivityTaskHeartbeatById":    MethodMetadata{Scope: ScopeNamespace, Access: AccessWrite},
		"RespondActivityTaskCompleted":       MethodMetadata{Scope: ScopeNamespace, Access: AccessWrite},
		"RespondActivityTaskCompletedById":   MethodMetadata{Scope: ScopeNamespace, Access: AccessWrite},
		"RespondActivityTaskFailed":          MethodMetadata{Scope: ScopeNamespace, Access: AccessWrite},
		"RespondActivityTaskFailedById":      MethodMetadata{Scope: ScopeNamespace, Access: AccessWrite},
		"RespondActivityTaskCanceled":        MethodMetadata{Scope: ScopeNamespace, Access: AccessWrite},
		"RespondActivityTaskCanceledById":    MethodMetadata{Scope: ScopeNamespace, Access: AccessWrite},
		"RequestCancelWorkflowExecution":     MethodMetadata{Scope: ScopeNamespace, Access: AccessWrite},
		"SignalWorkflowExecution":            MethodMetadata{Scope: ScopeNamespace, Access: AccessWrite},
		"SignalWithStartWorkflowExecution":   MethodMetadata{Scope: ScopeNamespace, Access: AccessWrite},
		"ResetWorkflowExecution":             MethodMetadata{Scope: ScopeNamespace, Access: AccessWrite},
		"TerminateWorkflowExecution":         MethodMetadata{Scope: ScopeNamespace, Access: AccessWrite},
		"DeleteWorkflowExecution":            MethodMetadata{Scope: ScopeNamespace, Access: AccessWrite},
		"ListOpenWorkflowExecutions":         MethodMetadata{Scope: ScopeNamespace, Access: AccessReadOnly},
		"ListClosedWorkflowExecutions":       MethodMetadata{Scope: ScopeNamespace, Access: AccessReadOnly},
		"ListWorkflowExecutions":             MethodMetadata{Scope: ScopeNamespace, Access: AccessReadOnly},
		"ListArchivedWorkflowExecutions":     MethodMetadata{Scope: ScopeNamespace, Access: AccessReadOnly},
		"ScanWorkflowExecutions":             MethodMetadata{Scope: ScopeNamespace, Access: AccessReadOnly},
		"CountWorkflowExecutions":            MethodMetadata{Scope: ScopeNamespace, Access: AccessReadOnly},
		"GetSearchAttributes":                MethodMetadata{Scope: ScopeCluster, Access: AccessReadOnly},
		"RespondQueryTaskCompleted":          MethodMetadata{Scope: ScopeNamespace, Access: AccessWrite},
		"ResetStickyTaskQueue":               MethodMetadata{Scope: ScopeNamespace, Access: AccessWrite},
		"QueryWorkflow":                      MethodMetadata{Scope: ScopeNamespace, Access: AccessReadOnly},
		"DescribeWorkflowExecution":          MethodMetadata{Scope: ScopeNamespace, Access: AccessReadOnly},
		"DescribeTaskQueue":                  MethodMetadata{Scope: ScopeNamespace, Access: AccessReadOnly},
		"GetClusterInfo":                     MethodMetadata{Scope: ScopeCluster, Access: AccessReadOnly},
		"GetSystemInfo":                      MethodMetadata{Scope: ScopeCluster, Access: AccessReadOnly},
		"ListTaskQueuePartitions":            MethodMetadata{Scope: ScopeNamespace, Access: AccessReadOnly},
		"CreateSchedule":                     MethodMetadata{Scope: ScopeNamespace, Access: AccessWrite},
		"DescribeSchedule":                   MethodMetadata{Scope: ScopeNamespace, Access: AccessReadOnly},
		"UpdateSchedule":                     MethodMetadata{Scope: ScopeNamespace, Access: AccessWrite},
		"PatchSchedule":                      MethodMetadata{Scope: ScopeNamespace, Access: AccessWrite},
		"ListScheduleMatchingTimes":          MethodMetadata{Scope: ScopeNamespace, Access: AccessReadOnly},
		"DeleteSchedule":                     MethodMetadata{Scope: ScopeNamespace, Access: AccessWrite},
		"ListSchedules":                      MethodMetadata{Scope: ScopeNamespace, Access: AccessReadOnly},
		"UpdateWorkerBuildIdCompatibility":   MethodMetadata{Scope: ScopeNamespace, Access: AccessWrite},
		"GetWorkerBuildIdCompatibility":      MethodMetadata{Scope: ScopeNamespace, Access: AccessReadOnly},
		"UpdateWorkerVersioningRules":        MethodMetadata{Scope: ScopeNamespace, Access: AccessWrite},
		"ListWorkerVersioningRules":          MethodMetadata{Scope: ScopeNamespace, Access: AccessReadOnly},
		"GetWorkerTaskReachability":          MethodMetadata{Scope: ScopeNamespace, Access: AccessReadOnly},
		"UpdateWorkflowExecution":            MethodMetadata{Scope: ScopeNamespace, Access: AccessWrite},
		"PollWorkflowExecutionUpdate":        MethodMetadata{Scope: ScopeNamespace, Access: AccessReadOnly},
		"StartBatchOperation":                MethodMetadata{Scope: ScopeNamespace, Access: AccessWrite},
		"StopBatchOperation":                 MethodMetadata{Scope: ScopeNamespace, Access: AccessWrite},
		"DescribeBatchOperation":             MethodMetadata{Scope: ScopeNamespace, Access: AccessReadOnly},
		"ListBatchOperations":                MethodMetadata{Scope: ScopeNamespace, Access: AccessReadOnly},
	}
	operatorServiceMetadata = map[string]MethodMetadata{
		"AddSearchAttributes":      MethodMetadata{Scope: ScopeNamespace, Access: AccessAdmin},
		"RemoveSearchAttributes":   MethodMetadata{Scope: ScopeNamespace, Access: AccessAdmin},
		"ListSearchAttributes":     MethodMetadata{Scope: ScopeNamespace, Access: AccessReadOnly},
		"DeleteNamespace":          MethodMetadata{Scope: ScopeNamespace, Access: AccessAdmin},
		"AddOrUpdateRemoteCluster": MethodMetadata{Scope: ScopeCluster, Access: AccessAdmin},
		"RemoveRemoteCluster":      MethodMetadata{Scope: ScopeCluster, Access: AccessAdmin},
		"ListClusters":             MethodMetadata{Scope: ScopeCluster, Access: AccessAdmin},
	}
)

// GetMethodMetadata gets metadata for a given API method in one of the services exported by
// frontend (WorkflowService, OperatorService, AdminService).
func GetMethodMetadata(fullApiName string) MethodMetadata {
	switch {
	case strings.HasPrefix(fullApiName, WorkflowServicePrefix):
		return workflowServiceMetadata[MethodName(fullApiName)]
	case strings.HasPrefix(fullApiName, OperatorServicePrefix):
		return operatorServiceMetadata[MethodName(fullApiName)]
	case strings.HasPrefix(fullApiName, AdminServicePrefix):
		return MethodMetadata{Scope: ScopeCluster, Access: AccessAdmin}
	default:
		return MethodMetadata{Scope: ScopeUnknown, Access: AccessUnknown}
	}
}

// BaseName returns just the method name from a fullly qualified name.
func MethodName(fullApiName string) string {
	index := strings.LastIndex(fullApiName, "/")
	if index > -1 {
		return fullApiName[index+1:]
	}
	return fullApiName
}
