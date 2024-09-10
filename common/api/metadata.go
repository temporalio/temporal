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
	HistoryServicePrefix  = "/temporal.server.api.historyservice.v1.HistoryService/"
	AdminServicePrefix    = "/temporal.server.api.adminservice.v1.AdminService/"
	// Technically not a gRPC service, but still using this format for metadata.
	NexusServicePrefix = "/temporal.api.nexusservice.v1.NexusService/"
)

var (
	workflowServiceMetadata = map[string]MethodMetadata{
		"RegisterNamespace":                  {Scope: ScopeNamespace, Access: AccessAdmin},
		"DescribeNamespace":                  {Scope: ScopeNamespace, Access: AccessReadOnly},
		"ListNamespaces":                     {Scope: ScopeCluster, Access: AccessReadOnly},
		"UpdateNamespace":                    {Scope: ScopeNamespace, Access: AccessAdmin},
		"DeprecateNamespace":                 {Scope: ScopeNamespace, Access: AccessAdmin},
		"StartWorkflowExecution":             {Scope: ScopeNamespace, Access: AccessWrite},
		"GetWorkflowExecutionHistory":        {Scope: ScopeNamespace, Access: AccessReadOnly},
		"GetWorkflowExecutionHistoryReverse": {Scope: ScopeNamespace, Access: AccessReadOnly},
		"PollWorkflowTaskQueue":              {Scope: ScopeNamespace, Access: AccessWrite},
		"RespondWorkflowTaskCompleted":       {Scope: ScopeNamespace, Access: AccessWrite},
		"RespondWorkflowTaskFailed":          {Scope: ScopeNamespace, Access: AccessWrite},
		"PollActivityTaskQueue":              {Scope: ScopeNamespace, Access: AccessWrite},
		"RecordActivityTaskHeartbeat":        {Scope: ScopeNamespace, Access: AccessWrite},
		"RecordActivityTaskHeartbeatById":    {Scope: ScopeNamespace, Access: AccessWrite},
		"RespondActivityTaskCompleted":       {Scope: ScopeNamespace, Access: AccessWrite},
		"RespondActivityTaskCompletedById":   {Scope: ScopeNamespace, Access: AccessWrite},
		"RespondActivityTaskFailed":          {Scope: ScopeNamespace, Access: AccessWrite},
		"RespondActivityTaskFailedById":      {Scope: ScopeNamespace, Access: AccessWrite},
		"RespondActivityTaskCanceled":        {Scope: ScopeNamespace, Access: AccessWrite},
		"RespondActivityTaskCanceledById":    {Scope: ScopeNamespace, Access: AccessWrite},
		"PollNexusTaskQueue":                 {Scope: ScopeNamespace, Access: AccessWrite},
		"RespondNexusTaskCompleted":          {Scope: ScopeNamespace, Access: AccessWrite},
		"RespondNexusTaskFailed":             {Scope: ScopeNamespace, Access: AccessWrite},
		"RequestCancelWorkflowExecution":     {Scope: ScopeNamespace, Access: AccessWrite},
		"SignalWorkflowExecution":            {Scope: ScopeNamespace, Access: AccessWrite},
		"SignalWithStartWorkflowExecution":   {Scope: ScopeNamespace, Access: AccessWrite},
		"ResetWorkflowExecution":             {Scope: ScopeNamespace, Access: AccessWrite},
		"TerminateWorkflowExecution":         {Scope: ScopeNamespace, Access: AccessWrite},
		"DeleteWorkflowExecution":            {Scope: ScopeNamespace, Access: AccessWrite},
		"ListOpenWorkflowExecutions":         {Scope: ScopeNamespace, Access: AccessReadOnly},
		"ListClosedWorkflowExecutions":       {Scope: ScopeNamespace, Access: AccessReadOnly},
		"ListWorkflowExecutions":             {Scope: ScopeNamespace, Access: AccessReadOnly},
		"ListArchivedWorkflowExecutions":     {Scope: ScopeNamespace, Access: AccessReadOnly},
		"ScanWorkflowExecutions":             {Scope: ScopeNamespace, Access: AccessReadOnly},
		"CountWorkflowExecutions":            {Scope: ScopeNamespace, Access: AccessReadOnly},
		"GetSearchAttributes":                {Scope: ScopeCluster, Access: AccessReadOnly},
		"RespondQueryTaskCompleted":          {Scope: ScopeNamespace, Access: AccessWrite},
		"ResetStickyTaskQueue":               {Scope: ScopeNamespace, Access: AccessWrite},
		"ShutdownWorker":                     {Scope: ScopeNamespace, Access: AccessWrite},
		"ExecuteMultiOperation":              {Scope: ScopeNamespace, Access: AccessWrite},
		"QueryWorkflow":                      {Scope: ScopeNamespace, Access: AccessReadOnly},
		"DescribeWorkflowExecution":          {Scope: ScopeNamespace, Access: AccessReadOnly},
		"DescribeTaskQueue":                  {Scope: ScopeNamespace, Access: AccessReadOnly},
		"GetClusterInfo":                     {Scope: ScopeCluster, Access: AccessReadOnly},
		"GetSystemInfo":                      {Scope: ScopeCluster, Access: AccessReadOnly},
		"ListTaskQueuePartitions":            {Scope: ScopeNamespace, Access: AccessReadOnly},
		"CreateSchedule":                     {Scope: ScopeNamespace, Access: AccessWrite},
		"DescribeSchedule":                   {Scope: ScopeNamespace, Access: AccessReadOnly},
		"UpdateSchedule":                     {Scope: ScopeNamespace, Access: AccessWrite},
		"PatchSchedule":                      {Scope: ScopeNamespace, Access: AccessWrite},
		"ListScheduleMatchingTimes":          {Scope: ScopeNamespace, Access: AccessReadOnly},
		"DeleteSchedule":                     {Scope: ScopeNamespace, Access: AccessWrite},
		"ListSchedules":                      {Scope: ScopeNamespace, Access: AccessReadOnly},
		"UpdateWorkerBuildIdCompatibility":   {Scope: ScopeNamespace, Access: AccessWrite},
		"GetWorkerBuildIdCompatibility":      {Scope: ScopeNamespace, Access: AccessReadOnly},
		"UpdateWorkerVersioningRules":        {Scope: ScopeNamespace, Access: AccessWrite},
		"GetWorkerVersioningRules":           {Scope: ScopeNamespace, Access: AccessReadOnly},
		"GetWorkerTaskReachability":          {Scope: ScopeNamespace, Access: AccessReadOnly},
		"UpdateWorkflowExecution":            {Scope: ScopeNamespace, Access: AccessWrite},
		"PollWorkflowExecutionUpdate":        {Scope: ScopeNamespace, Access: AccessReadOnly},
		"StartBatchOperation":                {Scope: ScopeNamespace, Access: AccessWrite},
		"StopBatchOperation":                 {Scope: ScopeNamespace, Access: AccessWrite},
		"DescribeBatchOperation":             {Scope: ScopeNamespace, Access: AccessReadOnly},
		"ListBatchOperations":                {Scope: ScopeNamespace, Access: AccessReadOnly},
		"UpdateActivityOptionsById":          {Scope: ScopeNamespace, Access: AccessWrite},
	}
	operatorServiceMetadata = map[string]MethodMetadata{
		"AddSearchAttributes":      {Scope: ScopeNamespace, Access: AccessAdmin},
		"RemoveSearchAttributes":   {Scope: ScopeNamespace, Access: AccessAdmin},
		"ListSearchAttributes":     {Scope: ScopeNamespace, Access: AccessReadOnly},
		"DeleteNamespace":          {Scope: ScopeNamespace, Access: AccessAdmin},
		"AddOrUpdateRemoteCluster": {Scope: ScopeCluster, Access: AccessAdmin},
		"RemoveRemoteCluster":      {Scope: ScopeCluster, Access: AccessAdmin},
		"ListClusters":             {Scope: ScopeCluster, Access: AccessAdmin},
		"CreateNexusEndpoint":      {Scope: ScopeCluster, Access: AccessAdmin},
		"UpdateNexusEndpoint":      {Scope: ScopeCluster, Access: AccessAdmin},
		"DeleteNexusEndpoint":      {Scope: ScopeCluster, Access: AccessAdmin},
		"GetNexusEndpoint":         {Scope: ScopeCluster, Access: AccessAdmin},
		"ListNexusEndpoints":       {Scope: ScopeCluster, Access: AccessAdmin},
	}
	nexusServiceMetadata = map[string]MethodMetadata{
		"DispatchNexusTask": {Scope: ScopeNamespace, Access: AccessWrite},
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
	case strings.HasPrefix(fullApiName, NexusServicePrefix):
		return nexusServiceMetadata[MethodName(fullApiName)]
	case strings.HasPrefix(fullApiName, AdminServicePrefix):
		return MethodMetadata{Scope: ScopeCluster, Access: AccessAdmin}
	default:
		return MethodMetadata{Scope: ScopeUnknown, Access: AccessUnknown}
	}
}

// MethodName returns just the method name from a fully qualified name.
func MethodName(fullApiName string) string {
	index := strings.LastIndex(fullApiName, "/")
	if index > -1 {
		return fullApiName[index+1:]
	}
	return fullApiName
}

func ServiceName(fullApiName string) string {
	index := strings.LastIndex(fullApiName, "/")
	if index > -1 {
		return fullApiName[:index+1]
	}
	return ""
}
