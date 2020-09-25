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

package frontend

import (
	"context"

	"go.temporal.io/api/workflowservice/v1"
)

var _ workflowservice.WorkflowServiceServer = (*WorkflowNilCheckHandler)(nil)

type (
	// WorkflowNilCheckHandler - gRPC handler interface for workflow workflowservice
	WorkflowNilCheckHandler struct {
		parentHandler Handler
	}
)

// Due to bug in gogo/protobuf https://github.com/gogo/protobuf/issues/651 response can't be nil when error is also nil.
// This handler makes sure response is always not nil, when error is nil.
// Can be removed from pipeline when bug is resolved.

// NewWorkflowNilCheckHandler creates handler that never returns nil response when error is nil
func NewWorkflowNilCheckHandler(
	parentHandler Handler,
) *WorkflowNilCheckHandler {
	handler := &WorkflowNilCheckHandler{
		parentHandler: parentHandler,
	}

	return handler
}

// RegisterNamespace creates a new namespace which can be used as a container for all resources.  Namespace is a top level
// entity within Temporal, used as a container for all resources like workflow executions, taskqueues, etc.  Namespace
// acts as a sandbox and provides isolation for all resources within the namespace.  All resources belongs to exactly one
// namespace.
func (wh *WorkflowNilCheckHandler) RegisterNamespace(ctx context.Context, request *workflowservice.RegisterNamespaceRequest) (_ *workflowservice.RegisterNamespaceResponse, retError error) {
	resp, err := wh.parentHandler.RegisterNamespace(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.RegisterNamespaceResponse{}
	}
	return resp, err
}

// DescribeNamespace returns the information and configuration for a registered namespace.
func (wh *WorkflowNilCheckHandler) DescribeNamespace(ctx context.Context, request *workflowservice.DescribeNamespaceRequest) (_ *workflowservice.DescribeNamespaceResponse, retError error) {
	resp, err := wh.parentHandler.DescribeNamespace(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.DescribeNamespaceResponse{}
	}
	return resp, err
}

// ListNamespaces returns the information and configuration for all namespaces.
func (wh *WorkflowNilCheckHandler) ListNamespaces(ctx context.Context, request *workflowservice.ListNamespacesRequest) (_ *workflowservice.ListNamespacesResponse, retError error) {
	resp, err := wh.parentHandler.ListNamespaces(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.ListNamespacesResponse{}
	}
	return resp, err
}

// UpdateNamespace is used to update the information and configuration for a registered namespace.
func (wh *WorkflowNilCheckHandler) UpdateNamespace(ctx context.Context, request *workflowservice.UpdateNamespaceRequest) (_ *workflowservice.UpdateNamespaceResponse, retError error) {
	resp, err := wh.parentHandler.UpdateNamespace(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.UpdateNamespaceResponse{}
	}
	return resp, err
}

// DeprecateNamespace us used to update status of a registered namespace to DEPRECATED.  Once the namespace is deprecated
// it cannot be used to start new workflow executions.  Existing workflow executions will continue to run on
// deprecated namespaces.
func (wh *WorkflowNilCheckHandler) DeprecateNamespace(ctx context.Context, request *workflowservice.DeprecateNamespaceRequest) (_ *workflowservice.DeprecateNamespaceResponse, retError error) {
	resp, err := wh.parentHandler.DeprecateNamespace(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.DeprecateNamespaceResponse{}
	}
	return resp, err
}

// StartWorkflowExecution starts a new long running workflow instance.  It will create the instance with
// 'WorkflowExecutionStarted' event in history and also schedule the first WorkflowTask for the worker to make the
// first command for this instance.  It will return 'WorkflowExecutionAlreadyStartedError', if an instance already
// exists with same workflowId.
func (wh *WorkflowNilCheckHandler) StartWorkflowExecution(ctx context.Context, request *workflowservice.StartWorkflowExecutionRequest) (_ *workflowservice.StartWorkflowExecutionResponse, retError error) {
	resp, err := wh.parentHandler.StartWorkflowExecution(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.StartWorkflowExecutionResponse{}
	}
	return resp, err
}

// GetWorkflowExecutionHistory returns the history of specified workflow execution.  It fails with 'EntityNotExistError' if speficied workflow
// execution in unknown to the service.
func (wh *WorkflowNilCheckHandler) GetWorkflowExecutionHistory(ctx context.Context, request *workflowservice.GetWorkflowExecutionHistoryRequest) (_ *workflowservice.GetWorkflowExecutionHistoryResponse, retError error) {
	resp, err := wh.parentHandler.GetWorkflowExecutionHistory(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.GetWorkflowExecutionHistoryResponse{}
	}
	return resp, err
}

// PollWorkflowTaskQueue is called by application worker to process WorkflowTask from a specific taskQueue.  A
// WorkflowTask is dispatched to callers for active workflow executions, with pending workflow tasks.
// Application is then expected to call 'RespondWorkflowTaskCompleted' API when it is done processing the WorkflowTask.
// It will also create a 'WorkflowTaskStarted' event in the history for that session before handing off WorkflowTask to
// application worker.
func (wh *WorkflowNilCheckHandler) PollWorkflowTaskQueue(ctx context.Context, request *workflowservice.PollWorkflowTaskQueueRequest) (_ *workflowservice.PollWorkflowTaskQueueResponse, retError error) {
	resp, err := wh.parentHandler.PollWorkflowTaskQueue(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.PollWorkflowTaskQueueResponse{}
	}
	return resp, err
}

// RespondWorkflowTaskCompleted is called by application worker to complete a WorkflowTask handed as a result of
// 'PollWorkflowTaskQueue' API call.  Completing a WorkflowTask will result in new events for the workflow execution and
// potentially new ActivityTask being created for corresponding commands.  It will also create a WorkflowTaskCompleted
// event in the history for that session.  Use the 'taskToken' provided as response of PollWorkflowTaskQueue API call
// for completing the WorkflowTask.
// The response could contain a new workflow task if there is one or if the request asking for one.
func (wh *WorkflowNilCheckHandler) RespondWorkflowTaskCompleted(ctx context.Context, request *workflowservice.RespondWorkflowTaskCompletedRequest) (_ *workflowservice.RespondWorkflowTaskCompletedResponse, retError error) {
	resp, err := wh.parentHandler.RespondWorkflowTaskCompleted(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.RespondWorkflowTaskCompletedResponse{}
	}
	return resp, err
}

// RespondWorkflowTaskFailed is called by application worker to indicate failure.  This results in
// WorkflowTaskFailedEvent written to the history and a new WorkflowTask created.  This API can be used by client to
// either clear sticky taskqueue or report any panics during WorkflowTask processing.  Temporal will only append first
// WorkflowTaskFailed event to the history of workflow execution for consecutive failures.
func (wh *WorkflowNilCheckHandler) RespondWorkflowTaskFailed(ctx context.Context, request *workflowservice.RespondWorkflowTaskFailedRequest) (_ *workflowservice.RespondWorkflowTaskFailedResponse, retError error) {
	resp, err := wh.parentHandler.RespondWorkflowTaskFailed(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.RespondWorkflowTaskFailedResponse{}
	}
	return resp, err
}

// PollActivityTaskQueue is called by application worker to process ActivityTask from a specific taskQueue.  ActivityTask
// is dispatched to callers whenever a ScheduleTask command is made for a workflow execution.
// Application is expected to call 'RespondActivityTaskCompleted' or 'RespondActivityTaskFailed' once it is done
// processing the task.
// Application also needs to call 'RecordActivityTaskHeartbeat' API within 'heartbeatTimeoutSeconds' interval to
// prevent the task from getting timed out.  An event 'ActivityTaskStarted' event is also written to workflow execution
// history before the ActivityTask is dispatched to application worker.
func (wh *WorkflowNilCheckHandler) PollActivityTaskQueue(ctx context.Context, request *workflowservice.PollActivityTaskQueueRequest) (_ *workflowservice.PollActivityTaskQueueResponse, retError error) {
	resp, err := wh.parentHandler.PollActivityTaskQueue(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.PollActivityTaskQueueResponse{}
	}
	return resp, err
}

// RecordActivityTaskHeartbeat is called by application worker while it is processing an ActivityTask.  If worker fails
// to heartbeat within 'heartbeatTimeoutSeconds' interval for the ActivityTask, then it will be marked as timedout and
// 'ActivityTaskTimedOut' event will be written to the workflow history.  Calling 'RecordActivityTaskHeartbeat' will
// fail with 'EntityNotExistsError' in such situations.  Use the 'taskToken' provided as response of
// PollActivityTaskQueue API call for heartbeating.
func (wh *WorkflowNilCheckHandler) RecordActivityTaskHeartbeat(ctx context.Context, request *workflowservice.RecordActivityTaskHeartbeatRequest) (_ *workflowservice.RecordActivityTaskHeartbeatResponse, retError error) {
	resp, err := wh.parentHandler.RecordActivityTaskHeartbeat(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.RecordActivityTaskHeartbeatResponse{}
	}
	return resp, err
}

// RecordActivityTaskHeartbeatById is called by application worker while it is processing an ActivityTask.  If worker fails
// to heartbeat within 'heartbeatTimeoutSeconds' interval for the ActivityTask, then it will be marked as timedout and
// 'ActivityTaskTimedOut' event will be written to the workflow history.  Calling 'RecordActivityTaskHeartbeatById' will
// fail with 'EntityNotExistsError' in such situations.  Instead of using 'taskToken' like in RecordActivityTaskHeartbeat,
// use Namespace, WorkflowID and ActivityID
func (wh *WorkflowNilCheckHandler) RecordActivityTaskHeartbeatById(ctx context.Context, request *workflowservice.RecordActivityTaskHeartbeatByIdRequest) (_ *workflowservice.RecordActivityTaskHeartbeatByIdResponse, retError error) {
	resp, err := wh.parentHandler.RecordActivityTaskHeartbeatById(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.RecordActivityTaskHeartbeatByIdResponse{}
	}
	return resp, err
}

// RespondActivityTaskCompleted is called by application worker when it is done processing an ActivityTask.  It will
// result in a new 'ActivityTaskCompleted' event being written to the workflow history and a new WorkflowTask
// created for the workflow so new commands could be made.  Use the 'taskToken' provided as response of
// PollActivityTaskQueue API call for completion. It fails with 'EntityNotExistsError' if the taskToken is not valid
// anymore due to activity timeout.
func (wh *WorkflowNilCheckHandler) RespondActivityTaskCompleted(ctx context.Context, request *workflowservice.RespondActivityTaskCompletedRequest) (_ *workflowservice.RespondActivityTaskCompletedResponse, retError error) {
	resp, err := wh.parentHandler.RespondActivityTaskCompleted(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.RespondActivityTaskCompletedResponse{}
	}
	return resp, err
}

// RespondActivityTaskCompletedById is called by application worker when it is done processing an ActivityTask.
// It will result in a new 'ActivityTaskCompleted' event being written to the workflow history and a new WorkflowTask
// created for the workflow so new commands could be made.  Similar to RespondActivityTaskCompleted but use Namespace,
// WorkflowID and ActivityID instead of 'taskToken' for completion. It fails with 'EntityNotExistsError'
// if the these IDs are not valid anymore due to activity timeout.
func (wh *WorkflowNilCheckHandler) RespondActivityTaskCompletedById(ctx context.Context, request *workflowservice.RespondActivityTaskCompletedByIdRequest) (_ *workflowservice.RespondActivityTaskCompletedByIdResponse, retError error) {
	resp, err := wh.parentHandler.RespondActivityTaskCompletedById(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.RespondActivityTaskCompletedByIdResponse{}
	}
	return resp, err
}

// RespondActivityTaskFailed is called by application worker when it is done processing an ActivityTask.  It will
// result in a new 'ActivityTaskFailed' event being written to the workflow history and a new WorkflowTask
// created for the workflow instance so new commands could be made.  Use the 'taskToken' provided as response of
// PollActivityTaskQueue API call for completion. It fails with 'EntityNotExistsError' if the taskToken is not valid
// anymore due to activity timeout.
func (wh *WorkflowNilCheckHandler) RespondActivityTaskFailed(ctx context.Context, request *workflowservice.RespondActivityTaskFailedRequest) (_ *workflowservice.RespondActivityTaskFailedResponse, retError error) {
	resp, err := wh.parentHandler.RespondActivityTaskFailed(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.RespondActivityTaskFailedResponse{}
	}
	return resp, err
}

// RespondActivityTaskFailedById is called by application worker when it is done processing an ActivityTask.
// It will result in a new 'ActivityTaskFailed' event being written to the workflow history and a new WorkflowTask
// created for the workflow instance so new commands could be made.  Similar to RespondActivityTaskFailed but use
// Namespace, WorkflowID and ActivityID instead of 'taskToken' for completion. It fails with 'EntityNotExistsError'
// if the these IDs are not valid anymore due to activity timeout.
func (wh *WorkflowNilCheckHandler) RespondActivityTaskFailedById(ctx context.Context, request *workflowservice.RespondActivityTaskFailedByIdRequest) (_ *workflowservice.RespondActivityTaskFailedByIdResponse, retError error) {
	resp, err := wh.parentHandler.RespondActivityTaskFailedById(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.RespondActivityTaskFailedByIdResponse{}
	}
	return resp, err
}

// RespondActivityTaskCanceled is called by application worker when it is successfully canceled an ActivityTask.  It will
// result in a new 'ActivityTaskCanceled' event being written to the workflow history and a new WorkflowTask
// created for the workflow instance so new commands could be made.  Use the 'taskToken' provided as response of
// PollActivityTaskQueue API call for completion. It fails with 'EntityNotExistsError' if the taskToken is not valid
// anymore due to activity timeout.
func (wh *WorkflowNilCheckHandler) RespondActivityTaskCanceled(ctx context.Context, request *workflowservice.RespondActivityTaskCanceledRequest) (_ *workflowservice.RespondActivityTaskCanceledResponse, retError error) {
	resp, err := wh.parentHandler.RespondActivityTaskCanceled(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.RespondActivityTaskCanceledResponse{}
	}
	return resp, err
}

// RespondActivityTaskCanceledById is called by application worker when it is successfully canceled an ActivityTask.
// It will result in a new 'ActivityTaskCanceled' event being written to the workflow history and a new WorkflowTask
// created for the workflow instance so new commands could be made.  Similar to RespondActivityTaskCanceled but use
// Namespace, WorkflowID and ActivityID instead of 'taskToken' for completion. It fails with 'EntityNotExistsError'
// if the these IDs are not valid anymore due to activity timeout.
func (wh *WorkflowNilCheckHandler) RespondActivityTaskCanceledById(ctx context.Context, request *workflowservice.RespondActivityTaskCanceledByIdRequest) (_ *workflowservice.RespondActivityTaskCanceledByIdResponse, retError error) {
	resp, err := wh.parentHandler.RespondActivityTaskCanceledById(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.RespondActivityTaskCanceledByIdResponse{}
	}
	return resp, err
}

// RequestCancelWorkflowExecution is called by application worker when it wants to request cancellation of a workflow instance.
// It will result in a new 'WorkflowExecutionCancelRequested' event being written to the workflow history and a new WorkflowTask
// created for the workflow instance so new commands could be made. It fails with 'EntityNotExistsError' if the workflow is not valid
// anymore due to completion or doesn't exist.
func (wh *WorkflowNilCheckHandler) RequestCancelWorkflowExecution(ctx context.Context, request *workflowservice.RequestCancelWorkflowExecutionRequest) (_ *workflowservice.RequestCancelWorkflowExecutionResponse, retError error) {
	resp, err := wh.parentHandler.RequestCancelWorkflowExecution(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.RequestCancelWorkflowExecutionResponse{}
	}
	return resp, err
}

// SignalWorkflowExecution is used to send a signal event to running workflow execution.  This results in
// WorkflowExecutionSignaled event recorded in the history and a workflow task being created for the execution.
func (wh *WorkflowNilCheckHandler) SignalWorkflowExecution(ctx context.Context, request *workflowservice.SignalWorkflowExecutionRequest) (_ *workflowservice.SignalWorkflowExecutionResponse, retError error) {
	resp, err := wh.parentHandler.SignalWorkflowExecution(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.SignalWorkflowExecutionResponse{}
	}
	return resp, err
}

// SignalWithStartWorkflowExecution is used to ensure sending signal to a workflow.
// If the workflow is running, this results in WorkflowExecutionSignaled event being recorded in the history
// and a workflow task being created for the execution.
// If the workflow is not running or not found, this results in WorkflowExecutionStarted and WorkflowExecutionSignaled
// events being recorded in history, and a workflow task being created for the execution
func (wh *WorkflowNilCheckHandler) SignalWithStartWorkflowExecution(ctx context.Context, request *workflowservice.SignalWithStartWorkflowExecutionRequest) (_ *workflowservice.SignalWithStartWorkflowExecutionResponse, retError error) {
	resp, err := wh.parentHandler.SignalWithStartWorkflowExecution(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.SignalWithStartWorkflowExecutionResponse{}
	}
	return resp, err
}

// ResetWorkflowExecution reset an existing workflow execution to WorkflowTaskCompleted event(exclusive).
// And it will immediately terminating the current execution instance.
func (wh *WorkflowNilCheckHandler) ResetWorkflowExecution(ctx context.Context, request *workflowservice.ResetWorkflowExecutionRequest) (_ *workflowservice.ResetWorkflowExecutionResponse, retError error) {
	resp, err := wh.parentHandler.ResetWorkflowExecution(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.ResetWorkflowExecutionResponse{}
	}
	return resp, err
}

// TerminateWorkflowExecution terminates an existing workflow execution by recording WorkflowExecutionTerminated event
// in the history and immediately terminating the execution instance.
func (wh *WorkflowNilCheckHandler) TerminateWorkflowExecution(ctx context.Context, request *workflowservice.TerminateWorkflowExecutionRequest) (_ *workflowservice.TerminateWorkflowExecutionResponse, retError error) {
	resp, err := wh.parentHandler.TerminateWorkflowExecution(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.TerminateWorkflowExecutionResponse{}
	}
	return resp, err
}

// ListOpenWorkflowExecutions is a visibility API to list the open executions in a specific namespace.
func (wh *WorkflowNilCheckHandler) ListOpenWorkflowExecutions(ctx context.Context, request *workflowservice.ListOpenWorkflowExecutionsRequest) (_ *workflowservice.ListOpenWorkflowExecutionsResponse, retError error) {
	resp, err := wh.parentHandler.ListOpenWorkflowExecutions(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.ListOpenWorkflowExecutionsResponse{}
	}
	return resp, err
}

// ListClosedWorkflowExecutions is a visibility API to list the closed executions in a specific namespace.
func (wh *WorkflowNilCheckHandler) ListClosedWorkflowExecutions(ctx context.Context, request *workflowservice.ListClosedWorkflowExecutionsRequest) (_ *workflowservice.ListClosedWorkflowExecutionsResponse, retError error) {
	resp, err := wh.parentHandler.ListClosedWorkflowExecutions(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.ListClosedWorkflowExecutionsResponse{}
	}
	return resp, err
}

// ListWorkflowExecutions is a visibility API to list workflow executions in a specific namespace.
func (wh *WorkflowNilCheckHandler) ListWorkflowExecutions(ctx context.Context, request *workflowservice.ListWorkflowExecutionsRequest) (_ *workflowservice.ListWorkflowExecutionsResponse, retError error) {
	resp, err := wh.parentHandler.ListWorkflowExecutions(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.ListWorkflowExecutionsResponse{}
	}
	return resp, err
}

// ListArchivedWorkflowExecutions is a visibility API to list archived workflow executions in a specific namespace.
func (wh *WorkflowNilCheckHandler) ListArchivedWorkflowExecutions(ctx context.Context, request *workflowservice.ListArchivedWorkflowExecutionsRequest) (_ *workflowservice.ListArchivedWorkflowExecutionsResponse, retError error) {
	resp, err := wh.parentHandler.ListArchivedWorkflowExecutions(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.ListArchivedWorkflowExecutionsResponse{}
	}
	return resp, err
}

// ScanWorkflowExecutions is a visibility API to list large amount of workflow executions in a specific namespace without order.
func (wh *WorkflowNilCheckHandler) ScanWorkflowExecutions(ctx context.Context, request *workflowservice.ScanWorkflowExecutionsRequest) (_ *workflowservice.ScanWorkflowExecutionsResponse, retError error) {
	resp, err := wh.parentHandler.ScanWorkflowExecutions(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.ScanWorkflowExecutionsResponse{}
	}
	return resp, err
}

// CountWorkflowExecutions is a visibility API to count of workflow executions in a specific namespace.
func (wh *WorkflowNilCheckHandler) CountWorkflowExecutions(ctx context.Context, request *workflowservice.CountWorkflowExecutionsRequest) (_ *workflowservice.CountWorkflowExecutionsResponse, retError error) {
	resp, err := wh.parentHandler.CountWorkflowExecutions(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.CountWorkflowExecutionsResponse{}
	}
	return resp, err
}

// GetSearchAttributes is a visibility API to get all legal keys that could be used in list APIs
func (wh *WorkflowNilCheckHandler) GetSearchAttributes(ctx context.Context, request *workflowservice.GetSearchAttributesRequest) (_ *workflowservice.GetSearchAttributesResponse, retError error) {
	resp, err := wh.parentHandler.GetSearchAttributes(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.GetSearchAttributesResponse{}
	}
	return resp, err
}

// RespondQueryTaskCompleted is called by application worker to complete a QueryTask (which is a WorkflowTask for query)
// as a result of 'PollWorkflowTaskQueue' API call. Completing a QueryTask will unblock the client call to 'QueryWorkflow'
// API and return the query result to client as a response to 'QueryWorkflow' API call.
func (wh *WorkflowNilCheckHandler) RespondQueryTaskCompleted(ctx context.Context, request *workflowservice.RespondQueryTaskCompletedRequest) (_ *workflowservice.RespondQueryTaskCompletedResponse, retError error) {
	resp, err := wh.parentHandler.RespondQueryTaskCompleted(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.RespondQueryTaskCompletedResponse{}
	}
	return resp, err
}

// ResetStickyTaskQueue resets the sticky taskqueue related information in mutable state of a given workflow.
// Things cleared are:
// 1. StickyTaskQueue
// 2. StickyScheduleToStartTimeout
// 3. ClientLibraryVersion
// 4. SupportedServerVersions
// 5. ClientName
func (wh *WorkflowNilCheckHandler) ResetStickyTaskQueue(ctx context.Context, request *workflowservice.ResetStickyTaskQueueRequest) (_ *workflowservice.ResetStickyTaskQueueResponse, retError error) {
	resp, err := wh.parentHandler.ResetStickyTaskQueue(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.ResetStickyTaskQueueResponse{}
	}
	return resp, err
}

// QueryWorkflow returns query result for a specified workflow execution
func (wh *WorkflowNilCheckHandler) QueryWorkflow(ctx context.Context, request *workflowservice.QueryWorkflowRequest) (_ *workflowservice.QueryWorkflowResponse, retError error) {
	resp, err := wh.parentHandler.QueryWorkflow(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.QueryWorkflowResponse{}
	}
	return resp, err
}

// DescribeWorkflowExecution returns information about the specified workflow execution.
func (wh *WorkflowNilCheckHandler) DescribeWorkflowExecution(ctx context.Context, request *workflowservice.DescribeWorkflowExecutionRequest) (_ *workflowservice.DescribeWorkflowExecutionResponse, retError error) {
	resp, err := wh.parentHandler.DescribeWorkflowExecution(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.DescribeWorkflowExecutionResponse{}
	}
	return resp, err
}

// DescribeTaskQueue returns information about the target taskqueue, right now this API returns the
// pollers which polled this taskqueue in last few minutes.
func (wh *WorkflowNilCheckHandler) DescribeTaskQueue(ctx context.Context, request *workflowservice.DescribeTaskQueueRequest) (_ *workflowservice.DescribeTaskQueueResponse, retError error) {
	resp, err := wh.parentHandler.DescribeTaskQueue(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.DescribeTaskQueueResponse{}
	}
	return resp, err
}

// GetClusterInfo ...
func (wh *WorkflowNilCheckHandler) GetClusterInfo(ctx context.Context, request *workflowservice.GetClusterInfoRequest) (_ *workflowservice.GetClusterInfoResponse, retError error) {
	resp, err := wh.parentHandler.GetClusterInfo(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.GetClusterInfoResponse{}
	}
	return resp, err
}

// ListTaskQueuePartitions ...
func (wh *WorkflowNilCheckHandler) ListTaskQueuePartitions(ctx context.Context, request *workflowservice.ListTaskQueuePartitionsRequest) (_ *workflowservice.ListTaskQueuePartitionsResponse, retError error) {
	resp, err := wh.parentHandler.ListTaskQueuePartitions(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.ListTaskQueuePartitionsResponse{}
	}
	return resp, err
}
