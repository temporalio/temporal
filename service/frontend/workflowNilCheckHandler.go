// Copyright (c) 2019 Temporal Technologies, Inc.
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

	"go.temporal.io/temporal-proto/workflowservice"
)

var _ workflowservice.WorkflowServiceServer = (*WorkflowNilCheckHandler)(nil)

type (
	// WorkflowNilCheckHandler - gRPC handler interface for workflow workflowservice
	WorkflowNilCheckHandler struct {
		parentHandler workflowservice.WorkflowServiceServer
	}
)

// Due to bug in gogo/protobuf https://github.com/gogo/protobuf/issues/651 response can't be nil when error is also nil.
// This handler makes sure response is always not nil, when error is nil.
// Can be removed from pipeline when bug is resolved.

// NewWorkflowNilCheckHandler creates handler that never returns nil response when error is nil
func NewWorkflowNilCheckHandler(
	parentHandler workflowservice.WorkflowServiceServer,
) *WorkflowNilCheckHandler {
	handler := &WorkflowNilCheckHandler{
		parentHandler: parentHandler,
	}

	return handler
}

// RegisterDomain creates a new domain which can be used as a container for all resources.  Domain is a top level
// entity within Cadence, used as a container for all resources like workflow executions, tasklists, etc.  Domain
// acts as a sandbox and provides isolation for all resources within the domain.  All resources belongs to exactly one
// domain.
func (wh *WorkflowNilCheckHandler) RegisterDomain(ctx context.Context, request *workflowservice.RegisterDomainRequest) (_ *workflowservice.RegisterDomainResponse, retError error) {
	resp, err := wh.parentHandler.RegisterDomain(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.RegisterDomainResponse{}
	}
	return resp, err
}

// DescribeDomain returns the information and configuration for a registered domain.
func (wh *WorkflowNilCheckHandler) DescribeDomain(ctx context.Context, request *workflowservice.DescribeDomainRequest) (_ *workflowservice.DescribeDomainResponse, retError error) {
	resp, err := wh.parentHandler.DescribeDomain(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.DescribeDomainResponse{}
	}
	return resp, err
}

// ListDomains returns the information and configuration for all domains.
func (wh *WorkflowNilCheckHandler) ListDomains(ctx context.Context, request *workflowservice.ListDomainsRequest) (_ *workflowservice.ListDomainsResponse, retError error) {
	resp, err := wh.parentHandler.ListDomains(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.ListDomainsResponse{}
	}
	return resp, err
}

// UpdateDomain is used to update the information and configuration for a registered domain.
func (wh *WorkflowNilCheckHandler) UpdateDomain(ctx context.Context, request *workflowservice.UpdateDomainRequest) (_ *workflowservice.UpdateDomainResponse, retError error) {
	resp, err := wh.parentHandler.UpdateDomain(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.UpdateDomainResponse{}
	}
	return resp, err
}

// DeprecateDomain us used to update status of a registered domain to DEPRECATED.  Once the domain is deprecated
// it cannot be used to start new workflow executions.  Existing workflow executions will continue to run on
// deprecated domains.
func (wh *WorkflowNilCheckHandler) DeprecateDomain(ctx context.Context, request *workflowservice.DeprecateDomainRequest) (_ *workflowservice.DeprecateDomainResponse, retError error) {
	resp, err := wh.parentHandler.DeprecateDomain(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.DeprecateDomainResponse{}
	}
	return resp, err
}

// StartWorkflowExecution starts a new long running workflow instance.  It will create the instance with
// 'WorkflowExecutionStarted' event in history and also schedule the first DecisionTask for the worker to make the
// first decision for this instance.  It will return 'WorkflowExecutionAlreadyStartedError', if an instance already
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

// PollForDecisionTask is called by application worker to process DecisionTask from a specific taskList.  A
// DecisionTask is dispatched to callers for active workflow executions, with pending decisions.
// Application is then expected to call 'RespondDecisionTaskCompleted' API when it is done processing the DecisionTask.
// It will also create a 'DecisionTaskStarted' event in the history for that session before handing off DecisionTask to
// application worker.
func (wh *WorkflowNilCheckHandler) PollForDecisionTask(ctx context.Context, request *workflowservice.PollForDecisionTaskRequest) (_ *workflowservice.PollForDecisionTaskResponse, retError error) {
	resp, err := wh.parentHandler.PollForDecisionTask(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.PollForDecisionTaskResponse{}
	}
	return resp, err
}

// RespondDecisionTaskCompleted is called by application worker to complete a DecisionTask handed as a result of
// 'PollForDecisionTask' API call.  Completing a DecisionTask will result in new events for the workflow execution and
// potentially new ActivityTask being created for corresponding decisions.  It will also create a DecisionTaskCompleted
// event in the history for that session.  Use the 'taskToken' provided as response of PollForDecisionTask API call
// for completing the DecisionTask.
// The response could contain a new decision task if there is one or if the request asking for one.
func (wh *WorkflowNilCheckHandler) RespondDecisionTaskCompleted(ctx context.Context, request *workflowservice.RespondDecisionTaskCompletedRequest) (_ *workflowservice.RespondDecisionTaskCompletedResponse, retError error) {
	resp, err := wh.parentHandler.RespondDecisionTaskCompleted(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.RespondDecisionTaskCompletedResponse{}
	}
	return resp, err
}

// RespondDecisionTaskFailed is called by application worker to indicate failure.  This results in
// DecisionTaskFailedEvent written to the history and a new DecisionTask created.  This API can be used by client to
// either clear sticky tasklist or report any panics during DecisionTask processing.  Cadence will only append first
// DecisionTaskFailed event to the history of workflow execution for consecutive failures.
func (wh *WorkflowNilCheckHandler) RespondDecisionTaskFailed(ctx context.Context, request *workflowservice.RespondDecisionTaskFailedRequest) (_ *workflowservice.RespondDecisionTaskFailedResponse, retError error) {
	resp, err := wh.parentHandler.RespondDecisionTaskFailed(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.RespondDecisionTaskFailedResponse{}
	}
	return resp, err
}

// PollForActivityTask is called by application worker to process ActivityTask from a specific taskList.  ActivityTask
// is dispatched to callers whenever a ScheduleTask decision is made for a workflow execution.
// Application is expected to call 'RespondActivityTaskCompleted' or 'RespondActivityTaskFailed' once it is done
// processing the task.
// Application also needs to call 'RecordActivityTaskHeartbeat' API within 'heartbeatTimeoutSeconds' interval to
// prevent the task from getting timed out.  An event 'ActivityTaskStarted' event is also written to workflow execution
// history before the ActivityTask is dispatched to application worker.
func (wh *WorkflowNilCheckHandler) PollForActivityTask(ctx context.Context, request *workflowservice.PollForActivityTaskRequest) (_ *workflowservice.PollForActivityTaskResponse, retError error) {
	resp, err := wh.parentHandler.PollForActivityTask(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.PollForActivityTaskResponse{}
	}
	return resp, err
}

// RecordActivityTaskHeartbeat is called by application worker while it is processing an ActivityTask.  If worker fails
// to heartbeat within 'heartbeatTimeoutSeconds' interval for the ActivityTask, then it will be marked as timedout and
// 'ActivityTaskTimedOut' event will be written to the workflow history.  Calling 'RecordActivityTaskHeartbeat' will
// fail with 'EntityNotExistsError' in such situations.  Use the 'taskToken' provided as response of
// PollForActivityTask API call for heartbeating.
func (wh *WorkflowNilCheckHandler) RecordActivityTaskHeartbeat(ctx context.Context, request *workflowservice.RecordActivityTaskHeartbeatRequest) (_ *workflowservice.RecordActivityTaskHeartbeatResponse, retError error) {
	resp, err := wh.parentHandler.RecordActivityTaskHeartbeat(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.RecordActivityTaskHeartbeatResponse{}
	}
	return resp, err
}

// RecordActivityTaskHeartbeatByID is called by application worker while it is processing an ActivityTask.  If worker fails
// to heartbeat within 'heartbeatTimeoutSeconds' interval for the ActivityTask, then it will be marked as timedout and
// 'ActivityTaskTimedOut' event will be written to the workflow history.  Calling 'RecordActivityTaskHeartbeatByID' will
// fail with 'EntityNotExistsError' in such situations.  Instead of using 'taskToken' like in RecordActivityTaskHeartbeat,
// use Domain, WorkflowID and ActivityID
func (wh *WorkflowNilCheckHandler) RecordActivityTaskHeartbeatByID(ctx context.Context, request *workflowservice.RecordActivityTaskHeartbeatByIDRequest) (_ *workflowservice.RecordActivityTaskHeartbeatByIDResponse, retError error) {
	resp, err := wh.parentHandler.RecordActivityTaskHeartbeatByID(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.RecordActivityTaskHeartbeatByIDResponse{}
	}
	return resp, err
}

// RespondActivityTaskCompleted is called by application worker when it is done processing an ActivityTask.  It will
// result in a new 'ActivityTaskCompleted' event being written to the workflow history and a new DecisionTask
// created for the workflow so new decisions could be made.  Use the 'taskToken' provided as response of
// PollForActivityTask API call for completion. It fails with 'EntityNotExistsError' if the taskToken is not valid
// anymore due to activity timeout.
func (wh *WorkflowNilCheckHandler) RespondActivityTaskCompleted(ctx context.Context, request *workflowservice.RespondActivityTaskCompletedRequest) (_ *workflowservice.RespondActivityTaskCompletedResponse, retError error) {
	resp, err := wh.parentHandler.RespondActivityTaskCompleted(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.RespondActivityTaskCompletedResponse{}
	}
	return resp, err
}

// RespondActivityTaskCompletedByID is called by application worker when it is done processing an ActivityTask.
// It will result in a new 'ActivityTaskCompleted' event being written to the workflow history and a new DecisionTask
// created for the workflow so new decisions could be made.  Similar to RespondActivityTaskCompleted but use Domain,
// WorkflowID and ActivityID instead of 'taskToken' for completion. It fails with 'EntityNotExistsError'
// if the these IDs are not valid anymore due to activity timeout.
func (wh *WorkflowNilCheckHandler) RespondActivityTaskCompletedByID(ctx context.Context, request *workflowservice.RespondActivityTaskCompletedByIDRequest) (_ *workflowservice.RespondActivityTaskCompletedByIDResponse, retError error) {
	resp, err := wh.parentHandler.RespondActivityTaskCompletedByID(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.RespondActivityTaskCompletedByIDResponse{}
	}
	return resp, err
}

// RespondActivityTaskFailed is called by application worker when it is done processing an ActivityTask.  It will
// result in a new 'ActivityTaskFailed' event being written to the workflow history and a new DecisionTask
// created for the workflow instance so new decisions could be made.  Use the 'taskToken' provided as response of
// PollForActivityTask API call for completion. It fails with 'EntityNotExistsError' if the taskToken is not valid
// anymore due to activity timeout.
func (wh *WorkflowNilCheckHandler) RespondActivityTaskFailed(ctx context.Context, request *workflowservice.RespondActivityTaskFailedRequest) (_ *workflowservice.RespondActivityTaskFailedResponse, retError error) {
	resp, err := wh.parentHandler.RespondActivityTaskFailed(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.RespondActivityTaskFailedResponse{}
	}
	return resp, err
}

// RespondActivityTaskFailedByID is called by application worker when it is done processing an ActivityTask.
// It will result in a new 'ActivityTaskFailed' event being written to the workflow history and a new DecisionTask
// created for the workflow instance so new decisions could be made.  Similar to RespondActivityTaskFailed but use
// Domain, WorkflowID and ActivityID instead of 'taskToken' for completion. It fails with 'EntityNotExistsError'
// if the these IDs are not valid anymore due to activity timeout.
func (wh *WorkflowNilCheckHandler) RespondActivityTaskFailedByID(ctx context.Context, request *workflowservice.RespondActivityTaskFailedByIDRequest) (_ *workflowservice.RespondActivityTaskFailedByIDResponse, retError error) {
	resp, err := wh.parentHandler.RespondActivityTaskFailedByID(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.RespondActivityTaskFailedByIDResponse{}
	}
	return resp, err
}

// RespondActivityTaskCanceled is called by application worker when it is successfully canceled an ActivityTask.  It will
// result in a new 'ActivityTaskCanceled' event being written to the workflow history and a new DecisionTask
// created for the workflow instance so new decisions could be made.  Use the 'taskToken' provided as response of
// PollForActivityTask API call for completion. It fails with 'EntityNotExistsError' if the taskToken is not valid
// anymore due to activity timeout.
func (wh *WorkflowNilCheckHandler) RespondActivityTaskCanceled(ctx context.Context, request *workflowservice.RespondActivityTaskCanceledRequest) (_ *workflowservice.RespondActivityTaskCanceledResponse, retError error) {
	resp, err := wh.parentHandler.RespondActivityTaskCanceled(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.RespondActivityTaskCanceledResponse{}
	}
	return resp, err
}

// RespondActivityTaskCanceledByID is called by application worker when it is successfully canceled an ActivityTask.
// It will result in a new 'ActivityTaskCanceled' event being written to the workflow history and a new DecisionTask
// created for the workflow instance so new decisions could be made.  Similar to RespondActivityTaskCanceled but use
// Domain, WorkflowID and ActivityID instead of 'taskToken' for completion. It fails with 'EntityNotExistsError'
// if the these IDs are not valid anymore due to activity timeout.
func (wh *WorkflowNilCheckHandler) RespondActivityTaskCanceledByID(ctx context.Context, request *workflowservice.RespondActivityTaskCanceledByIDRequest) (_ *workflowservice.RespondActivityTaskCanceledByIDResponse, retError error) {
	resp, err := wh.parentHandler.RespondActivityTaskCanceledByID(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.RespondActivityTaskCanceledByIDResponse{}
	}
	return resp, err
}

// RequestCancelWorkflowExecution is called by application worker when it wants to request cancellation of a workflow instance.
// It will result in a new 'WorkflowExecutionCancelRequested' event being written to the workflow history and a new DecisionTask
// created for the workflow instance so new decisions could be made. It fails with 'EntityNotExistsError' if the workflow is not valid
// anymore due to completion or doesn't exist.
func (wh *WorkflowNilCheckHandler) RequestCancelWorkflowExecution(ctx context.Context, request *workflowservice.RequestCancelWorkflowExecutionRequest) (_ *workflowservice.RequestCancelWorkflowExecutionResponse, retError error) {
	resp, err := wh.parentHandler.RequestCancelWorkflowExecution(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.RequestCancelWorkflowExecutionResponse{}
	}
	return resp, err
}

// SignalWorkflowExecution is used to send a signal event to running workflow execution.  This results in
// WorkflowExecutionSignaled event recorded in the history and a decision task being created for the execution.
func (wh *WorkflowNilCheckHandler) SignalWorkflowExecution(ctx context.Context, request *workflowservice.SignalWorkflowExecutionRequest) (_ *workflowservice.SignalWorkflowExecutionResponse, retError error) {
	resp, err := wh.parentHandler.SignalWorkflowExecution(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.SignalWorkflowExecutionResponse{}
	}
	return resp, err
}

// SignalWithStartWorkflowExecution is used to ensure sending signal to a workflow.
// If the workflow is running, this results in WorkflowExecutionSignaled event being recorded in the history
// and a decision task being created for the execution.
// If the workflow is not running or not found, this results in WorkflowExecutionStarted and WorkflowExecutionSignaled
// events being recorded in history, and a decision task being created for the execution
func (wh *WorkflowNilCheckHandler) SignalWithStartWorkflowExecution(ctx context.Context, request *workflowservice.SignalWithStartWorkflowExecutionRequest) (_ *workflowservice.SignalWithStartWorkflowExecutionResponse, retError error) {
	resp, err := wh.parentHandler.SignalWithStartWorkflowExecution(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.SignalWithStartWorkflowExecutionResponse{}
	}
	return resp, err
}

// ResetWorkflowExecution reset an existing workflow execution to DecisionTaskCompleted event(exclusive).
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

// ListOpenWorkflowExecutions is a visibility API to list the open executions in a specific domain.
func (wh *WorkflowNilCheckHandler) ListOpenWorkflowExecutions(ctx context.Context, request *workflowservice.ListOpenWorkflowExecutionsRequest) (_ *workflowservice.ListOpenWorkflowExecutionsResponse, retError error) {
	resp, err := wh.parentHandler.ListOpenWorkflowExecutions(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.ListOpenWorkflowExecutionsResponse{}
	}
	return resp, err
}

// ListClosedWorkflowExecutions is a visibility API to list the closed executions in a specific domain.
func (wh *WorkflowNilCheckHandler) ListClosedWorkflowExecutions(ctx context.Context, request *workflowservice.ListClosedWorkflowExecutionsRequest) (_ *workflowservice.ListClosedWorkflowExecutionsResponse, retError error) {
	resp, err := wh.parentHandler.ListClosedWorkflowExecutions(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.ListClosedWorkflowExecutionsResponse{}
	}
	return resp, err
}

// ListWorkflowExecutions is a visibility API to list workflow executions in a specific domain.
func (wh *WorkflowNilCheckHandler) ListWorkflowExecutions(ctx context.Context, request *workflowservice.ListWorkflowExecutionsRequest) (_ *workflowservice.ListWorkflowExecutionsResponse, retError error) {
	resp, err := wh.parentHandler.ListWorkflowExecutions(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.ListWorkflowExecutionsResponse{}
	}
	return resp, err
}

// ListArchivedWorkflowExecutions is a visibility API to list archived workflow executions in a specific domain.
func (wh *WorkflowNilCheckHandler) ListArchivedWorkflowExecutions(ctx context.Context, request *workflowservice.ListArchivedWorkflowExecutionsRequest) (_ *workflowservice.ListArchivedWorkflowExecutionsResponse, retError error) {
	resp, err := wh.parentHandler.ListArchivedWorkflowExecutions(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.ListArchivedWorkflowExecutionsResponse{}
	}
	return resp, err
}

// ScanWorkflowExecutions is a visibility API to list large amount of workflow executions in a specific domain without order.
func (wh *WorkflowNilCheckHandler) ScanWorkflowExecutions(ctx context.Context, request *workflowservice.ScanWorkflowExecutionsRequest) (_ *workflowservice.ScanWorkflowExecutionsResponse, retError error) {
	resp, err := wh.parentHandler.ScanWorkflowExecutions(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.ScanWorkflowExecutionsResponse{}
	}
	return resp, err
}

// CountWorkflowExecutions is a visibility API to count of workflow executions in a specific domain.
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

// RespondQueryTaskCompleted is called by application worker to complete a QueryTask (which is a DecisionTask for query)
// as a result of 'PollForDecisionTask' API call. Completing a QueryTask will unblock the client call to 'QueryWorkflow'
// API and return the query result to client as a response to 'QueryWorkflow' API call.
func (wh *WorkflowNilCheckHandler) RespondQueryTaskCompleted(ctx context.Context, request *workflowservice.RespondQueryTaskCompletedRequest) (_ *workflowservice.RespondQueryTaskCompletedResponse, retError error) {
	resp, err := wh.parentHandler.RespondQueryTaskCompleted(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.RespondQueryTaskCompletedResponse{}
	}
	return resp, err
}

// ResetStickyTaskList resets the sticky tasklist related information in mutable state of a given workflow.
// Things cleared are:
// 1. StickyTaskList
// 2. StickyScheduleToStartTimeout
// 3. ClientLibraryVersion
// 4. ClientFeatureVersion
// 5. ClientImpl
func (wh *WorkflowNilCheckHandler) ResetStickyTaskList(ctx context.Context, request *workflowservice.ResetStickyTaskListRequest) (_ *workflowservice.ResetStickyTaskListResponse, retError error) {
	resp, err := wh.parentHandler.ResetStickyTaskList(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.ResetStickyTaskListResponse{}
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

// DescribeTaskList returns information about the target tasklist, right now this API returns the
// pollers which polled this tasklist in last few minutes.
func (wh *WorkflowNilCheckHandler) DescribeTaskList(ctx context.Context, request *workflowservice.DescribeTaskListRequest) (_ *workflowservice.DescribeTaskListResponse, retError error) {
	resp, err := wh.parentHandler.DescribeTaskList(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.DescribeTaskListResponse{}
	}
	return resp, err
}

func (wh *WorkflowNilCheckHandler) PollForWorkflowExecutionRawHistory(ctx context.Context, request *workflowservice.PollForWorkflowExecutionRawHistoryRequest) (_ *workflowservice.PollForWorkflowExecutionRawHistoryResponse, retError error) {
	resp, err := wh.parentHandler.PollForWorkflowExecutionRawHistory(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.PollForWorkflowExecutionRawHistoryResponse{}
	}
	return resp, err
}

// GetWorkflowExecutionRawHistory retrieves raw history directly from DB layer.
func (wh *WorkflowNilCheckHandler) GetWorkflowExecutionRawHistory(ctx context.Context, request *workflowservice.GetWorkflowExecutionRawHistoryRequest) (_ *workflowservice.GetWorkflowExecutionRawHistoryResponse, retError error) {
	resp, err := wh.parentHandler.GetWorkflowExecutionRawHistory(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.GetWorkflowExecutionRawHistoryResponse{}
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

// ListTaskListPartitions ...
func (wh *WorkflowNilCheckHandler) ListTaskListPartitions(ctx context.Context, request *workflowservice.ListTaskListPartitionsRequest) (_ *workflowservice.ListTaskListPartitionsResponse, retError error) {
	resp, err := wh.parentHandler.ListTaskListPartitions(ctx, request)
	if resp == nil && err == nil {
		resp = &workflowservice.ListTaskListPartitionsResponse{}
	}
	return resp, err
}
