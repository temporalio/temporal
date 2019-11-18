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

	"github.com/temporalio/temporal-proto/workflowservice"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/service/frontend/adapter"
)

var _ workflowservice.WorkflowServiceYARPCServer = (*WorkflowHandlerGRPC)(nil)

type (
	// WorkflowHandlerGRPC - gRPC handler interface for workflow workflowservice
	WorkflowHandlerGRPC struct {
		workflowHandlerThrift *WorkflowHandler
	}
)

// NewWorkflowHandlerGRPC creates a thrift handler for the cadence workflowservice
func NewWorkflowHandlerGRPC(
	workflowHandlerThrift *WorkflowHandler,
) *WorkflowHandlerGRPC {
	handler := &WorkflowHandlerGRPC{
		workflowHandlerThrift: workflowHandlerThrift,
	}

	return handler
}

// RegisterHandler register this handler, must be called before Start()
// if DCRedirectionHandler is also used, use RegisterHandler in DCRedirectionHandler instead
func (wh *WorkflowHandlerGRPC) RegisterHandler() {
	wh.workflowHandlerThrift.Service.GetGRPCDispatcher().Register(workflowservice.BuildWorkflowServiceYARPCProcedures(wh))
}

// RegisterDomain creates a new domain which can be used as a container for all resources.  Domain is a top level
// entity within Cadence, used as a container for all resources like workflow executions, tasklists, etc.  Domain
// acts as a sandbox and provides isolation for all resources within the domain.  All resources belongs to exactly one
// domain.
func (wh *WorkflowHandlerGRPC) RegisterDomain(ctx context.Context, registerRequest *workflowservice.RegisterDomainRequest) (_ *workflowservice.RegisterDomainResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	err := wh.workflowHandlerThrift.RegisterDomain(ctx, adapter.ToThriftRegisterDomainRequest(registerRequest))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return &workflowservice.RegisterDomainResponse{}, nil
}

// DescribeDomain returns the information and configuration for a registered domain.
func (wh *WorkflowHandlerGRPC) DescribeDomain(ctx context.Context, describeRequest *workflowservice.DescribeDomainRequest) (_ *workflowservice.DescribeDomainResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	describeResponse, err := wh.workflowHandlerThrift.DescribeDomain(ctx, adapter.ToThriftDescribeDomainRequest(describeRequest))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoDescribeDomainResponse(describeResponse), nil
}

// ListDomains returns the information and configuration for all domains.
func (wh *WorkflowHandlerGRPC) ListDomains(ctx context.Context, listDomainRequest *workflowservice.ListDomainsRequest) (_ *workflowservice.ListDomainsResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	listDomainsResponse, err := wh.workflowHandlerThrift.ListDomains(ctx, adapter.ToThriftListDomainRequest(listDomainRequest))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoListDomainResponse(listDomainsResponse), nil
}

// UpdateDomain is used to update the information and configuration for a registered domain.
func (wh *WorkflowHandlerGRPC) UpdateDomain(ctx context.Context, updateDomainRequest *workflowservice.UpdateDomainRequest) (_ *workflowservice.UpdateDomainResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	updateDomainResponse, err := wh.workflowHandlerThrift.UpdateDomain(ctx, adapter.ToThriftUpdateDomainRequest(updateDomainRequest))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoUpdateDomainResponse(updateDomainResponse), nil
}

// DeprecateDomain us used to update status of a registered domain to DEPRECATED.  Once the domain is deprecated
// it cannot be used to start new workflow executions.  Existing workflow executions will continue to run on
// deprecated domains.
func (wh *WorkflowHandlerGRPC) DeprecateDomain(ctx context.Context, deprecateDomainRequest *workflowservice.DeprecateDomainRequest) (_ *workflowservice.DeprecateDomainResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	err := wh.workflowHandlerThrift.DeprecateDomain(ctx, adapter.ToThriftDeprecateDomainRequest(deprecateDomainRequest))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return &workflowservice.DeprecateDomainResponse{}, nil
}

// StartWorkflowExecution starts a new long running workflow instance.  It will create the instance with
// 'WorkflowExecutionStarted' event in history and also schedule the first DecisionTask for the worker to make the
// first decision for this instance.  It will return 'WorkflowExecutionAlreadyStartedError', if an instance already
// exists with same workflowId.
func (wh *WorkflowHandlerGRPC) StartWorkflowExecution(context.Context, *workflowservice.StartWorkflowExecutionRequest) (*workflowservice.StartWorkflowExecutionResponse, error) {
	panic("implement me")
}

// GetWorkflowExecutionHistory returns the history of specified workflow execution.  It fails with 'EntityNotExistError' if speficied workflow
// execution in unknown to the service.
func (wh *WorkflowHandlerGRPC) GetWorkflowExecutionHistory(context.Context, *workflowservice.GetWorkflowExecutionHistoryRequest) (*workflowservice.GetWorkflowExecutionHistoryResponse, error) {
	panic("implement me")
}

// PollForDecisionTask is called by application worker to process DecisionTask from a specific taskList.  A
// DecisionTask is dispatched to callers for active workflow executions, with pending decisions.
// Application is then expected to call 'RespondDecisionTaskCompleted' API when it is done processing the DecisionTask.
// It will also create a 'DecisionTaskStarted' event in the history for that session before handing off DecisionTask to
// application worker.
func (wh *WorkflowHandlerGRPC) PollForDecisionTask(context.Context, *workflowservice.PollForDecisionTaskRequest) (*workflowservice.PollForDecisionTaskResponse, error) {
	panic("implement me")
}

// RespondDecisionTaskCompleted is called by application worker to complete a DecisionTask handed as a result of
// 'PollForDecisionTask' API call.  Completing a DecisionTask will result in new events for the workflow execution and
// potentially new ActivityTask being created for corresponding decisions.  It will also create a DecisionTaskCompleted
// event in the history for that session.  Use the 'taskToken' provided as response of PollForDecisionTask API call
// for completing the DecisionTask.
// The response could contain a new decision task if there is one or if the request asking for one.
func (wh *WorkflowHandlerGRPC) RespondDecisionTaskCompleted(context.Context, *workflowservice.RespondDecisionTaskCompletedRequest) (*workflowservice.RespondDecisionTaskCompletedResponse, error) {
	panic("implement me")
}

// RespondDecisionTaskFailed is called by application worker to indicate failure.  This results in
// DecisionTaskFailedEvent written to the history and a new DecisionTask created.  This API can be used by client to
// either clear sticky tasklist or report any panics during DecisionTask processing.  Cadence will only append first
// DecisionTaskFailed event to the history of workflow execution for consecutive failures.
func (wh *WorkflowHandlerGRPC) RespondDecisionTaskFailed(context.Context, *workflowservice.RespondDecisionTaskFailedRequest) (*workflowservice.RespondDecisionTaskFailedResponse, error) {
	panic("implement me")
}

// PollForActivityTask is called by application worker to process ActivityTask from a specific taskList.  ActivityTask
// is dispatched to callers whenever a ScheduleTask decision is made for a workflow execution.
// Application is expected to call 'RespondActivityTaskCompleted' or 'RespondActivityTaskFailed' once it is done
// processing the task.
// Application also needs to call 'RecordActivityTaskHeartbeat' API within 'heartbeatTimeoutSeconds' interval to
// prevent the task from getting timed out.  An event 'ActivityTaskStarted' event is also written to workflow execution
// history before the ActivityTask is dispatched to application worker.
func (wh *WorkflowHandlerGRPC) PollForActivityTask(context.Context, *workflowservice.PollForActivityTaskRequest) (*workflowservice.PollForActivityTaskResponse, error) {
	panic("implement me")
}

// RecordActivityTaskHeartbeat is called by application worker while it is processing an ActivityTask.  If worker fails
// to heartbeat within 'heartbeatTimeoutSeconds' interval for the ActivityTask, then it will be marked as timedout and
// 'ActivityTaskTimedOut' event will be written to the workflow history.  Calling 'RecordActivityTaskHeartbeat' will
// fail with 'EntityNotExistsError' in such situations.  Use the 'taskToken' provided as response of
// PollForActivityTask API call for heartbeating.
func (wh *WorkflowHandlerGRPC) RecordActivityTaskHeartbeat(context.Context, *workflowservice.RecordActivityTaskHeartbeatRequest) (*workflowservice.RecordActivityTaskHeartbeatResponse, error) {
	panic("implement me")
}

// RecordActivityTaskHeartbeatByID is called by application worker while it is processing an ActivityTask.  If worker fails
// to heartbeat within 'heartbeatTimeoutSeconds' interval for the ActivityTask, then it will be marked as timedout and
// 'ActivityTaskTimedOut' event will be written to the workflow history.  Calling 'RecordActivityTaskHeartbeatByID' will
// fail with 'EntityNotExistsError' in such situations.  Instead of using 'taskToken' like in RecordActivityTaskHeartbeat,
// use Domain, WorkflowID and ActivityID
func (wh *WorkflowHandlerGRPC) RecordActivityTaskHeartbeatByID(context.Context, *workflowservice.RecordActivityTaskHeartbeatByIDRequest) (*workflowservice.RecordActivityTaskHeartbeatResponse, error) {
	panic("implement me")
}

// RespondActivityTaskCompleted is called by application worker when it is done processing an ActivityTask.  It will
// result in a new 'ActivityTaskCompleted' event being written to the workflow history and a new DecisionTask
// created for the workflow so new decisions could be made.  Use the 'taskToken' provided as response of
// PollForActivityTask API call for completion. It fails with 'EntityNotExistsError' if the taskToken is not valid
// anymore due to activity timeout.
func (wh *WorkflowHandlerGRPC) RespondActivityTaskCompleted(context.Context, *workflowservice.RespondActivityTaskCompletedRequest) (*workflowservice.RespondActivityTaskCompletedResponse, error) {
	panic("implement me")
}

// RespondActivityTaskCompletedByID is called by application worker when it is done processing an ActivityTask.
// It will result in a new 'ActivityTaskCompleted' event being written to the workflow history and a new DecisionTask
// created for the workflow so new decisions could be made.  Similar to RespondActivityTaskCompleted but use Domain,
// WorkflowID and ActivityID instead of 'taskToken' for completion. It fails with 'EntityNotExistsError'
// if the these IDs are not valid anymore due to activity timeout.
func (wh *WorkflowHandlerGRPC) RespondActivityTaskCompletedByID(context.Context, *workflowservice.RespondActivityTaskCompletedByIDRequest) (*workflowservice.RespondActivityTaskCompletedByIDResponse, error) {
	panic("implement me")
}

// RespondActivityTaskFailed is called by application worker when it is done processing an ActivityTask.  It will
// result in a new 'ActivityTaskFailed' event being written to the workflow history and a new DecisionTask
// created for the workflow instance so new decisions could be made.  Use the 'taskToken' provided as response of
// PollForActivityTask API call for completion. It fails with 'EntityNotExistsError' if the taskToken is not valid
// anymore due to activity timeout.
func (wh *WorkflowHandlerGRPC) RespondActivityTaskFailed(context.Context, *workflowservice.RespondActivityTaskFailedRequest) (*workflowservice.RespondActivityTaskFailedResponse, error) {
	panic("implement me")
}

// RespondActivityTaskFailedByID is called by application worker when it is done processing an ActivityTask.
// It will result in a new 'ActivityTaskFailed' event being written to the workflow history and a new DecisionTask
// created for the workflow instance so new decisions could be made.  Similar to RespondActivityTaskFailed but use
// Domain, WorkflowID and ActivityID instead of 'taskToken' for completion. It fails with 'EntityNotExistsError'
// if the these IDs are not valid anymore due to activity timeout.
func (wh *WorkflowHandlerGRPC) RespondActivityTaskFailedByID(context.Context, *workflowservice.RespondActivityTaskFailedByIDRequest) (*workflowservice.RespondActivityTaskFailedByIDResponse, error) {
	panic("implement me")
}

// RespondActivityTaskCanceled is called by application worker when it is successfully canceled an ActivityTask.  It will
// result in a new 'ActivityTaskCanceled' event being written to the workflow history and a new DecisionTask
// created for the workflow instance so new decisions could be made.  Use the 'taskToken' provided as response of
// PollForActivityTask API call for completion. It fails with 'EntityNotExistsError' if the taskToken is not valid
// anymore due to activity timeout.
func (wh *WorkflowHandlerGRPC) RespondActivityTaskCanceled(context.Context, *workflowservice.RespondActivityTaskCanceledRequest) (*workflowservice.RespondActivityTaskCanceledResponse, error) {
	panic("implement me")
}

// RespondActivityTaskCanceledByID is called by application worker when it is successfully canceled an ActivityTask.
// It will result in a new 'ActivityTaskCanceled' event being written to the workflow history and a new DecisionTask
// created for the workflow instance so new decisions could be made.  Similar to RespondActivityTaskCanceled but use
// Domain, WorkflowID and ActivityID instead of 'taskToken' for completion. It fails with 'EntityNotExistsError'
// if the these IDs are not valid anymore due to activity timeout.
func (wh *WorkflowHandlerGRPC) RespondActivityTaskCanceledByID(context.Context, *workflowservice.RespondActivityTaskCanceledByIDRequest) (*workflowservice.RespondActivityTaskCanceledByIDResponse, error) {
	panic("implement me")
}

// RequestCancelWorkflowExecution is called by application worker when it wants to request cancellation of a workflow instance.
// It will result in a new 'WorkflowExecutionCancelRequested' event being written to the workflow history and a new DecisionTask
// created for the workflow instance so new decisions could be made. It fails with 'EntityNotExistsError' if the workflow is not valid
// anymore due to completion or doesn't exist.
func (wh *WorkflowHandlerGRPC) RequestCancelWorkflowExecution(context.Context, *workflowservice.RequestCancelWorkflowExecutionRequest) (*workflowservice.RequestCancelWorkflowExecutionResponse, error) {
	panic("implement me")
}

// SignalWorkflowExecution is used to send a signal event to running workflow execution.  This results in
// WorkflowExecutionSignaled event recorded in the history and a decision task being created for the execution.
func (wh *WorkflowHandlerGRPC) SignalWorkflowExecution(context.Context, *workflowservice.SignalWorkflowExecutionRequest) (*workflowservice.SignalWorkflowExecutionResponse, error) {
	panic("implement me")
}

// SignalWithStartWorkflowExecution is used to ensure sending signal to a workflow.
// If the workflow is running, this results in WorkflowExecutionSignaled event being recorded in the history
// and a decision task being created for the execution.
// If the workflow is not running or not found, this results in WorkflowExecutionStarted and WorkflowExecutionSignaled
// events being recorded in history, and a decision task being created for the execution
func (wh *WorkflowHandlerGRPC) SignalWithStartWorkflowExecution(context.Context, *workflowservice.SignalWithStartWorkflowExecutionRequest) (*workflowservice.StartWorkflowExecutionResponse, error) {
	panic("implement me")
}

// ResetWorkflowExecution reset an existing workflow execution to DecisionTaskCompleted event(exclusive).
// And it will immediately terminating the current execution instance.
func (wh *WorkflowHandlerGRPC) ResetWorkflowExecution(context.Context, *workflowservice.ResetWorkflowExecutionRequest) (*workflowservice.ResetWorkflowExecutionResponse, error) {
	panic("implement me")
}

// TerminateWorkflowExecution terminates an existing workflow execution by recording WorkflowExecutionTerminated event
// in the history and immediately terminating the execution instance.
func (wh *WorkflowHandlerGRPC) TerminateWorkflowExecution(context.Context, *workflowservice.TerminateWorkflowExecutionRequest) (*workflowservice.TerminateWorkflowExecutionResponse, error) {
	panic("implement me")
}

// ListOpenWorkflowExecutions is a visibility API to list the open executions in a specific domain.
func (wh *WorkflowHandlerGRPC) ListOpenWorkflowExecutions(context.Context, *workflowservice.ListOpenWorkflowExecutionsRequest) (*workflowservice.ListOpenWorkflowExecutionsResponse, error) {
	panic("implement me")
}

// ListClosedWorkflowExecutions is a visibility API to list the closed executions in a specific domain.
func (wh *WorkflowHandlerGRPC) ListClosedWorkflowExecutions(context.Context, *workflowservice.ListClosedWorkflowExecutionsRequest) (*workflowservice.ListClosedWorkflowExecutionsResponse, error) {
	panic("implement me")
}

// ListWorkflowExecutions is a visibility API to list workflow executions in a specific domain.
func (wh *WorkflowHandlerGRPC) ListWorkflowExecutions(context.Context, *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	panic("implement me")
}

// ListArchivedWorkflowExecutions is a visibility API to list archived workflow executions in a specific domain.
func (wh *WorkflowHandlerGRPC) ListArchivedWorkflowExecutions(context.Context, *workflowservice.ListArchivedWorkflowExecutionsRequest) (*workflowservice.ListArchivedWorkflowExecutionsResponse, error) {
	panic("implement me")
}

// ScanWorkflowExecutions is a visibility API to list large amount of workflow executions in a specific domain without order.
func (wh *WorkflowHandlerGRPC) ScanWorkflowExecutions(context.Context, *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	panic("implement me")
}

// CountWorkflowExecutions is a visibility API to count of workflow executions in a specific domain.
func (wh *WorkflowHandlerGRPC) CountWorkflowExecutions(context.Context, *workflowservice.CountWorkflowExecutionsRequest) (*workflowservice.CountWorkflowExecutionsResponse, error) {
	panic("implement me")
}

// GetSearchAttributes is a visibility API to get all legal keys that could be used in list APIs
func (wh *WorkflowHandlerGRPC) GetSearchAttributes(context.Context, *workflowservice.GetSearchAttributesRequest) (*workflowservice.GetSearchAttributesResponse, error) {
	panic("implement me")
}

// RespondQueryTaskCompleted is called by application worker to complete a QueryTask (which is a DecisionTask for query)
// as a result of 'PollForDecisionTask' API call. Completing a QueryTask will unblock the client call to 'QueryWorkflow'
// API and return the query result to client as a response to 'QueryWorkflow' API call.
func (wh *WorkflowHandlerGRPC) RespondQueryTaskCompleted(context.Context, *workflowservice.RespondQueryTaskCompletedRequest) (*workflowservice.RespondQueryTaskCompletedResponse, error) {
	panic("implement me")
}

// ResetStickyTaskList resets the sticky tasklist related information in mutable state of a given workflow.
// Things cleared are:
// 1. StickyTaskList
// 2. StickyScheduleToStartTimeout
// 3. ClientLibraryVersion
// 4. ClientFeatureVersion
// 5. ClientImpl
func (wh *WorkflowHandlerGRPC) ResetStickyTaskList(context.Context, *workflowservice.ResetStickyTaskListRequest) (*workflowservice.ResetStickyTaskListResponse, error) {
	panic("implement me")
}

// QueryWorkflow returns query result for a specified workflow execution
func (wh *WorkflowHandlerGRPC) QueryWorkflow(context.Context, *workflowservice.QueryWorkflowRequest) (*workflowservice.QueryWorkflowResponse, error) {
	panic("implement me")
}

// DescribeWorkflowExecution returns information about the specified workflow execution.
func (wh *WorkflowHandlerGRPC) DescribeWorkflowExecution(context.Context, *workflowservice.DescribeWorkflowExecutionRequest) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	panic("implement me")
}

// DescribeTaskList returns information about the target tasklist, right now this API returns the
// pollers which polled this tasklist in last few minutes.
func (wh *WorkflowHandlerGRPC) DescribeTaskList(context.Context, *workflowservice.DescribeTaskListRequest) (*workflowservice.DescribeTaskListResponse, error) {
	panic("implement me")
}

// GetReplicationMessages returns new replication tasks since the read level provided in the token.
func (wh *WorkflowHandlerGRPC) GetReplicationMessages(context.Context, *workflowservice.GetReplicationMessagesRequest) (*workflowservice.GetReplicationMessagesResponse, error) {
	panic("implement me")
}

// GetDomainReplicationMessages returns new domain replication tasks since last retrieved task ID.
func (wh *WorkflowHandlerGRPC) GetDomainReplicationMessages(context.Context, *workflowservice.GetDomainReplicationMessagesRequest) (*workflowservice.GetDomainReplicationMessagesResponse, error) {
	panic("implement me")
}

// ReapplyEvents applies stale events to the current workflow and current run
func (wh *WorkflowHandlerGRPC) ReapplyEvents(context.Context, *workflowservice.ReapplyEventsRequest) (*workflowservice.ReapplyEventsResponse, error) {
	panic("implement me")
}
