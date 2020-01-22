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
	wh.workflowHandlerThrift.GetGRPCDispatcher().Register(workflowservice.BuildWorkflowServiceYARPCProcedures(wh))
}

// RegisterDomain creates a new domain which can be used as a container for all resources.  Domain is a top level
// entity within Cadence, used as a container for all resources like workflow executions, tasklists, etc.  Domain
// acts as a sandbox and provides isolation for all resources within the domain.  All resources belongs to exactly one
// domain.
func (wh *WorkflowHandlerGRPC) RegisterDomain(ctx context.Context, request *workflowservice.RegisterDomainRequest) (_ *workflowservice.RegisterDomainResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	err := wh.workflowHandlerThrift.RegisterDomain(ctx, adapter.ToThriftRegisterDomainRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return &workflowservice.RegisterDomainResponse{}, nil
}

// DescribeDomain returns the information and configuration for a registered domain.
func (wh *WorkflowHandlerGRPC) DescribeDomain(ctx context.Context, request *workflowservice.DescribeDomainRequest) (_ *workflowservice.DescribeDomainResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	response, err := wh.workflowHandlerThrift.DescribeDomain(ctx, adapter.ToThriftDescribeDomainRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoDescribeDomainResponse(response), nil
}

// ListDomains returns the information and configuration for all domains.
func (wh *WorkflowHandlerGRPC) ListDomains(ctx context.Context, request *workflowservice.ListDomainsRequest) (_ *workflowservice.ListDomainsResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	response, err := wh.workflowHandlerThrift.ListDomains(ctx, adapter.ToThriftListDomainsRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoListDomainsResponse(response), nil
}

// UpdateDomain is used to update the information and configuration for a registered domain.
func (wh *WorkflowHandlerGRPC) UpdateDomain(ctx context.Context, request *workflowservice.UpdateDomainRequest) (_ *workflowservice.UpdateDomainResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	response, err := wh.workflowHandlerThrift.UpdateDomain(ctx, adapter.ToThriftUpdateDomainRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoUpdateDomainResponse(response), nil
}

// DeprecateDomain us used to update status of a registered domain to DEPRECATED.  Once the domain is deprecated
// it cannot be used to start new workflow executions.  Existing workflow executions will continue to run on
// deprecated domains.
func (wh *WorkflowHandlerGRPC) DeprecateDomain(ctx context.Context, request *workflowservice.DeprecateDomainRequest) (_ *workflowservice.DeprecateDomainResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	err := wh.workflowHandlerThrift.DeprecateDomain(ctx, adapter.ToThriftDeprecateDomainRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return &workflowservice.DeprecateDomainResponse{}, nil
}

// StartWorkflowExecution starts a new long running workflow instance.  It will create the instance with
// 'WorkflowExecutionStarted' event in history and also schedule the first DecisionTask for the worker to make the
// first decision for this instance.  It will return 'WorkflowExecutionAlreadyStartedError', if an instance already
// exists with same workflowId.
func (wh *WorkflowHandlerGRPC) StartWorkflowExecution(ctx context.Context, request *workflowservice.StartWorkflowExecutionRequest) (_ *workflowservice.StartWorkflowExecutionResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	response, err := wh.workflowHandlerThrift.StartWorkflowExecution(ctx, adapter.ToThriftStartWorkflowExecutionRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoStartWorkflowExecutionResponse(response), nil
}

// GetWorkflowExecutionHistory returns the history of specified workflow execution.  It fails with 'EntityNotExistError' if speficied workflow
// execution in unknown to the service.
func (wh *WorkflowHandlerGRPC) GetWorkflowExecutionHistory(ctx context.Context, request *workflowservice.GetWorkflowExecutionHistoryRequest) (_ *workflowservice.GetWorkflowExecutionHistoryResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	response, err := wh.workflowHandlerThrift.GetWorkflowExecutionHistory(ctx, adapter.ToThriftGetWorkflowExecutionHistoryRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoGetWorkflowExecutionHistoryResponse(response), nil
}

// PollForDecisionTask is called by application worker to process DecisionTask from a specific taskList.  A
// DecisionTask is dispatched to callers for active workflow executions, with pending decisions.
// Application is then expected to call 'RespondDecisionTaskCompleted' API when it is done processing the DecisionTask.
// It will also create a 'DecisionTaskStarted' event in the history for that session before handing off DecisionTask to
// application worker.
func (wh *WorkflowHandlerGRPC) PollForDecisionTask(ctx context.Context, request *workflowservice.PollForDecisionTaskRequest) (_ *workflowservice.PollForDecisionTaskResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	response, err := wh.workflowHandlerThrift.PollForDecisionTask(ctx, adapter.ToThriftPollForDecisionTaskRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoPollForDecisionTaskResponse(response), nil
}

// RespondDecisionTaskCompleted is called by application worker to complete a DecisionTask handed as a result of
// 'PollForDecisionTask' API call.  Completing a DecisionTask will result in new events for the workflow execution and
// potentially new ActivityTask being created for corresponding decisions.  It will also create a DecisionTaskCompleted
// event in the history for that session.  Use the 'taskToken' provided as response of PollForDecisionTask API call
// for completing the DecisionTask.
// The response could contain a new decision task if there is one or if the request asking for one.
func (wh *WorkflowHandlerGRPC) RespondDecisionTaskCompleted(ctx context.Context, request *workflowservice.RespondDecisionTaskCompletedRequest) (_ *workflowservice.RespondDecisionTaskCompletedResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	response, err := wh.workflowHandlerThrift.RespondDecisionTaskCompleted(ctx, adapter.ToThriftRespondDecisionTaskCompletedRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoRespondDecisionTaskCompletedResponse(response), nil
}

// RespondDecisionTaskFailed is called by application worker to indicate failure.  This results in
// DecisionTaskFailedEvent written to the history and a new DecisionTask created.  This API can be used by client to
// either clear sticky tasklist or report any panics during DecisionTask processing.  Cadence will only append first
// DecisionTaskFailed event to the history of workflow execution for consecutive failures.
func (wh *WorkflowHandlerGRPC) RespondDecisionTaskFailed(ctx context.Context, request *workflowservice.RespondDecisionTaskFailedRequest) (_ *workflowservice.RespondDecisionTaskFailedResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	err := wh.workflowHandlerThrift.RespondDecisionTaskFailed(ctx, adapter.ToThriftRespondDecisionTaskFailedRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return &workflowservice.RespondDecisionTaskFailedResponse{}, nil
}

// PollForActivityTask is called by application worker to process ActivityTask from a specific taskList.  ActivityTask
// is dispatched to callers whenever a ScheduleTask decision is made for a workflow execution.
// Application is expected to call 'RespondActivityTaskCompleted' or 'RespondActivityTaskFailed' once it is done
// processing the task.
// Application also needs to call 'RecordActivityTaskHeartbeat' API within 'heartbeatTimeoutSeconds' interval to
// prevent the task from getting timed out.  An event 'ActivityTaskStarted' event is also written to workflow execution
// history before the ActivityTask is dispatched to application worker.
func (wh *WorkflowHandlerGRPC) PollForActivityTask(ctx context.Context, request *workflowservice.PollForActivityTaskRequest) (_ *workflowservice.PollForActivityTaskResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	response, err := wh.workflowHandlerThrift.PollForActivityTask(ctx, adapter.ToThriftPollForActivityTaskRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoPollForActivityTaskResponse(response), nil
}

// RecordActivityTaskHeartbeat is called by application worker while it is processing an ActivityTask.  If worker fails
// to heartbeat within 'heartbeatTimeoutSeconds' interval for the ActivityTask, then it will be marked as timedout and
// 'ActivityTaskTimedOut' event will be written to the workflow history.  Calling 'RecordActivityTaskHeartbeat' will
// fail with 'EntityNotExistsError' in such situations.  Use the 'taskToken' provided as response of
// PollForActivityTask API call for heartbeating.
func (wh *WorkflowHandlerGRPC) RecordActivityTaskHeartbeat(ctx context.Context, request *workflowservice.RecordActivityTaskHeartbeatRequest) (_ *workflowservice.RecordActivityTaskHeartbeatResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	response, err := wh.workflowHandlerThrift.RecordActivityTaskHeartbeat(ctx, adapter.ToThriftRecordActivityTaskHeartbeatRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoRecordActivityTaskHeartbeatResponse(response), nil
}

// RecordActivityTaskHeartbeatByID is called by application worker while it is processing an ActivityTask.  If worker fails
// to heartbeat within 'heartbeatTimeoutSeconds' interval for the ActivityTask, then it will be marked as timedout and
// 'ActivityTaskTimedOut' event will be written to the workflow history.  Calling 'RecordActivityTaskHeartbeatByID' will
// fail with 'EntityNotExistsError' in such situations.  Instead of using 'taskToken' like in RecordActivityTaskHeartbeat,
// use Domain, WorkflowID and ActivityID
func (wh *WorkflowHandlerGRPC) RecordActivityTaskHeartbeatByID(ctx context.Context, request *workflowservice.RecordActivityTaskHeartbeatByIDRequest) (_ *workflowservice.RecordActivityTaskHeartbeatByIDResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	response, err := wh.workflowHandlerThrift.RecordActivityTaskHeartbeatByID(ctx, adapter.ToThriftRecordActivityTaskHeartbeatByIDRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoRecordActivityTaskHeartbeatByIDResponse(response), nil
}

// RespondActivityTaskCompleted is called by application worker when it is done processing an ActivityTask.  It will
// result in a new 'ActivityTaskCompleted' event being written to the workflow history and a new DecisionTask
// created for the workflow so new decisions could be made.  Use the 'taskToken' provided as response of
// PollForActivityTask API call for completion. It fails with 'EntityNotExistsError' if the taskToken is not valid
// anymore due to activity timeout.
func (wh *WorkflowHandlerGRPC) RespondActivityTaskCompleted(ctx context.Context, request *workflowservice.RespondActivityTaskCompletedRequest) (_ *workflowservice.RespondActivityTaskCompletedResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	err := wh.workflowHandlerThrift.RespondActivityTaskCompleted(ctx, adapter.ToThriftRespondActivityTaskCompletedRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return &workflowservice.RespondActivityTaskCompletedResponse{}, nil
}

// RespondActivityTaskCompletedByID is called by application worker when it is done processing an ActivityTask.
// It will result in a new 'ActivityTaskCompleted' event being written to the workflow history and a new DecisionTask
// created for the workflow so new decisions could be made.  Similar to RespondActivityTaskCompleted but use Domain,
// WorkflowID and ActivityID instead of 'taskToken' for completion. It fails with 'EntityNotExistsError'
// if the these IDs are not valid anymore due to activity timeout.
func (wh *WorkflowHandlerGRPC) RespondActivityTaskCompletedByID(ctx context.Context, request *workflowservice.RespondActivityTaskCompletedByIDRequest) (_ *workflowservice.RespondActivityTaskCompletedByIDResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	err := wh.workflowHandlerThrift.RespondActivityTaskCompletedByID(ctx, adapter.ToThriftRespondActivityTaskCompletedByIDRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return &workflowservice.RespondActivityTaskCompletedByIDResponse{}, nil
}

// RespondActivityTaskFailed is called by application worker when it is done processing an ActivityTask.  It will
// result in a new 'ActivityTaskFailed' event being written to the workflow history and a new DecisionTask
// created for the workflow instance so new decisions could be made.  Use the 'taskToken' provided as response of
// PollForActivityTask API call for completion. It fails with 'EntityNotExistsError' if the taskToken is not valid
// anymore due to activity timeout.
func (wh *WorkflowHandlerGRPC) RespondActivityTaskFailed(ctx context.Context, request *workflowservice.RespondActivityTaskFailedRequest) (_ *workflowservice.RespondActivityTaskFailedResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	err := wh.workflowHandlerThrift.RespondActivityTaskFailed(ctx, adapter.ToThriftRespondActivityTaskFailedRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return &workflowservice.RespondActivityTaskFailedResponse{}, nil
}

// RespondActivityTaskFailedByID is called by application worker when it is done processing an ActivityTask.
// It will result in a new 'ActivityTaskFailed' event being written to the workflow history and a new DecisionTask
// created for the workflow instance so new decisions could be made.  Similar to RespondActivityTaskFailed but use
// Domain, WorkflowID and ActivityID instead of 'taskToken' for completion. It fails with 'EntityNotExistsError'
// if the these IDs are not valid anymore due to activity timeout.
func (wh *WorkflowHandlerGRPC) RespondActivityTaskFailedByID(ctx context.Context, request *workflowservice.RespondActivityTaskFailedByIDRequest) (_ *workflowservice.RespondActivityTaskFailedByIDResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	err := wh.workflowHandlerThrift.RespondActivityTaskFailedByID(ctx, adapter.ToThriftRespondActivityTaskFailedByIDRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return &workflowservice.RespondActivityTaskFailedByIDResponse{}, nil
}

// RespondActivityTaskCanceled is called by application worker when it is successfully canceled an ActivityTask.  It will
// result in a new 'ActivityTaskCanceled' event being written to the workflow history and a new DecisionTask
// created for the workflow instance so new decisions could be made.  Use the 'taskToken' provided as response of
// PollForActivityTask API call for completion. It fails with 'EntityNotExistsError' if the taskToken is not valid
// anymore due to activity timeout.
func (wh *WorkflowHandlerGRPC) RespondActivityTaskCanceled(ctx context.Context, request *workflowservice.RespondActivityTaskCanceledRequest) (_ *workflowservice.RespondActivityTaskCanceledResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	err := wh.workflowHandlerThrift.RespondActivityTaskCanceled(ctx, adapter.ToThriftRespondActivityTaskCanceledRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return &workflowservice.RespondActivityTaskCanceledResponse{}, nil
}

// RespondActivityTaskCanceledByID is called by application worker when it is successfully canceled an ActivityTask.
// It will result in a new 'ActivityTaskCanceled' event being written to the workflow history and a new DecisionTask
// created for the workflow instance so new decisions could be made.  Similar to RespondActivityTaskCanceled but use
// Domain, WorkflowID and ActivityID instead of 'taskToken' for completion. It fails with 'EntityNotExistsError'
// if the these IDs are not valid anymore due to activity timeout.
func (wh *WorkflowHandlerGRPC) RespondActivityTaskCanceledByID(ctx context.Context, request *workflowservice.RespondActivityTaskCanceledByIDRequest) (_ *workflowservice.RespondActivityTaskCanceledByIDResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	err := wh.workflowHandlerThrift.RespondActivityTaskCanceledByID(ctx, adapter.ToThriftRespondActivityTaskCanceledByIDRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return &workflowservice.RespondActivityTaskCanceledByIDResponse{}, nil
}

// RequestCancelWorkflowExecution is called by application worker when it wants to request cancellation of a workflow instance.
// It will result in a new 'WorkflowExecutionCancelRequested' event being written to the workflow history and a new DecisionTask
// created for the workflow instance so new decisions could be made. It fails with 'EntityNotExistsError' if the workflow is not valid
// anymore due to completion or doesn't exist.
func (wh *WorkflowHandlerGRPC) RequestCancelWorkflowExecution(ctx context.Context, request *workflowservice.RequestCancelWorkflowExecutionRequest) (_ *workflowservice.RequestCancelWorkflowExecutionResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	err := wh.workflowHandlerThrift.RequestCancelWorkflowExecution(ctx, adapter.ToThriftRequestCancelWorkflowExecutionRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return &workflowservice.RequestCancelWorkflowExecutionResponse{}, nil
}

// SignalWorkflowExecution is used to send a signal event to running workflow execution.  This results in
// WorkflowExecutionSignaled event recorded in the history and a decision task being created for the execution.
func (wh *WorkflowHandlerGRPC) SignalWorkflowExecution(ctx context.Context, request *workflowservice.SignalWorkflowExecutionRequest) (_ *workflowservice.SignalWorkflowExecutionResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	err := wh.workflowHandlerThrift.SignalWorkflowExecution(ctx, adapter.ToThriftSignalWorkflowExecutionRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return &workflowservice.SignalWorkflowExecutionResponse{}, nil
}

// SignalWithStartWorkflowExecution is used to ensure sending signal to a workflow.
// If the workflow is running, this results in WorkflowExecutionSignaled event being recorded in the history
// and a decision task being created for the execution.
// If the workflow is not running or not found, this results in WorkflowExecutionStarted and WorkflowExecutionSignaled
// events being recorded in history, and a decision task being created for the execution
func (wh *WorkflowHandlerGRPC) SignalWithStartWorkflowExecution(ctx context.Context, request *workflowservice.SignalWithStartWorkflowExecutionRequest) (_ *workflowservice.SignalWithStartWorkflowExecutionResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	response, err := wh.workflowHandlerThrift.SignalWithStartWorkflowExecution(ctx, adapter.ToThriftSignalWithStartWorkflowExecutionRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoSignalWithStartWorkflowExecutionResponse(response), nil
}

// ResetWorkflowExecution reset an existing workflow execution to DecisionTaskCompleted event(exclusive).
// And it will immediately terminating the current execution instance.
func (wh *WorkflowHandlerGRPC) ResetWorkflowExecution(ctx context.Context, request *workflowservice.ResetWorkflowExecutionRequest) (_ *workflowservice.ResetWorkflowExecutionResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	response, err := wh.workflowHandlerThrift.ResetWorkflowExecution(ctx, adapter.ToThriftResetWorkflowExecutionRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoResetWorkflowExecutionResponse(response), nil
}

// TerminateWorkflowExecution terminates an existing workflow execution by recording WorkflowExecutionTerminated event
// in the history and immediately terminating the execution instance.
func (wh *WorkflowHandlerGRPC) TerminateWorkflowExecution(ctx context.Context, request *workflowservice.TerminateWorkflowExecutionRequest) (_ *workflowservice.TerminateWorkflowExecutionResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	err := wh.workflowHandlerThrift.TerminateWorkflowExecution(ctx, adapter.ToThriftTerminateWorkflowExecutionRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return &workflowservice.TerminateWorkflowExecutionResponse{}, nil
}

// ListOpenWorkflowExecutions is a visibility API to list the open executions in a specific domain.
func (wh *WorkflowHandlerGRPC) ListOpenWorkflowExecutions(ctx context.Context, request *workflowservice.ListOpenWorkflowExecutionsRequest) (_ *workflowservice.ListOpenWorkflowExecutionsResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	response, err := wh.workflowHandlerThrift.ListOpenWorkflowExecutions(ctx, adapter.ToThriftListOpenWorkflowExecutionsRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoListOpenWorkflowExecutionsResponse(response), nil
}

// ListClosedWorkflowExecutions is a visibility API to list the closed executions in a specific domain.
func (wh *WorkflowHandlerGRPC) ListClosedWorkflowExecutions(ctx context.Context, request *workflowservice.ListClosedWorkflowExecutionsRequest) (_ *workflowservice.ListClosedWorkflowExecutionsResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	response, err := wh.workflowHandlerThrift.ListClosedWorkflowExecutions(ctx, adapter.ToThriftListClosedWorkflowExecutionsRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoListClosedWorkflowExecutionsResponse(response), nil
}

// ListWorkflowExecutions is a visibility API to list workflow executions in a specific domain.
func (wh *WorkflowHandlerGRPC) ListWorkflowExecutions(ctx context.Context, request *workflowservice.ListWorkflowExecutionsRequest) (_ *workflowservice.ListWorkflowExecutionsResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	response, err := wh.workflowHandlerThrift.ListWorkflowExecutions(ctx, adapter.ToThriftListWorkflowExecutionsRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoListWorkflowExecutionsResponse(response), nil
}

// ListArchivedWorkflowExecutions is a visibility API to list archived workflow executions in a specific domain.
func (wh *WorkflowHandlerGRPC) ListArchivedWorkflowExecutions(ctx context.Context, request *workflowservice.ListArchivedWorkflowExecutionsRequest) (_ *workflowservice.ListArchivedWorkflowExecutionsResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	response, err := wh.workflowHandlerThrift.ListArchivedWorkflowExecutions(ctx, adapter.ToThriftListArchivedWorkflowExecutionsRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoListArchivedWorkflowExecutionsResponse(response), nil
}

// ScanWorkflowExecutions is a visibility API to list large amount of workflow executions in a specific domain without order.
func (wh *WorkflowHandlerGRPC) ScanWorkflowExecutions(ctx context.Context, request *workflowservice.ScanWorkflowExecutionsRequest) (_ *workflowservice.ScanWorkflowExecutionsResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	response, err := wh.workflowHandlerThrift.ScanWorkflowExecutions(ctx, adapter.ToThriftScanWorkflowExecutionsRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoScanWorkflowExecutionsResponse(response), nil
}

// CountWorkflowExecutions is a visibility API to count of workflow executions in a specific domain.
func (wh *WorkflowHandlerGRPC) CountWorkflowExecutions(ctx context.Context, request *workflowservice.CountWorkflowExecutionsRequest) (_ *workflowservice.CountWorkflowExecutionsResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	response, err := wh.workflowHandlerThrift.CountWorkflowExecutions(ctx, adapter.ToThriftCountWorkflowExecutionsRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoCountWorkflowExecutionsResponse(response), nil
}

// GetSearchAttributes is a visibility API to get all legal keys that could be used in list APIs
func (wh *WorkflowHandlerGRPC) GetSearchAttributes(ctx context.Context, _ *workflowservice.GetSearchAttributesRequest) (_ *workflowservice.GetSearchAttributesResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	response, err := wh.workflowHandlerThrift.GetSearchAttributes(ctx)
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoGetSearchAttributesResponse(response), nil
}

// RespondQueryTaskCompleted is called by application worker to complete a QueryTask (which is a DecisionTask for query)
// as a result of 'PollForDecisionTask' API call. Completing a QueryTask will unblock the client call to 'QueryWorkflow'
// API and return the query result to client as a response to 'QueryWorkflow' API call.
func (wh *WorkflowHandlerGRPC) RespondQueryTaskCompleted(ctx context.Context, request *workflowservice.RespondQueryTaskCompletedRequest) (_ *workflowservice.RespondQueryTaskCompletedResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	err := wh.workflowHandlerThrift.RespondQueryTaskCompleted(ctx, adapter.ToThriftRespondQueryTaskCompletedRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return &workflowservice.RespondQueryTaskCompletedResponse{}, nil
}

// ResetStickyTaskList resets the sticky tasklist related information in mutable state of a given workflow.
// Things cleared are:
// 1. StickyTaskList
// 2. StickyScheduleToStartTimeout
// 3. ClientLibraryVersion
// 4. ClientFeatureVersion
// 5. ClientImpl
func (wh *WorkflowHandlerGRPC) ResetStickyTaskList(ctx context.Context, request *workflowservice.ResetStickyTaskListRequest) (_ *workflowservice.ResetStickyTaskListResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	_, err := wh.workflowHandlerThrift.ResetStickyTaskList(ctx, adapter.ToThriftResetStickyTaskListRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return &workflowservice.ResetStickyTaskListResponse{}, nil
}

// QueryWorkflow returns query result for a specified workflow execution
func (wh *WorkflowHandlerGRPC) QueryWorkflow(ctx context.Context, request *workflowservice.QueryWorkflowRequest) (_ *workflowservice.QueryWorkflowResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	response, err := wh.workflowHandlerThrift.QueryWorkflow(ctx, adapter.ToThriftQueryWorkflowRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoQueryWorkflowResponse(response), nil
}

// DescribeWorkflowExecution returns information about the specified workflow execution.
func (wh *WorkflowHandlerGRPC) DescribeWorkflowExecution(ctx context.Context, request *workflowservice.DescribeWorkflowExecutionRequest) (_ *workflowservice.DescribeWorkflowExecutionResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	response, err := wh.workflowHandlerThrift.DescribeWorkflowExecution(ctx, adapter.ToThriftDescribeWorkflowExecutionRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoDescribeWorkflowExecutionResponse(response), nil
}

// DescribeTaskList returns information about the target tasklist, right now this API returns the
// pollers which polled this tasklist in last few minutes.
func (wh *WorkflowHandlerGRPC) DescribeTaskList(ctx context.Context, request *workflowservice.DescribeTaskListRequest) (_ *workflowservice.DescribeTaskListResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	response, err := wh.workflowHandlerThrift.DescribeTaskList(ctx, adapter.ToThriftDescribeTaskListRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoDescribeTaskListResponse(response), nil
}

// GetReplicationMessages returns new replication tasks since the read level provided in the token.
func (wh *WorkflowHandlerGRPC) GetReplicationMessages(ctx context.Context, request *workflowservice.GetReplicationMessagesRequest) (_ *workflowservice.GetReplicationMessagesResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	response, err := wh.workflowHandlerThrift.GetReplicationMessages(ctx, adapter.ToThriftGetReplicationMessagesRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoGetReplicationMessagesResponse(response), nil
}

// GetDomainReplicationMessages returns new domain replication tasks since last retrieved task ID.
func (wh *WorkflowHandlerGRPC) GetDomainReplicationMessages(ctx context.Context, request *workflowservice.GetDomainReplicationMessagesRequest) (_ *workflowservice.GetDomainReplicationMessagesResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	response, err := wh.workflowHandlerThrift.GetDomainReplicationMessages(ctx, adapter.ToThriftGetDomainReplicationMessagesRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoGetDomainReplicationMessagesResponse(response), nil
}

// ReapplyEvents applies stale events to the current workflow and current run
func (wh *WorkflowHandlerGRPC) ReapplyEvents(ctx context.Context, request *workflowservice.ReapplyEventsRequest) (_ *workflowservice.ReapplyEventsResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	err := wh.workflowHandlerThrift.ReapplyEvents(ctx, adapter.ToThriftReapplyEventsRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return &workflowservice.ReapplyEventsResponse{}, nil
}

// GetClusterInfo ...
func (wh *WorkflowHandlerGRPC) GetClusterInfo(ctx context.Context, _ *workflowservice.GetClusterInfoRequest) (_ *workflowservice.GetClusterInfoResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	response, err := wh.workflowHandlerThrift.GetClusterInfo(ctx)
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoGetClusterInfoResponse(response), nil
}

// ListTaskListPartitions ...
func (wh *WorkflowHandlerGRPC) ListTaskListPartitions(ctx context.Context, request *workflowservice.ListTaskListPartitionsRequest) (_ *workflowservice.ListTaskListPartitionsResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	response, err := wh.workflowHandlerThrift.ListTaskListPartitions(ctx, adapter.ToThriftListTaskListPartitionsRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoListTaskListPartitionsResponse(response), nil
}
