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

	"github.com/temporalio/temporal/proto/workflow_service"

	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/service/frontend/adapter"
)

var _ workflow_service.WorkflowServiceYARPCServer = (*WorkflowHandlerGRPC)(nil)

type (
	// WorkflowHandlerGRPC - gRPC handler interface for workflow workflow_service
	WorkflowHandlerGRPC struct {
		workflowHandlerThrift *WorkflowHandler
	}
)

// NewWorkflowHandlerGRPC creates a thrift handler for the cadence workflow_service
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
	wh.workflowHandlerThrift.Service.GetDispatcher().Register(workflow_service.BuildWorkflowServiceYARPCProcedures(wh))
}

// RegisterDomain creates a new domain which can be used as a container for all resources.  Domain is a top level
// entity within Cadence, used as a container for all resources like workflow executions, tasklists, etc.  Domain
// acts as a sandbox and provides isolation for all resources within the domain.  All resources belongs to exactly one
// domain.
func (wh *WorkflowHandlerGRPC) RegisterDomain(ctx context.Context, registerRequest *workflow_service.RegisterDomainRequest) (_ *workflow_service.RegisterDomainResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	err := wh.workflowHandlerThrift.RegisterDomain(ctx, adapter.ToThriftRegisterDomainRequest(registerRequest))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return &workflow_service.RegisterDomainResponse{}, nil
}

func (wh *WorkflowHandlerGRPC) DescribeDomain(context.Context, *workflow_service.DescribeDomainRequest) (*workflow_service.DescribeDomainResponse, error) {
	panic("implement me")
}

func (wh *WorkflowHandlerGRPC) ListDomains(context.Context, *workflow_service.ListDomainsRequest) (*workflow_service.ListDomainsResponse, error) {
	panic("implement me")
}

func (wh *WorkflowHandlerGRPC) UpdateDomain(context.Context, *workflow_service.UpdateDomainRequest) (*workflow_service.UpdateDomainResponse, error) {
	panic("implement me")
}

func (wh *WorkflowHandlerGRPC) DeprecateDomain(context.Context, *workflow_service.DeprecateDomainRequest) (*workflow_service.DeprecateDomainResponse, error) {
	panic("implement me")
}

func (wh *WorkflowHandlerGRPC) StartWorkflowExecution(context.Context, *workflow_service.StartWorkflowExecutionRequest) (*workflow_service.StartWorkflowExecutionResponse, error) {
	panic("implement me")
}

func (wh *WorkflowHandlerGRPC) GetWorkflowExecutionHistory(context.Context, *workflow_service.GetWorkflowExecutionHistoryRequest) (*workflow_service.GetWorkflowExecutionHistoryResponse, error) {
	panic("implement me")
}

func (wh *WorkflowHandlerGRPC) PollForDecisionTask(context.Context, *workflow_service.PollForDecisionTaskRequest) (*workflow_service.PollForDecisionTaskResponse, error) {
	panic("implement me")
}

func (wh *WorkflowHandlerGRPC) RespondDecisionTaskCompleted(context.Context, *workflow_service.RespondDecisionTaskCompletedRequest) (*workflow_service.RespondDecisionTaskCompletedResponse, error) {
	panic("implement me")
}

func (wh *WorkflowHandlerGRPC) RespondDecisionTaskFailed(context.Context, *workflow_service.RespondDecisionTaskFailedRequest) (*workflow_service.RespondDecisionTaskFailedResponse, error) {
	panic("implement me")
}

func (wh *WorkflowHandlerGRPC) PollForActivityTask(context.Context, *workflow_service.PollForActivityTaskRequest) (*workflow_service.PollForActivityTaskResponse, error) {
	panic("implement me")
}

func (wh *WorkflowHandlerGRPC) RecordActivityTaskHeartbeat(context.Context, *workflow_service.RecordActivityTaskHeartbeatRequest) (*workflow_service.RecordActivityTaskHeartbeatResponse, error) {
	panic("implement me")
}

func (wh *WorkflowHandlerGRPC) RecordActivityTaskHeartbeatByID(context.Context, *workflow_service.RecordActivityTaskHeartbeatByIDRequest) (*workflow_service.RecordActivityTaskHeartbeatResponse, error) {
	panic("implement me")
}

func (wh *WorkflowHandlerGRPC) RespondActivityTaskCompleted(context.Context, *workflow_service.RespondActivityTaskCompletedRequest) (*workflow_service.RespondActivityTaskCompletedResponse, error) {
	panic("implement me")
}

func (wh *WorkflowHandlerGRPC) RespondActivityTaskCompletedByID(context.Context, *workflow_service.RespondActivityTaskCompletedByIDRequest) (*workflow_service.RespondActivityTaskCompletedByIDResponse, error) {
	panic("implement me")
}

func (wh *WorkflowHandlerGRPC) RespondActivityTaskFailed(context.Context, *workflow_service.RespondActivityTaskFailedRequest) (*workflow_service.RespondActivityTaskFailedResponse, error) {
	panic("implement me")
}

func (wh *WorkflowHandlerGRPC) RespondActivityTaskFailedByID(context.Context, *workflow_service.RespondActivityTaskFailedByIDRequest) (*workflow_service.RespondActivityTaskFailedByIDResponse, error) {
	panic("implement me")
}

func (wh *WorkflowHandlerGRPC) RespondActivityTaskCanceled(context.Context, *workflow_service.RespondActivityTaskCanceledRequest) (*workflow_service.RespondActivityTaskCanceledResponse, error) {
	panic("implement me")
}

func (wh *WorkflowHandlerGRPC) RespondActivityTaskCanceledByID(context.Context, *workflow_service.RespondActivityTaskCanceledByIDRequest) (*workflow_service.RespondActivityTaskCanceledByIDResponse, error) {
	panic("implement me")
}

func (wh *WorkflowHandlerGRPC) RequestCancelWorkflowExecution(context.Context, *workflow_service.RequestCancelWorkflowExecutionRequest) (*workflow_service.RequestCancelWorkflowExecutionResponse, error) {
	panic("implement me")
}

func (wh *WorkflowHandlerGRPC) SignalWorkflowExecution(context.Context, *workflow_service.SignalWorkflowExecutionRequest) (*workflow_service.SignalWorkflowExecutionResponse, error) {
	panic("implement me")
}

func (wh *WorkflowHandlerGRPC) SignalWithStartWorkflowExecution(context.Context, *workflow_service.SignalWithStartWorkflowExecutionRequest) (*workflow_service.StartWorkflowExecutionResponse, error) {
	panic("implement me")
}

func (wh *WorkflowHandlerGRPC) ResetWorkflowExecution(context.Context, *workflow_service.ResetWorkflowExecutionRequest) (*workflow_service.ResetWorkflowExecutionResponse, error) {
	panic("implement me")
}

func (wh *WorkflowHandlerGRPC) TerminateWorkflowExecution(context.Context, *workflow_service.TerminateWorkflowExecutionRequest) (*workflow_service.TerminateWorkflowExecutionResponse, error) {
	panic("implement me")
}

func (wh *WorkflowHandlerGRPC) ListOpenWorkflowExecutions(context.Context, *workflow_service.ListOpenWorkflowExecutionsRequest) (*workflow_service.ListOpenWorkflowExecutionsResponse, error) {
	panic("implement me")
}

func (wh *WorkflowHandlerGRPC) ListClosedWorkflowExecutions(context.Context, *workflow_service.ListClosedWorkflowExecutionsRequest) (*workflow_service.ListClosedWorkflowExecutionsResponse, error) {
	panic("implement me")
}

func (wh *WorkflowHandlerGRPC) ListWorkflowExecutions(context.Context, *workflow_service.ListWorkflowExecutionsRequest) (*workflow_service.ListWorkflowExecutionsResponse, error) {
	panic("implement me")
}

func (wh *WorkflowHandlerGRPC) ListArchivedWorkflowExecutions(context.Context, *workflow_service.ListArchivedWorkflowExecutionsRequest) (*workflow_service.ListArchivedWorkflowExecutionsResponse, error) {
	panic("implement me")
}

func (wh *WorkflowHandlerGRPC) ScanWorkflowExecutions(context.Context, *workflow_service.ListWorkflowExecutionsRequest) (*workflow_service.ListWorkflowExecutionsResponse, error) {
	panic("implement me")
}

func (wh *WorkflowHandlerGRPC) CountWorkflowExecutions(context.Context, *workflow_service.CountWorkflowExecutionsRequest) (*workflow_service.CountWorkflowExecutionsResponse, error) {
	panic("implement me")
}

func (wh *WorkflowHandlerGRPC) GetSearchAttributes(context.Context, *workflow_service.GetSearchAttributesRequest) (*workflow_service.GetSearchAttributesResponse, error) {
	panic("implement me")
}

func (wh *WorkflowHandlerGRPC) RespondQueryTaskCompleted(context.Context, *workflow_service.RespondQueryTaskCompletedRequest) (*workflow_service.RespondQueryTaskCompletedResponse, error) {
	panic("implement me")
}

func (wh *WorkflowHandlerGRPC) ResetStickyTaskList(context.Context, *workflow_service.ResetStickyTaskListRequest) (*workflow_service.ResetStickyTaskListResponse, error) {
	panic("implement me")
}

func (wh *WorkflowHandlerGRPC) QueryWorkflow(context.Context, *workflow_service.QueryWorkflowRequest) (*workflow_service.QueryWorkflowResponse, error) {
	panic("implement me")
}

func (wh *WorkflowHandlerGRPC) DescribeWorkflowExecution(context.Context, *workflow_service.DescribeWorkflowExecutionRequest) (*workflow_service.DescribeWorkflowExecutionResponse, error) {
	panic("implement me")
}

func (wh *WorkflowHandlerGRPC) DescribeTaskList(context.Context, *workflow_service.DescribeTaskListRequest) (*workflow_service.DescribeTaskListResponse, error) {
	panic("implement me")
}

func (wh *WorkflowHandlerGRPC) GetReplicationMessages(context.Context, *workflow_service.GetReplicationMessagesRequest) (*workflow_service.GetReplicationMessagesResponse, error) {
	panic("implement me")
}

func (wh *WorkflowHandlerGRPC) GetDomainReplicationMessages(context.Context, *workflow_service.GetDomainReplicationMessagesRequest) (*workflow_service.GetDomainReplicationMessagesResponse, error) {
	panic("implement me")
}

func (wh *WorkflowHandlerGRPC) ReapplyEvents(context.Context, *workflow_service.ReapplyEventsRequest) (*workflow_service.ReapplyEventsResponse, error) {
	panic("implement me")
}
