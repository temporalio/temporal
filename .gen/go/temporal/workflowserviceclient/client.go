// The MIT License (MIT)
// 
// Copyright (c) 2019 Uber Technologies, Inc.
// 
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
// 
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

// Code generated by thriftrw-plugin-yarpc
// @generated

package workflowserviceclient

import (
	context "context"
	replicator "github.com/temporalio/temporal/.gen/go/replicator"
	shared "github.com/temporalio/temporal/.gen/go/shared"
	temporal "github.com/temporalio/temporal/.gen/go/temporal"
	wire "go.uber.org/thriftrw/wire"
	yarpc "go.uber.org/yarpc"
	transport "go.uber.org/yarpc/api/transport"
	thrift "go.uber.org/yarpc/encoding/thrift"
	reflect "reflect"
)

// Interface is a client for the WorkflowService service.
type Interface interface {
	CountWorkflowExecutions(
		ctx context.Context,
		CountRequest *shared.CountWorkflowExecutionsRequest,
		opts ...yarpc.CallOption,
	) (*shared.CountWorkflowExecutionsResponse, error)

	DeprecateDomain(
		ctx context.Context,
		DeprecateRequest *shared.DeprecateDomainRequest,
		opts ...yarpc.CallOption,
	) error

	DescribeDomain(
		ctx context.Context,
		DescribeRequest *shared.DescribeDomainRequest,
		opts ...yarpc.CallOption,
	) (*shared.DescribeDomainResponse, error)

	DescribeTaskList(
		ctx context.Context,
		Request *shared.DescribeTaskListRequest,
		opts ...yarpc.CallOption,
	) (*shared.DescribeTaskListResponse, error)

	DescribeWorkflowExecution(
		ctx context.Context,
		DescribeRequest *shared.DescribeWorkflowExecutionRequest,
		opts ...yarpc.CallOption,
	) (*shared.DescribeWorkflowExecutionResponse, error)

	GetClusterInfo(
		ctx context.Context,
		opts ...yarpc.CallOption,
	) (*shared.ClusterInfo, error)

	GetDomainReplicationMessages(
		ctx context.Context,
		Request *replicator.GetDomainReplicationMessagesRequest,
		opts ...yarpc.CallOption,
	) (*replicator.GetDomainReplicationMessagesResponse, error)

	GetReplicationMessages(
		ctx context.Context,
		Request *replicator.GetReplicationMessagesRequest,
		opts ...yarpc.CallOption,
	) (*replicator.GetReplicationMessagesResponse, error)

	GetSearchAttributes(
		ctx context.Context,
		opts ...yarpc.CallOption,
	) (*shared.GetSearchAttributesResponse, error)

	GetWorkflowExecutionHistory(
		ctx context.Context,
		GetRequest *shared.GetWorkflowExecutionHistoryRequest,
		opts ...yarpc.CallOption,
	) (*shared.GetWorkflowExecutionHistoryResponse, error)

	ListArchivedWorkflowExecutions(
		ctx context.Context,
		ListRequest *shared.ListArchivedWorkflowExecutionsRequest,
		opts ...yarpc.CallOption,
	) (*shared.ListArchivedWorkflowExecutionsResponse, error)

	ListClosedWorkflowExecutions(
		ctx context.Context,
		ListRequest *shared.ListClosedWorkflowExecutionsRequest,
		opts ...yarpc.CallOption,
	) (*shared.ListClosedWorkflowExecutionsResponse, error)

	ListDomains(
		ctx context.Context,
		ListRequest *shared.ListDomainsRequest,
		opts ...yarpc.CallOption,
	) (*shared.ListDomainsResponse, error)

	ListOpenWorkflowExecutions(
		ctx context.Context,
		ListRequest *shared.ListOpenWorkflowExecutionsRequest,
		opts ...yarpc.CallOption,
	) (*shared.ListOpenWorkflowExecutionsResponse, error)

	ListWorkflowExecutions(
		ctx context.Context,
		ListRequest *shared.ListWorkflowExecutionsRequest,
		opts ...yarpc.CallOption,
	) (*shared.ListWorkflowExecutionsResponse, error)

	PollForActivityTask(
		ctx context.Context,
		PollRequest *shared.PollForActivityTaskRequest,
		opts ...yarpc.CallOption,
	) (*shared.PollForActivityTaskResponse, error)

	PollForDecisionTask(
		ctx context.Context,
		PollRequest *shared.PollForDecisionTaskRequest,
		opts ...yarpc.CallOption,
	) (*shared.PollForDecisionTaskResponse, error)

	QueryWorkflow(
		ctx context.Context,
		QueryRequest *shared.QueryWorkflowRequest,
		opts ...yarpc.CallOption,
	) (*shared.QueryWorkflowResponse, error)

	ReapplyEvents(
		ctx context.Context,
		ReapplyEventsRequest *shared.ReapplyEventsRequest,
		opts ...yarpc.CallOption,
	) error

	RecordActivityTaskHeartbeat(
		ctx context.Context,
		HeartbeatRequest *shared.RecordActivityTaskHeartbeatRequest,
		opts ...yarpc.CallOption,
	) (*shared.RecordActivityTaskHeartbeatResponse, error)

	RecordActivityTaskHeartbeatByID(
		ctx context.Context,
		HeartbeatRequest *shared.RecordActivityTaskHeartbeatByIDRequest,
		opts ...yarpc.CallOption,
	) (*shared.RecordActivityTaskHeartbeatResponse, error)

	RegisterDomain(
		ctx context.Context,
		RegisterRequest *shared.RegisterDomainRequest,
		opts ...yarpc.CallOption,
	) error

	RequestCancelWorkflowExecution(
		ctx context.Context,
		CancelRequest *shared.RequestCancelWorkflowExecutionRequest,
		opts ...yarpc.CallOption,
	) error

	ResetStickyTaskList(
		ctx context.Context,
		ResetRequest *shared.ResetStickyTaskListRequest,
		opts ...yarpc.CallOption,
	) (*shared.ResetStickyTaskListResponse, error)

	ResetWorkflowExecution(
		ctx context.Context,
		ResetRequest *shared.ResetWorkflowExecutionRequest,
		opts ...yarpc.CallOption,
	) (*shared.ResetWorkflowExecutionResponse, error)

	RespondActivityTaskCanceled(
		ctx context.Context,
		CanceledRequest *shared.RespondActivityTaskCanceledRequest,
		opts ...yarpc.CallOption,
	) error

	RespondActivityTaskCanceledByID(
		ctx context.Context,
		CanceledRequest *shared.RespondActivityTaskCanceledByIDRequest,
		opts ...yarpc.CallOption,
	) error

	RespondActivityTaskCompleted(
		ctx context.Context,
		CompleteRequest *shared.RespondActivityTaskCompletedRequest,
		opts ...yarpc.CallOption,
	) error

	RespondActivityTaskCompletedByID(
		ctx context.Context,
		CompleteRequest *shared.RespondActivityTaskCompletedByIDRequest,
		opts ...yarpc.CallOption,
	) error

	RespondActivityTaskFailed(
		ctx context.Context,
		FailRequest *shared.RespondActivityTaskFailedRequest,
		opts ...yarpc.CallOption,
	) error

	RespondActivityTaskFailedByID(
		ctx context.Context,
		FailRequest *shared.RespondActivityTaskFailedByIDRequest,
		opts ...yarpc.CallOption,
	) error

	RespondDecisionTaskCompleted(
		ctx context.Context,
		CompleteRequest *shared.RespondDecisionTaskCompletedRequest,
		opts ...yarpc.CallOption,
	) (*shared.RespondDecisionTaskCompletedResponse, error)

	RespondDecisionTaskFailed(
		ctx context.Context,
		FailedRequest *shared.RespondDecisionTaskFailedRequest,
		opts ...yarpc.CallOption,
	) error

	RespondQueryTaskCompleted(
		ctx context.Context,
		CompleteRequest *shared.RespondQueryTaskCompletedRequest,
		opts ...yarpc.CallOption,
	) error

	ScanWorkflowExecutions(
		ctx context.Context,
		ListRequest *shared.ListWorkflowExecutionsRequest,
		opts ...yarpc.CallOption,
	) (*shared.ListWorkflowExecutionsResponse, error)

	SignalWithStartWorkflowExecution(
		ctx context.Context,
		SignalWithStartRequest *shared.SignalWithStartWorkflowExecutionRequest,
		opts ...yarpc.CallOption,
	) (*shared.StartWorkflowExecutionResponse, error)

	SignalWorkflowExecution(
		ctx context.Context,
		SignalRequest *shared.SignalWorkflowExecutionRequest,
		opts ...yarpc.CallOption,
	) error

	StartWorkflowExecution(
		ctx context.Context,
		StartRequest *shared.StartWorkflowExecutionRequest,
		opts ...yarpc.CallOption,
	) (*shared.StartWorkflowExecutionResponse, error)

	TerminateWorkflowExecution(
		ctx context.Context,
		TerminateRequest *shared.TerminateWorkflowExecutionRequest,
		opts ...yarpc.CallOption,
	) error

	UpdateDomain(
		ctx context.Context,
		UpdateRequest *shared.UpdateDomainRequest,
		opts ...yarpc.CallOption,
	) (*shared.UpdateDomainResponse, error)
}

// New builds a new client for the WorkflowService service.
//
// 	client := workflowserviceclient.New(dispatcher.ClientConfig("workflowservice"))
func New(c transport.ClientConfig, opts ...thrift.ClientOption) Interface {
	return client{
		c: thrift.New(thrift.Config{
			Service:      "WorkflowService",
			ClientConfig: c,
		}, opts...),
	}
}

func init() {
	yarpc.RegisterClientBuilder(
		func(c transport.ClientConfig, f reflect.StructField) Interface {
			return New(c, thrift.ClientBuilderOptions(c, f)...)
		},
	)
}

type client struct {
	c thrift.Client
}

func (c client) CountWorkflowExecutions(
	ctx context.Context,
	_CountRequest *shared.CountWorkflowExecutionsRequest,
	opts ...yarpc.CallOption,
) (success *shared.CountWorkflowExecutionsResponse, err error) {

	args := temporal.WorkflowService_CountWorkflowExecutions_Helper.Args(_CountRequest)

	var body wire.Value
	body, err = c.c.Call(ctx, args, opts...)
	if err != nil {
		return
	}

	var result temporal.WorkflowService_CountWorkflowExecutions_Result
	if err = result.FromWire(body); err != nil {
		return
	}

	success, err = temporal.WorkflowService_CountWorkflowExecutions_Helper.UnwrapResponse(&result)
	return
}

func (c client) DeprecateDomain(
	ctx context.Context,
	_DeprecateRequest *shared.DeprecateDomainRequest,
	opts ...yarpc.CallOption,
) (err error) {

	args := temporal.WorkflowService_DeprecateDomain_Helper.Args(_DeprecateRequest)

	var body wire.Value
	body, err = c.c.Call(ctx, args, opts...)
	if err != nil {
		return
	}

	var result temporal.WorkflowService_DeprecateDomain_Result
	if err = result.FromWire(body); err != nil {
		return
	}

	err = temporal.WorkflowService_DeprecateDomain_Helper.UnwrapResponse(&result)
	return
}

func (c client) DescribeDomain(
	ctx context.Context,
	_DescribeRequest *shared.DescribeDomainRequest,
	opts ...yarpc.CallOption,
) (success *shared.DescribeDomainResponse, err error) {

	args := temporal.WorkflowService_DescribeDomain_Helper.Args(_DescribeRequest)

	var body wire.Value
	body, err = c.c.Call(ctx, args, opts...)
	if err != nil {
		return
	}

	var result temporal.WorkflowService_DescribeDomain_Result
	if err = result.FromWire(body); err != nil {
		return
	}

	success, err = temporal.WorkflowService_DescribeDomain_Helper.UnwrapResponse(&result)
	return
}

func (c client) DescribeTaskList(
	ctx context.Context,
	_Request *shared.DescribeTaskListRequest,
	opts ...yarpc.CallOption,
) (success *shared.DescribeTaskListResponse, err error) {

	args := temporal.WorkflowService_DescribeTaskList_Helper.Args(_Request)

	var body wire.Value
	body, err = c.c.Call(ctx, args, opts...)
	if err != nil {
		return
	}

	var result temporal.WorkflowService_DescribeTaskList_Result
	if err = result.FromWire(body); err != nil {
		return
	}

	success, err = temporal.WorkflowService_DescribeTaskList_Helper.UnwrapResponse(&result)
	return
}

func (c client) DescribeWorkflowExecution(
	ctx context.Context,
	_DescribeRequest *shared.DescribeWorkflowExecutionRequest,
	opts ...yarpc.CallOption,
) (success *shared.DescribeWorkflowExecutionResponse, err error) {

	args := temporal.WorkflowService_DescribeWorkflowExecution_Helper.Args(_DescribeRequest)

	var body wire.Value
	body, err = c.c.Call(ctx, args, opts...)
	if err != nil {
		return
	}

	var result temporal.WorkflowService_DescribeWorkflowExecution_Result
	if err = result.FromWire(body); err != nil {
		return
	}

	success, err = temporal.WorkflowService_DescribeWorkflowExecution_Helper.UnwrapResponse(&result)
	return
}

func (c client) GetClusterInfo(
	ctx context.Context,
	opts ...yarpc.CallOption,
) (success *shared.ClusterInfo, err error) {

	args := temporal.WorkflowService_GetClusterInfo_Helper.Args()

	var body wire.Value
	body, err = c.c.Call(ctx, args, opts...)
	if err != nil {
		return
	}

	var result temporal.WorkflowService_GetClusterInfo_Result
	if err = result.FromWire(body); err != nil {
		return
	}

	success, err = temporal.WorkflowService_GetClusterInfo_Helper.UnwrapResponse(&result)
	return
}

func (c client) GetDomainReplicationMessages(
	ctx context.Context,
	_Request *replicator.GetDomainReplicationMessagesRequest,
	opts ...yarpc.CallOption,
) (success *replicator.GetDomainReplicationMessagesResponse, err error) {

	args := temporal.WorkflowService_GetDomainReplicationMessages_Helper.Args(_Request)

	var body wire.Value
	body, err = c.c.Call(ctx, args, opts...)
	if err != nil {
		return
	}

	var result temporal.WorkflowService_GetDomainReplicationMessages_Result
	if err = result.FromWire(body); err != nil {
		return
	}

	success, err = temporal.WorkflowService_GetDomainReplicationMessages_Helper.UnwrapResponse(&result)
	return
}

func (c client) GetReplicationMessages(
	ctx context.Context,
	_Request *replicator.GetReplicationMessagesRequest,
	opts ...yarpc.CallOption,
) (success *replicator.GetReplicationMessagesResponse, err error) {

	args := temporal.WorkflowService_GetReplicationMessages_Helper.Args(_Request)

	var body wire.Value
	body, err = c.c.Call(ctx, args, opts...)
	if err != nil {
		return
	}

	var result temporal.WorkflowService_GetReplicationMessages_Result
	if err = result.FromWire(body); err != nil {
		return
	}

	success, err = temporal.WorkflowService_GetReplicationMessages_Helper.UnwrapResponse(&result)
	return
}

func (c client) GetSearchAttributes(
	ctx context.Context,
	opts ...yarpc.CallOption,
) (success *shared.GetSearchAttributesResponse, err error) {

	args := temporal.WorkflowService_GetSearchAttributes_Helper.Args()

	var body wire.Value
	body, err = c.c.Call(ctx, args, opts...)
	if err != nil {
		return
	}

	var result temporal.WorkflowService_GetSearchAttributes_Result
	if err = result.FromWire(body); err != nil {
		return
	}

	success, err = temporal.WorkflowService_GetSearchAttributes_Helper.UnwrapResponse(&result)
	return
}

func (c client) GetWorkflowExecutionHistory(
	ctx context.Context,
	_GetRequest *shared.GetWorkflowExecutionHistoryRequest,
	opts ...yarpc.CallOption,
) (success *shared.GetWorkflowExecutionHistoryResponse, err error) {

	args := temporal.WorkflowService_GetWorkflowExecutionHistory_Helper.Args(_GetRequest)

	var body wire.Value
	body, err = c.c.Call(ctx, args, opts...)
	if err != nil {
		return
	}

	var result temporal.WorkflowService_GetWorkflowExecutionHistory_Result
	if err = result.FromWire(body); err != nil {
		return
	}

	success, err = temporal.WorkflowService_GetWorkflowExecutionHistory_Helper.UnwrapResponse(&result)
	return
}

func (c client) ListArchivedWorkflowExecutions(
	ctx context.Context,
	_ListRequest *shared.ListArchivedWorkflowExecutionsRequest,
	opts ...yarpc.CallOption,
) (success *shared.ListArchivedWorkflowExecutionsResponse, err error) {

	args := temporal.WorkflowService_ListArchivedWorkflowExecutions_Helper.Args(_ListRequest)

	var body wire.Value
	body, err = c.c.Call(ctx, args, opts...)
	if err != nil {
		return
	}

	var result temporal.WorkflowService_ListArchivedWorkflowExecutions_Result
	if err = result.FromWire(body); err != nil {
		return
	}

	success, err = temporal.WorkflowService_ListArchivedWorkflowExecutions_Helper.UnwrapResponse(&result)
	return
}

func (c client) ListClosedWorkflowExecutions(
	ctx context.Context,
	_ListRequest *shared.ListClosedWorkflowExecutionsRequest,
	opts ...yarpc.CallOption,
) (success *shared.ListClosedWorkflowExecutionsResponse, err error) {

	args := temporal.WorkflowService_ListClosedWorkflowExecutions_Helper.Args(_ListRequest)

	var body wire.Value
	body, err = c.c.Call(ctx, args, opts...)
	if err != nil {
		return
	}

	var result temporal.WorkflowService_ListClosedWorkflowExecutions_Result
	if err = result.FromWire(body); err != nil {
		return
	}

	success, err = temporal.WorkflowService_ListClosedWorkflowExecutions_Helper.UnwrapResponse(&result)
	return
}

func (c client) ListDomains(
	ctx context.Context,
	_ListRequest *shared.ListDomainsRequest,
	opts ...yarpc.CallOption,
) (success *shared.ListDomainsResponse, err error) {

	args := temporal.WorkflowService_ListDomains_Helper.Args(_ListRequest)

	var body wire.Value
	body, err = c.c.Call(ctx, args, opts...)
	if err != nil {
		return
	}

	var result temporal.WorkflowService_ListDomains_Result
	if err = result.FromWire(body); err != nil {
		return
	}

	success, err = temporal.WorkflowService_ListDomains_Helper.UnwrapResponse(&result)
	return
}

func (c client) ListOpenWorkflowExecutions(
	ctx context.Context,
	_ListRequest *shared.ListOpenWorkflowExecutionsRequest,
	opts ...yarpc.CallOption,
) (success *shared.ListOpenWorkflowExecutionsResponse, err error) {

	args := temporal.WorkflowService_ListOpenWorkflowExecutions_Helper.Args(_ListRequest)

	var body wire.Value
	body, err = c.c.Call(ctx, args, opts...)
	if err != nil {
		return
	}

	var result temporal.WorkflowService_ListOpenWorkflowExecutions_Result
	if err = result.FromWire(body); err != nil {
		return
	}

	success, err = temporal.WorkflowService_ListOpenWorkflowExecutions_Helper.UnwrapResponse(&result)
	return
}

func (c client) ListWorkflowExecutions(
	ctx context.Context,
	_ListRequest *shared.ListWorkflowExecutionsRequest,
	opts ...yarpc.CallOption,
) (success *shared.ListWorkflowExecutionsResponse, err error) {

	args := temporal.WorkflowService_ListWorkflowExecutions_Helper.Args(_ListRequest)

	var body wire.Value
	body, err = c.c.Call(ctx, args, opts...)
	if err != nil {
		return
	}

	var result temporal.WorkflowService_ListWorkflowExecutions_Result
	if err = result.FromWire(body); err != nil {
		return
	}

	success, err = temporal.WorkflowService_ListWorkflowExecutions_Helper.UnwrapResponse(&result)
	return
}

func (c client) PollForActivityTask(
	ctx context.Context,
	_PollRequest *shared.PollForActivityTaskRequest,
	opts ...yarpc.CallOption,
) (success *shared.PollForActivityTaskResponse, err error) {

	args := temporal.WorkflowService_PollForActivityTask_Helper.Args(_PollRequest)

	var body wire.Value
	body, err = c.c.Call(ctx, args, opts...)
	if err != nil {
		return
	}

	var result temporal.WorkflowService_PollForActivityTask_Result
	if err = result.FromWire(body); err != nil {
		return
	}

	success, err = temporal.WorkflowService_PollForActivityTask_Helper.UnwrapResponse(&result)
	return
}

func (c client) PollForDecisionTask(
	ctx context.Context,
	_PollRequest *shared.PollForDecisionTaskRequest,
	opts ...yarpc.CallOption,
) (success *shared.PollForDecisionTaskResponse, err error) {

	args := temporal.WorkflowService_PollForDecisionTask_Helper.Args(_PollRequest)

	var body wire.Value
	body, err = c.c.Call(ctx, args, opts...)
	if err != nil {
		return
	}

	var result temporal.WorkflowService_PollForDecisionTask_Result
	if err = result.FromWire(body); err != nil {
		return
	}

	success, err = temporal.WorkflowService_PollForDecisionTask_Helper.UnwrapResponse(&result)
	return
}

func (c client) QueryWorkflow(
	ctx context.Context,
	_QueryRequest *shared.QueryWorkflowRequest,
	opts ...yarpc.CallOption,
) (success *shared.QueryWorkflowResponse, err error) {

	args := temporal.WorkflowService_QueryWorkflow_Helper.Args(_QueryRequest)

	var body wire.Value
	body, err = c.c.Call(ctx, args, opts...)
	if err != nil {
		return
	}

	var result temporal.WorkflowService_QueryWorkflow_Result
	if err = result.FromWire(body); err != nil {
		return
	}

	success, err = temporal.WorkflowService_QueryWorkflow_Helper.UnwrapResponse(&result)
	return
}

func (c client) ReapplyEvents(
	ctx context.Context,
	_ReapplyEventsRequest *shared.ReapplyEventsRequest,
	opts ...yarpc.CallOption,
) (err error) {

	args := temporal.WorkflowService_ReapplyEvents_Helper.Args(_ReapplyEventsRequest)

	var body wire.Value
	body, err = c.c.Call(ctx, args, opts...)
	if err != nil {
		return
	}

	var result temporal.WorkflowService_ReapplyEvents_Result
	if err = result.FromWire(body); err != nil {
		return
	}

	err = temporal.WorkflowService_ReapplyEvents_Helper.UnwrapResponse(&result)
	return
}

func (c client) RecordActivityTaskHeartbeat(
	ctx context.Context,
	_HeartbeatRequest *shared.RecordActivityTaskHeartbeatRequest,
	opts ...yarpc.CallOption,
) (success *shared.RecordActivityTaskHeartbeatResponse, err error) {

	args := temporal.WorkflowService_RecordActivityTaskHeartbeat_Helper.Args(_HeartbeatRequest)

	var body wire.Value
	body, err = c.c.Call(ctx, args, opts...)
	if err != nil {
		return
	}

	var result temporal.WorkflowService_RecordActivityTaskHeartbeat_Result
	if err = result.FromWire(body); err != nil {
		return
	}

	success, err = temporal.WorkflowService_RecordActivityTaskHeartbeat_Helper.UnwrapResponse(&result)
	return
}

func (c client) RecordActivityTaskHeartbeatByID(
	ctx context.Context,
	_HeartbeatRequest *shared.RecordActivityTaskHeartbeatByIDRequest,
	opts ...yarpc.CallOption,
) (success *shared.RecordActivityTaskHeartbeatResponse, err error) {

	args := temporal.WorkflowService_RecordActivityTaskHeartbeatByID_Helper.Args(_HeartbeatRequest)

	var body wire.Value
	body, err = c.c.Call(ctx, args, opts...)
	if err != nil {
		return
	}

	var result temporal.WorkflowService_RecordActivityTaskHeartbeatByID_Result
	if err = result.FromWire(body); err != nil {
		return
	}

	success, err = temporal.WorkflowService_RecordActivityTaskHeartbeatByID_Helper.UnwrapResponse(&result)
	return
}

func (c client) RegisterDomain(
	ctx context.Context,
	_RegisterRequest *shared.RegisterDomainRequest,
	opts ...yarpc.CallOption,
) (err error) {

	args := temporal.WorkflowService_RegisterDomain_Helper.Args(_RegisterRequest)

	var body wire.Value
	body, err = c.c.Call(ctx, args, opts...)
	if err != nil {
		return
	}

	var result temporal.WorkflowService_RegisterDomain_Result
	if err = result.FromWire(body); err != nil {
		return
	}

	err = temporal.WorkflowService_RegisterDomain_Helper.UnwrapResponse(&result)
	return
}

func (c client) RequestCancelWorkflowExecution(
	ctx context.Context,
	_CancelRequest *shared.RequestCancelWorkflowExecutionRequest,
	opts ...yarpc.CallOption,
) (err error) {

	args := temporal.WorkflowService_RequestCancelWorkflowExecution_Helper.Args(_CancelRequest)

	var body wire.Value
	body, err = c.c.Call(ctx, args, opts...)
	if err != nil {
		return
	}

	var result temporal.WorkflowService_RequestCancelWorkflowExecution_Result
	if err = result.FromWire(body); err != nil {
		return
	}

	err = temporal.WorkflowService_RequestCancelWorkflowExecution_Helper.UnwrapResponse(&result)
	return
}

func (c client) ResetStickyTaskList(
	ctx context.Context,
	_ResetRequest *shared.ResetStickyTaskListRequest,
	opts ...yarpc.CallOption,
) (success *shared.ResetStickyTaskListResponse, err error) {

	args := temporal.WorkflowService_ResetStickyTaskList_Helper.Args(_ResetRequest)

	var body wire.Value
	body, err = c.c.Call(ctx, args, opts...)
	if err != nil {
		return
	}

	var result temporal.WorkflowService_ResetStickyTaskList_Result
	if err = result.FromWire(body); err != nil {
		return
	}

	success, err = temporal.WorkflowService_ResetStickyTaskList_Helper.UnwrapResponse(&result)
	return
}

func (c client) ResetWorkflowExecution(
	ctx context.Context,
	_ResetRequest *shared.ResetWorkflowExecutionRequest,
	opts ...yarpc.CallOption,
) (success *shared.ResetWorkflowExecutionResponse, err error) {

	args := temporal.WorkflowService_ResetWorkflowExecution_Helper.Args(_ResetRequest)

	var body wire.Value
	body, err = c.c.Call(ctx, args, opts...)
	if err != nil {
		return
	}

	var result temporal.WorkflowService_ResetWorkflowExecution_Result
	if err = result.FromWire(body); err != nil {
		return
	}

	success, err = temporal.WorkflowService_ResetWorkflowExecution_Helper.UnwrapResponse(&result)
	return
}

func (c client) RespondActivityTaskCanceled(
	ctx context.Context,
	_CanceledRequest *shared.RespondActivityTaskCanceledRequest,
	opts ...yarpc.CallOption,
) (err error) {

	args := temporal.WorkflowService_RespondActivityTaskCanceled_Helper.Args(_CanceledRequest)

	var body wire.Value
	body, err = c.c.Call(ctx, args, opts...)
	if err != nil {
		return
	}

	var result temporal.WorkflowService_RespondActivityTaskCanceled_Result
	if err = result.FromWire(body); err != nil {
		return
	}

	err = temporal.WorkflowService_RespondActivityTaskCanceled_Helper.UnwrapResponse(&result)
	return
}

func (c client) RespondActivityTaskCanceledByID(
	ctx context.Context,
	_CanceledRequest *shared.RespondActivityTaskCanceledByIDRequest,
	opts ...yarpc.CallOption,
) (err error) {

	args := temporal.WorkflowService_RespondActivityTaskCanceledByID_Helper.Args(_CanceledRequest)

	var body wire.Value
	body, err = c.c.Call(ctx, args, opts...)
	if err != nil {
		return
	}

	var result temporal.WorkflowService_RespondActivityTaskCanceledByID_Result
	if err = result.FromWire(body); err != nil {
		return
	}

	err = temporal.WorkflowService_RespondActivityTaskCanceledByID_Helper.UnwrapResponse(&result)
	return
}

func (c client) RespondActivityTaskCompleted(
	ctx context.Context,
	_CompleteRequest *shared.RespondActivityTaskCompletedRequest,
	opts ...yarpc.CallOption,
) (err error) {

	args := temporal.WorkflowService_RespondActivityTaskCompleted_Helper.Args(_CompleteRequest)

	var body wire.Value
	body, err = c.c.Call(ctx, args, opts...)
	if err != nil {
		return
	}

	var result temporal.WorkflowService_RespondActivityTaskCompleted_Result
	if err = result.FromWire(body); err != nil {
		return
	}

	err = temporal.WorkflowService_RespondActivityTaskCompleted_Helper.UnwrapResponse(&result)
	return
}

func (c client) RespondActivityTaskCompletedByID(
	ctx context.Context,
	_CompleteRequest *shared.RespondActivityTaskCompletedByIDRequest,
	opts ...yarpc.CallOption,
) (err error) {

	args := temporal.WorkflowService_RespondActivityTaskCompletedByID_Helper.Args(_CompleteRequest)

	var body wire.Value
	body, err = c.c.Call(ctx, args, opts...)
	if err != nil {
		return
	}

	var result temporal.WorkflowService_RespondActivityTaskCompletedByID_Result
	if err = result.FromWire(body); err != nil {
		return
	}

	err = temporal.WorkflowService_RespondActivityTaskCompletedByID_Helper.UnwrapResponse(&result)
	return
}

func (c client) RespondActivityTaskFailed(
	ctx context.Context,
	_FailRequest *shared.RespondActivityTaskFailedRequest,
	opts ...yarpc.CallOption,
) (err error) {

	args := temporal.WorkflowService_RespondActivityTaskFailed_Helper.Args(_FailRequest)

	var body wire.Value
	body, err = c.c.Call(ctx, args, opts...)
	if err != nil {
		return
	}

	var result temporal.WorkflowService_RespondActivityTaskFailed_Result
	if err = result.FromWire(body); err != nil {
		return
	}

	err = temporal.WorkflowService_RespondActivityTaskFailed_Helper.UnwrapResponse(&result)
	return
}

func (c client) RespondActivityTaskFailedByID(
	ctx context.Context,
	_FailRequest *shared.RespondActivityTaskFailedByIDRequest,
	opts ...yarpc.CallOption,
) (err error) {

	args := temporal.WorkflowService_RespondActivityTaskFailedByID_Helper.Args(_FailRequest)

	var body wire.Value
	body, err = c.c.Call(ctx, args, opts...)
	if err != nil {
		return
	}

	var result temporal.WorkflowService_RespondActivityTaskFailedByID_Result
	if err = result.FromWire(body); err != nil {
		return
	}

	err = temporal.WorkflowService_RespondActivityTaskFailedByID_Helper.UnwrapResponse(&result)
	return
}

func (c client) RespondDecisionTaskCompleted(
	ctx context.Context,
	_CompleteRequest *shared.RespondDecisionTaskCompletedRequest,
	opts ...yarpc.CallOption,
) (success *shared.RespondDecisionTaskCompletedResponse, err error) {

	args := temporal.WorkflowService_RespondDecisionTaskCompleted_Helper.Args(_CompleteRequest)

	var body wire.Value
	body, err = c.c.Call(ctx, args, opts...)
	if err != nil {
		return
	}

	var result temporal.WorkflowService_RespondDecisionTaskCompleted_Result
	if err = result.FromWire(body); err != nil {
		return
	}

	success, err = temporal.WorkflowService_RespondDecisionTaskCompleted_Helper.UnwrapResponse(&result)
	return
}

func (c client) RespondDecisionTaskFailed(
	ctx context.Context,
	_FailedRequest *shared.RespondDecisionTaskFailedRequest,
	opts ...yarpc.CallOption,
) (err error) {

	args := temporal.WorkflowService_RespondDecisionTaskFailed_Helper.Args(_FailedRequest)

	var body wire.Value
	body, err = c.c.Call(ctx, args, opts...)
	if err != nil {
		return
	}

	var result temporal.WorkflowService_RespondDecisionTaskFailed_Result
	if err = result.FromWire(body); err != nil {
		return
	}

	err = temporal.WorkflowService_RespondDecisionTaskFailed_Helper.UnwrapResponse(&result)
	return
}

func (c client) RespondQueryTaskCompleted(
	ctx context.Context,
	_CompleteRequest *shared.RespondQueryTaskCompletedRequest,
	opts ...yarpc.CallOption,
) (err error) {

	args := temporal.WorkflowService_RespondQueryTaskCompleted_Helper.Args(_CompleteRequest)

	var body wire.Value
	body, err = c.c.Call(ctx, args, opts...)
	if err != nil {
		return
	}

	var result temporal.WorkflowService_RespondQueryTaskCompleted_Result
	if err = result.FromWire(body); err != nil {
		return
	}

	err = temporal.WorkflowService_RespondQueryTaskCompleted_Helper.UnwrapResponse(&result)
	return
}

func (c client) ScanWorkflowExecutions(
	ctx context.Context,
	_ListRequest *shared.ListWorkflowExecutionsRequest,
	opts ...yarpc.CallOption,
) (success *shared.ListWorkflowExecutionsResponse, err error) {

	args := temporal.WorkflowService_ScanWorkflowExecutions_Helper.Args(_ListRequest)

	var body wire.Value
	body, err = c.c.Call(ctx, args, opts...)
	if err != nil {
		return
	}

	var result temporal.WorkflowService_ScanWorkflowExecutions_Result
	if err = result.FromWire(body); err != nil {
		return
	}

	success, err = temporal.WorkflowService_ScanWorkflowExecutions_Helper.UnwrapResponse(&result)
	return
}

func (c client) SignalWithStartWorkflowExecution(
	ctx context.Context,
	_SignalWithStartRequest *shared.SignalWithStartWorkflowExecutionRequest,
	opts ...yarpc.CallOption,
) (success *shared.StartWorkflowExecutionResponse, err error) {

	args := temporal.WorkflowService_SignalWithStartWorkflowExecution_Helper.Args(_SignalWithStartRequest)

	var body wire.Value
	body, err = c.c.Call(ctx, args, opts...)
	if err != nil {
		return
	}

	var result temporal.WorkflowService_SignalWithStartWorkflowExecution_Result
	if err = result.FromWire(body); err != nil {
		return
	}

	success, err = temporal.WorkflowService_SignalWithStartWorkflowExecution_Helper.UnwrapResponse(&result)
	return
}

func (c client) SignalWorkflowExecution(
	ctx context.Context,
	_SignalRequest *shared.SignalWorkflowExecutionRequest,
	opts ...yarpc.CallOption,
) (err error) {

	args := temporal.WorkflowService_SignalWorkflowExecution_Helper.Args(_SignalRequest)

	var body wire.Value
	body, err = c.c.Call(ctx, args, opts...)
	if err != nil {
		return
	}

	var result temporal.WorkflowService_SignalWorkflowExecution_Result
	if err = result.FromWire(body); err != nil {
		return
	}

	err = temporal.WorkflowService_SignalWorkflowExecution_Helper.UnwrapResponse(&result)
	return
}

func (c client) StartWorkflowExecution(
	ctx context.Context,
	_StartRequest *shared.StartWorkflowExecutionRequest,
	opts ...yarpc.CallOption,
) (success *shared.StartWorkflowExecutionResponse, err error) {

	args := temporal.WorkflowService_StartWorkflowExecution_Helper.Args(_StartRequest)

	var body wire.Value
	body, err = c.c.Call(ctx, args, opts...)
	if err != nil {
		return
	}

	var result temporal.WorkflowService_StartWorkflowExecution_Result
	if err = result.FromWire(body); err != nil {
		return
	}

	success, err = temporal.WorkflowService_StartWorkflowExecution_Helper.UnwrapResponse(&result)
	return
}

func (c client) TerminateWorkflowExecution(
	ctx context.Context,
	_TerminateRequest *shared.TerminateWorkflowExecutionRequest,
	opts ...yarpc.CallOption,
) (err error) {

	args := temporal.WorkflowService_TerminateWorkflowExecution_Helper.Args(_TerminateRequest)

	var body wire.Value
	body, err = c.c.Call(ctx, args, opts...)
	if err != nil {
		return
	}

	var result temporal.WorkflowService_TerminateWorkflowExecution_Result
	if err = result.FromWire(body); err != nil {
		return
	}

	err = temporal.WorkflowService_TerminateWorkflowExecution_Helper.UnwrapResponse(&result)
	return
}

func (c client) UpdateDomain(
	ctx context.Context,
	_UpdateRequest *shared.UpdateDomainRequest,
	opts ...yarpc.CallOption,
) (success *shared.UpdateDomainResponse, err error) {

	args := temporal.WorkflowService_UpdateDomain_Helper.Args(_UpdateRequest)

	var body wire.Value
	body, err = c.c.Call(ctx, args, opts...)
	if err != nil {
		return
	}

	var result temporal.WorkflowService_UpdateDomain_Result
	if err = result.FromWire(body); err != nil {
		return
	}

	success, err = temporal.WorkflowService_UpdateDomain_Helper.UnwrapResponse(&result)
	return
}
