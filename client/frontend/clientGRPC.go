// Copyright (c) 2017 Uber Technologies, Inc.
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
	"time"

	"github.com/pborman/uuid"
	"go.temporal.io/temporal-proto/workflowservice"
	"go.uber.org/yarpc"

	"github.com/temporalio/temporal/common"
)

var _ ClientGRPC = (*clientGRPCImpl)(nil)

type clientGRPCImpl struct {
	timeout         time.Duration
	longPollTimeout time.Duration
	clients         common.ClientCache
}

// NewClientGRPC creates a new frontend service gRPC client
func NewClientGRPC(
	timeout time.Duration,
	longPollTimeout time.Duration,
	clients common.ClientCache,
) ClientGRPC {
	return &clientGRPCImpl{
		timeout:         timeout,
		longPollTimeout: longPollTimeout,
		clients:         clients,
	}
}

func (c *clientGRPCImpl) DeprecateDomain(
	ctx context.Context,
	request *workflowservice.DeprecateDomainRequest,
	opts ...yarpc.CallOption,
) (*workflowservice.DeprecateDomainResponse, error) {
	client, err := c.getRandomClient()
	opts = common.AggregateYarpcOptions(ctx, opts...)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.DeprecateDomain(ctx, request, opts...)
}

func (c *clientGRPCImpl) DescribeDomain(
	ctx context.Context,
	request *workflowservice.DescribeDomainRequest,
	opts ...yarpc.CallOption,
) (*workflowservice.DescribeDomainResponse, error) {
	client, err := c.getRandomClient()
	opts = common.AggregateYarpcOptions(ctx, opts...)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.DescribeDomain(ctx, request, opts...)
}

func (c *clientGRPCImpl) DescribeTaskList(
	ctx context.Context,
	request *workflowservice.DescribeTaskListRequest,
	opts ...yarpc.CallOption,
) (*workflowservice.DescribeTaskListResponse, error) {
	client, err := c.getRandomClient()
	opts = common.AggregateYarpcOptions(ctx, opts...)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.DescribeTaskList(ctx, request, opts...)
}

func (c *clientGRPCImpl) DescribeWorkflowExecution(
	ctx context.Context,
	request *workflowservice.DescribeWorkflowExecutionRequest,
	opts ...yarpc.CallOption,
) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	client, err := c.getRandomClient()
	opts = common.AggregateYarpcOptions(ctx, opts...)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.DescribeWorkflowExecution(ctx, request, opts...)
}

func (c *clientGRPCImpl) GetWorkflowExecutionHistory(
	ctx context.Context,
	request *workflowservice.GetWorkflowExecutionHistoryRequest,
	opts ...yarpc.CallOption,
) (*workflowservice.GetWorkflowExecutionHistoryResponse, error) {
	client, err := c.getRandomClient()
	opts = common.AggregateYarpcOptions(ctx, opts...)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.GetWorkflowExecutionHistory(ctx, request, opts...)
}

func (c *clientGRPCImpl) ListArchivedWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.ListArchivedWorkflowExecutionsRequest,
	opts ...yarpc.CallOption,
) (*workflowservice.ListArchivedWorkflowExecutionsResponse, error) {
	client, err := c.getRandomClient()
	opts = common.AggregateYarpcOptions(ctx, opts...)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createLongPollContext(ctx)
	defer cancel()
	return client.ListArchivedWorkflowExecutions(ctx, request, opts...)
}

func (c *clientGRPCImpl) ListClosedWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.ListClosedWorkflowExecutionsRequest,
	opts ...yarpc.CallOption,
) (*workflowservice.ListClosedWorkflowExecutionsResponse, error) {
	client, err := c.getRandomClient()
	opts = common.AggregateYarpcOptions(ctx, opts...)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.ListClosedWorkflowExecutions(ctx, request, opts...)
}

func (c *clientGRPCImpl) ListDomains(
	ctx context.Context,
	request *workflowservice.ListDomainsRequest,
	opts ...yarpc.CallOption,
) (*workflowservice.ListDomainsResponse, error) {
	client, err := c.getRandomClient()
	opts = common.AggregateYarpcOptions(ctx, opts...)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.ListDomains(ctx, request, opts...)
}

func (c *clientGRPCImpl) ListOpenWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.ListOpenWorkflowExecutionsRequest,
	opts ...yarpc.CallOption,
) (*workflowservice.ListOpenWorkflowExecutionsResponse, error) {
	client, err := c.getRandomClient()
	opts = common.AggregateYarpcOptions(ctx, opts...)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.ListOpenWorkflowExecutions(ctx, request, opts...)
}

func (c *clientGRPCImpl) ListWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.ListWorkflowExecutionsRequest,
	opts ...yarpc.CallOption,
) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	client, err := c.getRandomClient()
	opts = common.AggregateYarpcOptions(ctx, opts...)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.ListWorkflowExecutions(ctx, request, opts...)
}

func (c *clientGRPCImpl) ScanWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.ScanWorkflowExecutionsRequest,
	opts ...yarpc.CallOption,
) (*workflowservice.ScanWorkflowExecutionsResponse, error) {
	client, err := c.getRandomClient()
	opts = common.AggregateYarpcOptions(ctx, opts...)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.ScanWorkflowExecutions(ctx, request, opts...)
}

func (c *clientGRPCImpl) CountWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.CountWorkflowExecutionsRequest,
	opts ...yarpc.CallOption,
) (*workflowservice.CountWorkflowExecutionsResponse, error) {
	client, err := c.getRandomClient()
	opts = common.AggregateYarpcOptions(ctx, opts...)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.CountWorkflowExecutions(ctx, request, opts...)
}

func (c *clientGRPCImpl) GetSearchAttributes(
	ctx context.Context,
	request *workflowservice.GetSearchAttributesRequest,
	opts ...yarpc.CallOption,
) (*workflowservice.GetSearchAttributesResponse, error) {
	client, err := c.getRandomClient()
	opts = common.AggregateYarpcOptions(ctx, opts...)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.GetSearchAttributes(ctx, request, opts...)
}

func (c *clientGRPCImpl) PollForActivityTask(
	ctx context.Context,
	request *workflowservice.PollForActivityTaskRequest,
	opts ...yarpc.CallOption,
) (*workflowservice.PollForActivityTaskResponse, error) {
	client, err := c.getRandomClient()
	opts = common.AggregateYarpcOptions(ctx, opts...)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createLongPollContext(ctx)
	defer cancel()
	return client.PollForActivityTask(ctx, request, opts...)
}

func (c *clientGRPCImpl) PollForDecisionTask(
	ctx context.Context,
	request *workflowservice.PollForDecisionTaskRequest,
	opts ...yarpc.CallOption,
) (*workflowservice.PollForDecisionTaskResponse, error) {
	client, err := c.getRandomClient()
	opts = common.AggregateYarpcOptions(ctx, opts...)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createLongPollContext(ctx)
	defer cancel()
	return client.PollForDecisionTask(ctx, request, opts...)
}

func (c *clientGRPCImpl) QueryWorkflow(
	ctx context.Context,
	request *workflowservice.QueryWorkflowRequest,
	opts ...yarpc.CallOption,
) (*workflowservice.QueryWorkflowResponse, error) {
	client, err := c.getRandomClient()
	opts = common.AggregateYarpcOptions(ctx, opts...)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.QueryWorkflow(ctx, request, opts...)
}

func (c *clientGRPCImpl) RecordActivityTaskHeartbeat(
	ctx context.Context,
	request *workflowservice.RecordActivityTaskHeartbeatRequest,
	opts ...yarpc.CallOption,
) (*workflowservice.RecordActivityTaskHeartbeatResponse, error) {
	client, err := c.getRandomClient()
	opts = common.AggregateYarpcOptions(ctx, opts...)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.RecordActivityTaskHeartbeat(ctx, request, opts...)
}

func (c *clientGRPCImpl) RecordActivityTaskHeartbeatByID(
	ctx context.Context,
	request *workflowservice.RecordActivityTaskHeartbeatByIDRequest,
	opts ...yarpc.CallOption,
) (*workflowservice.RecordActivityTaskHeartbeatByIDResponse, error) {
	client, err := c.getRandomClient()
	opts = common.AggregateYarpcOptions(ctx, opts...)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.RecordActivityTaskHeartbeatByID(ctx, request, opts...)
}

func (c *clientGRPCImpl) RegisterDomain(
	ctx context.Context,
	request *workflowservice.RegisterDomainRequest,
	opts ...yarpc.CallOption,
) (*workflowservice.RegisterDomainResponse, error) {

	client, err := c.getRandomClient()
	opts = common.AggregateYarpcOptions(ctx, opts...)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.RegisterDomain(ctx, request, opts...)
}

func (c *clientGRPCImpl) RequestCancelWorkflowExecution(
	ctx context.Context,
	request *workflowservice.RequestCancelWorkflowExecutionRequest,
	opts ...yarpc.CallOption,
) (*workflowservice.RequestCancelWorkflowExecutionResponse, error) {
	client, err := c.getRandomClient()
	opts = common.AggregateYarpcOptions(ctx, opts...)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.RequestCancelWorkflowExecution(ctx, request, opts...)
}

func (c *clientGRPCImpl) ResetStickyTaskList(
	ctx context.Context,
	request *workflowservice.ResetStickyTaskListRequest,
	opts ...yarpc.CallOption,
) (*workflowservice.ResetStickyTaskListResponse, error) {
	client, err := c.getRandomClient()
	opts = common.AggregateYarpcOptions(ctx, opts...)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.ResetStickyTaskList(ctx, request, opts...)
}

func (c *clientGRPCImpl) ResetWorkflowExecution(
	ctx context.Context,
	request *workflowservice.ResetWorkflowExecutionRequest,
	opts ...yarpc.CallOption,
) (*workflowservice.ResetWorkflowExecutionResponse, error) {
	client, err := c.getRandomClient()
	opts = common.AggregateYarpcOptions(ctx, opts...)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.ResetWorkflowExecution(ctx, request, opts...)
}

func (c *clientGRPCImpl) RespondActivityTaskCanceled(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskCanceledRequest,
	opts ...yarpc.CallOption,
) (*workflowservice.RespondActivityTaskCanceledResponse, error) {
	client, err := c.getRandomClient()
	opts = common.AggregateYarpcOptions(ctx, opts...)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.RespondActivityTaskCanceled(ctx, request, opts...)
}

func (c *clientGRPCImpl) RespondActivityTaskCanceledByID(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskCanceledByIDRequest,
	opts ...yarpc.CallOption,
) (*workflowservice.RespondActivityTaskCanceledByIDResponse, error) {
	client, err := c.getRandomClient()
	opts = common.AggregateYarpcOptions(ctx, opts...)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.RespondActivityTaskCanceledByID(ctx, request, opts...)
}

func (c *clientGRPCImpl) RespondActivityTaskCompleted(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskCompletedRequest,
	opts ...yarpc.CallOption,
) (*workflowservice.RespondActivityTaskCompletedResponse, error) {
	client, err := c.getRandomClient()
	opts = common.AggregateYarpcOptions(ctx, opts...)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.RespondActivityTaskCompleted(ctx, request, opts...)
}

func (c *clientGRPCImpl) RespondActivityTaskCompletedByID(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskCompletedByIDRequest,
	opts ...yarpc.CallOption,
) (*workflowservice.RespondActivityTaskCompletedByIDResponse, error) {
	client, err := c.getRandomClient()
	opts = common.AggregateYarpcOptions(ctx, opts...)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.RespondActivityTaskCompletedByID(ctx, request, opts...)
}

func (c *clientGRPCImpl) RespondActivityTaskFailed(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskFailedRequest,
	opts ...yarpc.CallOption,
) (*workflowservice.RespondActivityTaskFailedResponse, error) {
	client, err := c.getRandomClient()
	opts = common.AggregateYarpcOptions(ctx, opts...)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.RespondActivityTaskFailed(ctx, request, opts...)
}

func (c *clientGRPCImpl) RespondActivityTaskFailedByID(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskFailedByIDRequest,
	opts ...yarpc.CallOption,
) (*workflowservice.RespondActivityTaskFailedByIDResponse, error) {
	client, err := c.getRandomClient()
	opts = common.AggregateYarpcOptions(ctx, opts...)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.RespondActivityTaskFailedByID(ctx, request, opts...)
}

func (c *clientGRPCImpl) RespondDecisionTaskCompleted(
	ctx context.Context,
	request *workflowservice.RespondDecisionTaskCompletedRequest,
	opts ...yarpc.CallOption,
) (*workflowservice.RespondDecisionTaskCompletedResponse, error) {
	client, err := c.getRandomClient()
	opts = common.AggregateYarpcOptions(ctx, opts...)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.RespondDecisionTaskCompleted(ctx, request, opts...)
}

func (c *clientGRPCImpl) RespondDecisionTaskFailed(
	ctx context.Context,
	request *workflowservice.RespondDecisionTaskFailedRequest,
	opts ...yarpc.CallOption,
) (*workflowservice.RespondDecisionTaskFailedResponse, error) {
	client, err := c.getRandomClient()
	opts = common.AggregateYarpcOptions(ctx, opts...)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.RespondDecisionTaskFailed(ctx, request, opts...)
}

func (c *clientGRPCImpl) RespondQueryTaskCompleted(
	ctx context.Context,
	request *workflowservice.RespondQueryTaskCompletedRequest,
	opts ...yarpc.CallOption,
) (*workflowservice.RespondQueryTaskCompletedResponse, error) {
	client, err := c.getRandomClient()
	opts = common.AggregateYarpcOptions(ctx, opts...)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.RespondQueryTaskCompleted(ctx, request, opts...)
}

func (c *clientGRPCImpl) SignalWithStartWorkflowExecution(
	ctx context.Context,
	request *workflowservice.SignalWithStartWorkflowExecutionRequest,
	opts ...yarpc.CallOption,
) (*workflowservice.SignalWithStartWorkflowExecutionResponse, error) {
	client, err := c.getRandomClient()
	opts = common.AggregateYarpcOptions(ctx, opts...)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.SignalWithStartWorkflowExecution(ctx, request, opts...)
}

func (c *clientGRPCImpl) SignalWorkflowExecution(
	ctx context.Context,
	request *workflowservice.SignalWorkflowExecutionRequest,
	opts ...yarpc.CallOption,
) (*workflowservice.SignalWorkflowExecutionResponse, error) {
	client, err := c.getRandomClient()
	opts = common.AggregateYarpcOptions(ctx, opts...)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.SignalWorkflowExecution(ctx, request, opts...)
}

func (c *clientGRPCImpl) StartWorkflowExecution(
	ctx context.Context,
	request *workflowservice.StartWorkflowExecutionRequest,
	opts ...yarpc.CallOption,
) (*workflowservice.StartWorkflowExecutionResponse, error) {
	client, err := c.getRandomClient()
	opts = common.AggregateYarpcOptions(ctx, opts...)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.StartWorkflowExecution(ctx, request, opts...)
}

func (c *clientGRPCImpl) TerminateWorkflowExecution(
	ctx context.Context,
	request *workflowservice.TerminateWorkflowExecutionRequest,
	opts ...yarpc.CallOption,
) (*workflowservice.TerminateWorkflowExecutionResponse, error) {
	client, err := c.getRandomClient()
	opts = common.AggregateYarpcOptions(ctx, opts...)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.TerminateWorkflowExecution(ctx, request, opts...)
}

func (c *clientGRPCImpl) UpdateDomain(
	ctx context.Context,
	request *workflowservice.UpdateDomainRequest,
	opts ...yarpc.CallOption,
) (*workflowservice.UpdateDomainResponse, error) {
	client, err := c.getRandomClient()
	opts = common.AggregateYarpcOptions(ctx, opts...)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.UpdateDomain(ctx, request, opts...)
}

func (c *clientGRPCImpl) createContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		return context.WithTimeout(context.Background(), c.timeout)
	}
	return context.WithTimeout(parent, c.timeout)
}

func (c *clientGRPCImpl) createLongPollContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		return context.WithTimeout(context.Background(), c.longPollTimeout)
	}
	return context.WithTimeout(parent, c.longPollTimeout)
}

func (c *clientGRPCImpl) getRandomClient() (workflowservice.WorkflowServiceYARPCClient, error) {
	// generate a random shard key to do load balancing
	key := uuid.New()
	client, err := c.clients.GetClientForKey(key)
	if err != nil {
		return nil, err
	}

	return client.(workflowservice.WorkflowServiceYARPCClient), nil
}

func (c *clientGRPCImpl) GetReplicationMessages(
	ctx context.Context,
	request *workflowservice.GetReplicationMessagesRequest,
	opts ...yarpc.CallOption,
) (*workflowservice.GetReplicationMessagesResponse, error) {
	client, err := c.getRandomClient()
	opts = common.AggregateYarpcOptions(ctx, opts...)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.GetReplicationMessages(ctx, request, opts...)
}

func (c *clientGRPCImpl) GetDomainReplicationMessages(
	ctx context.Context,
	request *workflowservice.GetDomainReplicationMessagesRequest,
	opts ...yarpc.CallOption,
) (*workflowservice.GetDomainReplicationMessagesResponse, error) {
	client, err := c.getRandomClient()
	opts = common.AggregateYarpcOptions(ctx, opts...)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.GetDomainReplicationMessages(ctx, request, opts...)
}

func (c *clientGRPCImpl) ReapplyEvents(
	ctx context.Context,
	request *workflowservice.ReapplyEventsRequest,
	opts ...yarpc.CallOption,
) (*workflowservice.ReapplyEventsResponse, error) {
	client, err := c.getRandomClient()
	opts = common.AggregateYarpcOptions(ctx, opts...)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.ReapplyEvents(ctx, request, opts...)
}

func (c *clientGRPCImpl) GetClusterInfo(
	ctx context.Context,
	request *workflowservice.GetClusterInfoRequest,
	opts ...yarpc.CallOption,
) (*workflowservice.GetClusterInfoResponse, error) {
	client, err := c.getRandomClient()
	opts = common.AggregateYarpcOptions(ctx, opts...)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.GetClusterInfo(ctx, request, opts...)
}

func (c *clientGRPCImpl) ListTaskListPartitions(
	ctx context.Context,
	request *workflowservice.ListTaskListPartitionsRequest,
	opts ...yarpc.CallOption,
) (*workflowservice.ListTaskListPartitionsResponse, error) {
	client, err := c.getRandomClient()
	opts = common.AggregateYarpcOptions(ctx, opts...)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()

	return client.ListTaskListPartitions(ctx, request, opts...)
}
