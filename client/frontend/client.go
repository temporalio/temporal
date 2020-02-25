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
	"go.uber.org/yarpc"

	"github.com/uber/cadence/.gen/go/cadence/workflowserviceclient"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
)

var _ Client = (*clientImpl)(nil)

const (
	// DefaultTimeout is the default timeout used to make calls
	DefaultTimeout = 10 * time.Second
	// DefaultLongPollTimeout is the long poll default timeout used to make calls
	DefaultLongPollTimeout = time.Minute * 3
)

type clientImpl struct {
	timeout         time.Duration
	longPollTimeout time.Duration
	clients         common.ClientCache
}

// NewClient creates a new frontend service TChannel client
func NewClient(
	timeout time.Duration,
	longPollTimeout time.Duration,
	clients common.ClientCache,
) Client {
	return &clientImpl{
		timeout:         timeout,
		longPollTimeout: longPollTimeout,
		clients:         clients,
	}
}

func (c *clientImpl) DeprecateDomain(
	ctx context.Context,
	request *shared.DeprecateDomainRequest,
	opts ...yarpc.CallOption,
) error {

	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.DeprecateDomain(ctx, request, opts...)
}

func (c *clientImpl) DescribeDomain(
	ctx context.Context,
	request *shared.DescribeDomainRequest,
	opts ...yarpc.CallOption,
) (*shared.DescribeDomainResponse, error) {

	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.DescribeDomain(ctx, request, opts...)
}

func (c *clientImpl) DescribeTaskList(
	ctx context.Context,
	request *shared.DescribeTaskListRequest,
	opts ...yarpc.CallOption,
) (*shared.DescribeTaskListResponse, error) {

	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.DescribeTaskList(ctx, request, opts...)
}

func (c *clientImpl) DescribeWorkflowExecution(
	ctx context.Context,
	request *shared.DescribeWorkflowExecutionRequest,
	opts ...yarpc.CallOption,
) (*shared.DescribeWorkflowExecutionResponse, error) {

	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.DescribeWorkflowExecution(ctx, request, opts...)
}

func (c *clientImpl) GetWorkflowExecutionHistory(
	ctx context.Context,
	request *shared.GetWorkflowExecutionHistoryRequest,
	opts ...yarpc.CallOption,
) (*shared.GetWorkflowExecutionHistoryResponse, error) {

	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.GetWorkflowExecutionHistory(ctx, request, opts...)
}

func (c *clientImpl) GetWorkflowExecutionRawHistory(
	ctx context.Context,
	request *shared.GetWorkflowExecutionRawHistoryRequest,
	opts ...yarpc.CallOption,
) (*shared.GetWorkflowExecutionRawHistoryResponse, error) {

	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.GetWorkflowExecutionRawHistory(ctx, request, opts...)
}

func (c *clientImpl) PollForWorkflowExecutionRawHistory(
	ctx context.Context,
	request *shared.PollForWorkflowExecutionRawHistoryRequest,
	opts ...yarpc.CallOption,
) (*shared.PollForWorkflowExecutionRawHistoryResponse, error) {

	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.PollForWorkflowExecutionRawHistory(ctx, request, opts...)
}

func (c *clientImpl) ListArchivedWorkflowExecutions(
	ctx context.Context,
	request *shared.ListArchivedWorkflowExecutionsRequest,
	opts ...yarpc.CallOption,
) (*shared.ListArchivedWorkflowExecutionsResponse, error) {

	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createLongPollContext(ctx)
	defer cancel()
	return client.ListArchivedWorkflowExecutions(ctx, request, opts...)
}

func (c *clientImpl) ListClosedWorkflowExecutions(
	ctx context.Context,
	request *shared.ListClosedWorkflowExecutionsRequest,
	opts ...yarpc.CallOption,
) (*shared.ListClosedWorkflowExecutionsResponse, error) {

	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.ListClosedWorkflowExecutions(ctx, request, opts...)
}

func (c *clientImpl) ListDomains(
	ctx context.Context,
	request *shared.ListDomainsRequest,
	opts ...yarpc.CallOption,
) (*shared.ListDomainsResponse, error) {

	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.ListDomains(ctx, request, opts...)
}

func (c *clientImpl) ListOpenWorkflowExecutions(
	ctx context.Context,
	request *shared.ListOpenWorkflowExecutionsRequest,
	opts ...yarpc.CallOption,
) (*shared.ListOpenWorkflowExecutionsResponse, error) {

	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.ListOpenWorkflowExecutions(ctx, request, opts...)
}

func (c *clientImpl) ListWorkflowExecutions(
	ctx context.Context,
	request *shared.ListWorkflowExecutionsRequest,
	opts ...yarpc.CallOption,
) (*shared.ListWorkflowExecutionsResponse, error) {

	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.ListWorkflowExecutions(ctx, request, opts...)
}

func (c *clientImpl) ScanWorkflowExecutions(
	ctx context.Context,
	request *shared.ListWorkflowExecutionsRequest,
	opts ...yarpc.CallOption,
) (*shared.ListWorkflowExecutionsResponse, error) {

	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.ScanWorkflowExecutions(ctx, request, opts...)
}

func (c *clientImpl) CountWorkflowExecutions(
	ctx context.Context,
	request *shared.CountWorkflowExecutionsRequest,
	opts ...yarpc.CallOption,
) (*shared.CountWorkflowExecutionsResponse, error) {

	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.CountWorkflowExecutions(ctx, request, opts...)
}

func (c *clientImpl) GetSearchAttributes(
	ctx context.Context,
	opts ...yarpc.CallOption,
) (*shared.GetSearchAttributesResponse, error) {

	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.GetSearchAttributes(ctx, opts...)
}

func (c *clientImpl) PollForActivityTask(
	ctx context.Context,
	request *shared.PollForActivityTaskRequest,
	opts ...yarpc.CallOption,
) (*shared.PollForActivityTaskResponse, error) {

	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createLongPollContext(ctx)
	defer cancel()
	return client.PollForActivityTask(ctx, request, opts...)
}

func (c *clientImpl) PollForDecisionTask(
	ctx context.Context,
	request *shared.PollForDecisionTaskRequest,
	opts ...yarpc.CallOption,
) (*shared.PollForDecisionTaskResponse, error) {

	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createLongPollContext(ctx)
	defer cancel()
	return client.PollForDecisionTask(ctx, request, opts...)
}

func (c *clientImpl) QueryWorkflow(
	ctx context.Context,
	request *shared.QueryWorkflowRequest,
	opts ...yarpc.CallOption,
) (*shared.QueryWorkflowResponse, error) {

	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.QueryWorkflow(ctx, request, opts...)
}

func (c *clientImpl) RecordActivityTaskHeartbeat(
	ctx context.Context,
	request *shared.RecordActivityTaskHeartbeatRequest,
	opts ...yarpc.CallOption,
) (*shared.RecordActivityTaskHeartbeatResponse, error) {

	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.RecordActivityTaskHeartbeat(ctx, request, opts...)
}

func (c *clientImpl) RecordActivityTaskHeartbeatByID(
	ctx context.Context,
	request *shared.RecordActivityTaskHeartbeatByIDRequest,
	opts ...yarpc.CallOption,
) (*shared.RecordActivityTaskHeartbeatResponse, error) {

	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.RecordActivityTaskHeartbeatByID(ctx, request, opts...)
}

func (c *clientImpl) RegisterDomain(
	ctx context.Context,
	request *shared.RegisterDomainRequest,
	opts ...yarpc.CallOption,
) error {

	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.RegisterDomain(ctx, request, opts...)
}

func (c *clientImpl) RequestCancelWorkflowExecution(
	ctx context.Context,
	request *shared.RequestCancelWorkflowExecutionRequest,
	opts ...yarpc.CallOption,
) error {

	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.RequestCancelWorkflowExecution(ctx, request, opts...)
}

func (c *clientImpl) ResetStickyTaskList(
	ctx context.Context,
	request *shared.ResetStickyTaskListRequest,
	opts ...yarpc.CallOption,
) (*shared.ResetStickyTaskListResponse, error) {

	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.ResetStickyTaskList(ctx, request, opts...)
}

func (c *clientImpl) ResetWorkflowExecution(
	ctx context.Context,
	request *shared.ResetWorkflowExecutionRequest,
	opts ...yarpc.CallOption,
) (*shared.ResetWorkflowExecutionResponse, error) {

	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.ResetWorkflowExecution(ctx, request, opts...)
}

func (c *clientImpl) RespondActivityTaskCanceled(
	ctx context.Context,
	request *shared.RespondActivityTaskCanceledRequest,
	opts ...yarpc.CallOption,
) error {

	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.RespondActivityTaskCanceled(ctx, request, opts...)
}

func (c *clientImpl) RespondActivityTaskCanceledByID(
	ctx context.Context,
	request *shared.RespondActivityTaskCanceledByIDRequest,
	opts ...yarpc.CallOption,
) error {

	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.RespondActivityTaskCanceledByID(ctx, request, opts...)
}

func (c *clientImpl) RespondActivityTaskCompleted(
	ctx context.Context,
	request *shared.RespondActivityTaskCompletedRequest,
	opts ...yarpc.CallOption,
) error {

	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.RespondActivityTaskCompleted(ctx, request, opts...)
}

func (c *clientImpl) RespondActivityTaskCompletedByID(
	ctx context.Context,
	request *shared.RespondActivityTaskCompletedByIDRequest,
	opts ...yarpc.CallOption,
) error {

	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.RespondActivityTaskCompletedByID(ctx, request, opts...)
}

func (c *clientImpl) RespondActivityTaskFailed(
	ctx context.Context,
	request *shared.RespondActivityTaskFailedRequest,
	opts ...yarpc.CallOption,
) error {

	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.RespondActivityTaskFailed(ctx, request, opts...)
}

func (c *clientImpl) RespondActivityTaskFailedByID(
	ctx context.Context,
	request *shared.RespondActivityTaskFailedByIDRequest,
	opts ...yarpc.CallOption,
) error {

	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.RespondActivityTaskFailedByID(ctx, request, opts...)
}

func (c *clientImpl) RespondDecisionTaskCompleted(
	ctx context.Context,
	request *shared.RespondDecisionTaskCompletedRequest,
	opts ...yarpc.CallOption,
) (*shared.RespondDecisionTaskCompletedResponse, error) {

	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.RespondDecisionTaskCompleted(ctx, request, opts...)
}

func (c *clientImpl) RespondDecisionTaskFailed(
	ctx context.Context,
	request *shared.RespondDecisionTaskFailedRequest,
	opts ...yarpc.CallOption,
) error {

	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.RespondDecisionTaskFailed(ctx, request, opts...)
}

func (c *clientImpl) RespondQueryTaskCompleted(
	ctx context.Context,
	request *shared.RespondQueryTaskCompletedRequest,
	opts ...yarpc.CallOption,
) error {

	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.RespondQueryTaskCompleted(ctx, request, opts...)
}

func (c *clientImpl) SignalWithStartWorkflowExecution(
	ctx context.Context,
	request *shared.SignalWithStartWorkflowExecutionRequest,
	opts ...yarpc.CallOption,
) (*shared.StartWorkflowExecutionResponse, error) {

	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.SignalWithStartWorkflowExecution(ctx, request, opts...)
}

func (c *clientImpl) SignalWorkflowExecution(
	ctx context.Context,
	request *shared.SignalWorkflowExecutionRequest,
	opts ...yarpc.CallOption,
) error {

	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.SignalWorkflowExecution(ctx, request, opts...)
}

func (c *clientImpl) StartWorkflowExecution(
	ctx context.Context,
	request *shared.StartWorkflowExecutionRequest,
	opts ...yarpc.CallOption,
) (*shared.StartWorkflowExecutionResponse, error) {

	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.StartWorkflowExecution(ctx, request, opts...)
}

func (c *clientImpl) TerminateWorkflowExecution(
	ctx context.Context,
	request *shared.TerminateWorkflowExecutionRequest,
	opts ...yarpc.CallOption,
) error {

	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.TerminateWorkflowExecution(ctx, request, opts...)
}

func (c *clientImpl) UpdateDomain(
	ctx context.Context,
	request *shared.UpdateDomainRequest,
	opts ...yarpc.CallOption,
) (*shared.UpdateDomainResponse, error) {

	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.UpdateDomain(ctx, request, opts...)
}

func (c *clientImpl) createContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		return context.WithTimeout(context.Background(), c.timeout)
	}
	return context.WithTimeout(parent, c.timeout)
}

func (c *clientImpl) createLongPollContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		return context.WithTimeout(context.Background(), c.longPollTimeout)
	}
	return context.WithTimeout(parent, c.longPollTimeout)
}

func (c *clientImpl) getRandomClient() (workflowserviceclient.Interface, error) {
	// generate a random shard key to do load balancing
	key := uuid.New()
	client, err := c.clients.GetClientForKey(key)
	if err != nil {
		return nil, err
	}

	return client.(workflowserviceclient.Interface), nil
}

func (c *clientImpl) GetClusterInfo(
	ctx context.Context,
	opts ...yarpc.CallOption,
) (*shared.ClusterInfo, error) {

	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.GetClusterInfo(ctx, opts...)
}

func (c *clientImpl) ListTaskListPartitions(
	ctx context.Context,
	request *shared.ListTaskListPartitionsRequest,
	opts ...yarpc.CallOption,
) (*shared.ListTaskListPartitionsResponse, error) {

	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()

	return client.ListTaskListPartitions(ctx, request, opts...)
}
