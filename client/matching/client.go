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

package matching

import (
	"context"
	"time"

	"go.uber.org/yarpc"

	m "github.com/uber/cadence/.gen/go/matching"
	"github.com/uber/cadence/.gen/go/matching/matchingserviceclient"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/persistence"
)

var _ Client = (*clientImpl)(nil)

const (
	// DefaultTimeout is the default timeout used to make calls
	DefaultTimeout = time.Minute
	// DefaultLongPollTimeout is the long poll default timeout used to make calls
	DefaultLongPollTimeout = time.Minute * 2
)

type clientImpl struct {
	timeout         time.Duration
	longPollTimeout time.Duration
	clients         common.ClientCache
	loadBalancer    LoadBalancer
}

// NewClient creates a new history service TChannel client
func NewClient(
	timeout time.Duration,
	longPollTimeout time.Duration,
	clients common.ClientCache,
	lb LoadBalancer,
) Client {
	return &clientImpl{
		timeout:         timeout,
		longPollTimeout: longPollTimeout,
		clients:         clients,
		loadBalancer:    lb,
	}
}

func (c *clientImpl) AddActivityTask(
	ctx context.Context,
	request *m.AddActivityTaskRequest,
	opts ...yarpc.CallOption) error {
	opts = common.AggregateYarpcOptions(ctx, opts...)
	partition := c.loadBalancer.PickWritePartition(
		request.GetDomainUUID(),
		*request.GetTaskList(),
		persistence.TaskListTypeActivity,
		request.GetForwardedFrom(),
	)
	request.TaskList.Name = &partition
	client, err := c.getClientForTasklist(partition)
	if err != nil {
		return err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.AddActivityTask(ctx, request, opts...)
}

func (c *clientImpl) AddDecisionTask(
	ctx context.Context,
	request *m.AddDecisionTaskRequest,
	opts ...yarpc.CallOption) error {
	opts = common.AggregateYarpcOptions(ctx, opts...)
	partition := c.loadBalancer.PickWritePartition(
		request.GetDomainUUID(),
		*request.GetTaskList(),
		persistence.TaskListTypeDecision,
		request.GetForwardedFrom(),
	)
	request.TaskList.Name = &partition
	client, err := c.getClientForTasklist(request.TaskList.GetName())
	if err != nil {
		return err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.AddDecisionTask(ctx, request, opts...)
}

func (c *clientImpl) PollForActivityTask(
	ctx context.Context,
	request *m.PollForActivityTaskRequest,
	opts ...yarpc.CallOption) (*workflow.PollForActivityTaskResponse, error) {
	opts = common.AggregateYarpcOptions(ctx, opts...)
	partition := c.loadBalancer.PickReadPartition(
		request.GetDomainUUID(),
		*request.PollRequest.GetTaskList(),
		persistence.TaskListTypeActivity,
		request.GetForwardedFrom(),
	)
	request.PollRequest.TaskList.Name = &partition
	client, err := c.getClientForTasklist(request.PollRequest.TaskList.GetName())
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createLongPollContext(ctx)
	defer cancel()
	return client.PollForActivityTask(ctx, request, opts...)
}

func (c *clientImpl) PollForDecisionTask(
	ctx context.Context,
	request *m.PollForDecisionTaskRequest,
	opts ...yarpc.CallOption) (*m.PollForDecisionTaskResponse, error) {
	opts = common.AggregateYarpcOptions(ctx, opts...)
	partition := c.loadBalancer.PickReadPartition(
		request.GetDomainUUID(),
		*request.PollRequest.GetTaskList(),
		persistence.TaskListTypeDecision,
		request.GetForwardedFrom(),
	)
	request.PollRequest.TaskList.Name = &partition
	client, err := c.getClientForTasklist(request.PollRequest.TaskList.GetName())
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createLongPollContext(ctx)
	defer cancel()
	return client.PollForDecisionTask(ctx, request, opts...)
}

func (c *clientImpl) QueryWorkflow(ctx context.Context, request *m.QueryWorkflowRequest, opts ...yarpc.CallOption) (*workflow.QueryWorkflowResponse, error) {
	opts = common.AggregateYarpcOptions(ctx, opts...)
	partition := c.loadBalancer.PickReadPartition(
		request.GetDomainUUID(),
		*request.GetTaskList(),
		persistence.TaskListTypeDecision,
		request.GetForwardedFrom(),
	)
	request.TaskList.Name = &partition
	client, err := c.getClientForTasklist(request.TaskList.GetName())
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.QueryWorkflow(ctx, request, opts...)
}

func (c *clientImpl) RespondQueryTaskCompleted(ctx context.Context, request *m.RespondQueryTaskCompletedRequest, opts ...yarpc.CallOption) error {
	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getClientForTasklist(request.TaskList.GetName())
	if err != nil {
		return err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.RespondQueryTaskCompleted(ctx, request, opts...)
}

func (c *clientImpl) CancelOutstandingPoll(ctx context.Context, request *m.CancelOutstandingPollRequest, opts ...yarpc.CallOption) error {
	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getClientForTasklist(request.TaskList.GetName())
	if err != nil {
		return err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.CancelOutstandingPoll(ctx, request, opts...)
}

func (c *clientImpl) DescribeTaskList(ctx context.Context, request *m.DescribeTaskListRequest, opts ...yarpc.CallOption) (*workflow.DescribeTaskListResponse, error) {
	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getClientForTasklist(request.DescRequest.TaskList.GetName())
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.DescribeTaskList(ctx, request, opts...)
}

func (c *clientImpl) ListTaskListPartitions(ctx context.Context, request *m.ListTaskListPartitionsRequest, opts ...yarpc.CallOption) (*workflow.ListTaskListPartitionsResponse, error) {
	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getClientForTasklist(request.TaskList.GetName())
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.ListTaskListPartitions(ctx, request, opts...)
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

func (c *clientImpl) getClientForTasklist(key string) (matchingserviceclient.Interface, error) {
	client, err := c.clients.GetClientForKey(key)
	if err != nil {
		return nil, err
	}
	return client.(matchingserviceclient.Interface), nil
}
