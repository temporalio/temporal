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

package admin

import (
	"context"
	"time"

	"github.com/pborman/uuid"
	"go.uber.org/yarpc"

	"github.com/uber/cadence/.gen/go/admin"
	"github.com/uber/cadence/.gen/go/admin/adminserviceclient"
	"github.com/uber/cadence/.gen/go/replicator"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
)

var _ Client = (*clientImpl)(nil)

const (
	// DefaultTimeout is the default timeout used to make calls
	DefaultTimeout = 10 * time.Second
)

type clientImpl struct {
	timeout time.Duration
	clients common.ClientCache
}

// NewClient creates a new admin service TChannel client
func NewClient(
	timeout time.Duration,
	clients common.ClientCache,
) Client {
	return &clientImpl{
		timeout: timeout,
		clients: clients,
	}
}

func (c *clientImpl) AddSearchAttribute(
	ctx context.Context,
	request *admin.AddSearchAttributeRequest,
	opts ...yarpc.CallOption,
) error {

	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.AddSearchAttribute(ctx, request, opts...)
}

func (c *clientImpl) DescribeHistoryHost(
	ctx context.Context,
	request *shared.DescribeHistoryHostRequest,
	opts ...yarpc.CallOption,
) (*shared.DescribeHistoryHostResponse, error) {

	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.DescribeHistoryHost(ctx, request, opts...)
}

func (c *clientImpl) RemoveTask(
	ctx context.Context,
	request *shared.RemoveTaskRequest,
	opts ...yarpc.CallOption,
) error {

	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.RemoveTask(ctx, request, opts...)
}

func (c *clientImpl) CloseShard(
	ctx context.Context,
	request *shared.CloseShardRequest,
	opts ...yarpc.CallOption,
) error {

	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.CloseShard(ctx, request, opts...)
}

func (c *clientImpl) DescribeWorkflowExecution(
	ctx context.Context,
	request *admin.DescribeWorkflowExecutionRequest,
	opts ...yarpc.CallOption,
) (*admin.DescribeWorkflowExecutionResponse, error) {

	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.DescribeWorkflowExecution(ctx, request, opts...)
}

func (c *clientImpl) GetWorkflowExecutionRawHistory(
	ctx context.Context,
	request *admin.GetWorkflowExecutionRawHistoryRequest,
	opts ...yarpc.CallOption,
) (*admin.GetWorkflowExecutionRawHistoryResponse, error) {

	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.GetWorkflowExecutionRawHistory(ctx, request, opts...)
}

func (c *clientImpl) GetWorkflowExecutionRawHistoryV2(
	ctx context.Context,
	request *admin.GetWorkflowExecutionRawHistoryV2Request,
	opts ...yarpc.CallOption,
) (*admin.GetWorkflowExecutionRawHistoryV2Response, error) {

	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.GetWorkflowExecutionRawHistoryV2(ctx, request, opts...)
}

func (c *clientImpl) DescribeCluster(
	ctx context.Context,
	opts ...yarpc.CallOption,
) (*admin.DescribeClusterResponse, error) {
	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.DescribeCluster(ctx, opts...)
}

func (c *clientImpl) GetReplicationMessages(
	ctx context.Context,
	request *replicator.GetReplicationMessagesRequest,
	opts ...yarpc.CallOption,
) (*replicator.GetReplicationMessagesResponse, error) {
	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.GetReplicationMessages(ctx, request, opts...)
}

func (c *clientImpl) GetDomainReplicationMessages(
	ctx context.Context,
	request *replicator.GetDomainReplicationMessagesRequest,
	opts ...yarpc.CallOption,
) (*replicator.GetDomainReplicationMessagesResponse, error) {
	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.GetDomainReplicationMessages(ctx, request, opts...)
}

func (c *clientImpl) GetDLQReplicationMessages(
	ctx context.Context,
	request *replicator.GetDLQReplicationMessagesRequest,
	opts ...yarpc.CallOption,
) (*replicator.GetDLQReplicationMessagesResponse, error) {
	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.GetDLQReplicationMessages(ctx, request, opts...)
}

func (c *clientImpl) ReapplyEvents(
	ctx context.Context,
	request *shared.ReapplyEventsRequest,
	opts ...yarpc.CallOption,
) error {

	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.ReapplyEvents(ctx, request, opts...)
}

func (c *clientImpl) ReadDLQMessages(
	ctx context.Context,
	request *replicator.ReadDLQMessagesRequest,
	opts ...yarpc.CallOption,
) (*replicator.ReadDLQMessagesResponse, error) {

	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.ReadDLQMessages(ctx, request, opts...)
}

func (c *clientImpl) PurgeDLQMessages(
	ctx context.Context,
	request *replicator.PurgeDLQMessagesRequest,
	opts ...yarpc.CallOption,
) error {

	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.PurgeDLQMessages(ctx, request, opts...)
}

func (c *clientImpl) MergeDLQMessages(
	ctx context.Context,
	request *replicator.MergeDLQMessagesRequest,
	opts ...yarpc.CallOption,
) (*replicator.MergeDLQMessagesResponse, error) {

	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.MergeDLQMessages(ctx, request, opts...)

}

func (c *clientImpl) RefreshWorkflowTasks(
	ctx context.Context,
	request *shared.RefreshWorkflowTasksRequest,
	opts ...yarpc.CallOption,
) error {
	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.RefreshWorkflowTasks(ctx, request, opts...)
}

func (c *clientImpl) createContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		return context.WithTimeout(context.Background(), c.timeout)
	}
	return context.WithTimeout(parent, c.timeout)
}

func (c *clientImpl) getRandomClient() (adminserviceclient.Interface, error) {
	// generate a random shard key to do load balancing
	key := uuid.New()
	client, err := c.clients.GetClientForKey(key)
	if err != nil {
		return nil, err
	}

	return client.(adminserviceclient.Interface), nil
}
