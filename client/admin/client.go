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

	"github.com/temporalio/temporal/.gen/proto/adminservice"
	"github.com/temporalio/temporal/common"
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
	request *adminservice.AddSearchAttributeRequest,
	opts ...yarpc.CallOption,
) (*adminservice.AddSearchAttributeResponse, error) {

	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.AddSearchAttribute(ctx, request, opts...)
}

func (c *clientImpl) DescribeHistoryHost(
	ctx context.Context,
	request *adminservice.DescribeHistoryHostRequest,
	opts ...yarpc.CallOption,
) (*adminservice.DescribeHistoryHostResponse, error) {

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
	request *adminservice.RemoveTaskRequest,
	opts ...yarpc.CallOption,
) (*adminservice.RemoveTaskResponse, error) {

	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.RemoveTask(ctx, request, opts...)
}

func (c *clientImpl) CloseShard(
	ctx context.Context,
	request *adminservice.CloseShardRequest,
	opts ...yarpc.CallOption,
) (*adminservice.CloseShardResponse, error) {

	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.CloseShard(ctx, request, opts...)
}

func (c *clientImpl) DescribeWorkflowExecution(
	ctx context.Context,
	request *adminservice.DescribeWorkflowExecutionRequest,
	opts ...yarpc.CallOption,
) (*adminservice.DescribeWorkflowExecutionResponse, error) {

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
	request *adminservice.GetWorkflowExecutionRawHistoryRequest,
	opts ...yarpc.CallOption,
) (*adminservice.GetWorkflowExecutionRawHistoryResponse, error) {

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
	request *adminservice.GetWorkflowExecutionRawHistoryV2Request,
	opts ...yarpc.CallOption,
) (*adminservice.GetWorkflowExecutionRawHistoryV2Response, error) {

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
	request *adminservice.DescribeClusterRequest,
	opts ...yarpc.CallOption,
) (*adminservice.DescribeClusterResponse, error) {

	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getRandomClient()
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.DescribeCluster(ctx, request, opts...)
}

func (c *clientImpl) createContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		return context.WithTimeout(context.Background(), c.timeout)
	}
	return context.WithTimeout(parent, c.timeout)
}

func (c *clientImpl) getRandomClient() (adminservice.AdminServiceYARPCClient, error) {
	// generate a random shard key to do load balancing
	key := uuid.New()
	client, err := c.clients.GetClientForKey(key)
	if err != nil {
		return nil, err
	}

	return client.(adminservice.AdminServiceYARPCClient), nil
}
