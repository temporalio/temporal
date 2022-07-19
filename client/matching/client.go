// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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

// Generates all three generated files in this package:
//go:generate go run ../../cmd/tools/rpcwrappers -service matching

package matching

import (
	"context"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"google.golang.org/grpc"

	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/namespace"
)

var _ matchingservice.MatchingServiceClient = (*clientImpl)(nil)

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

// NewClient creates a new history service gRPC client
func NewClient(
	timeout time.Duration,
	longPollTimeout time.Duration,
	clients common.ClientCache,
	lb LoadBalancer,
) matchingservice.MatchingServiceClient {
	return &clientImpl{
		timeout:         timeout,
		longPollTimeout: longPollTimeout,
		clients:         clients,
		loadBalancer:    lb,
	}
}

func (c *clientImpl) AddActivityTask(
	ctx context.Context,
	request *matchingservice.AddActivityTaskRequest,
	opts ...grpc.CallOption) (*matchingservice.AddActivityTaskResponse, error) {
	partition := c.loadBalancer.PickWritePartition(
		namespace.ID(request.GetNamespaceId()),
		*request.GetTaskQueue(),
		enumspb.TASK_QUEUE_TYPE_ACTIVITY,
		request.GetForwardedSource(),
	)
	request.TaskQueue.Name = partition
	client, err := c.getClientForTaskqueue(partition)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.AddActivityTask(ctx, request, opts...)
}

func (c *clientImpl) AddWorkflowTask(
	ctx context.Context,
	request *matchingservice.AddWorkflowTaskRequest,
	opts ...grpc.CallOption) (*matchingservice.AddWorkflowTaskResponse, error) {
	partition := c.loadBalancer.PickWritePartition(
		namespace.ID(request.GetNamespaceId()),
		*request.GetTaskQueue(),
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		request.GetForwardedSource(),
	)
	request.TaskQueue.Name = partition
	client, err := c.getClientForTaskqueue(request.TaskQueue.GetName())
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.AddWorkflowTask(ctx, request, opts...)
}

func (c *clientImpl) PollActivityTaskQueue(
	ctx context.Context,
	request *matchingservice.PollActivityTaskQueueRequest,
	opts ...grpc.CallOption) (*matchingservice.PollActivityTaskQueueResponse, error) {
	partition := c.loadBalancer.PickReadPartition(
		namespace.ID(request.GetNamespaceId()),
		*request.PollRequest.GetTaskQueue(),
		enumspb.TASK_QUEUE_TYPE_ACTIVITY,
		request.GetForwardedSource(),
	)
	request.PollRequest.TaskQueue.Name = partition
	client, err := c.getClientForTaskqueue(request.PollRequest.TaskQueue.GetName())
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createLongPollContext(ctx)
	defer cancel()
	return client.PollActivityTaskQueue(ctx, request, opts...)
}

func (c *clientImpl) PollWorkflowTaskQueue(
	ctx context.Context,
	request *matchingservice.PollWorkflowTaskQueueRequest,
	opts ...grpc.CallOption) (*matchingservice.PollWorkflowTaskQueueResponse, error) {
	partition := c.loadBalancer.PickReadPartition(
		namespace.ID(request.GetNamespaceId()),
		*request.PollRequest.GetTaskQueue(),
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		request.GetForwardedSource(),
	)
	request.PollRequest.TaskQueue.Name = partition
	client, err := c.getClientForTaskqueue(request.PollRequest.TaskQueue.GetName())
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createLongPollContext(ctx)
	defer cancel()
	return client.PollWorkflowTaskQueue(ctx, request, opts...)
}

func (c *clientImpl) QueryWorkflow(ctx context.Context, request *matchingservice.QueryWorkflowRequest, opts ...grpc.CallOption) (*matchingservice.QueryWorkflowResponse, error) {
	partition := c.loadBalancer.PickReadPartition(
		namespace.ID(request.GetNamespaceId()),
		*request.GetTaskQueue(),
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		request.GetForwardedSource(),
	)
	request.TaskQueue.Name = partition
	client, err := c.getClientForTaskqueue(request.TaskQueue.GetName())
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.QueryWorkflow(ctx, request, opts...)
}

func (c *clientImpl) createContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, c.timeout)
}

func (c *clientImpl) createLongPollContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, c.longPollTimeout)
}

func (c *clientImpl) getClientForTaskqueue(key string) (matchingservice.MatchingServiceClient, error) {
	client, err := c.clients.GetClientForKey(key)
	if err != nil {
		return nil, err
	}
	return client.(matchingservice.MatchingServiceClient), nil
}
