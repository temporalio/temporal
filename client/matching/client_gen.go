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

// Code generated by cmd/tools/rpcwrappers. DO NOT EDIT.

package matching

import (
	"context"
	"fmt"
	"math/rand"

	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"google.golang.org/grpc"
)

func (c *clientImpl) ApplyTaskQueueUserDataReplicationEvent(
	ctx context.Context,
	request *matchingservice.ApplyTaskQueueUserDataReplicationEventRequest,
	opts ...grpc.CallOption,
) (*matchingservice.ApplyTaskQueueUserDataReplicationEventResponse, error) {

	client, err := c.getClientForTaskqueue(request.GetNamespaceId(), &taskqueuepb.TaskQueue{Name: request.GetTaskQueue()}, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.ApplyTaskQueueUserDataReplicationEvent(ctx, request, opts...)
}

func (c *clientImpl) CancelOutstandingPoll(
	ctx context.Context,
	request *matchingservice.CancelOutstandingPollRequest,
	opts ...grpc.CallOption,
) (*matchingservice.CancelOutstandingPollResponse, error) {

	client, err := c.getClientForTaskqueue(request.GetNamespaceId(), request.GetTaskQueue(), request.GetTaskQueueType())
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.CancelOutstandingPoll(ctx, request, opts...)
}

func (c *clientImpl) DescribeTaskQueue(
	ctx context.Context,
	request *matchingservice.DescribeTaskQueueRequest,
	opts ...grpc.CallOption,
) (*matchingservice.DescribeTaskQueueResponse, error) {

	client, err := c.getClientForTaskqueue(request.GetNamespaceId(), request.GetDescRequest().GetTaskQueue(), request.GetDescRequest().GetTaskQueueType())
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.DescribeTaskQueue(ctx, request, opts...)
}

func (c *clientImpl) ForceUnloadTaskQueue(
	ctx context.Context,
	request *matchingservice.ForceUnloadTaskQueueRequest,
	opts ...grpc.CallOption,
) (*matchingservice.ForceUnloadTaskQueueResponse, error) {

	client, err := c.getClientForTaskqueue(request.GetNamespaceId(), &taskqueuepb.TaskQueue{Name: request.GetTaskQueue()}, request.GetTaskQueueType())
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.ForceUnloadTaskQueue(ctx, request, opts...)
}

func (c *clientImpl) GetBuildIdTaskQueueMapping(
	ctx context.Context,
	request *matchingservice.GetBuildIdTaskQueueMappingRequest,
	opts ...grpc.CallOption,
) (*matchingservice.GetBuildIdTaskQueueMappingResponse, error) {

	client, err := c.getClientForTaskqueue(request.GetNamespaceId(), &taskqueuepb.TaskQueue{Name: fmt.Sprintf("not-applicable-%s", rand.Int())}, enumspb.TASK_QUEUE_TYPE_UNSPECIFIED)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.GetBuildIdTaskQueueMapping(ctx, request, opts...)
}

func (c *clientImpl) GetTaskQueueUserData(
	ctx context.Context,
	request *matchingservice.GetTaskQueueUserDataRequest,
	opts ...grpc.CallOption,
) (*matchingservice.GetTaskQueueUserDataResponse, error) {

	client, err := c.getClientForTaskqueue(request.GetNamespaceId(), &taskqueuepb.TaskQueue{Name: request.GetTaskQueue()}, request.GetTaskQueueType())
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createLongPollContext(ctx)
	defer cancel()
	return client.GetTaskQueueUserData(ctx, request, opts...)
}

func (c *clientImpl) GetWorkerBuildIdCompatibility(
	ctx context.Context,
	request *matchingservice.GetWorkerBuildIdCompatibilityRequest,
	opts ...grpc.CallOption,
) (*matchingservice.GetWorkerBuildIdCompatibilityResponse, error) {

	client, err := c.getClientForTaskqueue(request.GetNamespaceId(), &taskqueuepb.TaskQueue{Name: request.GetRequest().GetTaskQueue()}, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.GetWorkerBuildIdCompatibility(ctx, request, opts...)
}

func (c *clientImpl) ListTaskQueuePartitions(
	ctx context.Context,
	request *matchingservice.ListTaskQueuePartitionsRequest,
	opts ...grpc.CallOption,
) (*matchingservice.ListTaskQueuePartitionsResponse, error) {

	client, err := c.getClientForTaskqueue(request.GetNamespaceId(), request.GetTaskQueue(), enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.ListTaskQueuePartitions(ctx, request, opts...)
}

func (c *clientImpl) ReplicateTaskQueueUserData(
	ctx context.Context,
	request *matchingservice.ReplicateTaskQueueUserDataRequest,
	opts ...grpc.CallOption,
) (*matchingservice.ReplicateTaskQueueUserDataResponse, error) {

	client, err := c.getClientForTaskqueue(request.GetNamespaceId(), &taskqueuepb.TaskQueue{Name: "not-applicable"}, enumspb.TASK_QUEUE_TYPE_UNSPECIFIED)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.ReplicateTaskQueueUserData(ctx, request, opts...)
}

func (c *clientImpl) RespondQueryTaskCompleted(
	ctx context.Context,
	request *matchingservice.RespondQueryTaskCompletedRequest,
	opts ...grpc.CallOption,
) (*matchingservice.RespondQueryTaskCompletedResponse, error) {

	client, err := c.getClientForTaskqueue(request.GetNamespaceId(), request.GetTaskQueue(), enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.RespondQueryTaskCompleted(ctx, request, opts...)
}

func (c *clientImpl) UpdateTaskQueueUserData(
	ctx context.Context,
	request *matchingservice.UpdateTaskQueueUserDataRequest,
	opts ...grpc.CallOption,
) (*matchingservice.UpdateTaskQueueUserDataResponse, error) {

	client, err := c.getClientForTaskqueue(request.GetNamespaceId(), &taskqueuepb.TaskQueue{Name: "not-applicable"}, enumspb.TASK_QUEUE_TYPE_UNSPECIFIED)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.UpdateTaskQueueUserData(ctx, request, opts...)
}

func (c *clientImpl) UpdateWorkerBuildIdCompatibility(
	ctx context.Context,
	request *matchingservice.UpdateWorkerBuildIdCompatibilityRequest,
	opts ...grpc.CallOption,
) (*matchingservice.UpdateWorkerBuildIdCompatibilityResponse, error) {

	client, err := c.getClientForTaskqueue(request.GetNamespaceId(), &taskqueuepb.TaskQueue{Name: request.GetTaskQueue()}, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.UpdateWorkerBuildIdCompatibility(ctx, request, opts...)
}
