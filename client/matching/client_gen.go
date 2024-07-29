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
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/tqid"
	"google.golang.org/grpc"
)

func (c *clientImpl) ApplyTaskQueueUserDataReplicationEvent(
	ctx context.Context,
	request *matchingservice.ApplyTaskQueueUserDataReplicationEventRequest,
	opts ...grpc.CallOption,
) (*matchingservice.ApplyTaskQueueUserDataReplicationEventResponse, error) {

	p, err := tqid.NormalPartitionFromRpcName(request.GetTaskQueue(), request.GetNamespaceId(), enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	if err != nil {
		return nil, err
	}

	client, err := c.getClientForTaskQueuePartition(p)
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

	p, err := tqid.PartitionFromProto(request.GetTaskQueue(), request.GetNamespaceId(), request.GetTaskQueueType())
	if err != nil {
		return nil, err
	}

	client, err := c.getClientForTaskQueuePartition(p)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.CancelOutstandingPoll(ctx, request, opts...)
}

func (c *clientImpl) CreateNexusEndpoint(
	ctx context.Context,
	request *matchingservice.CreateNexusEndpointRequest,
	opts ...grpc.CallOption,
) (*matchingservice.CreateNexusEndpointResponse, error) {

	p, err := tqid.NormalPartitionFromRpcName("not-applicable", "not-applicable", enumspb.TASK_QUEUE_TYPE_UNSPECIFIED)
	if err != nil {
		return nil, err
	}

	client, err := c.getClientForTaskQueuePartition(p)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.CreateNexusEndpoint(ctx, request, opts...)
}

func (c *clientImpl) DeleteNexusEndpoint(
	ctx context.Context,
	request *matchingservice.DeleteNexusEndpointRequest,
	opts ...grpc.CallOption,
) (*matchingservice.DeleteNexusEndpointResponse, error) {

	p, err := tqid.NormalPartitionFromRpcName("not-applicable", "not-applicable", enumspb.TASK_QUEUE_TYPE_UNSPECIFIED)
	if err != nil {
		return nil, err
	}

	client, err := c.getClientForTaskQueuePartition(p)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.DeleteNexusEndpoint(ctx, request, opts...)
}

func (c *clientImpl) DescribeTaskQueue(
	ctx context.Context,
	request *matchingservice.DescribeTaskQueueRequest,
	opts ...grpc.CallOption,
) (*matchingservice.DescribeTaskQueueResponse, error) {

	p, err := tqid.PartitionFromProto(request.GetDescRequest().GetTaskQueue(), request.GetNamespaceId(), request.GetDescRequest().GetTaskQueueType())
	if err != nil {
		return nil, err
	}

	client, err := c.getClientForTaskQueuePartition(p)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.DescribeTaskQueue(ctx, request, opts...)
}

func (c *clientImpl) DescribeTaskQueuePartition(
	ctx context.Context,
	request *matchingservice.DescribeTaskQueuePartitionRequest,
	opts ...grpc.CallOption,
) (*matchingservice.DescribeTaskQueuePartitionResponse, error) {

	p := tqid.PartitionFromPartitionProto(request.GetTaskQueuePartition(), request.GetNamespaceId())

	client, err := c.getClientForTaskQueuePartition(p)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.DescribeTaskQueuePartition(ctx, request, opts...)
}

func (c *clientImpl) DispatchNexusTask(
	ctx context.Context,
	request *matchingservice.DispatchNexusTaskRequest,
	opts ...grpc.CallOption,
) (*matchingservice.DispatchNexusTaskResponse, error) {

	p, err := tqid.PartitionFromProto(request.GetTaskQueue(), request.GetNamespaceId(), enumspb.TASK_QUEUE_TYPE_NEXUS)
	if err != nil {
		return nil, err
	}

	client, err := c.getClientForTaskQueuePartition(p)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.DispatchNexusTask(ctx, request, opts...)
}

func (c *clientImpl) ForceLoadTaskQueuePartition(
	ctx context.Context,
	request *matchingservice.ForceLoadTaskQueuePartitionRequest,
	opts ...grpc.CallOption,
) (*matchingservice.ForceLoadTaskQueuePartitionResponse, error) {

	p, err := tqid.NormalPartitionFromRpcName(request.GetTaskQueuePartition().GetTaskQueue(), request.GetNamespaceId(), request.GetTaskQueuePartition().GetTaskQueueType())
	if err != nil {
		return nil, err
	}
	client, err := c.getClientForTaskQueuePartition(p)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.ForceLoadTaskQueuePartition(ctx, request, opts...)
}

func (c *clientImpl) ForceUnloadTaskQueue(
	ctx context.Context,
	request *matchingservice.ForceUnloadTaskQueueRequest,
	opts ...grpc.CallOption,
) (*matchingservice.ForceUnloadTaskQueueResponse, error) {

	p, err := tqid.NormalPartitionFromRpcName(request.GetTaskQueue(), request.GetNamespaceId(), request.GetTaskQueueType())
	if err != nil {
		return nil, err
	}

	client, err := c.getClientForTaskQueuePartition(p)
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

	p, err := tqid.NormalPartitionFromRpcName(fmt.Sprintf("not-applicable-%d", rand.Int()), request.GetNamespaceId(), enumspb.TASK_QUEUE_TYPE_UNSPECIFIED)
	if err != nil {
		return nil, err
	}

	client, err := c.getClientForTaskQueuePartition(p)
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

	p, err := tqid.NormalPartitionFromRpcName(request.GetTaskQueue(), request.GetNamespaceId(), request.GetTaskQueueType())
	if err != nil {
		return nil, err
	}

	client, err := c.getClientForTaskQueuePartition(p)
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

	p, err := tqid.NormalPartitionFromRpcName(request.GetRequest().GetTaskQueue(), request.GetNamespaceId(), enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	if err != nil {
		return nil, err
	}

	client, err := c.getClientForTaskQueuePartition(p)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.GetWorkerBuildIdCompatibility(ctx, request, opts...)
}

func (c *clientImpl) GetWorkerVersioningRules(
	ctx context.Context,
	request *matchingservice.GetWorkerVersioningRulesRequest,
	opts ...grpc.CallOption,
) (*matchingservice.GetWorkerVersioningRulesResponse, error) {

	p, err := tqid.NormalPartitionFromRpcName(request.GetTaskQueue(), request.GetNamespaceId(), enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	if err != nil {
		return nil, err
	}

	client, err := c.getClientForTaskQueuePartition(p)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.GetWorkerVersioningRules(ctx, request, opts...)
}

func (c *clientImpl) ListNexusEndpoints(
	ctx context.Context,
	request *matchingservice.ListNexusEndpointsRequest,
	opts ...grpc.CallOption,
) (*matchingservice.ListNexusEndpointsResponse, error) {

	p, err := tqid.NormalPartitionFromRpcName("not-applicable", "not-applicable", enumspb.TASK_QUEUE_TYPE_UNSPECIFIED)
	if err != nil {
		return nil, err
	}

	client, err := c.getClientForTaskQueuePartition(p)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createLongPollContext(ctx)
	defer cancel()
	return client.ListNexusEndpoints(ctx, request, opts...)
}

func (c *clientImpl) ListTaskQueuePartitions(
	ctx context.Context,
	request *matchingservice.ListTaskQueuePartitionsRequest,
	opts ...grpc.CallOption,
) (*matchingservice.ListTaskQueuePartitionsResponse, error) {

	p, err := tqid.PartitionFromProto(request.GetTaskQueue(), request.GetNamespaceId(), enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	if err != nil {
		return nil, err
	}

	client, err := c.getClientForTaskQueuePartition(p)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.ListTaskQueuePartitions(ctx, request, opts...)
}

func (c *clientImpl) PollNexusTaskQueue(
	ctx context.Context,
	request *matchingservice.PollNexusTaskQueueRequest,
	opts ...grpc.CallOption,
) (*matchingservice.PollNexusTaskQueueResponse, error) {

	p, err := tqid.PartitionFromProto(request.GetRequest().GetTaskQueue(), request.GetNamespaceId(), enumspb.TASK_QUEUE_TYPE_NEXUS)
	if err != nil {
		return nil, err
	}

	client, err := c.getClientForTaskQueuePartition(p)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.PollNexusTaskQueue(ctx, request, opts...)
}

func (c *clientImpl) ReplicateTaskQueueUserData(
	ctx context.Context,
	request *matchingservice.ReplicateTaskQueueUserDataRequest,
	opts ...grpc.CallOption,
) (*matchingservice.ReplicateTaskQueueUserDataResponse, error) {

	p, err := tqid.NormalPartitionFromRpcName("not-applicable", request.GetNamespaceId(), enumspb.TASK_QUEUE_TYPE_UNSPECIFIED)
	if err != nil {
		return nil, err
	}

	client, err := c.getClientForTaskQueuePartition(p)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.ReplicateTaskQueueUserData(ctx, request, opts...)
}

func (c *clientImpl) RespondNexusTaskCompleted(
	ctx context.Context,
	request *matchingservice.RespondNexusTaskCompletedRequest,
	opts ...grpc.CallOption,
) (*matchingservice.RespondNexusTaskCompletedResponse, error) {

	p, err := tqid.PartitionFromProto(request.GetTaskQueue(), request.GetNamespaceId(), enumspb.TASK_QUEUE_TYPE_NEXUS)
	if err != nil {
		return nil, err
	}

	client, err := c.getClientForTaskQueuePartition(p)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.RespondNexusTaskCompleted(ctx, request, opts...)
}

func (c *clientImpl) RespondNexusTaskFailed(
	ctx context.Context,
	request *matchingservice.RespondNexusTaskFailedRequest,
	opts ...grpc.CallOption,
) (*matchingservice.RespondNexusTaskFailedResponse, error) {

	p, err := tqid.PartitionFromProto(request.GetTaskQueue(), request.GetNamespaceId(), enumspb.TASK_QUEUE_TYPE_NEXUS)
	if err != nil {
		return nil, err
	}

	client, err := c.getClientForTaskQueuePartition(p)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.RespondNexusTaskFailed(ctx, request, opts...)
}

func (c *clientImpl) RespondQueryTaskCompleted(
	ctx context.Context,
	request *matchingservice.RespondQueryTaskCompletedRequest,
	opts ...grpc.CallOption,
) (*matchingservice.RespondQueryTaskCompletedResponse, error) {

	p, err := tqid.PartitionFromProto(request.GetTaskQueue(), request.GetNamespaceId(), enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	if err != nil {
		return nil, err
	}

	client, err := c.getClientForTaskQueuePartition(p)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.RespondQueryTaskCompleted(ctx, request, opts...)
}

func (c *clientImpl) UpdateNexusEndpoint(
	ctx context.Context,
	request *matchingservice.UpdateNexusEndpointRequest,
	opts ...grpc.CallOption,
) (*matchingservice.UpdateNexusEndpointResponse, error) {

	p, err := tqid.NormalPartitionFromRpcName("not-applicable", "not-applicable", enumspb.TASK_QUEUE_TYPE_UNSPECIFIED)
	if err != nil {
		return nil, err
	}

	client, err := c.getClientForTaskQueuePartition(p)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.UpdateNexusEndpoint(ctx, request, opts...)
}

func (c *clientImpl) UpdateTaskQueueUserData(
	ctx context.Context,
	request *matchingservice.UpdateTaskQueueUserDataRequest,
	opts ...grpc.CallOption,
) (*matchingservice.UpdateTaskQueueUserDataResponse, error) {

	p, err := tqid.NormalPartitionFromRpcName("not-applicable", request.GetNamespaceId(), enumspb.TASK_QUEUE_TYPE_UNSPECIFIED)
	if err != nil {
		return nil, err
	}

	client, err := c.getClientForTaskQueuePartition(p)
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

	p, err := tqid.NormalPartitionFromRpcName(request.GetTaskQueue(), request.GetNamespaceId(), enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	if err != nil {
		return nil, err
	}

	client, err := c.getClientForTaskQueuePartition(p)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.UpdateWorkerBuildIdCompatibility(ctx, request, opts...)
}

func (c *clientImpl) UpdateWorkerVersioningRules(
	ctx context.Context,
	request *matchingservice.UpdateWorkerVersioningRulesRequest,
	opts ...grpc.CallOption,
) (*matchingservice.UpdateWorkerVersioningRulesResponse, error) {

	p, err := tqid.NormalPartitionFromRpcName(request.GetTaskQueue(), request.GetNamespaceId(), enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	if err != nil {
		return nil, err
	}

	client, err := c.getClientForTaskQueuePartition(p)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.UpdateWorkerVersioningRules(ctx, request, opts...)
}
