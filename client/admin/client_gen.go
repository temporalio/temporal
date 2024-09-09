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

package admin

import (
	"context"

	"go.temporal.io/server/api/adminservice/v1"
	"google.golang.org/grpc"
)

func (c *clientImpl) AddOrUpdateRemoteCluster(
	ctx context.Context,
	request *adminservice.AddOrUpdateRemoteClusterRequest,
	opts ...grpc.CallOption,
) (*adminservice.AddOrUpdateRemoteClusterResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.AddOrUpdateRemoteCluster(ctx, request, opts...)
}

func (c *clientImpl) AddSearchAttributes(
	ctx context.Context,
	request *adminservice.AddSearchAttributesRequest,
	opts ...grpc.CallOption,
) (*adminservice.AddSearchAttributesResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.AddSearchAttributes(ctx, request, opts...)
}

func (c *clientImpl) AddTasks(
	ctx context.Context,
	request *adminservice.AddTasksRequest,
	opts ...grpc.CallOption,
) (*adminservice.AddTasksResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.AddTasks(ctx, request, opts...)
}

func (c *clientImpl) CancelDLQJob(
	ctx context.Context,
	request *adminservice.CancelDLQJobRequest,
	opts ...grpc.CallOption,
) (*adminservice.CancelDLQJobResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.CancelDLQJob(ctx, request, opts...)
}

func (c *clientImpl) CloseShard(
	ctx context.Context,
	request *adminservice.CloseShardRequest,
	opts ...grpc.CallOption,
) (*adminservice.CloseShardResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.CloseShard(ctx, request, opts...)
}

func (c *clientImpl) DeepHealthCheck(
	ctx context.Context,
	request *adminservice.DeepHealthCheckRequest,
	opts ...grpc.CallOption,
) (*adminservice.DeepHealthCheckResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.DeepHealthCheck(ctx, request, opts...)
}

func (c *clientImpl) DeleteWorkflowExecution(
	ctx context.Context,
	request *adminservice.DeleteWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (*adminservice.DeleteWorkflowExecutionResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.DeleteWorkflowExecution(ctx, request, opts...)
}

func (c *clientImpl) DescribeCluster(
	ctx context.Context,
	request *adminservice.DescribeClusterRequest,
	opts ...grpc.CallOption,
) (*adminservice.DescribeClusterResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.DescribeCluster(ctx, request, opts...)
}

func (c *clientImpl) DescribeDLQJob(
	ctx context.Context,
	request *adminservice.DescribeDLQJobRequest,
	opts ...grpc.CallOption,
) (*adminservice.DescribeDLQJobResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.DescribeDLQJob(ctx, request, opts...)
}

func (c *clientImpl) DescribeHistoryHost(
	ctx context.Context,
	request *adminservice.DescribeHistoryHostRequest,
	opts ...grpc.CallOption,
) (*adminservice.DescribeHistoryHostResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.DescribeHistoryHost(ctx, request, opts...)
}

func (c *clientImpl) DescribeMutableState(
	ctx context.Context,
	request *adminservice.DescribeMutableStateRequest,
	opts ...grpc.CallOption,
) (*adminservice.DescribeMutableStateResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.DescribeMutableState(ctx, request, opts...)
}

func (c *clientImpl) DescribeTaskQueuePartition(
	ctx context.Context,
	request *adminservice.DescribeTaskQueuePartitionRequest,
	opts ...grpc.CallOption,
) (*adminservice.DescribeTaskQueuePartitionResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.DescribeTaskQueuePartition(ctx, request, opts...)
}

func (c *clientImpl) GetDLQMessages(
	ctx context.Context,
	request *adminservice.GetDLQMessagesRequest,
	opts ...grpc.CallOption,
) (*adminservice.GetDLQMessagesResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.GetDLQMessages(ctx, request, opts...)
}

func (c *clientImpl) GetDLQReplicationMessages(
	ctx context.Context,
	request *adminservice.GetDLQReplicationMessagesRequest,
	opts ...grpc.CallOption,
) (*adminservice.GetDLQReplicationMessagesResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.GetDLQReplicationMessages(ctx, request, opts...)
}

func (c *clientImpl) GetDLQTasks(
	ctx context.Context,
	request *adminservice.GetDLQTasksRequest,
	opts ...grpc.CallOption,
) (*adminservice.GetDLQTasksResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.GetDLQTasks(ctx, request, opts...)
}

func (c *clientImpl) GetNamespace(
	ctx context.Context,
	request *adminservice.GetNamespaceRequest,
	opts ...grpc.CallOption,
) (*adminservice.GetNamespaceResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.GetNamespace(ctx, request, opts...)
}

func (c *clientImpl) GetNamespaceReplicationMessages(
	ctx context.Context,
	request *adminservice.GetNamespaceReplicationMessagesRequest,
	opts ...grpc.CallOption,
) (*adminservice.GetNamespaceReplicationMessagesResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.GetNamespaceReplicationMessages(ctx, request, opts...)
}

func (c *clientImpl) GetReplicationMessages(
	ctx context.Context,
	request *adminservice.GetReplicationMessagesRequest,
	opts ...grpc.CallOption,
) (*adminservice.GetReplicationMessagesResponse, error) {
	ctx, cancel := c.createContextWithLargeTimeout(ctx)
	defer cancel()
	return c.client.GetReplicationMessages(ctx, request, opts...)
}

func (c *clientImpl) GetSearchAttributes(
	ctx context.Context,
	request *adminservice.GetSearchAttributesRequest,
	opts ...grpc.CallOption,
) (*adminservice.GetSearchAttributesResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.GetSearchAttributes(ctx, request, opts...)
}

func (c *clientImpl) GetShard(
	ctx context.Context,
	request *adminservice.GetShardRequest,
	opts ...grpc.CallOption,
) (*adminservice.GetShardResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.GetShard(ctx, request, opts...)
}

func (c *clientImpl) GetTaskQueueTasks(
	ctx context.Context,
	request *adminservice.GetTaskQueueTasksRequest,
	opts ...grpc.CallOption,
) (*adminservice.GetTaskQueueTasksResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.GetTaskQueueTasks(ctx, request, opts...)
}

func (c *clientImpl) GetWorkflowExecutionRawHistory(
	ctx context.Context,
	request *adminservice.GetWorkflowExecutionRawHistoryRequest,
	opts ...grpc.CallOption,
) (*adminservice.GetWorkflowExecutionRawHistoryResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.GetWorkflowExecutionRawHistory(ctx, request, opts...)
}

func (c *clientImpl) GetWorkflowExecutionRawHistoryV2(
	ctx context.Context,
	request *adminservice.GetWorkflowExecutionRawHistoryV2Request,
	opts ...grpc.CallOption,
) (*adminservice.GetWorkflowExecutionRawHistoryV2Response, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.GetWorkflowExecutionRawHistoryV2(ctx, request, opts...)
}

func (c *clientImpl) ImportWorkflowExecution(
	ctx context.Context,
	request *adminservice.ImportWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (*adminservice.ImportWorkflowExecutionResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.ImportWorkflowExecution(ctx, request, opts...)
}

func (c *clientImpl) ListClusterMembers(
	ctx context.Context,
	request *adminservice.ListClusterMembersRequest,
	opts ...grpc.CallOption,
) (*adminservice.ListClusterMembersResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.ListClusterMembers(ctx, request, opts...)
}

func (c *clientImpl) ListClusters(
	ctx context.Context,
	request *adminservice.ListClustersRequest,
	opts ...grpc.CallOption,
) (*adminservice.ListClustersResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.ListClusters(ctx, request, opts...)
}

func (c *clientImpl) ListHistoryTasks(
	ctx context.Context,
	request *adminservice.ListHistoryTasksRequest,
	opts ...grpc.CallOption,
) (*adminservice.ListHistoryTasksResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.ListHistoryTasks(ctx, request, opts...)
}

func (c *clientImpl) ListQueues(
	ctx context.Context,
	request *adminservice.ListQueuesRequest,
	opts ...grpc.CallOption,
) (*adminservice.ListQueuesResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.ListQueues(ctx, request, opts...)
}

func (c *clientImpl) MergeDLQMessages(
	ctx context.Context,
	request *adminservice.MergeDLQMessagesRequest,
	opts ...grpc.CallOption,
) (*adminservice.MergeDLQMessagesResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.MergeDLQMessages(ctx, request, opts...)
}

func (c *clientImpl) MergeDLQTasks(
	ctx context.Context,
	request *adminservice.MergeDLQTasksRequest,
	opts ...grpc.CallOption,
) (*adminservice.MergeDLQTasksResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.MergeDLQTasks(ctx, request, opts...)
}

func (c *clientImpl) PurgeDLQMessages(
	ctx context.Context,
	request *adminservice.PurgeDLQMessagesRequest,
	opts ...grpc.CallOption,
) (*adminservice.PurgeDLQMessagesResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.PurgeDLQMessages(ctx, request, opts...)
}

func (c *clientImpl) PurgeDLQTasks(
	ctx context.Context,
	request *adminservice.PurgeDLQTasksRequest,
	opts ...grpc.CallOption,
) (*adminservice.PurgeDLQTasksResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.PurgeDLQTasks(ctx, request, opts...)
}

func (c *clientImpl) ReapplyEvents(
	ctx context.Context,
	request *adminservice.ReapplyEventsRequest,
	opts ...grpc.CallOption,
) (*adminservice.ReapplyEventsResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.ReapplyEvents(ctx, request, opts...)
}

func (c *clientImpl) RebuildMutableState(
	ctx context.Context,
	request *adminservice.RebuildMutableStateRequest,
	opts ...grpc.CallOption,
) (*adminservice.RebuildMutableStateResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.RebuildMutableState(ctx, request, opts...)
}

func (c *clientImpl) RefreshWorkflowTasks(
	ctx context.Context,
	request *adminservice.RefreshWorkflowTasksRequest,
	opts ...grpc.CallOption,
) (*adminservice.RefreshWorkflowTasksResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.RefreshWorkflowTasks(ctx, request, opts...)
}

func (c *clientImpl) RemoveRemoteCluster(
	ctx context.Context,
	request *adminservice.RemoveRemoteClusterRequest,
	opts ...grpc.CallOption,
) (*adminservice.RemoveRemoteClusterResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.RemoveRemoteCluster(ctx, request, opts...)
}

func (c *clientImpl) RemoveSearchAttributes(
	ctx context.Context,
	request *adminservice.RemoveSearchAttributesRequest,
	opts ...grpc.CallOption,
) (*adminservice.RemoveSearchAttributesResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.RemoveSearchAttributes(ctx, request, opts...)
}

func (c *clientImpl) RemoveTask(
	ctx context.Context,
	request *adminservice.RemoveTaskRequest,
	opts ...grpc.CallOption,
) (*adminservice.RemoveTaskResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.RemoveTask(ctx, request, opts...)
}

func (c *clientImpl) ResendReplicationTasks(
	ctx context.Context,
	request *adminservice.ResendReplicationTasksRequest,
	opts ...grpc.CallOption,
) (*adminservice.ResendReplicationTasksResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.ResendReplicationTasks(ctx, request, opts...)
}

func (c *clientImpl) SyncWorkflowState(
	ctx context.Context,
	request *adminservice.SyncWorkflowStateRequest,
	opts ...grpc.CallOption,
) (*adminservice.SyncWorkflowStateResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.SyncWorkflowState(ctx, request, opts...)
}
