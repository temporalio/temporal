// The MIT License
//
// Copyright (c) 2025 Temporal Technologies Inc.  All rights reserved.
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

// Code generated by cmd/tools/genrpcwrappers. DO NOT EDIT.

package admin

import (
	"context"

	"go.temporal.io/server/api/adminservice/v1"
	"google.golang.org/grpc"
)

func (c *metricClient) AddOrUpdateRemoteCluster(
	ctx context.Context,
	request *adminservice.AddOrUpdateRemoteClusterRequest,
	opts ...grpc.CallOption,
) (_ *adminservice.AddOrUpdateRemoteClusterResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "AdminClientAddOrUpdateRemoteCluster")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.AddOrUpdateRemoteCluster(ctx, request, opts...)
}

func (c *metricClient) AddSearchAttributes(
	ctx context.Context,
	request *adminservice.AddSearchAttributesRequest,
	opts ...grpc.CallOption,
) (_ *adminservice.AddSearchAttributesResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "AdminClientAddSearchAttributes")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.AddSearchAttributes(ctx, request, opts...)
}

func (c *metricClient) AddTasks(
	ctx context.Context,
	request *adminservice.AddTasksRequest,
	opts ...grpc.CallOption,
) (_ *adminservice.AddTasksResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "AdminClientAddTasks")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.AddTasks(ctx, request, opts...)
}

func (c *metricClient) CancelDLQJob(
	ctx context.Context,
	request *adminservice.CancelDLQJobRequest,
	opts ...grpc.CallOption,
) (_ *adminservice.CancelDLQJobResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "AdminClientCancelDLQJob")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.CancelDLQJob(ctx, request, opts...)
}

func (c *metricClient) CloseShard(
	ctx context.Context,
	request *adminservice.CloseShardRequest,
	opts ...grpc.CallOption,
) (_ *adminservice.CloseShardResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "AdminClientCloseShard")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.CloseShard(ctx, request, opts...)
}

func (c *metricClient) DeepHealthCheck(
	ctx context.Context,
	request *adminservice.DeepHealthCheckRequest,
	opts ...grpc.CallOption,
) (_ *adminservice.DeepHealthCheckResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "AdminClientDeepHealthCheck")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.DeepHealthCheck(ctx, request, opts...)
}

func (c *metricClient) DeleteWorkflowExecution(
	ctx context.Context,
	request *adminservice.DeleteWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (_ *adminservice.DeleteWorkflowExecutionResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "AdminClientDeleteWorkflowExecution")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.DeleteWorkflowExecution(ctx, request, opts...)
}

func (c *metricClient) DescribeCluster(
	ctx context.Context,
	request *adminservice.DescribeClusterRequest,
	opts ...grpc.CallOption,
) (_ *adminservice.DescribeClusterResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "AdminClientDescribeCluster")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.DescribeCluster(ctx, request, opts...)
}

func (c *metricClient) DescribeDLQJob(
	ctx context.Context,
	request *adminservice.DescribeDLQJobRequest,
	opts ...grpc.CallOption,
) (_ *adminservice.DescribeDLQJobResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "AdminClientDescribeDLQJob")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.DescribeDLQJob(ctx, request, opts...)
}

func (c *metricClient) DescribeHistoryHost(
	ctx context.Context,
	request *adminservice.DescribeHistoryHostRequest,
	opts ...grpc.CallOption,
) (_ *adminservice.DescribeHistoryHostResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "AdminClientDescribeHistoryHost")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.DescribeHistoryHost(ctx, request, opts...)
}

func (c *metricClient) DescribeMutableState(
	ctx context.Context,
	request *adminservice.DescribeMutableStateRequest,
	opts ...grpc.CallOption,
) (_ *adminservice.DescribeMutableStateResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "AdminClientDescribeMutableState")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.DescribeMutableState(ctx, request, opts...)
}

func (c *metricClient) DescribeTaskQueuePartition(
	ctx context.Context,
	request *adminservice.DescribeTaskQueuePartitionRequest,
	opts ...grpc.CallOption,
) (_ *adminservice.DescribeTaskQueuePartitionResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "AdminClientDescribeTaskQueuePartition")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.DescribeTaskQueuePartition(ctx, request, opts...)
}

func (c *metricClient) ForceUnloadTaskQueuePartition(
	ctx context.Context,
	request *adminservice.ForceUnloadTaskQueuePartitionRequest,
	opts ...grpc.CallOption,
) (_ *adminservice.ForceUnloadTaskQueuePartitionResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "AdminClientForceUnloadTaskQueuePartition")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.ForceUnloadTaskQueuePartition(ctx, request, opts...)
}

func (c *metricClient) GenerateLastHistoryReplicationTasks(
	ctx context.Context,
	request *adminservice.GenerateLastHistoryReplicationTasksRequest,
	opts ...grpc.CallOption,
) (_ *adminservice.GenerateLastHistoryReplicationTasksResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "AdminClientGenerateLastHistoryReplicationTasks")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.GenerateLastHistoryReplicationTasks(ctx, request, opts...)
}

func (c *metricClient) GetDLQMessages(
	ctx context.Context,
	request *adminservice.GetDLQMessagesRequest,
	opts ...grpc.CallOption,
) (_ *adminservice.GetDLQMessagesResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "AdminClientGetDLQMessages")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.GetDLQMessages(ctx, request, opts...)
}

func (c *metricClient) GetDLQReplicationMessages(
	ctx context.Context,
	request *adminservice.GetDLQReplicationMessagesRequest,
	opts ...grpc.CallOption,
) (_ *adminservice.GetDLQReplicationMessagesResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "AdminClientGetDLQReplicationMessages")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.GetDLQReplicationMessages(ctx, request, opts...)
}

func (c *metricClient) GetDLQTasks(
	ctx context.Context,
	request *adminservice.GetDLQTasksRequest,
	opts ...grpc.CallOption,
) (_ *adminservice.GetDLQTasksResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "AdminClientGetDLQTasks")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.GetDLQTasks(ctx, request, opts...)
}

func (c *metricClient) GetNamespace(
	ctx context.Context,
	request *adminservice.GetNamespaceRequest,
	opts ...grpc.CallOption,
) (_ *adminservice.GetNamespaceResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "AdminClientGetNamespace")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.GetNamespace(ctx, request, opts...)
}

func (c *metricClient) GetNamespaceReplicationMessages(
	ctx context.Context,
	request *adminservice.GetNamespaceReplicationMessagesRequest,
	opts ...grpc.CallOption,
) (_ *adminservice.GetNamespaceReplicationMessagesResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "AdminClientGetNamespaceReplicationMessages")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.GetNamespaceReplicationMessages(ctx, request, opts...)
}

func (c *metricClient) GetReplicationMessages(
	ctx context.Context,
	request *adminservice.GetReplicationMessagesRequest,
	opts ...grpc.CallOption,
) (_ *adminservice.GetReplicationMessagesResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "AdminClientGetReplicationMessages")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.GetReplicationMessages(ctx, request, opts...)
}

func (c *metricClient) GetSearchAttributes(
	ctx context.Context,
	request *adminservice.GetSearchAttributesRequest,
	opts ...grpc.CallOption,
) (_ *adminservice.GetSearchAttributesResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "AdminClientGetSearchAttributes")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.GetSearchAttributes(ctx, request, opts...)
}

func (c *metricClient) GetShard(
	ctx context.Context,
	request *adminservice.GetShardRequest,
	opts ...grpc.CallOption,
) (_ *adminservice.GetShardResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "AdminClientGetShard")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.GetShard(ctx, request, opts...)
}

func (c *metricClient) GetTaskQueueTasks(
	ctx context.Context,
	request *adminservice.GetTaskQueueTasksRequest,
	opts ...grpc.CallOption,
) (_ *adminservice.GetTaskQueueTasksResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "AdminClientGetTaskQueueTasks")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.GetTaskQueueTasks(ctx, request, opts...)
}

func (c *metricClient) GetWorkflowExecutionRawHistory(
	ctx context.Context,
	request *adminservice.GetWorkflowExecutionRawHistoryRequest,
	opts ...grpc.CallOption,
) (_ *adminservice.GetWorkflowExecutionRawHistoryResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "AdminClientGetWorkflowExecutionRawHistory")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.GetWorkflowExecutionRawHistory(ctx, request, opts...)
}

func (c *metricClient) GetWorkflowExecutionRawHistoryV2(
	ctx context.Context,
	request *adminservice.GetWorkflowExecutionRawHistoryV2Request,
	opts ...grpc.CallOption,
) (_ *adminservice.GetWorkflowExecutionRawHistoryV2Response, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "AdminClientGetWorkflowExecutionRawHistoryV2")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.GetWorkflowExecutionRawHistoryV2(ctx, request, opts...)
}

func (c *metricClient) ImportWorkflowExecution(
	ctx context.Context,
	request *adminservice.ImportWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (_ *adminservice.ImportWorkflowExecutionResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "AdminClientImportWorkflowExecution")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.ImportWorkflowExecution(ctx, request, opts...)
}

func (c *metricClient) ListClusterMembers(
	ctx context.Context,
	request *adminservice.ListClusterMembersRequest,
	opts ...grpc.CallOption,
) (_ *adminservice.ListClusterMembersResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "AdminClientListClusterMembers")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.ListClusterMembers(ctx, request, opts...)
}

func (c *metricClient) ListClusters(
	ctx context.Context,
	request *adminservice.ListClustersRequest,
	opts ...grpc.CallOption,
) (_ *adminservice.ListClustersResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "AdminClientListClusters")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.ListClusters(ctx, request, opts...)
}

func (c *metricClient) ListHistoryTasks(
	ctx context.Context,
	request *adminservice.ListHistoryTasksRequest,
	opts ...grpc.CallOption,
) (_ *adminservice.ListHistoryTasksResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "AdminClientListHistoryTasks")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.ListHistoryTasks(ctx, request, opts...)
}

func (c *metricClient) ListQueues(
	ctx context.Context,
	request *adminservice.ListQueuesRequest,
	opts ...grpc.CallOption,
) (_ *adminservice.ListQueuesResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "AdminClientListQueues")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.ListQueues(ctx, request, opts...)
}

func (c *metricClient) MergeDLQMessages(
	ctx context.Context,
	request *adminservice.MergeDLQMessagesRequest,
	opts ...grpc.CallOption,
) (_ *adminservice.MergeDLQMessagesResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "AdminClientMergeDLQMessages")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.MergeDLQMessages(ctx, request, opts...)
}

func (c *metricClient) MergeDLQTasks(
	ctx context.Context,
	request *adminservice.MergeDLQTasksRequest,
	opts ...grpc.CallOption,
) (_ *adminservice.MergeDLQTasksResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "AdminClientMergeDLQTasks")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.MergeDLQTasks(ctx, request, opts...)
}

func (c *metricClient) PurgeDLQMessages(
	ctx context.Context,
	request *adminservice.PurgeDLQMessagesRequest,
	opts ...grpc.CallOption,
) (_ *adminservice.PurgeDLQMessagesResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "AdminClientPurgeDLQMessages")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.PurgeDLQMessages(ctx, request, opts...)
}

func (c *metricClient) PurgeDLQTasks(
	ctx context.Context,
	request *adminservice.PurgeDLQTasksRequest,
	opts ...grpc.CallOption,
) (_ *adminservice.PurgeDLQTasksResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "AdminClientPurgeDLQTasks")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.PurgeDLQTasks(ctx, request, opts...)
}

func (c *metricClient) ReapplyEvents(
	ctx context.Context,
	request *adminservice.ReapplyEventsRequest,
	opts ...grpc.CallOption,
) (_ *adminservice.ReapplyEventsResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "AdminClientReapplyEvents")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.ReapplyEvents(ctx, request, opts...)
}

func (c *metricClient) RebuildMutableState(
	ctx context.Context,
	request *adminservice.RebuildMutableStateRequest,
	opts ...grpc.CallOption,
) (_ *adminservice.RebuildMutableStateResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "AdminClientRebuildMutableState")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.RebuildMutableState(ctx, request, opts...)
}

func (c *metricClient) RefreshWorkflowTasks(
	ctx context.Context,
	request *adminservice.RefreshWorkflowTasksRequest,
	opts ...grpc.CallOption,
) (_ *adminservice.RefreshWorkflowTasksResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "AdminClientRefreshWorkflowTasks")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.RefreshWorkflowTasks(ctx, request, opts...)
}

func (c *metricClient) RemoveRemoteCluster(
	ctx context.Context,
	request *adminservice.RemoveRemoteClusterRequest,
	opts ...grpc.CallOption,
) (_ *adminservice.RemoveRemoteClusterResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "AdminClientRemoveRemoteCluster")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.RemoveRemoteCluster(ctx, request, opts...)
}

func (c *metricClient) RemoveSearchAttributes(
	ctx context.Context,
	request *adminservice.RemoveSearchAttributesRequest,
	opts ...grpc.CallOption,
) (_ *adminservice.RemoveSearchAttributesResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "AdminClientRemoveSearchAttributes")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.RemoveSearchAttributes(ctx, request, opts...)
}

func (c *metricClient) RemoveTask(
	ctx context.Context,
	request *adminservice.RemoveTaskRequest,
	opts ...grpc.CallOption,
) (_ *adminservice.RemoveTaskResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "AdminClientRemoveTask")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.RemoveTask(ctx, request, opts...)
}

func (c *metricClient) ResendReplicationTasks(
	ctx context.Context,
	request *adminservice.ResendReplicationTasksRequest,
	opts ...grpc.CallOption,
) (_ *adminservice.ResendReplicationTasksResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "AdminClientResendReplicationTasks")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.ResendReplicationTasks(ctx, request, opts...)
}

func (c *metricClient) SyncWorkflowState(
	ctx context.Context,
	request *adminservice.SyncWorkflowStateRequest,
	opts ...grpc.CallOption,
) (_ *adminservice.SyncWorkflowStateResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "AdminClientSyncWorkflowState")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.SyncWorkflowState(ctx, request, opts...)
}
