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

	"go.temporal.io/server/common/backoff"
)

func (c *retryableClient) AddOrUpdateRemoteCluster(
	ctx context.Context,
	request *adminservice.AddOrUpdateRemoteClusterRequest,
	opts ...grpc.CallOption,
) (*adminservice.AddOrUpdateRemoteClusterResponse, error) {
	var resp *adminservice.AddOrUpdateRemoteClusterResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.AddOrUpdateRemoteCluster(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) AddSearchAttributes(
	ctx context.Context,
	request *adminservice.AddSearchAttributesRequest,
	opts ...grpc.CallOption,
) (*adminservice.AddSearchAttributesResponse, error) {
	var resp *adminservice.AddSearchAttributesResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.AddSearchAttributes(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) AddTasks(
	ctx context.Context,
	request *adminservice.AddTasksRequest,
	opts ...grpc.CallOption,
) (*adminservice.AddTasksResponse, error) {
	var resp *adminservice.AddTasksResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.AddTasks(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) CancelDLQJob(
	ctx context.Context,
	request *adminservice.CancelDLQJobRequest,
	opts ...grpc.CallOption,
) (*adminservice.CancelDLQJobResponse, error) {
	var resp *adminservice.CancelDLQJobResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.CancelDLQJob(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) CloseShard(
	ctx context.Context,
	request *adminservice.CloseShardRequest,
	opts ...grpc.CallOption,
) (*adminservice.CloseShardResponse, error) {
	var resp *adminservice.CloseShardResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.CloseShard(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) DeepHealthCheck(
	ctx context.Context,
	request *adminservice.DeepHealthCheckRequest,
	opts ...grpc.CallOption,
) (*adminservice.DeepHealthCheckResponse, error) {
	var resp *adminservice.DeepHealthCheckResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.DeepHealthCheck(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) DeleteWorkflowExecution(
	ctx context.Context,
	request *adminservice.DeleteWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (*adminservice.DeleteWorkflowExecutionResponse, error) {
	var resp *adminservice.DeleteWorkflowExecutionResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.DeleteWorkflowExecution(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) DescribeCluster(
	ctx context.Context,
	request *adminservice.DescribeClusterRequest,
	opts ...grpc.CallOption,
) (*adminservice.DescribeClusterResponse, error) {
	var resp *adminservice.DescribeClusterResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.DescribeCluster(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) DescribeDLQJob(
	ctx context.Context,
	request *adminservice.DescribeDLQJobRequest,
	opts ...grpc.CallOption,
) (*adminservice.DescribeDLQJobResponse, error) {
	var resp *adminservice.DescribeDLQJobResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.DescribeDLQJob(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) DescribeHistoryHost(
	ctx context.Context,
	request *adminservice.DescribeHistoryHostRequest,
	opts ...grpc.CallOption,
) (*adminservice.DescribeHistoryHostResponse, error) {
	var resp *adminservice.DescribeHistoryHostResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.DescribeHistoryHost(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) DescribeMutableState(
	ctx context.Context,
	request *adminservice.DescribeMutableStateRequest,
	opts ...grpc.CallOption,
) (*adminservice.DescribeMutableStateResponse, error) {
	var resp *adminservice.DescribeMutableStateResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.DescribeMutableState(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) DescribeTaskQueuePartition(
	ctx context.Context,
	request *adminservice.DescribeTaskQueuePartitionRequest,
	opts ...grpc.CallOption,
) (*adminservice.DescribeTaskQueuePartitionResponse, error) {
	var resp *adminservice.DescribeTaskQueuePartitionResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.DescribeTaskQueuePartition(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) GetDLQMessages(
	ctx context.Context,
	request *adminservice.GetDLQMessagesRequest,
	opts ...grpc.CallOption,
) (*adminservice.GetDLQMessagesResponse, error) {
	var resp *adminservice.GetDLQMessagesResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.GetDLQMessages(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) GetDLQReplicationMessages(
	ctx context.Context,
	request *adminservice.GetDLQReplicationMessagesRequest,
	opts ...grpc.CallOption,
) (*adminservice.GetDLQReplicationMessagesResponse, error) {
	var resp *adminservice.GetDLQReplicationMessagesResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.GetDLQReplicationMessages(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) GetDLQTasks(
	ctx context.Context,
	request *adminservice.GetDLQTasksRequest,
	opts ...grpc.CallOption,
) (*adminservice.GetDLQTasksResponse, error) {
	var resp *adminservice.GetDLQTasksResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.GetDLQTasks(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) GetNamespace(
	ctx context.Context,
	request *adminservice.GetNamespaceRequest,
	opts ...grpc.CallOption,
) (*adminservice.GetNamespaceResponse, error) {
	var resp *adminservice.GetNamespaceResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.GetNamespace(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) GetNamespaceReplicationMessages(
	ctx context.Context,
	request *adminservice.GetNamespaceReplicationMessagesRequest,
	opts ...grpc.CallOption,
) (*adminservice.GetNamespaceReplicationMessagesResponse, error) {
	var resp *adminservice.GetNamespaceReplicationMessagesResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.GetNamespaceReplicationMessages(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) GetReplicationMessages(
	ctx context.Context,
	request *adminservice.GetReplicationMessagesRequest,
	opts ...grpc.CallOption,
) (*adminservice.GetReplicationMessagesResponse, error) {
	var resp *adminservice.GetReplicationMessagesResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.GetReplicationMessages(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) GetSearchAttributes(
	ctx context.Context,
	request *adminservice.GetSearchAttributesRequest,
	opts ...grpc.CallOption,
) (*adminservice.GetSearchAttributesResponse, error) {
	var resp *adminservice.GetSearchAttributesResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.GetSearchAttributes(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) GetShard(
	ctx context.Context,
	request *adminservice.GetShardRequest,
	opts ...grpc.CallOption,
) (*adminservice.GetShardResponse, error) {
	var resp *adminservice.GetShardResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.GetShard(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) GetTaskQueueTasks(
	ctx context.Context,
	request *adminservice.GetTaskQueueTasksRequest,
	opts ...grpc.CallOption,
) (*adminservice.GetTaskQueueTasksResponse, error) {
	var resp *adminservice.GetTaskQueueTasksResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.GetTaskQueueTasks(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) GetWorkflowExecutionRawHistory(
	ctx context.Context,
	request *adminservice.GetWorkflowExecutionRawHistoryRequest,
	opts ...grpc.CallOption,
) (*adminservice.GetWorkflowExecutionRawHistoryResponse, error) {
	var resp *adminservice.GetWorkflowExecutionRawHistoryResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.GetWorkflowExecutionRawHistory(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) GetWorkflowExecutionRawHistoryV2(
	ctx context.Context,
	request *adminservice.GetWorkflowExecutionRawHistoryV2Request,
	opts ...grpc.CallOption,
) (*adminservice.GetWorkflowExecutionRawHistoryV2Response, error) {
	var resp *adminservice.GetWorkflowExecutionRawHistoryV2Response
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.GetWorkflowExecutionRawHistoryV2(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) ImportWorkflowExecution(
	ctx context.Context,
	request *adminservice.ImportWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (*adminservice.ImportWorkflowExecutionResponse, error) {
	var resp *adminservice.ImportWorkflowExecutionResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.ImportWorkflowExecution(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) ListClusterMembers(
	ctx context.Context,
	request *adminservice.ListClusterMembersRequest,
	opts ...grpc.CallOption,
) (*adminservice.ListClusterMembersResponse, error) {
	var resp *adminservice.ListClusterMembersResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.ListClusterMembers(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) ListClusters(
	ctx context.Context,
	request *adminservice.ListClustersRequest,
	opts ...grpc.CallOption,
) (*adminservice.ListClustersResponse, error) {
	var resp *adminservice.ListClustersResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.ListClusters(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) ListHistoryTasks(
	ctx context.Context,
	request *adminservice.ListHistoryTasksRequest,
	opts ...grpc.CallOption,
) (*adminservice.ListHistoryTasksResponse, error) {
	var resp *adminservice.ListHistoryTasksResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.ListHistoryTasks(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) ListQueues(
	ctx context.Context,
	request *adminservice.ListQueuesRequest,
	opts ...grpc.CallOption,
) (*adminservice.ListQueuesResponse, error) {
	var resp *adminservice.ListQueuesResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.ListQueues(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) MergeDLQMessages(
	ctx context.Context,
	request *adminservice.MergeDLQMessagesRequest,
	opts ...grpc.CallOption,
) (*adminservice.MergeDLQMessagesResponse, error) {
	var resp *adminservice.MergeDLQMessagesResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.MergeDLQMessages(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) MergeDLQTasks(
	ctx context.Context,
	request *adminservice.MergeDLQTasksRequest,
	opts ...grpc.CallOption,
) (*adminservice.MergeDLQTasksResponse, error) {
	var resp *adminservice.MergeDLQTasksResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.MergeDLQTasks(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) PurgeDLQMessages(
	ctx context.Context,
	request *adminservice.PurgeDLQMessagesRequest,
	opts ...grpc.CallOption,
) (*adminservice.PurgeDLQMessagesResponse, error) {
	var resp *adminservice.PurgeDLQMessagesResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.PurgeDLQMessages(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) PurgeDLQTasks(
	ctx context.Context,
	request *adminservice.PurgeDLQTasksRequest,
	opts ...grpc.CallOption,
) (*adminservice.PurgeDLQTasksResponse, error) {
	var resp *adminservice.PurgeDLQTasksResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.PurgeDLQTasks(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) ReapplyEvents(
	ctx context.Context,
	request *adminservice.ReapplyEventsRequest,
	opts ...grpc.CallOption,
) (*adminservice.ReapplyEventsResponse, error) {
	var resp *adminservice.ReapplyEventsResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.ReapplyEvents(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) RebuildMutableState(
	ctx context.Context,
	request *adminservice.RebuildMutableStateRequest,
	opts ...grpc.CallOption,
) (*adminservice.RebuildMutableStateResponse, error) {
	var resp *adminservice.RebuildMutableStateResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.RebuildMutableState(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) RefreshWorkflowTasks(
	ctx context.Context,
	request *adminservice.RefreshWorkflowTasksRequest,
	opts ...grpc.CallOption,
) (*adminservice.RefreshWorkflowTasksResponse, error) {
	var resp *adminservice.RefreshWorkflowTasksResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.RefreshWorkflowTasks(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) RemoveRemoteCluster(
	ctx context.Context,
	request *adminservice.RemoveRemoteClusterRequest,
	opts ...grpc.CallOption,
) (*adminservice.RemoveRemoteClusterResponse, error) {
	var resp *adminservice.RemoveRemoteClusterResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.RemoveRemoteCluster(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) RemoveSearchAttributes(
	ctx context.Context,
	request *adminservice.RemoveSearchAttributesRequest,
	opts ...grpc.CallOption,
) (*adminservice.RemoveSearchAttributesResponse, error) {
	var resp *adminservice.RemoveSearchAttributesResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.RemoveSearchAttributes(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) RemoveTask(
	ctx context.Context,
	request *adminservice.RemoveTaskRequest,
	opts ...grpc.CallOption,
) (*adminservice.RemoveTaskResponse, error) {
	var resp *adminservice.RemoveTaskResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.RemoveTask(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) ResendReplicationTasks(
	ctx context.Context,
	request *adminservice.ResendReplicationTasksRequest,
	opts ...grpc.CallOption,
) (*adminservice.ResendReplicationTasksResponse, error) {
	var resp *adminservice.ResendReplicationTasksResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.ResendReplicationTasks(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) SyncWorkflowState(
	ctx context.Context,
	request *adminservice.SyncWorkflowStateRequest,
	opts ...grpc.CallOption,
) (*adminservice.SyncWorkflowStateResponse, error) {
	var resp *adminservice.SyncWorkflowStateResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.SyncWorkflowState(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}
