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

package admin

import (
	"context"

	"google.golang.org/grpc"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common/metrics"
)

var _ adminservice.AdminServiceClient = (*metricClient)(nil)

type metricClient struct {
	client        adminservice.AdminServiceClient
	metricsClient metrics.Client
}

// NewMetricClient creates a new instance of adminservice.AdminServiceClient that emits metrics
func NewMetricClient(client adminservice.AdminServiceClient, metricsClient metrics.Client) adminservice.AdminServiceClient {
	return &metricClient{
		client:        client,
		metricsClient: metricsClient,
	}
}

func (c *metricClient) AddSearchAttributes(
	ctx context.Context,
	request *adminservice.AddSearchAttributesRequest,
	opts ...grpc.CallOption,
) (*adminservice.AddSearchAttributesResponse, error) {

	c.metricsClient.IncCounter(metrics.AdminClientAddSearchAttributesScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.AdminClientAddSearchAttributesScope, metrics.ClientLatency)
	resp, err := c.client.AddSearchAttributes(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.AdminClientAddSearchAttributesScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) RemoveSearchAttributes(
	ctx context.Context,
	request *adminservice.RemoveSearchAttributesRequest,
	opts ...grpc.CallOption,
) (*adminservice.RemoveSearchAttributesResponse, error) {

	c.metricsClient.IncCounter(metrics.AdminClientRemoveSearchAttributesScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.AdminClientRemoveSearchAttributesScope, metrics.ClientLatency)
	resp, err := c.client.RemoveSearchAttributes(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.AdminClientRemoveSearchAttributesScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) GetSearchAttributes(
	ctx context.Context,
	request *adminservice.GetSearchAttributesRequest,
	opts ...grpc.CallOption,
) (*adminservice.GetSearchAttributesResponse, error) {

	c.metricsClient.IncCounter(metrics.AdminClientGetSearchAttributesScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.AdminClientGetSearchAttributesScope, metrics.ClientLatency)
	resp, err := c.client.GetSearchAttributes(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.AdminClientGetSearchAttributesScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) DescribeHistoryHost(
	ctx context.Context,
	request *adminservice.DescribeHistoryHostRequest,
	opts ...grpc.CallOption,
) (*adminservice.DescribeHistoryHostResponse, error) {

	c.metricsClient.IncCounter(metrics.AdminClientDescribeHistoryHostScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.AdminClientDescribeHistoryHostScope, metrics.ClientLatency)
	resp, err := c.client.DescribeHistoryHost(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.AdminClientDescribeHistoryHostScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) RemoveTask(
	ctx context.Context,
	request *adminservice.RemoveTaskRequest,
	opts ...grpc.CallOption,
) (*adminservice.RemoveTaskResponse, error) {

	c.metricsClient.IncCounter(metrics.AdminClientCloseShardScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.AdminClientCloseShardScope, metrics.ClientLatency)
	resp, err := c.client.RemoveTask(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.AdminClientCloseShardScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) CloseShard(
	ctx context.Context,
	request *adminservice.CloseShardRequest,
	opts ...grpc.CallOption,
) (*adminservice.CloseShardResponse, error) {

	c.metricsClient.IncCounter(metrics.AdminClientCloseShardScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.AdminClientCloseShardScope, metrics.ClientLatency)
	resp, err := c.client.CloseShard(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.AdminClientCloseShardScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) GetShard(
	ctx context.Context,
	request *adminservice.GetShardRequest,
	opts ...grpc.CallOption,
) (*adminservice.GetShardResponse, error) {

	c.metricsClient.IncCounter(metrics.AdminClientGetShardScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.AdminClientGetShardScope, metrics.ClientLatency)
	resp, err := c.client.GetShard(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.AdminClientGetShardScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) ListTimerTasks(
	ctx context.Context,
	request *adminservice.ListTimerTasksRequest,
	opts ...grpc.CallOption,
) (*adminservice.ListTimerTasksResponse, error) {

	c.metricsClient.IncCounter(metrics.AdminClientListTimerTasksScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.AdminClientListTimerTasksScope, metrics.ClientLatency)
	resp, err := c.client.ListTimerTasks(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.AdminClientListTimerTasksScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) ListReplicationTasks(
	ctx context.Context,
	request *adminservice.ListReplicationTasksRequest,
	opts ...grpc.CallOption,
) (*adminservice.ListReplicationTasksResponse, error) {

	c.metricsClient.IncCounter(metrics.AdminClientListReplicationTasksScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.AdminClientListReplicationTasksScope, metrics.ClientLatency)
	resp, err := c.client.ListReplicationTasks(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.AdminClientListReplicationTasksScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) ListTransferTasks(
	ctx context.Context,
	request *adminservice.ListTransferTasksRequest,
	opts ...grpc.CallOption,
) (*adminservice.ListTransferTasksResponse, error) {

	c.metricsClient.IncCounter(metrics.AdminClientListTransferTasksScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.AdminClientListTransferTasksScope, metrics.ClientLatency)
	resp, err := c.client.ListTransferTasks(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.AdminClientListTransferTasksScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) ListVisibilityTasks(
	ctx context.Context,
	request *adminservice.ListVisibilityTasksRequest,
	opts ...grpc.CallOption,
) (*adminservice.ListVisibilityTasksResponse, error) {

	c.metricsClient.IncCounter(metrics.AdminClientListVisibilityTasksScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.AdminClientListVisibilityTasksScope, metrics.ClientLatency)
	resp, err := c.client.ListVisibilityTasks(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.AdminClientListVisibilityTasksScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) DescribeMutableState(
	ctx context.Context,
	request *adminservice.DescribeMutableStateRequest,
	opts ...grpc.CallOption,
) (*adminservice.DescribeMutableStateResponse, error) {

	c.metricsClient.IncCounter(metrics.AdminClientDescribeWorkflowMutableStateScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.AdminClientDescribeWorkflowMutableStateScope, metrics.ClientLatency)
	resp, err := c.client.DescribeMutableState(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.AdminClientDescribeWorkflowMutableStateScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) GetWorkflowExecutionRawHistoryV2(
	ctx context.Context,
	request *adminservice.GetWorkflowExecutionRawHistoryV2Request,
	opts ...grpc.CallOption,
) (*adminservice.GetWorkflowExecutionRawHistoryV2Response, error) {

	c.metricsClient.IncCounter(metrics.AdminClientGetWorkflowExecutionRawHistoryV2Scope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.AdminClientGetWorkflowExecutionRawHistoryV2Scope, metrics.ClientLatency)
	resp, err := c.client.GetWorkflowExecutionRawHistoryV2(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.AdminClientGetWorkflowExecutionRawHistoryV2Scope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) DescribeCluster(
	ctx context.Context,
	request *adminservice.DescribeClusterRequest,
	opts ...grpc.CallOption,
) (*adminservice.DescribeClusterResponse, error) {

	c.metricsClient.IncCounter(metrics.AdminClientDescribeClusterScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.AdminClientDescribeClusterScope, metrics.ClientLatency)
	resp, err := c.client.DescribeCluster(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.AdminClientDescribeClusterScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) ListClusterMembers(
	ctx context.Context,
	request *adminservice.ListClusterMembersRequest,
	opts ...grpc.CallOption,
) (*adminservice.ListClusterMembersResponse, error) {

	c.metricsClient.IncCounter(metrics.AdminClientListClusterMembersScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.AdminClientListClusterMembersScope, metrics.ClientLatency)
	resp, err := c.client.ListClusterMembers(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.AdminClientListClusterMembersScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) AddOrUpdateRemoteCluster(
	ctx context.Context,
	request *adminservice.AddOrUpdateRemoteClusterRequest,
	opts ...grpc.CallOption,
) (*adminservice.AddOrUpdateRemoteClusterResponse, error) {

	c.metricsClient.IncCounter(metrics.AdminClientAddOrUpdateRemoteClusterScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.AdminClientAddOrUpdateRemoteClusterScope, metrics.ClientLatency)
	resp, err := c.client.AddOrUpdateRemoteCluster(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.AdminClientAddOrUpdateRemoteClusterScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) RemoveRemoteCluster(
	ctx context.Context,
	request *adminservice.RemoveRemoteClusterRequest,
	opts ...grpc.CallOption,
) (*adminservice.RemoveRemoteClusterResponse, error) {

	c.metricsClient.IncCounter(metrics.AdminClientRemoveRemoteClusterScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.AdminClientRemoveRemoteClusterScope, metrics.ClientLatency)
	resp, err := c.client.RemoveRemoteCluster(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.AdminClientRemoveRemoteClusterScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) GetReplicationMessages(
	ctx context.Context,
	request *adminservice.GetReplicationMessagesRequest,
	opts ...grpc.CallOption,
) (*adminservice.GetReplicationMessagesResponse, error) {
	c.metricsClient.IncCounter(metrics.FrontendClientGetReplicationTasksScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientGetReplicationTasksScope, metrics.ClientLatency)
	resp, err := c.client.GetReplicationMessages(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientGetReplicationTasksScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) GetNamespaceReplicationMessages(
	ctx context.Context,
	request *adminservice.GetNamespaceReplicationMessagesRequest,
	opts ...grpc.CallOption,
) (*adminservice.GetNamespaceReplicationMessagesResponse, error) {
	c.metricsClient.IncCounter(metrics.FrontendClientGetNamespaceReplicationTasksScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientGetNamespaceReplicationTasksScope, metrics.ClientLatency)
	resp, err := c.client.GetNamespaceReplicationMessages(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientGetNamespaceReplicationTasksScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) GetDLQReplicationMessages(
	ctx context.Context,
	request *adminservice.GetDLQReplicationMessagesRequest,
	opts ...grpc.CallOption,
) (*adminservice.GetDLQReplicationMessagesResponse, error) {
	c.metricsClient.IncCounter(metrics.FrontendClientGetDLQReplicationTasksScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientGetDLQReplicationTasksScope, metrics.ClientLatency)
	resp, err := c.client.GetDLQReplicationMessages(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientGetDLQReplicationTasksScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) ReapplyEvents(
	ctx context.Context,
	request *adminservice.ReapplyEventsRequest,
	opts ...grpc.CallOption,
) (*adminservice.ReapplyEventsResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientReapplyEventsScope, metrics.ClientRequests)
	sw := c.metricsClient.StartTimer(metrics.FrontendClientReapplyEventsScope, metrics.ClientLatency)
	resp, err := c.client.ReapplyEvents(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientReapplyEventsScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) GetDLQMessages(
	ctx context.Context,
	request *adminservice.GetDLQMessagesRequest,
	opts ...grpc.CallOption,
) (*adminservice.GetDLQMessagesResponse, error) {

	c.metricsClient.IncCounter(metrics.AdminClientGetDLQMessagesScope, metrics.ClientRequests)
	sw := c.metricsClient.StartTimer(metrics.AdminClientGetDLQMessagesScope, metrics.ClientLatency)
	resp, err := c.client.GetDLQMessages(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.AdminClientGetDLQMessagesScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) PurgeDLQMessages(
	ctx context.Context,
	request *adminservice.PurgeDLQMessagesRequest,
	opts ...grpc.CallOption,
) (*adminservice.PurgeDLQMessagesResponse, error) {

	c.metricsClient.IncCounter(metrics.AdminClientPurgeDLQMessagesScope, metrics.ClientRequests)
	sw := c.metricsClient.StartTimer(metrics.AdminClientPurgeDLQMessagesScope, metrics.ClientLatency)
	resp, err := c.client.PurgeDLQMessages(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.AdminClientPurgeDLQMessagesScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) MergeDLQMessages(
	ctx context.Context,
	request *adminservice.MergeDLQMessagesRequest,
	opts ...grpc.CallOption,
) (*adminservice.MergeDLQMessagesResponse, error) {

	c.metricsClient.IncCounter(metrics.AdminClientMergeDLQMessagesScope, metrics.ClientRequests)
	sw := c.metricsClient.StartTimer(metrics.AdminClientMergeDLQMessagesScope, metrics.ClientLatency)
	resp, err := c.client.MergeDLQMessages(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.AdminClientMergeDLQMessagesScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) RefreshWorkflowTasks(
	ctx context.Context,
	request *adminservice.RefreshWorkflowTasksRequest,
	opts ...grpc.CallOption,
) (*adminservice.RefreshWorkflowTasksResponse, error) {

	c.metricsClient.IncCounter(metrics.AdminClientRefreshWorkflowTasksScope, metrics.ClientRequests)
	sw := c.metricsClient.StartTimer(metrics.AdminClientRefreshWorkflowTasksScope, metrics.ClientLatency)
	resp, err := c.client.RefreshWorkflowTasks(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.AdminClientRefreshWorkflowTasksScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) ResendReplicationTasks(
	ctx context.Context,
	request *adminservice.ResendReplicationTasksRequest,
	opts ...grpc.CallOption,
) (*adminservice.ResendReplicationTasksResponse, error) {

	c.metricsClient.IncCounter(metrics.AdminClientResendReplicationTasksScope, metrics.ClientRequests)
	sw := c.metricsClient.StartTimer(metrics.AdminClientResendReplicationTasksScope, metrics.ClientLatency)
	resp, err := c.client.ResendReplicationTasks(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.AdminClientResendReplicationTasksScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) GetTaskQueueTasks(
	ctx context.Context,
	request *adminservice.GetTaskQueueTasksRequest,
	opts ...grpc.CallOption,
) (*adminservice.GetTaskQueueTasksResponse, error) {

	c.metricsClient.IncCounter(metrics.AdminClientResendReplicationTasksScope, metrics.ClientRequests)
	sw := c.metricsClient.StartTimer(metrics.AdminClientResendReplicationTasksScope, metrics.ClientLatency)
	resp, err := c.client.GetTaskQueueTasks(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.AdminClientResendReplicationTasksScope, metrics.ClientFailures)
	}
	return resp, err
}
