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

	"google.golang.org/grpc"

	"github.com/temporalio/temporal/.gen/proto/adminservice"
	"github.com/temporalio/temporal/common/metrics"
)

var _ Client = (*metricClient)(nil)

type metricClient struct {
	client        Client
	metricsClient metrics.Client
}

// NewMetricClient creates a new instance of Client that emits metrics
func NewMetricClient(client Client, metricsClient metrics.Client) Client {
	return &metricClient{
		client:        client,
		metricsClient: metricsClient,
	}
}

func (c *metricClient) AddSearchAttribute(
	ctx context.Context,
	request *adminservice.AddSearchAttributeRequest,
	opts ...grpc.CallOption,
) (*adminservice.AddSearchAttributeResponse, error) {

	c.metricsClient.IncCounter(metrics.AdminClientAddSearchAttributeScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.AdminClientAddSearchAttributeScope, metrics.CadenceClientLatency)
	resp, err := c.client.AddSearchAttribute(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.AdminClientAddSearchAttributeScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) DescribeHistoryHost(
	ctx context.Context,
	request *adminservice.DescribeHistoryHostRequest,
	opts ...grpc.CallOption,
) (*adminservice.DescribeHistoryHostResponse, error) {

	c.metricsClient.IncCounter(metrics.AdminClientDescribeHistoryHostScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.AdminClientDescribeHistoryHostScope, metrics.CadenceClientLatency)
	resp, err := c.client.DescribeHistoryHost(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.AdminClientDescribeHistoryHostScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) RemoveTask(
	ctx context.Context,
	request *adminservice.RemoveTaskRequest,
	opts ...grpc.CallOption,
) (*adminservice.RemoveTaskResponse, error) {

	c.metricsClient.IncCounter(metrics.AdminClientCloseShardScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.AdminClientCloseShardScope, metrics.CadenceClientLatency)
	resp, err := c.client.RemoveTask(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.AdminClientCloseShardScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) CloseShard(
	ctx context.Context,
	request *adminservice.CloseShardRequest,
	opts ...grpc.CallOption,
) (*adminservice.CloseShardResponse, error) {

	c.metricsClient.IncCounter(metrics.AdminClientCloseShardScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.AdminClientCloseShardScope, metrics.CadenceClientLatency)
	resp, err := c.client.CloseShard(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.AdminClientCloseShardScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) DescribeWorkflowExecution(
	ctx context.Context,
	request *adminservice.DescribeWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (*adminservice.DescribeWorkflowExecutionResponse, error) {

	c.metricsClient.IncCounter(metrics.AdminClientDescribeWorkflowExecutionScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.AdminClientDescribeWorkflowExecutionScope, metrics.CadenceClientLatency)
	resp, err := c.client.DescribeWorkflowExecution(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.AdminClientDescribeWorkflowExecutionScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) GetWorkflowExecutionRawHistory(
	ctx context.Context,
	request *adminservice.GetWorkflowExecutionRawHistoryRequest,
	opts ...grpc.CallOption,
) (*adminservice.GetWorkflowExecutionRawHistoryResponse, error) {

	c.metricsClient.IncCounter(metrics.AdminClientGetWorkflowExecutionRawHistoryScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.AdminClientGetWorkflowExecutionRawHistoryScope, metrics.CadenceClientLatency)
	resp, err := c.client.GetWorkflowExecutionRawHistory(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.AdminClientGetWorkflowExecutionRawHistoryScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) GetWorkflowExecutionRawHistoryV2(
	ctx context.Context,
	request *adminservice.GetWorkflowExecutionRawHistoryV2Request,
	opts ...grpc.CallOption,
) (*adminservice.GetWorkflowExecutionRawHistoryV2Response, error) {

	c.metricsClient.IncCounter(metrics.AdminClientGetWorkflowExecutionRawHistoryV2Scope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.AdminClientGetWorkflowExecutionRawHistoryV2Scope, metrics.CadenceClientLatency)
	resp, err := c.client.GetWorkflowExecutionRawHistoryV2(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.AdminClientGetWorkflowExecutionRawHistoryV2Scope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) DescribeCluster(
	ctx context.Context,
	request *adminservice.DescribeClusterRequest,
	opts ...grpc.CallOption,
) (*adminservice.DescribeClusterResponse, error) {

	c.metricsClient.IncCounter(metrics.AdminClientDescribeClusterScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.AdminClientDescribeClusterScope, metrics.CadenceClientLatency)
	resp, err := c.client.DescribeCluster(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.AdminClientDescribeClusterScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) GetReplicationMessages(
	ctx context.Context,
	request *adminservice.GetReplicationMessagesRequest,
	opts ...grpc.CallOption,
) (*adminservice.GetReplicationMessagesResponse, error) {
	c.metricsClient.IncCounter(metrics.FrontendClientGetReplicationTasksScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientGetReplicationTasksScope, metrics.CadenceClientLatency)
	resp, err := c.client.GetReplicationMessages(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientGetReplicationTasksScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) GetDomainReplicationMessages(
	ctx context.Context,
	request *adminservice.GetDomainReplicationMessagesRequest,
	opts ...grpc.CallOption,
) (*adminservice.GetDomainReplicationMessagesResponse, error) {
	c.metricsClient.IncCounter(metrics.FrontendClientGetDomainReplicationTasksScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientGetDomainReplicationTasksScope, metrics.CadenceClientLatency)
	resp, err := c.client.GetDomainReplicationMessages(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientGetDomainReplicationTasksScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) GetDLQReplicationMessages(
	ctx context.Context,
	request *adminservice.GetDLQReplicationMessagesRequest,
	opts ...grpc.CallOption,
) (*adminservice.GetDLQReplicationMessagesResponse, error) {
	c.metricsClient.IncCounter(metrics.FrontendClientGetDLQReplicationTasksScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientGetDLQReplicationTasksScope, metrics.CadenceClientLatency)
	resp, err := c.client.GetDLQReplicationMessages(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientGetDLQReplicationTasksScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) ReapplyEvents(
	ctx context.Context,
	request *adminservice.ReapplyEventsRequest,
	opts ...grpc.CallOption,
) (*adminservice.ReapplyEventsResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientReapplyEventsScope, metrics.CadenceClientRequests)
	sw := c.metricsClient.StartTimer(metrics.FrontendClientReapplyEventsScope, metrics.CadenceClientLatency)
	resp, err := c.client.ReapplyEvents(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientReapplyEventsScope, metrics.CadenceClientFailures)
	}
	return resp, err
}
