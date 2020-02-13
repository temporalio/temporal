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

	"go.uber.org/yarpc"

	"github.com/uber/cadence/.gen/go/admin"
	"github.com/uber/cadence/.gen/go/replicator"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/metrics"
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
	request *admin.AddSearchAttributeRequest,
	opts ...yarpc.CallOption,
) error {

	c.metricsClient.IncCounter(metrics.AdminClientAddSearchAttributeScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.AdminClientAddSearchAttributeScope, metrics.CadenceClientLatency)
	err := c.client.AddSearchAttribute(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.AdminClientAddSearchAttributeScope, metrics.CadenceClientFailures)
	}
	return err
}

func (c *metricClient) DescribeHistoryHost(
	ctx context.Context,
	request *shared.DescribeHistoryHostRequest,
	opts ...yarpc.CallOption,
) (*shared.DescribeHistoryHostResponse, error) {

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
	request *shared.RemoveTaskRequest,
	opts ...yarpc.CallOption,
) error {

	c.metricsClient.IncCounter(metrics.AdminClientCloseShardScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.AdminClientCloseShardScope, metrics.CadenceClientLatency)
	err := c.client.RemoveTask(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.AdminClientCloseShardScope, metrics.CadenceClientFailures)
	}
	return err
}

func (c *metricClient) CloseShard(
	ctx context.Context,
	request *shared.CloseShardRequest,
	opts ...yarpc.CallOption,
) error {

	c.metricsClient.IncCounter(metrics.AdminClientCloseShardScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.AdminClientCloseShardScope, metrics.CadenceClientLatency)
	err := c.client.CloseShard(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.AdminClientCloseShardScope, metrics.CadenceClientFailures)
	}
	return err
}

func (c *metricClient) DescribeWorkflowExecution(
	ctx context.Context,
	request *admin.DescribeWorkflowExecutionRequest,
	opts ...yarpc.CallOption,
) (*admin.DescribeWorkflowExecutionResponse, error) {

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
	request *admin.GetWorkflowExecutionRawHistoryRequest,
	opts ...yarpc.CallOption,
) (*admin.GetWorkflowExecutionRawHistoryResponse, error) {

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
	request *admin.GetWorkflowExecutionRawHistoryV2Request,
	opts ...yarpc.CallOption,
) (*admin.GetWorkflowExecutionRawHistoryV2Response, error) {

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
	opts ...yarpc.CallOption,
) (*admin.DescribeClusterResponse, error) {

	c.metricsClient.IncCounter(metrics.AdminClientDescribeClusterScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.AdminClientDescribeClusterScope, metrics.CadenceClientLatency)
	resp, err := c.client.DescribeCluster(ctx, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.AdminClientDescribeClusterScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) GetReplicationMessages(
	ctx context.Context,
	request *replicator.GetReplicationMessagesRequest,
	opts ...yarpc.CallOption,
) (*replicator.GetReplicationMessagesResponse, error) {
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
	request *replicator.GetDomainReplicationMessagesRequest,
	opts ...yarpc.CallOption,
) (*replicator.GetDomainReplicationMessagesResponse, error) {
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
	request *replicator.GetDLQReplicationMessagesRequest,
	opts ...yarpc.CallOption,
) (*replicator.GetDLQReplicationMessagesResponse, error) {
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
	request *shared.ReapplyEventsRequest,
	opts ...yarpc.CallOption,
) error {

	c.metricsClient.IncCounter(metrics.FrontendClientReapplyEventsScope, metrics.CadenceClientRequests)
	sw := c.metricsClient.StartTimer(metrics.FrontendClientReapplyEventsScope, metrics.CadenceClientLatency)
	err := c.client.ReapplyEvents(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientReapplyEventsScope, metrics.CadenceClientFailures)
	}
	return err
}

func (c *metricClient) ReadDLQMessages(
	ctx context.Context,
	request *replicator.ReadDLQMessagesRequest,
	opts ...yarpc.CallOption,
) (*replicator.ReadDLQMessagesResponse, error) {

	c.metricsClient.IncCounter(metrics.AdminClientReadDLQMessagesScope, metrics.CadenceClientRequests)
	sw := c.metricsClient.StartTimer(metrics.AdminClientReadDLQMessagesScope, metrics.CadenceClientLatency)
	resp, err := c.client.ReadDLQMessages(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.AdminClientReadDLQMessagesScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) PurgeDLQMessages(
	ctx context.Context,
	request *replicator.PurgeDLQMessagesRequest,
	opts ...yarpc.CallOption,
) error {

	c.metricsClient.IncCounter(metrics.AdminClientPurgeDLQMessagesScope, metrics.CadenceClientRequests)
	sw := c.metricsClient.StartTimer(metrics.AdminClientPurgeDLQMessagesScope, metrics.CadenceClientLatency)
	err := c.client.PurgeDLQMessages(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.AdminClientPurgeDLQMessagesScope, metrics.CadenceClientFailures)
	}
	return err
}

func (c *metricClient) MergeDLQMessages(
	ctx context.Context,
	request *replicator.MergeDLQMessagesRequest,
	opts ...yarpc.CallOption,
) (*replicator.MergeDLQMessagesResponse, error) {

	c.metricsClient.IncCounter(metrics.AdminClientMergeDLQMessagesScope, metrics.CadenceClientRequests)
	sw := c.metricsClient.StartTimer(metrics.AdminClientMergeDLQMessagesScope, metrics.CadenceClientLatency)
	resp, err := c.client.MergeDLQMessages(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.AdminClientMergeDLQMessagesScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) RefreshWorkflowTasks(
	ctx context.Context,
	request *shared.RefreshWorkflowTasksRequest,
	opts ...yarpc.CallOption,
) error {

	c.metricsClient.IncCounter(metrics.AdminClientRefreshWorkflowTasksScope, metrics.CadenceClientRequests)
	sw := c.metricsClient.StartTimer(metrics.AdminClientRefreshWorkflowTasksScope, metrics.CadenceClientLatency)
	err := c.client.RefreshWorkflowTasks(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.AdminClientRefreshWorkflowTasksScope, metrics.CadenceClientFailures)
	}
	return err
}
