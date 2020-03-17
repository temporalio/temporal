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

package frontend

import (
	"context"

	"go.temporal.io/temporal-proto/workflowservice"
	"google.golang.org/grpc"

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

func (c *metricClient) DeprecateDomain(
	ctx context.Context,
	request *workflowservice.DeprecateDomainRequest,
	opts ...grpc.CallOption,
) (*workflowservice.DeprecateDomainResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientDeprecateDomainScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientDeprecateDomainScope, metrics.ClientLatency)
	resp, err := c.client.DeprecateDomain(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientDeprecateDomainScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) DescribeDomain(
	ctx context.Context,
	request *workflowservice.DescribeDomainRequest,
	opts ...grpc.CallOption,
) (*workflowservice.DescribeDomainResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientDescribeDomainScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientDescribeDomainScope, metrics.ClientLatency)
	resp, err := c.client.DescribeDomain(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientDescribeDomainScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) DescribeTaskList(
	ctx context.Context,
	request *workflowservice.DescribeTaskListRequest,
	opts ...grpc.CallOption,
) (*workflowservice.DescribeTaskListResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientDescribeTaskListScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientDescribeTaskListScope, metrics.ClientLatency)
	resp, err := c.client.DescribeTaskList(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientDescribeTaskListScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) DescribeWorkflowExecution(
	ctx context.Context,
	request *workflowservice.DescribeWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (*workflowservice.DescribeWorkflowExecutionResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientDescribeWorkflowExecutionScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientDescribeWorkflowExecutionScope, metrics.ClientLatency)
	resp, err := c.client.DescribeWorkflowExecution(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientDescribeWorkflowExecutionScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) GetWorkflowExecutionHistory(
	ctx context.Context,
	request *workflowservice.GetWorkflowExecutionHistoryRequest,
	opts ...grpc.CallOption,
) (*workflowservice.GetWorkflowExecutionHistoryResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientGetWorkflowExecutionHistoryScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientGetWorkflowExecutionHistoryScope, metrics.ClientLatency)
	resp, err := c.client.GetWorkflowExecutionHistory(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientGetWorkflowExecutionHistoryScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) GetWorkflowExecutionRawHistory(
	ctx context.Context,
	request *workflowservice.GetWorkflowExecutionRawHistoryRequest,
	opts ...grpc.CallOption,
) (*workflowservice.GetWorkflowExecutionRawHistoryResponse, error) {
	c.metricsClient.IncCounter(metrics.FrontendClientGetWorkflowExecutionRawHistoryScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientGetWorkflowExecutionRawHistoryScope, metrics.ClientLatency)
	resp, err := c.client.GetWorkflowExecutionRawHistory(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientGetWorkflowExecutionRawHistoryScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) PollForWorkflowExecutionRawHistory(
	ctx context.Context,
	request *workflowservice.PollForWorkflowExecutionRawHistoryRequest,
	opts ...grpc.CallOption,
) (*workflowservice.PollForWorkflowExecutionRawHistoryResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientPollForWorkflowExecutionRawHistoryScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientPollForWorkflowExecutionRawHistoryScope, metrics.ClientLatency)
	resp, err := c.client.PollForWorkflowExecutionRawHistory(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientPollForWorkflowExecutionRawHistoryScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) ListArchivedWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.ListArchivedWorkflowExecutionsRequest,
	opts ...grpc.CallOption,
) (*workflowservice.ListArchivedWorkflowExecutionsResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientListArchivedWorkflowExecutionsScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientListArchivedWorkflowExecutionsScope, metrics.ClientLatency)
	resp, err := c.client.ListArchivedWorkflowExecutions(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientListArchivedWorkflowExecutionsScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) ListClosedWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.ListClosedWorkflowExecutionsRequest,
	opts ...grpc.CallOption,
) (*workflowservice.ListClosedWorkflowExecutionsResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientListClosedWorkflowExecutionsScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientListClosedWorkflowExecutionsScope, metrics.ClientLatency)
	resp, err := c.client.ListClosedWorkflowExecutions(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientListClosedWorkflowExecutionsScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) ListDomains(
	ctx context.Context,
	request *workflowservice.ListDomainsRequest,
	opts ...grpc.CallOption,
) (*workflowservice.ListDomainsResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientListDomainsScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientListDomainsScope, metrics.ClientLatency)
	resp, err := c.client.ListDomains(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientListDomainsScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) ListOpenWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.ListOpenWorkflowExecutionsRequest,
	opts ...grpc.CallOption,
) (*workflowservice.ListOpenWorkflowExecutionsResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientListOpenWorkflowExecutionsScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientListOpenWorkflowExecutionsScope, metrics.ClientLatency)
	resp, err := c.client.ListOpenWorkflowExecutions(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientListOpenWorkflowExecutionsScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) ListWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.ListWorkflowExecutionsRequest,
	opts ...grpc.CallOption,
) (*workflowservice.ListWorkflowExecutionsResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientListWorkflowExecutionsScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientListWorkflowExecutionsScope, metrics.ClientLatency)
	resp, err := c.client.ListWorkflowExecutions(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientListWorkflowExecutionsScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) ScanWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.ScanWorkflowExecutionsRequest,
	opts ...grpc.CallOption,
) (*workflowservice.ScanWorkflowExecutionsResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientScanWorkflowExecutionsScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientScanWorkflowExecutionsScope, metrics.ClientLatency)
	resp, err := c.client.ScanWorkflowExecutions(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientScanWorkflowExecutionsScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) CountWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.CountWorkflowExecutionsRequest,
	opts ...grpc.CallOption,
) (*workflowservice.CountWorkflowExecutionsResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientCountWorkflowExecutionsScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientCountWorkflowExecutionsScope, metrics.ClientLatency)
	resp, err := c.client.CountWorkflowExecutions(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientCountWorkflowExecutionsScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) GetSearchAttributes(
	ctx context.Context,
	request *workflowservice.GetSearchAttributesRequest,
	opts ...grpc.CallOption,
) (*workflowservice.GetSearchAttributesResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientGetSearchAttributesScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientGetSearchAttributesScope, metrics.ClientLatency)
	resp, err := c.client.GetSearchAttributes(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientGetSearchAttributesScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) PollForActivityTask(
	ctx context.Context,
	request *workflowservice.PollForActivityTaskRequest,
	opts ...grpc.CallOption,
) (*workflowservice.PollForActivityTaskResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientPollForActivityTaskScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientPollForActivityTaskScope, metrics.ClientLatency)
	resp, err := c.client.PollForActivityTask(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientPollForActivityTaskScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) PollForDecisionTask(
	ctx context.Context,
	request *workflowservice.PollForDecisionTaskRequest,
	opts ...grpc.CallOption,
) (*workflowservice.PollForDecisionTaskResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientPollForDecisionTaskScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientPollForDecisionTaskScope, metrics.ClientLatency)
	resp, err := c.client.PollForDecisionTask(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientPollForDecisionTaskScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) QueryWorkflow(
	ctx context.Context,
	request *workflowservice.QueryWorkflowRequest,
	opts ...grpc.CallOption,
) (*workflowservice.QueryWorkflowResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientQueryWorkflowScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientQueryWorkflowScope, metrics.ClientLatency)
	resp, err := c.client.QueryWorkflow(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientQueryWorkflowScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) RecordActivityTaskHeartbeat(
	ctx context.Context,
	request *workflowservice.RecordActivityTaskHeartbeatRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RecordActivityTaskHeartbeatResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientRecordActivityTaskHeartbeatScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientRecordActivityTaskHeartbeatScope, metrics.ClientLatency)
	resp, err := c.client.RecordActivityTaskHeartbeat(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientRecordActivityTaskHeartbeatScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) RecordActivityTaskHeartbeatByID(
	ctx context.Context,
	request *workflowservice.RecordActivityTaskHeartbeatByIDRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RecordActivityTaskHeartbeatByIDResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientRecordActivityTaskHeartbeatByIDScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientRecordActivityTaskHeartbeatByIDScope, metrics.ClientLatency)
	resp, err := c.client.RecordActivityTaskHeartbeatByID(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientRecordActivityTaskHeartbeatByIDScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) RegisterDomain(
	ctx context.Context,
	request *workflowservice.RegisterDomainRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RegisterDomainResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientRegisterDomainScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientRegisterDomainScope, metrics.ClientLatency)
	resp, err := c.client.RegisterDomain(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientRegisterDomainScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) RequestCancelWorkflowExecution(
	ctx context.Context,
	request *workflowservice.RequestCancelWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RequestCancelWorkflowExecutionResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientRequestCancelWorkflowExecutionScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientRequestCancelWorkflowExecutionScope, metrics.ClientLatency)
	resp, err := c.client.RequestCancelWorkflowExecution(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientRequestCancelWorkflowExecutionScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) ResetStickyTaskList(
	ctx context.Context,
	request *workflowservice.ResetStickyTaskListRequest,
	opts ...grpc.CallOption,
) (*workflowservice.ResetStickyTaskListResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientResetStickyTaskListScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientResetStickyTaskListScope, metrics.ClientLatency)
	resp, err := c.client.ResetStickyTaskList(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientResetStickyTaskListScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) ResetWorkflowExecution(
	ctx context.Context,
	request *workflowservice.ResetWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (*workflowservice.ResetWorkflowExecutionResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientResetWorkflowExecutionScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientResetWorkflowExecutionScope, metrics.ClientLatency)
	resp, err := c.client.ResetWorkflowExecution(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientResetWorkflowExecutionScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) RespondActivityTaskCanceled(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskCanceledRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RespondActivityTaskCanceledResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientRespondActivityTaskCanceledScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientRespondActivityTaskCanceledScope, metrics.ClientLatency)
	resp, err := c.client.RespondActivityTaskCanceled(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientRespondActivityTaskCanceledScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) RespondActivityTaskCanceledByID(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskCanceledByIDRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RespondActivityTaskCanceledByIDResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientRespondActivityTaskCanceledByIDScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientRespondActivityTaskCanceledByIDScope, metrics.ClientLatency)
	resp, err := c.client.RespondActivityTaskCanceledByID(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientRespondActivityTaskCanceledByIDScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) RespondActivityTaskCompleted(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskCompletedRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RespondActivityTaskCompletedResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientRespondActivityTaskCompletedScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientRespondActivityTaskCompletedScope, metrics.ClientLatency)
	resp, err := c.client.RespondActivityTaskCompleted(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientRespondActivityTaskCompletedScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) RespondActivityTaskCompletedByID(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskCompletedByIDRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RespondActivityTaskCompletedByIDResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientRespondActivityTaskCompletedByIDScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientRespondActivityTaskCompletedByIDScope, metrics.ClientLatency)
	resp, err := c.client.RespondActivityTaskCompletedByID(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientRespondActivityTaskCompletedByIDScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) RespondActivityTaskFailed(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskFailedRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RespondActivityTaskFailedResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientRespondActivityTaskFailedScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientRespondActivityTaskFailedScope, metrics.ClientLatency)
	resp, err := c.client.RespondActivityTaskFailed(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientRespondActivityTaskFailedScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) RespondActivityTaskFailedByID(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskFailedByIDRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RespondActivityTaskFailedByIDResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientRespondActivityTaskFailedByIDScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientRespondActivityTaskFailedByIDScope, metrics.ClientLatency)
	resp, err := c.client.RespondActivityTaskFailedByID(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientRespondActivityTaskFailedByIDScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) RespondDecisionTaskCompleted(
	ctx context.Context,
	request *workflowservice.RespondDecisionTaskCompletedRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RespondDecisionTaskCompletedResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientRespondDecisionTaskCompletedScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientRespondDecisionTaskCompletedScope, metrics.ClientLatency)
	resp, err := c.client.RespondDecisionTaskCompleted(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientRespondDecisionTaskCompletedScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) RespondDecisionTaskFailed(
	ctx context.Context,
	request *workflowservice.RespondDecisionTaskFailedRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RespondDecisionTaskFailedResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientRespondDecisionTaskFailedScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientRespondDecisionTaskFailedScope, metrics.ClientLatency)
	resp, err := c.client.RespondDecisionTaskFailed(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientRespondDecisionTaskFailedScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) RespondQueryTaskCompleted(
	ctx context.Context,
	request *workflowservice.RespondQueryTaskCompletedRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RespondQueryTaskCompletedResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientRespondQueryTaskCompletedScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientRespondQueryTaskCompletedScope, metrics.ClientLatency)
	resp, err := c.client.RespondQueryTaskCompleted(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientRespondQueryTaskCompletedScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) SignalWithStartWorkflowExecution(
	ctx context.Context,
	request *workflowservice.SignalWithStartWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (*workflowservice.SignalWithStartWorkflowExecutionResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientSignalWithStartWorkflowExecutionScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientSignalWithStartWorkflowExecutionScope, metrics.ClientLatency)
	resp, err := c.client.SignalWithStartWorkflowExecution(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientSignalWithStartWorkflowExecutionScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) SignalWorkflowExecution(
	ctx context.Context,
	request *workflowservice.SignalWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (*workflowservice.SignalWorkflowExecutionResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientSignalWorkflowExecutionScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientSignalWorkflowExecutionScope, metrics.ClientLatency)
	resp, err := c.client.SignalWorkflowExecution(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientSignalWorkflowExecutionScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) StartWorkflowExecution(
	ctx context.Context,
	request *workflowservice.StartWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (*workflowservice.StartWorkflowExecutionResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientStartWorkflowExecutionScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientStartWorkflowExecutionScope, metrics.ClientLatency)
	resp, err := c.client.StartWorkflowExecution(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientStartWorkflowExecutionScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) TerminateWorkflowExecution(
	ctx context.Context,
	request *workflowservice.TerminateWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (*workflowservice.TerminateWorkflowExecutionResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientTerminateWorkflowExecutionScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientTerminateWorkflowExecutionScope, metrics.ClientLatency)
	resp, err := c.client.TerminateWorkflowExecution(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientTerminateWorkflowExecutionScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) UpdateDomain(
	ctx context.Context,
	request *workflowservice.UpdateDomainRequest,
	opts ...grpc.CallOption,
) (*workflowservice.UpdateDomainResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientUpdateDomainScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientUpdateDomainScope, metrics.ClientLatency)
	resp, err := c.client.UpdateDomain(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientUpdateDomainScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) GetClusterInfo(
	ctx context.Context,
	request *workflowservice.GetClusterInfoRequest,
	opts ...grpc.CallOption,
) (*workflowservice.GetClusterInfoResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientGetClusterInfoScope, metrics.ClientRequests)
	sw := c.metricsClient.StartTimer(metrics.FrontendClientGetClusterInfoScope, metrics.ClientLatency)
	resp, err := c.client.GetClusterInfo(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientGetClusterInfoScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) ListTaskListPartitions(
	ctx context.Context,
	request *workflowservice.ListTaskListPartitionsRequest,
	opts ...grpc.CallOption,
) (*workflowservice.ListTaskListPartitionsResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientListTaskListPartitionsScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientListTaskListPartitionsScope, metrics.ClientLatency)
	resp, err := c.client.ListTaskListPartitions(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientListTaskListPartitionsScope, metrics.ClientFailures)
	}
	return resp, err
}
