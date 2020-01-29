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

var _ ClientGRPC = (*metricClientGRPC)(nil)

type metricClientGRPC struct {
	client        ClientGRPC
	metricsClient metrics.Client
}

// NewMetricClientGRPC creates a new instance of Client that emits metrics
func NewMetricClientGRPC(client ClientGRPC, metricsClient metrics.Client) ClientGRPC {
	return &metricClientGRPC{
		client:        client,
		metricsClient: metricsClient,
	}
}

func (c *metricClientGRPC) DeprecateDomain(
	ctx context.Context,
	request *workflowservice.DeprecateDomainRequest,
	opts ...grpc.CallOption,
) (*workflowservice.DeprecateDomainResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientDeprecateDomainScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientDeprecateDomainScope, metrics.CadenceClientLatency)
	resp, err := c.client.DeprecateDomain(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientDeprecateDomainScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClientGRPC) DescribeDomain(
	ctx context.Context,
	request *workflowservice.DescribeDomainRequest,
	opts ...grpc.CallOption,
) (*workflowservice.DescribeDomainResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientDescribeDomainScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientDescribeDomainScope, metrics.CadenceClientLatency)
	resp, err := c.client.DescribeDomain(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientDescribeDomainScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClientGRPC) DescribeTaskList(
	ctx context.Context,
	request *workflowservice.DescribeTaskListRequest,
	opts ...grpc.CallOption,
) (*workflowservice.DescribeTaskListResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientDescribeTaskListScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientDescribeTaskListScope, metrics.CadenceClientLatency)
	resp, err := c.client.DescribeTaskList(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientDescribeTaskListScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClientGRPC) DescribeWorkflowExecution(
	ctx context.Context,
	request *workflowservice.DescribeWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (*workflowservice.DescribeWorkflowExecutionResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientDescribeWorkflowExecutionScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientDescribeWorkflowExecutionScope, metrics.CadenceClientLatency)
	resp, err := c.client.DescribeWorkflowExecution(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientDescribeWorkflowExecutionScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClientGRPC) GetWorkflowExecutionHistory(
	ctx context.Context,
	request *workflowservice.GetWorkflowExecutionHistoryRequest,
	opts ...grpc.CallOption,
) (*workflowservice.GetWorkflowExecutionHistoryResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientGetWorkflowExecutionHistoryScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientGetWorkflowExecutionHistoryScope, metrics.CadenceClientLatency)
	resp, err := c.client.GetWorkflowExecutionHistory(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientGetWorkflowExecutionHistoryScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClientGRPC) GetWorkflowExecutionRawHistory(
	ctx context.Context,
	request *workflowservice.GetWorkflowExecutionRawHistoryRequest,
	opts ...grpc.CallOption,
) (*workflowservice.GetWorkflowExecutionRawHistoryResponse, error) {
	c.metricsClient.IncCounter(metrics.FrontendClientGetWorkflowExecutionRawHistoryScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientGetWorkflowExecutionRawHistoryScope, metrics.CadenceClientLatency)
	resp, err := c.client.GetWorkflowExecutionRawHistory(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientGetWorkflowExecutionRawHistoryScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClientGRPC) ListArchivedWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.ListArchivedWorkflowExecutionsRequest,
	opts ...grpc.CallOption,
) (*workflowservice.ListArchivedWorkflowExecutionsResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientListArchivedWorkflowExecutionsScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientListArchivedWorkflowExecutionsScope, metrics.CadenceClientLatency)
	resp, err := c.client.ListArchivedWorkflowExecutions(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientListArchivedWorkflowExecutionsScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClientGRPC) ListClosedWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.ListClosedWorkflowExecutionsRequest,
	opts ...grpc.CallOption,
) (*workflowservice.ListClosedWorkflowExecutionsResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientListClosedWorkflowExecutionsScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientListClosedWorkflowExecutionsScope, metrics.CadenceClientLatency)
	resp, err := c.client.ListClosedWorkflowExecutions(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientListClosedWorkflowExecutionsScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClientGRPC) ListDomains(
	ctx context.Context,
	request *workflowservice.ListDomainsRequest,
	opts ...grpc.CallOption,
) (*workflowservice.ListDomainsResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientListDomainsScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientListDomainsScope, metrics.CadenceClientLatency)
	resp, err := c.client.ListDomains(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientListDomainsScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClientGRPC) ListOpenWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.ListOpenWorkflowExecutionsRequest,
	opts ...grpc.CallOption,
) (*workflowservice.ListOpenWorkflowExecutionsResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientListOpenWorkflowExecutionsScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientListOpenWorkflowExecutionsScope, metrics.CadenceClientLatency)
	resp, err := c.client.ListOpenWorkflowExecutions(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientListOpenWorkflowExecutionsScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClientGRPC) ListWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.ListWorkflowExecutionsRequest,
	opts ...grpc.CallOption,
) (*workflowservice.ListWorkflowExecutionsResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientListWorkflowExecutionsScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientListWorkflowExecutionsScope, metrics.CadenceClientLatency)
	resp, err := c.client.ListWorkflowExecutions(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientListWorkflowExecutionsScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClientGRPC) ScanWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.ScanWorkflowExecutionsRequest,
	opts ...grpc.CallOption,
) (*workflowservice.ScanWorkflowExecutionsResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientScanWorkflowExecutionsScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientScanWorkflowExecutionsScope, metrics.CadenceClientLatency)
	resp, err := c.client.ScanWorkflowExecutions(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientScanWorkflowExecutionsScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClientGRPC) CountWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.CountWorkflowExecutionsRequest,
	opts ...grpc.CallOption,
) (*workflowservice.CountWorkflowExecutionsResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientCountWorkflowExecutionsScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientCountWorkflowExecutionsScope, metrics.CadenceClientLatency)
	resp, err := c.client.CountWorkflowExecutions(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientCountWorkflowExecutionsScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClientGRPC) GetSearchAttributes(
	ctx context.Context,
	request *workflowservice.GetSearchAttributesRequest,
	opts ...grpc.CallOption,
) (*workflowservice.GetSearchAttributesResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientGetSearchAttributesScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientGetSearchAttributesScope, metrics.CadenceClientLatency)
	resp, err := c.client.GetSearchAttributes(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientGetSearchAttributesScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClientGRPC) PollForActivityTask(
	ctx context.Context,
	request *workflowservice.PollForActivityTaskRequest,
	opts ...grpc.CallOption,
) (*workflowservice.PollForActivityTaskResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientPollForActivityTaskScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientPollForActivityTaskScope, metrics.CadenceClientLatency)
	resp, err := c.client.PollForActivityTask(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientPollForActivityTaskScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClientGRPC) PollForDecisionTask(
	ctx context.Context,
	request *workflowservice.PollForDecisionTaskRequest,
	opts ...grpc.CallOption,
) (*workflowservice.PollForDecisionTaskResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientPollForDecisionTaskScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientPollForDecisionTaskScope, metrics.CadenceClientLatency)
	resp, err := c.client.PollForDecisionTask(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientPollForDecisionTaskScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClientGRPC) QueryWorkflow(
	ctx context.Context,
	request *workflowservice.QueryWorkflowRequest,
	opts ...grpc.CallOption,
) (*workflowservice.QueryWorkflowResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientQueryWorkflowScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientQueryWorkflowScope, metrics.CadenceClientLatency)
	resp, err := c.client.QueryWorkflow(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientQueryWorkflowScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClientGRPC) RecordActivityTaskHeartbeat(
	ctx context.Context,
	request *workflowservice.RecordActivityTaskHeartbeatRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RecordActivityTaskHeartbeatResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientRecordActivityTaskHeartbeatScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientRecordActivityTaskHeartbeatScope, metrics.CadenceClientLatency)
	resp, err := c.client.RecordActivityTaskHeartbeat(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientRecordActivityTaskHeartbeatScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClientGRPC) RecordActivityTaskHeartbeatByID(
	ctx context.Context,
	request *workflowservice.RecordActivityTaskHeartbeatByIDRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RecordActivityTaskHeartbeatByIDResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientRecordActivityTaskHeartbeatByIDScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientRecordActivityTaskHeartbeatByIDScope, metrics.CadenceClientLatency)
	resp, err := c.client.RecordActivityTaskHeartbeatByID(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientRecordActivityTaskHeartbeatByIDScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClientGRPC) RegisterDomain(
	ctx context.Context,
	request *workflowservice.RegisterDomainRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RegisterDomainResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientRegisterDomainScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientRegisterDomainScope, metrics.CadenceClientLatency)
	resp, err := c.client.RegisterDomain(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientRegisterDomainScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClientGRPC) RequestCancelWorkflowExecution(
	ctx context.Context,
	request *workflowservice.RequestCancelWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RequestCancelWorkflowExecutionResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientRequestCancelWorkflowExecutionScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientRequestCancelWorkflowExecutionScope, metrics.CadenceClientLatency)
	resp, err := c.client.RequestCancelWorkflowExecution(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientRequestCancelWorkflowExecutionScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClientGRPC) ResetStickyTaskList(
	ctx context.Context,
	request *workflowservice.ResetStickyTaskListRequest,
	opts ...grpc.CallOption,
) (*workflowservice.ResetStickyTaskListResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientResetStickyTaskListScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientResetStickyTaskListScope, metrics.CadenceClientLatency)
	resp, err := c.client.ResetStickyTaskList(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientResetStickyTaskListScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClientGRPC) ResetWorkflowExecution(
	ctx context.Context,
	request *workflowservice.ResetWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (*workflowservice.ResetWorkflowExecutionResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientResetWorkflowExecutionScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientResetWorkflowExecutionScope, metrics.CadenceClientLatency)
	resp, err := c.client.ResetWorkflowExecution(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientResetWorkflowExecutionScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClientGRPC) RespondActivityTaskCanceled(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskCanceledRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RespondActivityTaskCanceledResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientRespondActivityTaskCanceledScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientRespondActivityTaskCanceledScope, metrics.CadenceClientLatency)
	resp, err := c.client.RespondActivityTaskCanceled(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientRespondActivityTaskCanceledScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClientGRPC) RespondActivityTaskCanceledByID(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskCanceledByIDRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RespondActivityTaskCanceledByIDResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientRespondActivityTaskCanceledByIDScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientRespondActivityTaskCanceledByIDScope, metrics.CadenceClientLatency)
	resp, err := c.client.RespondActivityTaskCanceledByID(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientRespondActivityTaskCanceledByIDScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClientGRPC) RespondActivityTaskCompleted(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskCompletedRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RespondActivityTaskCompletedResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientRespondActivityTaskCompletedScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientRespondActivityTaskCompletedScope, metrics.CadenceClientLatency)
	resp, err := c.client.RespondActivityTaskCompleted(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientRespondActivityTaskCompletedScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClientGRPC) RespondActivityTaskCompletedByID(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskCompletedByIDRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RespondActivityTaskCompletedByIDResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientRespondActivityTaskCompletedByIDScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientRespondActivityTaskCompletedByIDScope, metrics.CadenceClientLatency)
	resp, err := c.client.RespondActivityTaskCompletedByID(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientRespondActivityTaskCompletedByIDScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClientGRPC) RespondActivityTaskFailed(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskFailedRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RespondActivityTaskFailedResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientRespondActivityTaskFailedScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientRespondActivityTaskFailedScope, metrics.CadenceClientLatency)
	resp, err := c.client.RespondActivityTaskFailed(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientRespondActivityTaskFailedScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClientGRPC) RespondActivityTaskFailedByID(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskFailedByIDRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RespondActivityTaskFailedByIDResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientRespondActivityTaskFailedByIDScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientRespondActivityTaskFailedByIDScope, metrics.CadenceClientLatency)
	resp, err := c.client.RespondActivityTaskFailedByID(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientRespondActivityTaskFailedByIDScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClientGRPC) RespondDecisionTaskCompleted(
	ctx context.Context,
	request *workflowservice.RespondDecisionTaskCompletedRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RespondDecisionTaskCompletedResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientRespondDecisionTaskCompletedScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientRespondDecisionTaskCompletedScope, metrics.CadenceClientLatency)
	resp, err := c.client.RespondDecisionTaskCompleted(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientRespondDecisionTaskCompletedScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClientGRPC) RespondDecisionTaskFailed(
	ctx context.Context,
	request *workflowservice.RespondDecisionTaskFailedRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RespondDecisionTaskFailedResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientRespondDecisionTaskFailedScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientRespondDecisionTaskFailedScope, metrics.CadenceClientLatency)
	resp, err := c.client.RespondDecisionTaskFailed(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientRespondDecisionTaskFailedScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClientGRPC) RespondQueryTaskCompleted(
	ctx context.Context,
	request *workflowservice.RespondQueryTaskCompletedRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RespondQueryTaskCompletedResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientRespondQueryTaskCompletedScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientRespondQueryTaskCompletedScope, metrics.CadenceClientLatency)
	resp, err := c.client.RespondQueryTaskCompleted(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientRespondQueryTaskCompletedScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClientGRPC) SignalWithStartWorkflowExecution(
	ctx context.Context,
	request *workflowservice.SignalWithStartWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (*workflowservice.SignalWithStartWorkflowExecutionResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientSignalWithStartWorkflowExecutionScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientSignalWithStartWorkflowExecutionScope, metrics.CadenceClientLatency)
	resp, err := c.client.SignalWithStartWorkflowExecution(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientSignalWithStartWorkflowExecutionScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClientGRPC) SignalWorkflowExecution(
	ctx context.Context,
	request *workflowservice.SignalWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (*workflowservice.SignalWorkflowExecutionResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientSignalWorkflowExecutionScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientSignalWorkflowExecutionScope, metrics.CadenceClientLatency)
	resp, err := c.client.SignalWorkflowExecution(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientSignalWorkflowExecutionScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClientGRPC) StartWorkflowExecution(
	ctx context.Context,
	request *workflowservice.StartWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (*workflowservice.StartWorkflowExecutionResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientStartWorkflowExecutionScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientStartWorkflowExecutionScope, metrics.CadenceClientLatency)
	resp, err := c.client.StartWorkflowExecution(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientStartWorkflowExecutionScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClientGRPC) TerminateWorkflowExecution(
	ctx context.Context,
	request *workflowservice.TerminateWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (*workflowservice.TerminateWorkflowExecutionResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientTerminateWorkflowExecutionScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientTerminateWorkflowExecutionScope, metrics.CadenceClientLatency)
	resp, err := c.client.TerminateWorkflowExecution(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientTerminateWorkflowExecutionScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClientGRPC) UpdateDomain(
	ctx context.Context,
	request *workflowservice.UpdateDomainRequest,
	opts ...grpc.CallOption,
) (*workflowservice.UpdateDomainResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientUpdateDomainScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientUpdateDomainScope, metrics.CadenceClientLatency)
	resp, err := c.client.UpdateDomain(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientUpdateDomainScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClientGRPC) GetClusterInfo(
	ctx context.Context,
	request *workflowservice.GetClusterInfoRequest,
	opts ...grpc.CallOption,
) (*workflowservice.GetClusterInfoResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientGetClusterInfoScope, metrics.CadenceClientRequests)
	sw := c.metricsClient.StartTimer(metrics.FrontendClientGetClusterInfoScope, metrics.CadenceClientLatency)
	resp, err := c.client.GetClusterInfo(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientGetClusterInfoScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClientGRPC) ListTaskListPartitions(
	ctx context.Context,
	request *workflowservice.ListTaskListPartitionsRequest,
	opts ...grpc.CallOption,
) (*workflowservice.ListTaskListPartitionsResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientListTaskListPartitionsScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientListTaskListPartitionsScope, metrics.CadenceClientLatency)
	resp, err := c.client.ListTaskListPartitions(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientListTaskListPartitionsScope, metrics.CadenceClientFailures)
	}
	return resp, err
}
