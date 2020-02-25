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

	"go.uber.org/yarpc"

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

func (c *metricClient) DeprecateDomain(
	ctx context.Context,
	request *shared.DeprecateDomainRequest,
	opts ...yarpc.CallOption,
) error {

	c.metricsClient.IncCounter(metrics.FrontendClientDeprecateDomainScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientDeprecateDomainScope, metrics.CadenceClientLatency)
	err := c.client.DeprecateDomain(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientDeprecateDomainScope, metrics.CadenceClientFailures)
	}
	return err
}

func (c *metricClient) DescribeDomain(
	ctx context.Context,
	request *shared.DescribeDomainRequest,
	opts ...yarpc.CallOption,
) (*shared.DescribeDomainResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientDescribeDomainScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientDescribeDomainScope, metrics.CadenceClientLatency)
	resp, err := c.client.DescribeDomain(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientDescribeDomainScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) DescribeTaskList(
	ctx context.Context,
	request *shared.DescribeTaskListRequest,
	opts ...yarpc.CallOption,
) (*shared.DescribeTaskListResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientDescribeTaskListScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientDescribeTaskListScope, metrics.CadenceClientLatency)
	resp, err := c.client.DescribeTaskList(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientDescribeTaskListScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) DescribeWorkflowExecution(
	ctx context.Context,
	request *shared.DescribeWorkflowExecutionRequest,
	opts ...yarpc.CallOption,
) (*shared.DescribeWorkflowExecutionResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientDescribeWorkflowExecutionScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientDescribeWorkflowExecutionScope, metrics.CadenceClientLatency)
	resp, err := c.client.DescribeWorkflowExecution(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientDescribeWorkflowExecutionScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) GetWorkflowExecutionHistory(
	ctx context.Context,
	request *shared.GetWorkflowExecutionHistoryRequest,
	opts ...yarpc.CallOption,
) (*shared.GetWorkflowExecutionHistoryResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientGetWorkflowExecutionHistoryScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientGetWorkflowExecutionHistoryScope, metrics.CadenceClientLatency)
	resp, err := c.client.GetWorkflowExecutionHistory(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientGetWorkflowExecutionHistoryScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) GetWorkflowExecutionRawHistory(
	ctx context.Context,
	request *shared.GetWorkflowExecutionRawHistoryRequest,
	opts ...yarpc.CallOption,
) (*shared.GetWorkflowExecutionRawHistoryResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientGetWorkflowExecutionRawHistoryScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientGetWorkflowExecutionRawHistoryScope, metrics.CadenceClientLatency)
	resp, err := c.client.GetWorkflowExecutionRawHistory(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientGetWorkflowExecutionRawHistoryScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) PollForWorkflowExecutionRawHistory(
	ctx context.Context,
	request *shared.PollForWorkflowExecutionRawHistoryRequest,
	opts ...yarpc.CallOption,
) (*shared.PollForWorkflowExecutionRawHistoryResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientPollForWorkflowExecutionRawHistoryScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientPollForWorkflowExecutionRawHistoryScope, metrics.CadenceClientLatency)
	resp, err := c.client.PollForWorkflowExecutionRawHistory(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientPollForWorkflowExecutionRawHistoryScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) ListArchivedWorkflowExecutions(
	ctx context.Context,
	request *shared.ListArchivedWorkflowExecutionsRequest,
	opts ...yarpc.CallOption,
) (*shared.ListArchivedWorkflowExecutionsResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientListArchivedWorkflowExecutionsScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientListArchivedWorkflowExecutionsScope, metrics.CadenceClientLatency)
	resp, err := c.client.ListArchivedWorkflowExecutions(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientListArchivedWorkflowExecutionsScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) ListClosedWorkflowExecutions(
	ctx context.Context,
	request *shared.ListClosedWorkflowExecutionsRequest,
	opts ...yarpc.CallOption,
) (*shared.ListClosedWorkflowExecutionsResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientListClosedWorkflowExecutionsScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientListClosedWorkflowExecutionsScope, metrics.CadenceClientLatency)
	resp, err := c.client.ListClosedWorkflowExecutions(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientListClosedWorkflowExecutionsScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) ListDomains(
	ctx context.Context,
	request *shared.ListDomainsRequest,
	opts ...yarpc.CallOption,
) (*shared.ListDomainsResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientListDomainsScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientListDomainsScope, metrics.CadenceClientLatency)
	resp, err := c.client.ListDomains(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientListDomainsScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) ListOpenWorkflowExecutions(
	ctx context.Context,
	request *shared.ListOpenWorkflowExecutionsRequest,
	opts ...yarpc.CallOption,
) (*shared.ListOpenWorkflowExecutionsResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientListOpenWorkflowExecutionsScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientListOpenWorkflowExecutionsScope, metrics.CadenceClientLatency)
	resp, err := c.client.ListOpenWorkflowExecutions(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientListOpenWorkflowExecutionsScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) ListWorkflowExecutions(
	ctx context.Context,
	request *shared.ListWorkflowExecutionsRequest,
	opts ...yarpc.CallOption,
) (*shared.ListWorkflowExecutionsResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientListWorkflowExecutionsScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientListWorkflowExecutionsScope, metrics.CadenceClientLatency)
	resp, err := c.client.ListWorkflowExecutions(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientListWorkflowExecutionsScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) ScanWorkflowExecutions(
	ctx context.Context,
	request *shared.ListWorkflowExecutionsRequest,
	opts ...yarpc.CallOption,
) (*shared.ListWorkflowExecutionsResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientScanWorkflowExecutionsScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientScanWorkflowExecutionsScope, metrics.CadenceClientLatency)
	resp, err := c.client.ListWorkflowExecutions(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientScanWorkflowExecutionsScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) CountWorkflowExecutions(
	ctx context.Context,
	request *shared.CountWorkflowExecutionsRequest,
	opts ...yarpc.CallOption,
) (*shared.CountWorkflowExecutionsResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientCountWorkflowExecutionsScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientCountWorkflowExecutionsScope, metrics.CadenceClientLatency)
	resp, err := c.client.CountWorkflowExecutions(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientCountWorkflowExecutionsScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) GetSearchAttributes(
	ctx context.Context,
	opts ...yarpc.CallOption,
) (*shared.GetSearchAttributesResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientGetSearchAttributesScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientGetSearchAttributesScope, metrics.CadenceClientLatency)
	resp, err := c.client.GetSearchAttributes(ctx, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientGetSearchAttributesScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) PollForActivityTask(
	ctx context.Context,
	request *shared.PollForActivityTaskRequest,
	opts ...yarpc.CallOption,
) (*shared.PollForActivityTaskResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientPollForActivityTaskScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientPollForActivityTaskScope, metrics.CadenceClientLatency)
	resp, err := c.client.PollForActivityTask(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientPollForActivityTaskScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) PollForDecisionTask(
	ctx context.Context,
	request *shared.PollForDecisionTaskRequest,
	opts ...yarpc.CallOption,
) (*shared.PollForDecisionTaskResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientPollForDecisionTaskScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientPollForDecisionTaskScope, metrics.CadenceClientLatency)
	resp, err := c.client.PollForDecisionTask(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientPollForDecisionTaskScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) QueryWorkflow(
	ctx context.Context,
	request *shared.QueryWorkflowRequest,
	opts ...yarpc.CallOption,
) (*shared.QueryWorkflowResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientQueryWorkflowScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientQueryWorkflowScope, metrics.CadenceClientLatency)
	resp, err := c.client.QueryWorkflow(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientQueryWorkflowScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) RecordActivityTaskHeartbeat(
	ctx context.Context,
	request *shared.RecordActivityTaskHeartbeatRequest,
	opts ...yarpc.CallOption,
) (*shared.RecordActivityTaskHeartbeatResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientRecordActivityTaskHeartbeatScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientRecordActivityTaskHeartbeatScope, metrics.CadenceClientLatency)
	resp, err := c.client.RecordActivityTaskHeartbeat(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientRecordActivityTaskHeartbeatScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) RecordActivityTaskHeartbeatByID(
	ctx context.Context,
	request *shared.RecordActivityTaskHeartbeatByIDRequest,
	opts ...yarpc.CallOption,
) (*shared.RecordActivityTaskHeartbeatResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientRecordActivityTaskHeartbeatByIDScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientRecordActivityTaskHeartbeatByIDScope, metrics.CadenceClientLatency)
	resp, err := c.client.RecordActivityTaskHeartbeatByID(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientRecordActivityTaskHeartbeatByIDScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) RegisterDomain(
	ctx context.Context,
	request *shared.RegisterDomainRequest,
	opts ...yarpc.CallOption,
) error {

	c.metricsClient.IncCounter(metrics.FrontendClientRegisterDomainScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientRegisterDomainScope, metrics.CadenceClientLatency)
	err := c.client.RegisterDomain(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientRegisterDomainScope, metrics.CadenceClientFailures)
	}
	return err
}

func (c *metricClient) RequestCancelWorkflowExecution(
	ctx context.Context,
	request *shared.RequestCancelWorkflowExecutionRequest,
	opts ...yarpc.CallOption,
) error {

	c.metricsClient.IncCounter(metrics.FrontendClientRequestCancelWorkflowExecutionScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientRequestCancelWorkflowExecutionScope, metrics.CadenceClientLatency)
	err := c.client.RequestCancelWorkflowExecution(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientRequestCancelWorkflowExecutionScope, metrics.CadenceClientFailures)
	}
	return err
}

func (c *metricClient) ResetStickyTaskList(
	ctx context.Context,
	request *shared.ResetStickyTaskListRequest,
	opts ...yarpc.CallOption,
) (*shared.ResetStickyTaskListResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientResetStickyTaskListScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientResetStickyTaskListScope, metrics.CadenceClientLatency)
	resp, err := c.client.ResetStickyTaskList(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientResetStickyTaskListScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) ResetWorkflowExecution(
	ctx context.Context,
	request *shared.ResetWorkflowExecutionRequest,
	opts ...yarpc.CallOption,
) (*shared.ResetWorkflowExecutionResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientResetWorkflowExecutionScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientResetWorkflowExecutionScope, metrics.CadenceClientLatency)
	resp, err := c.client.ResetWorkflowExecution(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientResetWorkflowExecutionScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) RespondActivityTaskCanceled(
	ctx context.Context,
	request *shared.RespondActivityTaskCanceledRequest,
	opts ...yarpc.CallOption,
) error {

	c.metricsClient.IncCounter(metrics.FrontendClientRespondActivityTaskCanceledScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientRespondActivityTaskCanceledScope, metrics.CadenceClientLatency)
	err := c.client.RespondActivityTaskCanceled(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientRespondActivityTaskCanceledScope, metrics.CadenceClientFailures)
	}
	return err
}

func (c *metricClient) RespondActivityTaskCanceledByID(
	ctx context.Context,
	request *shared.RespondActivityTaskCanceledByIDRequest,
	opts ...yarpc.CallOption,
) error {

	c.metricsClient.IncCounter(metrics.FrontendClientRespondActivityTaskCanceledByIDScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientRespondActivityTaskCanceledByIDScope, metrics.CadenceClientLatency)
	err := c.client.RespondActivityTaskCanceledByID(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientRespondActivityTaskCanceledByIDScope, metrics.CadenceClientFailures)
	}
	return err
}

func (c *metricClient) RespondActivityTaskCompleted(
	ctx context.Context,
	request *shared.RespondActivityTaskCompletedRequest,
	opts ...yarpc.CallOption,
) error {

	c.metricsClient.IncCounter(metrics.FrontendClientRespondActivityTaskCompletedScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientRespondActivityTaskCompletedScope, metrics.CadenceClientLatency)
	err := c.client.RespondActivityTaskCompleted(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientRespondActivityTaskCompletedScope, metrics.CadenceClientFailures)
	}
	return err
}

func (c *metricClient) RespondActivityTaskCompletedByID(
	ctx context.Context,
	request *shared.RespondActivityTaskCompletedByIDRequest,
	opts ...yarpc.CallOption,
) error {

	c.metricsClient.IncCounter(metrics.FrontendClientRespondActivityTaskCompletedByIDScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientRespondActivityTaskCompletedByIDScope, metrics.CadenceClientLatency)
	err := c.client.RespondActivityTaskCompletedByID(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientRespondActivityTaskCompletedByIDScope, metrics.CadenceClientFailures)
	}
	return err
}

func (c *metricClient) RespondActivityTaskFailed(
	ctx context.Context,
	request *shared.RespondActivityTaskFailedRequest,
	opts ...yarpc.CallOption,
) error {

	c.metricsClient.IncCounter(metrics.FrontendClientRespondActivityTaskFailedScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientRespondActivityTaskFailedScope, metrics.CadenceClientLatency)
	err := c.client.RespondActivityTaskFailed(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientRespondActivityTaskFailedScope, metrics.CadenceClientFailures)
	}
	return err
}

func (c *metricClient) RespondActivityTaskFailedByID(
	ctx context.Context,
	request *shared.RespondActivityTaskFailedByIDRequest,
	opts ...yarpc.CallOption,
) error {

	c.metricsClient.IncCounter(metrics.FrontendClientRespondActivityTaskFailedByIDScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientRespondActivityTaskFailedByIDScope, metrics.CadenceClientLatency)
	err := c.client.RespondActivityTaskFailedByID(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientRespondActivityTaskFailedByIDScope, metrics.CadenceClientFailures)
	}
	return err
}

func (c *metricClient) RespondDecisionTaskCompleted(
	ctx context.Context,
	request *shared.RespondDecisionTaskCompletedRequest,
	opts ...yarpc.CallOption,
) (*shared.RespondDecisionTaskCompletedResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientRespondDecisionTaskCompletedScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientRespondDecisionTaskCompletedScope, metrics.CadenceClientLatency)
	resp, err := c.client.RespondDecisionTaskCompleted(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientRespondDecisionTaskCompletedScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) RespondDecisionTaskFailed(
	ctx context.Context,
	request *shared.RespondDecisionTaskFailedRequest,
	opts ...yarpc.CallOption,
) error {

	c.metricsClient.IncCounter(metrics.FrontendClientRespondDecisionTaskFailedScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientRespondDecisionTaskFailedScope, metrics.CadenceClientLatency)
	err := c.client.RespondDecisionTaskFailed(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientRespondDecisionTaskFailedScope, metrics.CadenceClientFailures)
	}
	return err
}

func (c *metricClient) RespondQueryTaskCompleted(
	ctx context.Context,
	request *shared.RespondQueryTaskCompletedRequest,
	opts ...yarpc.CallOption,
) error {

	c.metricsClient.IncCounter(metrics.FrontendClientRespondQueryTaskCompletedScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientRespondQueryTaskCompletedScope, metrics.CadenceClientLatency)
	err := c.client.RespondQueryTaskCompleted(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientRespondQueryTaskCompletedScope, metrics.CadenceClientFailures)
	}
	return err
}

func (c *metricClient) SignalWithStartWorkflowExecution(
	ctx context.Context,
	request *shared.SignalWithStartWorkflowExecutionRequest,
	opts ...yarpc.CallOption,
) (*shared.StartWorkflowExecutionResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientSignalWithStartWorkflowExecutionScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientSignalWithStartWorkflowExecutionScope, metrics.CadenceClientLatency)
	resp, err := c.client.SignalWithStartWorkflowExecution(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientSignalWithStartWorkflowExecutionScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) SignalWorkflowExecution(
	ctx context.Context,
	request *shared.SignalWorkflowExecutionRequest,
	opts ...yarpc.CallOption,
) error {

	c.metricsClient.IncCounter(metrics.FrontendClientSignalWorkflowExecutionScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientSignalWorkflowExecutionScope, metrics.CadenceClientLatency)
	err := c.client.SignalWorkflowExecution(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientSignalWorkflowExecutionScope, metrics.CadenceClientFailures)
	}
	return err
}

func (c *metricClient) StartWorkflowExecution(
	ctx context.Context,
	request *shared.StartWorkflowExecutionRequest,
	opts ...yarpc.CallOption,
) (*shared.StartWorkflowExecutionResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientStartWorkflowExecutionScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientStartWorkflowExecutionScope, metrics.CadenceClientLatency)
	resp, err := c.client.StartWorkflowExecution(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientStartWorkflowExecutionScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) TerminateWorkflowExecution(
	ctx context.Context,
	request *shared.TerminateWorkflowExecutionRequest,
	opts ...yarpc.CallOption,
) error {

	c.metricsClient.IncCounter(metrics.FrontendClientTerminateWorkflowExecutionScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientTerminateWorkflowExecutionScope, metrics.CadenceClientLatency)
	err := c.client.TerminateWorkflowExecution(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientTerminateWorkflowExecutionScope, metrics.CadenceClientFailures)
	}
	return err
}

func (c *metricClient) UpdateDomain(
	ctx context.Context,
	request *shared.UpdateDomainRequest,
	opts ...yarpc.CallOption,
) (*shared.UpdateDomainResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientUpdateDomainScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientUpdateDomainScope, metrics.CadenceClientLatency)
	resp, err := c.client.UpdateDomain(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientUpdateDomainScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) GetClusterInfo(
	ctx context.Context,
	opts ...yarpc.CallOption,
) (*shared.ClusterInfo, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientGetClusterInfoScope, metrics.CadenceClientRequests)
	sw := c.metricsClient.StartTimer(metrics.FrontendClientGetClusterInfoScope, metrics.CadenceClientLatency)
	resp, err := c.client.GetClusterInfo(ctx, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientGetClusterInfoScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) ListTaskListPartitions(
	ctx context.Context,
	request *shared.ListTaskListPartitionsRequest,
	opts ...yarpc.CallOption,
) (*shared.ListTaskListPartitionsResponse, error) {

	c.metricsClient.IncCounter(metrics.FrontendClientListTaskListPartitionsScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClientListTaskListPartitionsScope, metrics.CadenceClientLatency)
	resp, err := c.client.ListTaskListPartitions(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClientListTaskListPartitionsScope, metrics.CadenceClientFailures)
	}
	return resp, err
}
