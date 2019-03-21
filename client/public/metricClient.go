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

package public

import (
	"context"
	"github.com/uber/cadence/common/metrics"
	"go.uber.org/cadence/.gen/go/shared"
	"go.uber.org/yarpc"
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

	c.metricsClient.IncCounter(metrics.PublicClientDeprecateDomainScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.PublicClientDeprecateDomainScope, metrics.CadenceClientLatency)
	err := c.client.DeprecateDomain(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.PublicClientDeprecateDomainScope, metrics.CadenceClientFailures)
	}
	return err
}

func (c *metricClient) DescribeDomain(
	ctx context.Context,
	request *shared.DescribeDomainRequest,
	opts ...yarpc.CallOption,
) (*shared.DescribeDomainResponse, error) {

	c.metricsClient.IncCounter(metrics.PublicClientDescribeDomainScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.PublicClientDescribeDomainScope, metrics.CadenceClientLatency)
	resp, err := c.client.DescribeDomain(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.PublicClientDescribeDomainScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) DescribeTaskList(
	ctx context.Context,
	request *shared.DescribeTaskListRequest,
	opts ...yarpc.CallOption,
) (*shared.DescribeTaskListResponse, error) {

	c.metricsClient.IncCounter(metrics.PublicClientDescribeTaskListScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.PublicClientDescribeTaskListScope, metrics.CadenceClientLatency)
	resp, err := c.client.DescribeTaskList(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.PublicClientDescribeTaskListScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) DescribeWorkflowExecution(
	ctx context.Context,
	request *shared.DescribeWorkflowExecutionRequest,
	opts ...yarpc.CallOption,
) (*shared.DescribeWorkflowExecutionResponse, error) {

	c.metricsClient.IncCounter(metrics.PublicClientDescribeWorkflowExecutionScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.PublicClientDescribeWorkflowExecutionScope, metrics.CadenceClientLatency)
	resp, err := c.client.DescribeWorkflowExecution(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.PublicClientDescribeWorkflowExecutionScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) GetWorkflowExecutionHistory(
	ctx context.Context,
	request *shared.GetWorkflowExecutionHistoryRequest,
	opts ...yarpc.CallOption,
) (*shared.GetWorkflowExecutionHistoryResponse, error) {

	c.metricsClient.IncCounter(metrics.PublicClientGetWorkflowExecutionHistoryScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.PublicClientGetWorkflowExecutionHistoryScope, metrics.CadenceClientLatency)
	resp, err := c.client.GetWorkflowExecutionHistory(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.PublicClientGetWorkflowExecutionHistoryScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) ListClosedWorkflowExecutions(
	ctx context.Context,
	request *shared.ListClosedWorkflowExecutionsRequest,
	opts ...yarpc.CallOption,
) (*shared.ListClosedWorkflowExecutionsResponse, error) {

	c.metricsClient.IncCounter(metrics.PublicClientListClosedWorkflowExecutionsScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.PublicClientListClosedWorkflowExecutionsScope, metrics.CadenceClientLatency)
	resp, err := c.client.ListClosedWorkflowExecutions(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.PublicClientListClosedWorkflowExecutionsScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) ListDomains(
	ctx context.Context,
	request *shared.ListDomainsRequest,
	opts ...yarpc.CallOption,
) (*shared.ListDomainsResponse, error) {

	c.metricsClient.IncCounter(metrics.PublicClientListDomainsScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.PublicClientListDomainsScope, metrics.CadenceClientLatency)
	resp, err := c.client.ListDomains(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.PublicClientListDomainsScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) ListOpenWorkflowExecutions(
	ctx context.Context,
	request *shared.ListOpenWorkflowExecutionsRequest,
	opts ...yarpc.CallOption,
) (*shared.ListOpenWorkflowExecutionsResponse, error) {

	c.metricsClient.IncCounter(metrics.PublicClientListOpenWorkflowExecutionsScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.PublicClientListOpenWorkflowExecutionsScope, metrics.CadenceClientLatency)
	resp, err := c.client.ListOpenWorkflowExecutions(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.PublicClientListOpenWorkflowExecutionsScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) PollForActivityTask(
	ctx context.Context,
	request *shared.PollForActivityTaskRequest,
	opts ...yarpc.CallOption,
) (*shared.PollForActivityTaskResponse, error) {

	c.metricsClient.IncCounter(metrics.PublicClientPollForActivityTaskScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.PublicClientPollForActivityTaskScope, metrics.CadenceClientLatency)
	resp, err := c.client.PollForActivityTask(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.PublicClientPollForActivityTaskScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) PollForDecisionTask(
	ctx context.Context,
	request *shared.PollForDecisionTaskRequest,
	opts ...yarpc.CallOption,
) (*shared.PollForDecisionTaskResponse, error) {

	c.metricsClient.IncCounter(metrics.PublicClientPollForDecisionTaskScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.PublicClientPollForDecisionTaskScope, metrics.CadenceClientLatency)
	resp, err := c.client.PollForDecisionTask(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.PublicClientPollForDecisionTaskScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) QueryWorkflow(
	ctx context.Context,
	request *shared.QueryWorkflowRequest,
	opts ...yarpc.CallOption,
) (*shared.QueryWorkflowResponse, error) {

	c.metricsClient.IncCounter(metrics.PublicClientQueryWorkflowScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.PublicClientQueryWorkflowScope, metrics.CadenceClientLatency)
	resp, err := c.client.QueryWorkflow(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.PublicClientQueryWorkflowScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) RecordActivityTaskHeartbeat(
	ctx context.Context,
	request *shared.RecordActivityTaskHeartbeatRequest,
	opts ...yarpc.CallOption,
) (*shared.RecordActivityTaskHeartbeatResponse, error) {

	c.metricsClient.IncCounter(metrics.PublicClientRecordActivityTaskHeartbeatScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.PublicClientRecordActivityTaskHeartbeatScope, metrics.CadenceClientLatency)
	resp, err := c.client.RecordActivityTaskHeartbeat(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.PublicClientRecordActivityTaskHeartbeatScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) RecordActivityTaskHeartbeatByID(
	ctx context.Context,
	request *shared.RecordActivityTaskHeartbeatByIDRequest,
	opts ...yarpc.CallOption,
) (*shared.RecordActivityTaskHeartbeatResponse, error) {

	c.metricsClient.IncCounter(metrics.PublicClientRecordActivityTaskHeartbeatByIDScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.PublicClientRecordActivityTaskHeartbeatByIDScope, metrics.CadenceClientLatency)
	resp, err := c.client.RecordActivityTaskHeartbeatByID(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.PublicClientRecordActivityTaskHeartbeatByIDScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) RegisterDomain(
	ctx context.Context,
	request *shared.RegisterDomainRequest,
	opts ...yarpc.CallOption,
) error {

	c.metricsClient.IncCounter(metrics.PublicClientRegisterDomainScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.PublicClientRegisterDomainScope, metrics.CadenceClientLatency)
	err := c.client.RegisterDomain(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.PublicClientRegisterDomainScope, metrics.CadenceClientFailures)
	}
	return err
}

func (c *metricClient) RequestCancelWorkflowExecution(
	ctx context.Context,
	request *shared.RequestCancelWorkflowExecutionRequest,
	opts ...yarpc.CallOption,
) error {

	c.metricsClient.IncCounter(metrics.PublicClientRequestCancelWorkflowExecutionScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.PublicClientRequestCancelWorkflowExecutionScope, metrics.CadenceClientLatency)
	err := c.client.RequestCancelWorkflowExecution(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.PublicClientRequestCancelWorkflowExecutionScope, metrics.CadenceClientFailures)
	}
	return err
}

func (c *metricClient) ResetStickyTaskList(
	ctx context.Context,
	request *shared.ResetStickyTaskListRequest,
	opts ...yarpc.CallOption,
) (*shared.ResetStickyTaskListResponse, error) {

	c.metricsClient.IncCounter(metrics.PublicClientResetStickyTaskListScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.PublicClientResetStickyTaskListScope, metrics.CadenceClientLatency)
	resp, err := c.client.ResetStickyTaskList(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.PublicClientResetStickyTaskListScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) ResetWorkflowExecution(
	ctx context.Context,
	request *shared.ResetWorkflowExecutionRequest,
	opts ...yarpc.CallOption,
) (*shared.ResetWorkflowExecutionResponse, error) {

	c.metricsClient.IncCounter(metrics.PublicClientResetWorkflowExecutionScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.PublicClientResetWorkflowExecutionScope, metrics.CadenceClientLatency)
	resp, err := c.client.ResetWorkflowExecution(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.PublicClientResetWorkflowExecutionScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) RespondActivityTaskCanceled(
	ctx context.Context,
	request *shared.RespondActivityTaskCanceledRequest,
	opts ...yarpc.CallOption,
) error {

	c.metricsClient.IncCounter(metrics.PublicClientRespondActivityTaskCanceledScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.PublicClientRespondActivityTaskCanceledScope, metrics.CadenceClientLatency)
	err := c.client.RespondActivityTaskCanceled(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.PublicClientRespondActivityTaskCanceledScope, metrics.CadenceClientFailures)
	}
	return err
}

func (c *metricClient) RespondActivityTaskCanceledByID(
	ctx context.Context,
	request *shared.RespondActivityTaskCanceledByIDRequest,
	opts ...yarpc.CallOption,
) error {

	c.metricsClient.IncCounter(metrics.PublicClientRespondActivityTaskCanceledByIDScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.PublicClientRespondActivityTaskCanceledByIDScope, metrics.CadenceClientLatency)
	err := c.client.RespondActivityTaskCanceledByID(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.PublicClientRespondActivityTaskCanceledByIDScope, metrics.CadenceClientFailures)
	}
	return err
}

func (c *metricClient) RespondActivityTaskCompleted(
	ctx context.Context,
	request *shared.RespondActivityTaskCompletedRequest,
	opts ...yarpc.CallOption,
) error {

	c.metricsClient.IncCounter(metrics.PublicClientRespondActivityTaskCompletedScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.PublicClientRespondActivityTaskCompletedScope, metrics.CadenceClientLatency)
	err := c.client.RespondActivityTaskCompleted(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.PublicClientRespondActivityTaskCompletedScope, metrics.CadenceClientFailures)
	}
	return err
}

func (c *metricClient) RespondActivityTaskCompletedByID(
	ctx context.Context,
	request *shared.RespondActivityTaskCompletedByIDRequest,
	opts ...yarpc.CallOption,
) error {

	c.metricsClient.IncCounter(metrics.PublicClientRespondActivityTaskCompletedByIDScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.PublicClientRespondActivityTaskCompletedByIDScope, metrics.CadenceClientLatency)
	err := c.client.RespondActivityTaskCompletedByID(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.PublicClientRespondActivityTaskCompletedByIDScope, metrics.CadenceClientFailures)
	}
	return err
}

func (c *metricClient) RespondActivityTaskFailed(
	ctx context.Context,
	request *shared.RespondActivityTaskFailedRequest,
	opts ...yarpc.CallOption,
) error {

	c.metricsClient.IncCounter(metrics.PublicClientRespondActivityTaskFailedScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.PublicClientRespondActivityTaskFailedScope, metrics.CadenceClientLatency)
	err := c.client.RespondActivityTaskFailed(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.PublicClientRespondActivityTaskFailedScope, metrics.CadenceClientFailures)
	}
	return err
}

func (c *metricClient) RespondActivityTaskFailedByID(
	ctx context.Context,
	request *shared.RespondActivityTaskFailedByIDRequest,
	opts ...yarpc.CallOption,
) error {

	c.metricsClient.IncCounter(metrics.PublicClientRespondActivityTaskFailedByIDScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.PublicClientRespondActivityTaskFailedByIDScope, metrics.CadenceClientLatency)
	err := c.client.RespondActivityTaskFailedByID(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.PublicClientRespondActivityTaskFailedByIDScope, metrics.CadenceClientFailures)
	}
	return err
}

func (c *metricClient) RespondDecisionTaskCompleted(
	ctx context.Context,
	request *shared.RespondDecisionTaskCompletedRequest,
	opts ...yarpc.CallOption,
) (*shared.RespondDecisionTaskCompletedResponse, error) {

	c.metricsClient.IncCounter(metrics.PublicClientRespondDecisionTaskCompletedScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.PublicClientRespondDecisionTaskCompletedScope, metrics.CadenceClientLatency)
	resp, err := c.client.RespondDecisionTaskCompleted(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.PublicClientRespondDecisionTaskCompletedScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) RespondDecisionTaskFailed(
	ctx context.Context,
	request *shared.RespondDecisionTaskFailedRequest,
	opts ...yarpc.CallOption,
) error {

	c.metricsClient.IncCounter(metrics.PublicClientRespondDecisionTaskFailedScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.PublicClientRespondDecisionTaskFailedScope, metrics.CadenceClientLatency)
	err := c.client.RespondDecisionTaskFailed(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.PublicClientRespondDecisionTaskFailedScope, metrics.CadenceClientFailures)
	}
	return err
}

func (c *metricClient) RespondQueryTaskCompleted(
	ctx context.Context,
	request *shared.RespondQueryTaskCompletedRequest,
	opts ...yarpc.CallOption,
) error {

	c.metricsClient.IncCounter(metrics.PublicClientRespondQueryTaskCompletedScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.PublicClientRespondQueryTaskCompletedScope, metrics.CadenceClientLatency)
	err := c.client.RespondQueryTaskCompleted(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.PublicClientRespondQueryTaskCompletedScope, metrics.CadenceClientFailures)
	}
	return err
}

func (c *metricClient) SignalWithStartWorkflowExecution(
	ctx context.Context,
	request *shared.SignalWithStartWorkflowExecutionRequest,
	opts ...yarpc.CallOption,
) (*shared.StartWorkflowExecutionResponse, error) {

	c.metricsClient.IncCounter(metrics.PublicClientSignalWithStartWorkflowExecutionScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.PublicClientSignalWithStartWorkflowExecutionScope, metrics.CadenceClientLatency)
	resp, err := c.client.SignalWithStartWorkflowExecution(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.PublicClientSignalWithStartWorkflowExecutionScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) SignalWorkflowExecution(
	ctx context.Context,
	request *shared.SignalWorkflowExecutionRequest,
	opts ...yarpc.CallOption,
) error {

	c.metricsClient.IncCounter(metrics.PublicClientSignalWorkflowExecutionScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.PublicClientSignalWorkflowExecutionScope, metrics.CadenceClientLatency)
	err := c.client.SignalWorkflowExecution(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.PublicClientSignalWorkflowExecutionScope, metrics.CadenceClientFailures)
	}
	return err
}

func (c *metricClient) StartWorkflowExecution(
	ctx context.Context,
	request *shared.StartWorkflowExecutionRequest,
	opts ...yarpc.CallOption,
) (*shared.StartWorkflowExecutionResponse, error) {

	c.metricsClient.IncCounter(metrics.PublicClientStartWorkflowExecutionScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.PublicClientStartWorkflowExecutionScope, metrics.CadenceClientLatency)
	resp, err := c.client.StartWorkflowExecution(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.PublicClientStartWorkflowExecutionScope, metrics.CadenceClientFailures)
	}
	return resp, err
}

func (c *metricClient) TerminateWorkflowExecution(
	ctx context.Context,
	request *shared.TerminateWorkflowExecutionRequest,
	opts ...yarpc.CallOption,
) error {

	c.metricsClient.IncCounter(metrics.PublicClientTerminateWorkflowExecutionScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.PublicClientTerminateWorkflowExecutionScope, metrics.CadenceClientLatency)
	err := c.client.TerminateWorkflowExecution(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.PublicClientTerminateWorkflowExecutionScope, metrics.CadenceClientFailures)
	}
	return err
}

func (c *metricClient) UpdateDomain(
	ctx context.Context,
	request *shared.UpdateDomainRequest,
	opts ...yarpc.CallOption,
) (*shared.UpdateDomainResponse, error) {

	c.metricsClient.IncCounter(metrics.PublicClientUpdateDomainScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.PublicClientUpdateDomainScope, metrics.CadenceClientLatency)
	resp, err := c.client.UpdateDomain(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.PublicClientUpdateDomainScope, metrics.CadenceClientFailures)
	}
	return resp, err
}
