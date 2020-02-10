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

package history

import (
	"context"

	"google.golang.org/grpc"

	"github.com/temporalio/temporal/.gen/proto/historyservice"
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

func (c *metricClientGRPC) StartWorkflowExecution(
	context context.Context,
	request *historyservice.StartWorkflowExecutionRequest,
	opts ...grpc.CallOption) (*historyservice.StartWorkflowExecutionResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientStartWorkflowExecutionScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientStartWorkflowExecutionScope, metrics.CadenceClientLatency)
	resp, err := c.client.StartWorkflowExecution(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientStartWorkflowExecutionScope, metrics.CadenceClientFailures)
	}

	return resp, err
}

func (c *metricClientGRPC) DescribeHistoryHost(
	context context.Context,
	request *historyservice.DescribeHistoryHostRequest,
	opts ...grpc.CallOption) (*historyservice.DescribeHistoryHostResponse, error) {
	resp, err := c.client.DescribeHistoryHost(context, request, opts...)

	return resp, err
}

func (c *metricClientGRPC) RemoveTask(
	context context.Context,
	request *historyservice.RemoveTaskRequest,
	opts ...grpc.CallOption) (*historyservice.RemoveTaskResponse, error) {
	resp, err := c.client.RemoveTask(context, request, opts...)

	return resp, err
}

func (c *metricClientGRPC) CloseShard(
	context context.Context,
	request *historyservice.CloseShardRequest,
	opts ...grpc.CallOption) (*historyservice.CloseShardResponse, error) {
	resp, err := c.client.CloseShard(context, request, opts...)

	return resp, err
}

func (c *metricClientGRPC) DescribeMutableState(
	context context.Context,
	request *historyservice.DescribeMutableStateRequest,
	opts ...grpc.CallOption) (*historyservice.DescribeMutableStateResponse, error) {
	resp, err := c.client.DescribeMutableState(context, request, opts...)

	return resp, err
}

func (c *metricClientGRPC) GetMutableState(
	context context.Context,
	request *historyservice.GetMutableStateRequest,
	opts ...grpc.CallOption) (*historyservice.GetMutableStateResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientGetMutableStateScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientGetMutableStateScope, metrics.CadenceClientLatency)
	resp, err := c.client.GetMutableState(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientGetMutableStateScope, metrics.CadenceClientFailures)
	}

	return resp, err
}

func (c *metricClientGRPC) PollMutableState(
	context context.Context,
	request *historyservice.PollMutableStateRequest,
	opts ...grpc.CallOption) (*historyservice.PollMutableStateResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientPollMutableStateScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientPollMutableStateScope, metrics.CadenceClientLatency)
	resp, err := c.client.PollMutableState(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientPollMutableStateScope, metrics.CadenceClientFailures)
	}

	return resp, err
}

func (c *metricClientGRPC) ResetStickyTaskList(
	context context.Context,
	request *historyservice.ResetStickyTaskListRequest,
	opts ...grpc.CallOption) (*historyservice.ResetStickyTaskListResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientResetStickyTaskListScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientResetStickyTaskListScope, metrics.CadenceClientLatency)
	resp, err := c.client.ResetStickyTaskList(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientResetStickyTaskListScope, metrics.CadenceClientFailures)
	}

	return resp, err
}

func (c *metricClientGRPC) DescribeWorkflowExecution(
	context context.Context,
	request *historyservice.DescribeWorkflowExecutionRequest,
	opts ...grpc.CallOption) (*historyservice.DescribeWorkflowExecutionResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientDescribeWorkflowExecutionScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientDescribeWorkflowExecutionScope, metrics.CadenceClientLatency)
	resp, err := c.client.DescribeWorkflowExecution(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientDescribeWorkflowExecutionScope, metrics.CadenceClientFailures)
	}

	return resp, err
}

func (c *metricClientGRPC) RecordDecisionTaskStarted(
	context context.Context,
	request *historyservice.RecordDecisionTaskStartedRequest,
	opts ...grpc.CallOption) (*historyservice.RecordDecisionTaskStartedResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientRecordDecisionTaskStartedScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientRecordDecisionTaskStartedScope, metrics.CadenceClientLatency)
	resp, err := c.client.RecordDecisionTaskStarted(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientRecordDecisionTaskStartedScope, metrics.CadenceClientFailures)
	}

	return resp, err
}

func (c *metricClientGRPC) RecordActivityTaskStarted(
	context context.Context,
	request *historyservice.RecordActivityTaskStartedRequest,
	opts ...grpc.CallOption) (*historyservice.RecordActivityTaskStartedResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientRecordActivityTaskStartedScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientRecordActivityTaskStartedScope, metrics.CadenceClientLatency)
	resp, err := c.client.RecordActivityTaskStarted(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientRecordActivityTaskStartedScope, metrics.CadenceClientFailures)
	}

	return resp, err
}

func (c *metricClientGRPC) RespondDecisionTaskCompleted(
	context context.Context,
	request *historyservice.RespondDecisionTaskCompletedRequest,
	opts ...grpc.CallOption) (*historyservice.RespondDecisionTaskCompletedResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientRespondDecisionTaskCompletedScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientRespondDecisionTaskCompletedScope, metrics.CadenceClientLatency)
	response, err := c.client.RespondDecisionTaskCompleted(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientRespondDecisionTaskCompletedScope, metrics.CadenceClientFailures)
	}

	return response, err
}

func (c *metricClientGRPC) RespondDecisionTaskFailed(
	context context.Context,
	request *historyservice.RespondDecisionTaskFailedRequest,
	opts ...grpc.CallOption) (*historyservice.RespondDecisionTaskFailedResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientRespondDecisionTaskFailedScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientRespondDecisionTaskFailedScope, metrics.CadenceClientLatency)
	resp, err := c.client.RespondDecisionTaskFailed(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientRespondDecisionTaskFailedScope, metrics.CadenceClientFailures)
	}

	return resp, err
}

func (c *metricClientGRPC) RespondActivityTaskCompleted(
	context context.Context,
	request *historyservice.RespondActivityTaskCompletedRequest,
	opts ...grpc.CallOption) (*historyservice.RespondActivityTaskCompletedResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientRespondActivityTaskCompletedScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientRespondActivityTaskCompletedScope, metrics.CadenceClientLatency)
	resp, err := c.client.RespondActivityTaskCompleted(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientRespondActivityTaskCompletedScope, metrics.CadenceClientFailures)
	}

	return resp, err
}

func (c *metricClientGRPC) RespondActivityTaskFailed(
	context context.Context,
	request *historyservice.RespondActivityTaskFailedRequest,
	opts ...grpc.CallOption) (*historyservice.RespondActivityTaskFailedResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientRespondActivityTaskFailedScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientRespondActivityTaskFailedScope, metrics.CadenceClientLatency)
	resp, err := c.client.RespondActivityTaskFailed(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientRespondActivityTaskFailedScope, metrics.CadenceClientFailures)
	}

	return resp, err
}

func (c *metricClientGRPC) RespondActivityTaskCanceled(
	context context.Context,
	request *historyservice.RespondActivityTaskCanceledRequest,
	opts ...grpc.CallOption) (*historyservice.RespondActivityTaskCanceledResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientRespondActivityTaskCanceledScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientRespondActivityTaskCanceledScope, metrics.CadenceClientLatency)
	resp, err := c.client.RespondActivityTaskCanceled(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientRespondActivityTaskCanceledScope, metrics.CadenceClientFailures)
	}

	return resp, err
}

func (c *metricClientGRPC) RecordActivityTaskHeartbeat(
	context context.Context,
	request *historyservice.RecordActivityTaskHeartbeatRequest,
	opts ...grpc.CallOption) (*historyservice.RecordActivityTaskHeartbeatResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientRecordActivityTaskHeartbeatScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientRecordActivityTaskHeartbeatScope, metrics.CadenceClientLatency)
	resp, err := c.client.RecordActivityTaskHeartbeat(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientRecordActivityTaskHeartbeatScope, metrics.CadenceClientFailures)
	}

	return resp, err
}

func (c *metricClientGRPC) RequestCancelWorkflowExecution(
	context context.Context,
	request *historyservice.RequestCancelWorkflowExecutionRequest,
	opts ...grpc.CallOption) (*historyservice.RequestCancelWorkflowExecutionResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientRequestCancelWorkflowExecutionScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientRequestCancelWorkflowExecutionScope, metrics.CadenceClientLatency)
	resp, err := c.client.RequestCancelWorkflowExecution(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientRequestCancelWorkflowExecutionScope, metrics.CadenceClientFailures)
	}

	return resp, err
}

func (c *metricClientGRPC) SignalWorkflowExecution(
	context context.Context,
	request *historyservice.SignalWorkflowExecutionRequest,
	opts ...grpc.CallOption) (*historyservice.SignalWorkflowExecutionResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientSignalWorkflowExecutionScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientSignalWorkflowExecutionScope, metrics.CadenceClientLatency)
	resp, err := c.client.SignalWorkflowExecution(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientSignalWorkflowExecutionScope, metrics.CadenceClientFailures)
	}

	return resp, err
}

func (c *metricClientGRPC) SignalWithStartWorkflowExecution(
	context context.Context,
	request *historyservice.SignalWithStartWorkflowExecutionRequest,
	opts ...grpc.CallOption) (*historyservice.SignalWithStartWorkflowExecutionResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientSignalWithStartWorkflowExecutionScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientSignalWithStartWorkflowExecutionScope, metrics.CadenceClientLatency)
	resp, err := c.client.SignalWithStartWorkflowExecution(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientSignalWithStartWorkflowExecutionScope, metrics.CadenceClientFailures)
	}

	return resp, err
}

func (c *metricClientGRPC) RemoveSignalMutableState(
	context context.Context,
	request *historyservice.RemoveSignalMutableStateRequest,
	opts ...grpc.CallOption) (*historyservice.RemoveSignalMutableStateResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientRemoveSignalMutableStateScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientRemoveSignalMutableStateScope, metrics.CadenceClientLatency)
	resp, err := c.client.RemoveSignalMutableState(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientRemoveSignalMutableStateScope, metrics.CadenceClientFailures)
	}

	return resp, err
}

func (c *metricClientGRPC) TerminateWorkflowExecution(
	context context.Context,
	request *historyservice.TerminateWorkflowExecutionRequest,
	opts ...grpc.CallOption) (*historyservice.TerminateWorkflowExecutionResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientTerminateWorkflowExecutionScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientTerminateWorkflowExecutionScope, metrics.CadenceClientLatency)
	resp, err := c.client.TerminateWorkflowExecution(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientTerminateWorkflowExecutionScope, metrics.CadenceClientFailures)
	}

	return resp, err
}

func (c *metricClientGRPC) ResetWorkflowExecution(
	context context.Context,
	request *historyservice.ResetWorkflowExecutionRequest,
	opts ...grpc.CallOption) (*historyservice.ResetWorkflowExecutionResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientResetWorkflowExecutionScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientResetWorkflowExecutionScope, metrics.CadenceClientLatency)
	resp, err := c.client.ResetWorkflowExecution(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientResetWorkflowExecutionScope, metrics.CadenceClientFailures)
	}

	return resp, err
}

func (c *metricClientGRPC) ScheduleDecisionTask(
	context context.Context,
	request *historyservice.ScheduleDecisionTaskRequest,
	opts ...grpc.CallOption) (*historyservice.ScheduleDecisionTaskResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientScheduleDecisionTaskScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientScheduleDecisionTaskScope, metrics.CadenceClientLatency)
	resp, err := c.client.ScheduleDecisionTask(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientScheduleDecisionTaskScope, metrics.CadenceClientFailures)
	}

	return resp, err
}

func (c *metricClientGRPC) RecordChildExecutionCompleted(
	context context.Context,
	request *historyservice.RecordChildExecutionCompletedRequest,
	opts ...grpc.CallOption) (*historyservice.RecordChildExecutionCompletedResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientRecordChildExecutionCompletedScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientRecordChildExecutionCompletedScope, metrics.CadenceClientLatency)
	resp, err := c.client.RecordChildExecutionCompleted(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientRecordChildExecutionCompletedScope, metrics.CadenceClientFailures)
	}

	return resp, err
}

func (c *metricClientGRPC) ReplicateEvents(
	context context.Context,
	request *historyservice.ReplicateEventsRequest,
	opts ...grpc.CallOption) (*historyservice.ReplicateEventsResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientReplicateEventsScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientReplicateEventsScope, metrics.CadenceClientLatency)
	resp, err := c.client.ReplicateEvents(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientReplicateEventsScope, metrics.CadenceClientFailures)
	}

	return resp, err
}

func (c *metricClientGRPC) ReplicateRawEvents(
	context context.Context,
	request *historyservice.ReplicateRawEventsRequest,
	opts ...grpc.CallOption) (*historyservice.ReplicateRawEventsResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientReplicateRawEventsScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientReplicateRawEventsScope, metrics.CadenceClientLatency)
	resp, err := c.client.ReplicateRawEvents(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientReplicateRawEventsScope, metrics.CadenceClientFailures)
	}

	return resp, err
}

func (c *metricClientGRPC) ReplicateEventsV2(
	context context.Context,
	request *historyservice.ReplicateEventsV2Request,
	opts ...grpc.CallOption) (*historyservice.ReplicateEventsV2Response, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientReplicateEventsV2Scope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientReplicateEventsV2Scope, metrics.CadenceClientLatency)
	resp, err := c.client.ReplicateEventsV2(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientReplicateEventsV2Scope, metrics.CadenceClientFailures)
	}

	return resp, err
}

func (c *metricClientGRPC) SyncShardStatus(
	context context.Context,
	request *historyservice.SyncShardStatusRequest,
	opts ...grpc.CallOption) (*historyservice.SyncShardStatusResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientSyncShardStatusScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientSyncShardStatusScope, metrics.CadenceClientLatency)
	resp, err := c.client.SyncShardStatus(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientSyncShardStatusScope, metrics.CadenceClientFailures)
	}

	return resp, err
}

func (c *metricClientGRPC) SyncActivity(
	context context.Context,
	request *historyservice.SyncActivityRequest,
	opts ...grpc.CallOption) (*historyservice.SyncActivityResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientSyncActivityScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientSyncActivityScope, metrics.CadenceClientLatency)
	resp, err := c.client.SyncActivity(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientSyncActivityScope, metrics.CadenceClientFailures)
	}

	return resp, err
}

func (c *metricClientGRPC) GetReplicationMessages(
	context context.Context,
	request *historyservice.GetReplicationMessagesRequest,
	opts ...grpc.CallOption) (*historyservice.GetReplicationMessagesResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientGetReplicationTasksScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientGetReplicationTasksScope, metrics.CadenceClientLatency)
	resp, err := c.client.GetReplicationMessages(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientGetReplicationTasksScope, metrics.CadenceClientFailures)
	}

	return resp, err
}

func (c *metricClientGRPC) GetDLQReplicationMessages(
	context context.Context,
	request *historyservice.GetDLQReplicationMessagesRequest,
	opts ...grpc.CallOption) (*historyservice.GetDLQReplicationMessagesResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientGetDLQReplicationTasksScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientGetDLQReplicationTasksScope, metrics.CadenceClientLatency)
	resp, err := c.client.GetDLQReplicationMessages(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientGetDLQReplicationTasksScope, metrics.CadenceClientFailures)
	}

	return resp, err
}

func (c *metricClientGRPC) QueryWorkflow(
	context context.Context,
	request *historyservice.QueryWorkflowRequest,
	opts ...grpc.CallOption) (*historyservice.QueryWorkflowResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientQueryWorkflowScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientQueryWorkflowScope, metrics.CadenceClientLatency)
	resp, err := c.client.QueryWorkflow(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientQueryWorkflowScope, metrics.CadenceClientFailures)
	}

	return resp, err
}

func (c *metricClientGRPC) ReapplyEvents(
	context context.Context,
	request *historyservice.ReapplyEventsRequest,
	opts ...grpc.CallOption) (*historyservice.ReapplyEventsResponse, error) {

	c.metricsClient.IncCounter(metrics.HistoryClientReapplyEventsScope, metrics.CadenceClientRequests)
	sw := c.metricsClient.StartTimer(metrics.HistoryClientReapplyEventsScope, metrics.CadenceClientLatency)
	resp, err := c.client.ReapplyEvents(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientReapplyEventsScope, metrics.CadenceClientFailures)
	}
	return resp, err
}
