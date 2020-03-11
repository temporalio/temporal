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
	"sync"
	"time"

	"go.temporal.io/temporal-proto/serviceerror"
	"google.golang.org/grpc"

	"github.com/temporalio/temporal/.gen/proto/historyservice"
	"github.com/temporalio/temporal/.gen/proto/replication"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
)

var _ Client = (*clientImpl)(nil)

const (
	// DefaultTimeout is the default timeout used to make calls
	DefaultTimeout = time.Second * 30
)

type clientImpl struct {
	numberOfShards  int
	tokenSerializer common.TaskTokenSerializer
	timeout         time.Duration
	clients         common.ClientCache
	logger          log.Logger
}

// NewClient creates a new history service gRPC client
func NewClient(
	numberOfShards int,
	timeout time.Duration,
	clients common.ClientCache,
	logger log.Logger,
) Client {
	return &clientImpl{
		numberOfShards:  numberOfShards,
		tokenSerializer: common.NewProtoTaskTokenSerializer(),
		timeout:         timeout,
		clients:         clients,
		logger:          logger,
	}
}

func (c *clientImpl) StartWorkflowExecution(
	ctx context.Context,
	request *historyservice.StartWorkflowExecutionRequest,
	opts ...grpc.CallOption) (*historyservice.StartWorkflowExecutionResponse, error) {
	client, err := c.getClientForWorkflowID(request.StartRequest.WorkflowId)
	if err != nil {
		return nil, err
	}

	var response *historyservice.StartWorkflowExecutionResponse
	op := func(ctx context.Context, client historyservice.HistoryServiceClient) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.StartWorkflowExecution(ctx, request, opts...)
		return err
	}
	err = c.executeWithRedirect(ctx, client, op)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (c *clientImpl) GetMutableState(
	ctx context.Context,
	request *historyservice.GetMutableStateRequest,
	opts ...grpc.CallOption) (*historyservice.GetMutableStateResponse, error) {
	client, err := c.getClientForWorkflowID(request.Execution.WorkflowId)
	if err != nil {
		return nil, err
	}

	var response *historyservice.GetMutableStateResponse
	op := func(ctx context.Context, client historyservice.HistoryServiceClient) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.GetMutableState(ctx, request, opts...)
		return err
	}
	err = c.executeWithRedirect(ctx, client, op)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (c *clientImpl) PollMutableState(
	ctx context.Context,
	request *historyservice.PollMutableStateRequest,
	opts ...grpc.CallOption) (*historyservice.PollMutableStateResponse, error) {
	client, err := c.getClientForWorkflowID(request.Execution.WorkflowId)
	if err != nil {
		return nil, err
	}

	var response *historyservice.PollMutableStateResponse
	op := func(ctx context.Context, client historyservice.HistoryServiceClient) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.PollMutableState(ctx, request, opts...)
		return err
	}
	err = c.executeWithRedirect(ctx, client, op)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (c *clientImpl) DescribeHistoryHost(
	ctx context.Context,
	request *historyservice.DescribeHistoryHostRequest,
	opts ...grpc.CallOption) (*historyservice.DescribeHistoryHostResponse, error) {

	var err error
	var client historyservice.HistoryServiceClient

	if request.GetShardIdForHost() != 0 {
		client, err = c.getClientForShardID(int(request.GetShardIdForHost()))
	} else if request.ExecutionForHost != nil {
		client, err = c.getClientForWorkflowID(request.ExecutionForHost.GetWorkflowId())
	} else {
		ret, err := c.clients.GetClientForClientKey(request.GetHostAddress())
		if err != nil {
			return nil, err
		}
		client = ret.(historyservice.HistoryServiceClient)
	}
	if err != nil {
		return nil, err
	}

	var response *historyservice.DescribeHistoryHostResponse
	op := func(ctx context.Context, client historyservice.HistoryServiceClient) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.DescribeHistoryHost(ctx, request, opts...)
		return err
	}
	err = c.executeWithRedirect(ctx, client, op)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (c *clientImpl) RemoveTask(
	ctx context.Context,
	request *historyservice.RemoveTaskRequest,
	opts ...grpc.CallOption) (*historyservice.RemoveTaskResponse, error) {
	var err error
	var client historyservice.HistoryServiceClient
	if request.GetShardID() != 0 {
		client, err = c.getClientForShardID(int(request.GetShardID()))
		if err != nil {
			return nil, err
		}
	}
	var response *historyservice.RemoveTaskResponse
	op := func(ctx context.Context, client historyservice.HistoryServiceClient) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.RemoveTask(ctx, request, opts...)
		return err
	}

	err = c.executeWithRedirect(ctx, client, op)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (c *clientImpl) CloseShard(
	ctx context.Context,
	request *historyservice.CloseShardRequest,
	opts ...grpc.CallOption) (*historyservice.CloseShardResponse, error) {

	var err error
	var client historyservice.HistoryServiceClient
	if request.ShardID != 0 {
		client, err = c.getClientForShardID(int(request.GetShardID()))
		if err != nil {
			return nil, err
		}
	}
	var response *historyservice.CloseShardResponse
	op := func(ctx context.Context, client historyservice.HistoryServiceClient) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.CloseShard(ctx, request, opts...)
		return err
	}

	err = c.executeWithRedirect(ctx, client, op)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (c *clientImpl) DescribeMutableState(
	ctx context.Context,
	request *historyservice.DescribeMutableStateRequest,
	opts ...grpc.CallOption) (*historyservice.DescribeMutableStateResponse, error) {
	client, err := c.getClientForWorkflowID(request.Execution.WorkflowId)
	if err != nil {
		return nil, err
	}

	var response *historyservice.DescribeMutableStateResponse
	op := func(ctx context.Context, client historyservice.HistoryServiceClient) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.DescribeMutableState(ctx, request, opts...)
		return err
	}
	err = c.executeWithRedirect(ctx, client, op)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (c *clientImpl) ResetStickyTaskList(
	ctx context.Context,
	request *historyservice.ResetStickyTaskListRequest,
	opts ...grpc.CallOption) (*historyservice.ResetStickyTaskListResponse, error) {
	client, err := c.getClientForWorkflowID(request.Execution.WorkflowId)
	if err != nil {
		return nil, err
	}

	var response *historyservice.ResetStickyTaskListResponse
	op := func(ctx context.Context, client historyservice.HistoryServiceClient) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.ResetStickyTaskList(ctx, request, opts...)
		return err
	}
	err = c.executeWithRedirect(ctx, client, op)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (c *clientImpl) DescribeWorkflowExecution(
	ctx context.Context,
	request *historyservice.DescribeWorkflowExecutionRequest,
	opts ...grpc.CallOption) (*historyservice.DescribeWorkflowExecutionResponse, error) {
	client, err := c.getClientForWorkflowID(request.Request.Execution.WorkflowId)
	if err != nil {
		return nil, err
	}

	var response *historyservice.DescribeWorkflowExecutionResponse
	op := func(ctx context.Context, client historyservice.HistoryServiceClient) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.DescribeWorkflowExecution(ctx, request, opts...)
		return err
	}
	err = c.executeWithRedirect(ctx, client, op)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (c *clientImpl) RecordDecisionTaskStarted(
	ctx context.Context,
	request *historyservice.RecordDecisionTaskStartedRequest,
	opts ...grpc.CallOption) (*historyservice.RecordDecisionTaskStartedResponse, error) {
	client, err := c.getClientForWorkflowID(request.WorkflowExecution.WorkflowId)
	if err != nil {
		return nil, err
	}

	var response *historyservice.RecordDecisionTaskStartedResponse
	op := func(ctx context.Context, client historyservice.HistoryServiceClient) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.RecordDecisionTaskStarted(ctx, request, opts...)
		return err
	}
	err = c.executeWithRedirect(ctx, client, op)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (c *clientImpl) RecordActivityTaskStarted(
	ctx context.Context,
	request *historyservice.RecordActivityTaskStartedRequest,
	opts ...grpc.CallOption) (*historyservice.RecordActivityTaskStartedResponse, error) {
	client, err := c.getClientForWorkflowID(request.WorkflowExecution.WorkflowId)
	if err != nil {
		return nil, err
	}

	var response *historyservice.RecordActivityTaskStartedResponse
	op := func(ctx context.Context, client historyservice.HistoryServiceClient) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.RecordActivityTaskStarted(ctx, request, opts...)
		return err
	}
	err = c.executeWithRedirect(ctx, client, op)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (c *clientImpl) RespondDecisionTaskCompleted(
	ctx context.Context,
	request *historyservice.RespondDecisionTaskCompletedRequest,
	opts ...grpc.CallOption) (*historyservice.RespondDecisionTaskCompletedResponse, error) {
	taskToken, err := c.tokenSerializer.Deserialize(request.CompleteRequest.TaskToken)
	if err != nil {
		return nil, err
	}
	client, err := c.getClientForWorkflowID(taskToken.GetWorkflowId())
	if err != nil {
		return nil, err
	}

	var response *historyservice.RespondDecisionTaskCompletedResponse
	op := func(ctx context.Context, client historyservice.HistoryServiceClient) error {
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.RespondDecisionTaskCompleted(ctx, request, opts...)
		return err
	}
	err = c.executeWithRedirect(ctx, client, op)
	return response, err
}

func (c *clientImpl) RespondDecisionTaskFailed(
	ctx context.Context,
	request *historyservice.RespondDecisionTaskFailedRequest,
	opts ...grpc.CallOption) (*historyservice.RespondDecisionTaskFailedResponse, error) {
	taskToken, err := c.tokenSerializer.Deserialize(request.FailedRequest.TaskToken)
	if err != nil {
		return nil, err
	}
	client, err := c.getClientForWorkflowID(taskToken.GetWorkflowId())
	if err != nil {
		return nil, err
	}

	var response *historyservice.RespondDecisionTaskFailedResponse
	op := func(ctx context.Context, client historyservice.HistoryServiceClient) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.RespondDecisionTaskFailed(ctx, request, opts...)
		return err
	}
	err = c.executeWithRedirect(ctx, client, op)
	if err != nil {
		return nil, err
	}
	return response, nil

}

func (c *clientImpl) RespondActivityTaskCompleted(
	ctx context.Context,
	request *historyservice.RespondActivityTaskCompletedRequest,
	opts ...grpc.CallOption) (*historyservice.RespondActivityTaskCompletedResponse, error) {
	taskToken, err := c.tokenSerializer.Deserialize(request.CompleteRequest.TaskToken)
	if err != nil {
		return nil, err
	}
	client, err := c.getClientForWorkflowID(taskToken.GetWorkflowId())
	if err != nil {
		return nil, err
	}

	var response *historyservice.RespondActivityTaskCompletedResponse
	op := func(ctx context.Context, client historyservice.HistoryServiceClient) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.RespondActivityTaskCompleted(ctx, request, opts...)
		return err
	}
	err = c.executeWithRedirect(ctx, client, op)
	if err != nil {
		return nil, err
	}
	return response, nil

}

func (c *clientImpl) RespondActivityTaskFailed(
	ctx context.Context,
	request *historyservice.RespondActivityTaskFailedRequest,
	opts ...grpc.CallOption) (*historyservice.RespondActivityTaskFailedResponse, error) {
	taskToken, err := c.tokenSerializer.Deserialize(request.FailedRequest.TaskToken)
	if err != nil {
		return nil, err
	}
	client, err := c.getClientForWorkflowID(taskToken.GetWorkflowId())
	if err != nil {
		return nil, err
	}

	var response *historyservice.RespondActivityTaskFailedResponse
	op := func(ctx context.Context, client historyservice.HistoryServiceClient) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.RespondActivityTaskFailed(ctx, request, opts...)
		return err
	}
	err = c.executeWithRedirect(ctx, client, op)
	if err != nil {
		return nil, err
	}
	return response, nil

}

func (c *clientImpl) RespondActivityTaskCanceled(
	ctx context.Context,
	request *historyservice.RespondActivityTaskCanceledRequest,
	opts ...grpc.CallOption) (*historyservice.RespondActivityTaskCanceledResponse, error) {
	taskToken, err := c.tokenSerializer.Deserialize(request.CancelRequest.TaskToken)
	if err != nil {
		return nil, err
	}
	client, err := c.getClientForWorkflowID(taskToken.GetWorkflowId())
	if err != nil {
		return nil, err
	}

	var response *historyservice.RespondActivityTaskCanceledResponse
	op := func(ctx context.Context, client historyservice.HistoryServiceClient) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.RespondActivityTaskCanceled(ctx, request, opts...)
		return err
	}
	err = c.executeWithRedirect(ctx, client, op)
	if err != nil {
		return nil, err
	}
	return response, nil

}

func (c *clientImpl) RecordActivityTaskHeartbeat(
	ctx context.Context,
	request *historyservice.RecordActivityTaskHeartbeatRequest,
	opts ...grpc.CallOption) (*historyservice.RecordActivityTaskHeartbeatResponse, error) {
	taskToken, err := c.tokenSerializer.Deserialize(request.HeartbeatRequest.TaskToken)
	if err != nil {
		return nil, err
	}
	client, err := c.getClientForWorkflowID(taskToken.GetWorkflowId())
	if err != nil {
		return nil, err
	}

	var response *historyservice.RecordActivityTaskHeartbeatResponse
	op := func(ctx context.Context, client historyservice.HistoryServiceClient) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.RecordActivityTaskHeartbeat(ctx, request, opts...)
		return err
	}
	err = c.executeWithRedirect(ctx, client, op)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (c *clientImpl) RequestCancelWorkflowExecution(
	ctx context.Context,
	request *historyservice.RequestCancelWorkflowExecutionRequest,
	opts ...grpc.CallOption) (*historyservice.RequestCancelWorkflowExecutionResponse, error) {
	client, err := c.getClientForWorkflowID(request.CancelRequest.WorkflowExecution.WorkflowId)
	if err != nil {
		return nil, err
	}

	var response *historyservice.RequestCancelWorkflowExecutionResponse
	op := func(ctx context.Context, client historyservice.HistoryServiceClient) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.RequestCancelWorkflowExecution(ctx, request, opts...)
		return err
	}
	err = c.executeWithRedirect(ctx, client, op)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (c *clientImpl) SignalWorkflowExecution(
	ctx context.Context,
	request *historyservice.SignalWorkflowExecutionRequest,
	opts ...grpc.CallOption) (*historyservice.SignalWorkflowExecutionResponse, error) {
	client, err := c.getClientForWorkflowID(request.SignalRequest.WorkflowExecution.WorkflowId)
	if err != nil {
		return nil, err
	}

	var response *historyservice.SignalWorkflowExecutionResponse
	op := func(ctx context.Context, client historyservice.HistoryServiceClient) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.SignalWorkflowExecution(ctx, request, opts...)
		return err
	}
	err = c.executeWithRedirect(ctx, client, op)
	if err != nil {
		return nil, err
	}
	return response, nil

}

func (c *clientImpl) SignalWithStartWorkflowExecution(
	ctx context.Context,
	request *historyservice.SignalWithStartWorkflowExecutionRequest,
	opts ...grpc.CallOption) (*historyservice.SignalWithStartWorkflowExecutionResponse, error) {
	client, err := c.getClientForWorkflowID(request.SignalWithStartRequest.WorkflowId)
	if err != nil {
		return nil, err
	}

	var response *historyservice.SignalWithStartWorkflowExecutionResponse
	op := func(ctx context.Context, client historyservice.HistoryServiceClient) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.SignalWithStartWorkflowExecution(ctx, request, opts...)
		return err
	}
	err = c.executeWithRedirect(ctx, client, op)
	if err != nil {
		return nil, err
	}
	return response, err
}

func (c *clientImpl) RemoveSignalMutableState(
	ctx context.Context,
	request *historyservice.RemoveSignalMutableStateRequest,
	opts ...grpc.CallOption) (*historyservice.RemoveSignalMutableStateResponse, error) {
	client, err := c.getClientForWorkflowID(request.WorkflowExecution.WorkflowId)
	if err != nil {
		return nil, err
	}
	var response *historyservice.RemoveSignalMutableStateResponse
	op := func(ctx context.Context, client historyservice.HistoryServiceClient) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.RemoveSignalMutableState(ctx, request, opts...)
		return err
	}
	err = c.executeWithRedirect(ctx, client, op)
	if err != nil {
		return nil, err
	}
	return response, err
}

func (c *clientImpl) TerminateWorkflowExecution(
	ctx context.Context,
	request *historyservice.TerminateWorkflowExecutionRequest,
	opts ...grpc.CallOption) (*historyservice.TerminateWorkflowExecutionResponse, error) {
	client, err := c.getClientForWorkflowID(request.TerminateRequest.WorkflowExecution.WorkflowId)
	if err != nil {
		return nil, err
	}

	var response *historyservice.TerminateWorkflowExecutionResponse
	op := func(ctx context.Context, client historyservice.HistoryServiceClient) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.TerminateWorkflowExecution(ctx, request, opts...)
		return err
	}
	err = c.executeWithRedirect(ctx, client, op)
	if err != nil {
		return nil, err
	}
	return response, nil

}

func (c *clientImpl) ResetWorkflowExecution(
	ctx context.Context,
	request *historyservice.ResetWorkflowExecutionRequest,
	opts ...grpc.CallOption) (*historyservice.ResetWorkflowExecutionResponse, error) {
	client, err := c.getClientForWorkflowID(request.ResetRequest.WorkflowExecution.WorkflowId)
	if err != nil {
		return nil, err
	}

	var response *historyservice.ResetWorkflowExecutionResponse
	op := func(ctx context.Context, client historyservice.HistoryServiceClient) error {
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.ResetWorkflowExecution(ctx, request, opts...)
		return err
	}
	err = c.executeWithRedirect(ctx, client, op)
	if err != nil {
		return nil, err
	}
	return response, err
}

func (c *clientImpl) ScheduleDecisionTask(
	ctx context.Context,
	request *historyservice.ScheduleDecisionTaskRequest,
	opts ...grpc.CallOption) (*historyservice.ScheduleDecisionTaskResponse, error) {
	client, err := c.getClientForWorkflowID(request.WorkflowExecution.WorkflowId)
	if err != nil {
		return nil, err
	}

	var response *historyservice.ScheduleDecisionTaskResponse
	op := func(ctx context.Context, client historyservice.HistoryServiceClient) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.ScheduleDecisionTask(ctx, request, opts...)
		return err
	}
	err = c.executeWithRedirect(ctx, client, op)
	if err != nil {
		return nil, err
	}
	return response, nil

}

func (c *clientImpl) RecordChildExecutionCompleted(
	ctx context.Context,
	request *historyservice.RecordChildExecutionCompletedRequest,
	opts ...grpc.CallOption) (*historyservice.RecordChildExecutionCompletedResponse, error) {
	client, err := c.getClientForWorkflowID(request.WorkflowExecution.WorkflowId)
	if err != nil {
		return nil, err
	}

	var response *historyservice.RecordChildExecutionCompletedResponse
	op := func(ctx context.Context, client historyservice.HistoryServiceClient) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.RecordChildExecutionCompleted(ctx, request, opts...)
		return err
	}
	err = c.executeWithRedirect(ctx, client, op)
	if err != nil {
		return nil, err
	}
	return response, nil

}

func (c *clientImpl) ReplicateEvents(
	ctx context.Context,
	request *historyservice.ReplicateEventsRequest,
	opts ...grpc.CallOption) (*historyservice.ReplicateEventsResponse, error) {
	client, err := c.getClientForWorkflowID(request.WorkflowExecution.GetWorkflowId())
	if err != nil {
		return nil, err
	}

	var response *historyservice.ReplicateEventsResponse
	op := func(ctx context.Context, client historyservice.HistoryServiceClient) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.ReplicateEvents(ctx, request, opts...)
		return err
	}
	err = c.executeWithRedirect(ctx, client, op)
	if err != nil {
		return nil, err
	}
	return response, nil

}

func (c *clientImpl) ReplicateRawEvents(
	ctx context.Context,
	request *historyservice.ReplicateRawEventsRequest,
	opts ...grpc.CallOption) (*historyservice.ReplicateRawEventsResponse, error) {
	client, err := c.getClientForWorkflowID(request.WorkflowExecution.GetWorkflowId())
	if err != nil {
		return nil, err
	}

	var response *historyservice.ReplicateRawEventsResponse
	op := func(ctx context.Context, client historyservice.HistoryServiceClient) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.ReplicateRawEvents(ctx, request, opts...)
		return err
	}
	err = c.executeWithRedirect(ctx, client, op)
	if err != nil {
		return nil, err
	}
	return response, nil

}

func (c *clientImpl) ReplicateEventsV2(
	ctx context.Context,
	request *historyservice.ReplicateEventsV2Request,
	opts ...grpc.CallOption) (*historyservice.ReplicateEventsV2Response, error) {
	client, err := c.getClientForWorkflowID(request.WorkflowExecution.GetWorkflowId())
	if err != nil {
		return nil, err
	}

	var response *historyservice.ReplicateEventsV2Response
	op := func(ctx context.Context, client historyservice.HistoryServiceClient) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.ReplicateEventsV2(ctx, request, opts...)
		return err
	}
	err = c.executeWithRedirect(ctx, client, op)
	if err != nil {
		return nil, err
	}
	return response, nil

}

func (c *clientImpl) SyncShardStatus(
	ctx context.Context,
	request *historyservice.SyncShardStatusRequest,
	opts ...grpc.CallOption) (*historyservice.SyncShardStatusResponse, error) {

	// we do not have a workflow ID here, instead, we have something even better
	client, err := c.getClientForShardID(int(request.GetShardId()))
	if err != nil {
		return nil, err
	}

	var response *historyservice.SyncShardStatusResponse
	op := func(ctx context.Context, client historyservice.HistoryServiceClient) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.SyncShardStatus(ctx, request, opts...)
		return err
	}
	err = c.executeWithRedirect(ctx, client, op)
	if err != nil {
		return nil, err
	}
	return response, nil

}

func (c *clientImpl) SyncActivity(
	ctx context.Context,
	request *historyservice.SyncActivityRequest,
	opts ...grpc.CallOption) (*historyservice.SyncActivityResponse, error) {

	client, err := c.getClientForWorkflowID(request.GetWorkflowId())
	if err != nil {
		return nil, err
	}

	var response *historyservice.SyncActivityResponse
	op := func(ctx context.Context, client historyservice.HistoryServiceClient) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.SyncActivity(ctx, request, opts...)
		return err
	}
	err = c.executeWithRedirect(ctx, client, op)
	if err != nil {
		return nil, err
	}
	return response, nil

}

func (c *clientImpl) QueryWorkflow(
	ctx context.Context,
	request *historyservice.QueryWorkflowRequest,
	opts ...grpc.CallOption,
) (*historyservice.QueryWorkflowResponse, error) {
	client, err := c.getClientForWorkflowID(request.GetRequest().GetExecution().GetWorkflowId())
	if err != nil {
		return nil, err
	}

	var response *historyservice.QueryWorkflowResponse
	op := func(ctx context.Context, client historyservice.HistoryServiceClient) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.QueryWorkflow(ctx, request, opts...)
		return err
	}
	err = c.executeWithRedirect(ctx, client, op)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (c *clientImpl) GetReplicationMessages(
	ctx context.Context,
	request *historyservice.GetReplicationMessagesRequest,
	opts ...grpc.CallOption,
) (*historyservice.GetReplicationMessagesResponse, error) {
	requestsByClient := make(map[historyservice.HistoryServiceClient]*historyservice.GetReplicationMessagesRequest)

	for _, token := range request.Tokens {
		client, err := c.getClientForShardID(int(token.GetShardID()))
		if err != nil {
			return nil, err
		}

		if _, ok := requestsByClient[client]; !ok {
			requestsByClient[client] = &historyservice.GetReplicationMessagesRequest{
				ClusterName: request.ClusterName,
			}
		}

		req := requestsByClient[client]
		req.Tokens = append(req.Tokens, token)
	}

	var wg sync.WaitGroup
	wg.Add(len(requestsByClient))
	respChan := make(chan *historyservice.GetReplicationMessagesResponse, len(requestsByClient))
	for client, req := range requestsByClient {
		go func(client historyservice.HistoryServiceClient, request *historyservice.GetReplicationMessagesRequest) {
			defer wg.Done()

			ctx, cancel := c.createContext(ctx)
			defer cancel()
			resp, err := client.GetReplicationMessages(ctx, request, opts...)
			if err != nil {
				c.logger.Warn("Failed to get replication tasks from client", tag.Error(err))
				return
			}
			respChan <- resp
		}(client, req)
	}

	wg.Wait()
	close(respChan)

	response := &historyservice.GetReplicationMessagesResponse{MessagesByShard: make(map[int32]*replication.ReplicationMessages)}
	for resp := range respChan {
		for shardID, tasks := range resp.MessagesByShard {
			response.MessagesByShard[shardID] = tasks
		}
	}

	return response, nil
}

func (c *clientImpl) GetDLQReplicationMessages(
	ctx context.Context,
	request *historyservice.GetDLQReplicationMessagesRequest,
	opts ...grpc.CallOption,
) (*historyservice.GetDLQReplicationMessagesResponse, error) {
	// All workflow IDs are in the same shard per request
	workflowID := request.GetTaskInfos()[0].GetWorkflowId()
	client, err := c.getClientForWorkflowID(workflowID)
	if err != nil {
		return nil, err
	}

	return client.GetDLQReplicationMessages(
		ctx,
		request,
		opts...,
	)
}

func (c *clientImpl) ReapplyEvents(
	ctx context.Context,
	request *historyservice.ReapplyEventsRequest,
	opts ...grpc.CallOption,
) (*historyservice.ReapplyEventsResponse, error) {
	client, err := c.getClientForWorkflowID(request.GetRequest().GetWorkflowExecution().GetWorkflowId())
	if err != nil {
		return nil, err
	}

	var response *historyservice.ReapplyEventsResponse
	op := func(ctx context.Context, client historyservice.HistoryServiceClient) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.ReapplyEvents(ctx, request, opts...)
		return err
	}
	err = c.executeWithRedirect(ctx, client, op)
	if err != nil {
		return nil, err
	}
	return response, nil

}

func (c *clientImpl) ReadDLQMessages(
	ctx context.Context,
	request *historyservice.ReadDLQMessagesRequest,
	opts ...grpc.CallOption,
) (*historyservice.ReadDLQMessagesResponse, error) {

	client, err := c.getClientForShardID(int(request.GetShardID()))
	if err != nil {
		return nil, err
	}
	return client.ReadDLQMessages(ctx, request, opts...)
}

func (c *clientImpl) PurgeDLQMessages(
	ctx context.Context,
	request *historyservice.PurgeDLQMessagesRequest,
	opts ...grpc.CallOption,
) (*historyservice.PurgeDLQMessagesResponse, error) {

	client, err := c.getClientForShardID(int(request.GetShardID()))
	if err != nil {
		return nil, err
	}
	return client.PurgeDLQMessages(ctx, request, opts...)
}

func (c *clientImpl) MergeDLQMessages(
	ctx context.Context,
	request *historyservice.MergeDLQMessagesRequest,
	opts ...grpc.CallOption,
) (*historyservice.MergeDLQMessagesResponse, error) {

	client, err := c.getClientForShardID(int(request.GetShardID()))
	if err != nil {
		return nil, err
	}
	return client.MergeDLQMessages(ctx, request, opts...)
}

func (c *clientImpl) RefreshWorkflowTasks(
	ctx context.Context,
	request *historyservice.RefreshWorkflowTasksRequest,
	opts ...grpc.CallOption,
) (*historyservice.RefreshWorkflowTasksResponse, error) {
	client, err := c.getClientForWorkflowID(request.GetRequest().GetExecution().GetWorkflowId())
	var response *historyservice.RefreshWorkflowTasksResponse
	op := func(ctx context.Context, client historyservice.HistoryServiceClient) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.RefreshWorkflowTasks(ctx, request, opts...)
		return err
	}
	err = c.executeWithRedirect(ctx, client, op)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (c *clientImpl) createContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, c.timeout)
}

func (c *clientImpl) getClientForWorkflowID(workflowID string) (historyservice.HistoryServiceClient, error) {
	key := common.WorkflowIDToHistoryShard(workflowID, c.numberOfShards)
	return c.getClientForShardID(key)
}

func (c *clientImpl) getClientForShardID(shardID int) (historyservice.HistoryServiceClient, error) {
	client, err := c.clients.GetClientForKey(string(shardID))
	if err != nil {
		return nil, err
	}
	return client.(historyservice.HistoryServiceClient), nil
}

func (c *clientImpl) executeWithRedirect(ctx context.Context,
	client historyservice.HistoryServiceClient,
	op func(ctx context.Context, client historyservice.HistoryServiceClient) error) error {

	var err error
redirectLoop:
	for {
		err = common.IsValidContext(ctx)
		if err != nil {
			break redirectLoop
		}
		err = op(ctx, client)
		if err != nil {
			if s, ok := err.(*serviceerror.ShardOwnershipLost); ok {
				// TODO: consider emitting a metric for number of redirects
				ret, err := c.clients.GetClientForClientKey(s.Owner)
				if err != nil {
					return err
				}
				client = ret.(historyservice.HistoryServiceClient)
				continue redirectLoop
			}
		}
		break redirectLoop
	}
	return err
}
