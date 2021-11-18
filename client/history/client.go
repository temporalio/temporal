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

package history

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.temporal.io/api/serviceerror"
	"google.golang.org/grpc"

	"go.temporal.io/server/api/historyservice/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	serviceerrors "go.temporal.io/server/common/serviceerror"
)

var _ historyservice.HistoryServiceClient = (*clientImpl)(nil)

const (
	// DefaultTimeout is the default timeout used to make calls
	DefaultTimeout = time.Second * 30
)

type clientImpl struct {
	numberOfShards  int32
	tokenSerializer common.TaskTokenSerializer
	timeout         time.Duration
	clients         common.ClientCache
	logger          log.Logger
}

// NewClient creates a new history service gRPC client
func NewClient(
	numberOfShards int32,
	timeout time.Duration,
	clients common.ClientCache,
	logger log.Logger,
) historyservice.HistoryServiceClient {
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
	client, err := c.getClientForWorkflowID(request.NamespaceId, request.StartRequest.WorkflowId)
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
	client, err := c.getClientForWorkflowID(request.NamespaceId, request.Execution.WorkflowId)
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
	client, err := c.getClientForWorkflowID(request.NamespaceId, request.Execution.WorkflowId)
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

	if request.GetShardId() != 0 {
		client, err = c.getClientForShardID(request.GetShardId())
	} else if request.GetWorkflowExecution() != nil {
		client, err = c.getClientForWorkflowID(request.GetNamespaceId(), request.GetWorkflowExecution().GetWorkflowId())
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
	if request.GetShardId() != 0 {
		client, err = c.getClientForShardID(request.GetShardId())
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
	if request.ShardId != 0 {
		client, err = c.getClientForShardID(request.GetShardId())
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

func (c *clientImpl) GetShard(
	ctx context.Context,
	request *historyservice.GetShardRequest,
	opts ...grpc.CallOption) (*historyservice.GetShardResponse, error) {

	var err error
	var client historyservice.HistoryServiceClient
	if request.ShardId != 0 {
		client, err = c.getClientForShardID(request.GetShardId())
		if err != nil {
			return nil, err
		}
	}
	var response *historyservice.GetShardResponse
	op := func(ctx context.Context, client historyservice.HistoryServiceClient) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.GetShard(ctx, request, opts...)
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
	client, err := c.getClientForWorkflowID(request.NamespaceId, request.Execution.WorkflowId)
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

func (c *clientImpl) ResetStickyTaskQueue(
	ctx context.Context,
	request *historyservice.ResetStickyTaskQueueRequest,
	opts ...grpc.CallOption) (*historyservice.ResetStickyTaskQueueResponse, error) {
	client, err := c.getClientForWorkflowID(request.NamespaceId, request.Execution.WorkflowId)
	if err != nil {
		return nil, err
	}

	var response *historyservice.ResetStickyTaskQueueResponse
	op := func(ctx context.Context, client historyservice.HistoryServiceClient) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.ResetStickyTaskQueue(ctx, request, opts...)
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
	client, err := c.getClientForWorkflowID(request.NamespaceId, request.Request.Execution.WorkflowId)
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

func (c *clientImpl) RecordWorkflowTaskStarted(
	ctx context.Context,
	request *historyservice.RecordWorkflowTaskStartedRequest,
	opts ...grpc.CallOption) (*historyservice.RecordWorkflowTaskStartedResponse, error) {
	client, err := c.getClientForWorkflowID(request.NamespaceId, request.WorkflowExecution.WorkflowId)
	if err != nil {
		return nil, err
	}

	var response *historyservice.RecordWorkflowTaskStartedResponse
	op := func(ctx context.Context, client historyservice.HistoryServiceClient) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.RecordWorkflowTaskStarted(ctx, request, opts...)
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
	client, err := c.getClientForWorkflowID(request.NamespaceId, request.WorkflowExecution.WorkflowId)
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

func (c *clientImpl) RespondWorkflowTaskCompleted(
	ctx context.Context,
	request *historyservice.RespondWorkflowTaskCompletedRequest,
	opts ...grpc.CallOption) (*historyservice.RespondWorkflowTaskCompletedResponse, error) {
	taskToken, err := c.tokenSerializer.Deserialize(request.CompleteRequest.TaskToken)
	if err != nil {
		return nil, err
	}
	client, err := c.getClientForWorkflowID(request.NamespaceId, taskToken.GetWorkflowId())
	if err != nil {
		return nil, err
	}

	var response *historyservice.RespondWorkflowTaskCompletedResponse
	op := func(ctx context.Context, client historyservice.HistoryServiceClient) error {
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.RespondWorkflowTaskCompleted(ctx, request, opts...)
		return err
	}
	err = c.executeWithRedirect(ctx, client, op)
	return response, err
}

func (c *clientImpl) RespondWorkflowTaskFailed(
	ctx context.Context,
	request *historyservice.RespondWorkflowTaskFailedRequest,
	opts ...grpc.CallOption) (*historyservice.RespondWorkflowTaskFailedResponse, error) {
	taskToken, err := c.tokenSerializer.Deserialize(request.FailedRequest.TaskToken)
	if err != nil {
		return nil, err
	}
	client, err := c.getClientForWorkflowID(request.NamespaceId, taskToken.GetWorkflowId())
	if err != nil {
		return nil, err
	}

	var response *historyservice.RespondWorkflowTaskFailedResponse
	op := func(ctx context.Context, client historyservice.HistoryServiceClient) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.RespondWorkflowTaskFailed(ctx, request, opts...)
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
	client, err := c.getClientForWorkflowID(request.NamespaceId, taskToken.GetWorkflowId())
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
	client, err := c.getClientForWorkflowID(request.NamespaceId, taskToken.GetWorkflowId())
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
	client, err := c.getClientForWorkflowID(request.NamespaceId, taskToken.GetWorkflowId())
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
	client, err := c.getClientForWorkflowID(request.NamespaceId, taskToken.GetWorkflowId())
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
	client, err := c.getClientForWorkflowID(request.NamespaceId, request.CancelRequest.WorkflowExecution.WorkflowId)
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
	client, err := c.getClientForWorkflowID(request.NamespaceId, request.SignalRequest.WorkflowExecution.WorkflowId)
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
	client, err := c.getClientForWorkflowID(request.NamespaceId, request.SignalWithStartRequest.WorkflowId)
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
	client, err := c.getClientForWorkflowID(request.NamespaceId, request.WorkflowExecution.WorkflowId)
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
	client, err := c.getClientForWorkflowID(request.NamespaceId, request.TerminateRequest.WorkflowExecution.WorkflowId)
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
	client, err := c.getClientForWorkflowID(request.NamespaceId, request.ResetRequest.WorkflowExecution.WorkflowId)
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

func (c *clientImpl) ScheduleWorkflowTask(
	ctx context.Context,
	request *historyservice.ScheduleWorkflowTaskRequest,
	opts ...grpc.CallOption) (*historyservice.ScheduleWorkflowTaskResponse, error) {
	client, err := c.getClientForWorkflowID(request.NamespaceId, request.WorkflowExecution.WorkflowId)
	if err != nil {
		return nil, err
	}

	var response *historyservice.ScheduleWorkflowTaskResponse
	op := func(ctx context.Context, client historyservice.HistoryServiceClient) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.ScheduleWorkflowTask(ctx, request, opts...)
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
	client, err := c.getClientForWorkflowID(request.NamespaceId, request.WorkflowExecution.WorkflowId)
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

func (c *clientImpl) ReplicateEventsV2(
	ctx context.Context,
	request *historyservice.ReplicateEventsV2Request,
	opts ...grpc.CallOption) (*historyservice.ReplicateEventsV2Response, error) {
	client, err := c.getClientForWorkflowID(request.NamespaceId, request.WorkflowExecution.GetWorkflowId())
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
	client, err := c.getClientForShardID(int32(request.GetShardId()))
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

	client, err := c.getClientForWorkflowID(request.NamespaceId, request.GetWorkflowId())
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
	client, err := c.getClientForWorkflowID(request.NamespaceId, request.GetRequest().GetExecution().GetWorkflowId())
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
		client, err := c.getClientForShardID(token.GetShardId())
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
	errChan := make(chan error, 1)
	for client, req := range requestsByClient {
		go func(client historyservice.HistoryServiceClient, request *historyservice.GetReplicationMessagesRequest) {
			defer wg.Done()

			ctx, cancel := c.createContext(ctx)
			defer cancel()
			resp, err := client.GetReplicationMessages(ctx, request, opts...)
			if err != nil {
				c.logger.Warn("Failed to get replication tasks from client", tag.Error(err))
				// Returns service busy error to notify replication
				if _, ok := err.(*serviceerror.ResourceExhausted); ok {
					select {
					case errChan <- err:
					default:
					}
				}
				return
			}
			respChan <- resp
		}(client, req)
	}

	wg.Wait()
	close(respChan)
	close(errChan)

	response := &historyservice.GetReplicationMessagesResponse{ShardMessages: make(map[int32]*replicationspb.ReplicationMessages)}
	for resp := range respChan {
		for shardID, tasks := range resp.ShardMessages {
			response.ShardMessages[shardID] = tasks
		}
	}
	var err error
	if len(errChan) > 0 {
		err = <-errChan
	}
	return response, err
}

func (c *clientImpl) GetDLQReplicationMessages(
	ctx context.Context,
	request *historyservice.GetDLQReplicationMessagesRequest,
	opts ...grpc.CallOption,
) (*historyservice.GetDLQReplicationMessagesResponse, error) {
	// All workflow IDs are in the same shard per request
	namespaceID := request.GetTaskInfos()[0].GetNamespaceId()
	workflowID := request.GetTaskInfos()[0].GetWorkflowId()
	client, err := c.getClientForWorkflowID(namespaceID, workflowID)
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
	client, err := c.getClientForWorkflowID(request.NamespaceId,
		request.GetRequest().GetWorkflowExecution().GetWorkflowId())
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

func (c *clientImpl) GetDLQMessages(
	ctx context.Context,
	request *historyservice.GetDLQMessagesRequest,
	opts ...grpc.CallOption,
) (*historyservice.GetDLQMessagesResponse, error) {

	client, err := c.getClientForShardID(request.GetShardId())
	if err != nil {
		return nil, err
	}
	return client.GetDLQMessages(ctx, request, opts...)
}

func (c *clientImpl) PurgeDLQMessages(
	ctx context.Context,
	request *historyservice.PurgeDLQMessagesRequest,
	opts ...grpc.CallOption,
) (*historyservice.PurgeDLQMessagesResponse, error) {

	client, err := c.getClientForShardID(request.GetShardId())
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

	client, err := c.getClientForShardID(request.GetShardId())
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
	client, err := c.getClientForWorkflowID(request.NamespaceId, request.GetRequest().GetExecution().GetWorkflowId())
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

func (c *clientImpl) GenerateLastHistoryReplicationTasks(
	ctx context.Context,
	request *historyservice.GenerateLastHistoryReplicationTasksRequest,
	opts ...grpc.CallOption,
) (*historyservice.GenerateLastHistoryReplicationTasksResponse, error) {
	client, err := c.getClientForWorkflowID(request.NamespaceId, request.GetExecution().GetWorkflowId())
	var response *historyservice.GenerateLastHistoryReplicationTasksResponse
	op := func(ctx context.Context, client historyservice.HistoryServiceClient) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.GenerateLastHistoryReplicationTasks(ctx, request, opts...)
		return err
	}
	err = c.executeWithRedirect(ctx, client, op)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (c *clientImpl) GetReplicationStatus(
	ctx context.Context,
	request *historyservice.GetReplicationStatusRequest,
	opts ...grpc.CallOption,
) (*historyservice.GetReplicationStatusResponse, error) {
	clients, err := c.clients.GetAllClients()
	if err != nil {
		return nil, err
	}

	respChan := make(chan *historyservice.GetReplicationStatusResponse, len(clients))
	errChan := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(len(clients))
	for _, client := range clients {
		historyClient := client.(historyservice.HistoryServiceClient)
		go func(client historyservice.HistoryServiceClient) {
			defer wg.Done()
			resp, err := historyClient.GetReplicationStatus(ctx, request, opts...)
			if err != nil {
				select {
				case errChan <- err:
				default:
				}
			} else {
				respChan <- resp
			}
		}(historyClient)
	}
	wg.Wait()
	close(respChan)
	close(errChan)

	response := &historyservice.GetReplicationStatusResponse{}
	for resp := range respChan {
		response.Shards = append(response.Shards, resp.Shards...)
	}

	if len(errChan) > 0 {
		err = <-errChan
		return response, err
	}

	return response, nil
}

func (c *clientImpl) createContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, c.timeout)
}

func (c *clientImpl) getClientForWorkflowID(namespaceID, workflowID string) (historyservice.HistoryServiceClient, error) {
	key := common.WorkflowIDToHistoryShard(namespaceID, workflowID, c.numberOfShards)
	return c.getClientForShardID(key)
}

func (c *clientImpl) getClientForShardID(shardID int32) (historyservice.HistoryServiceClient, error) {
	if shardID <= 0 {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("Invalid ShardID: %d", shardID))
	}
	client, err := c.clients.GetClientForKey(convert.Int32ToString(shardID))
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
			if s, ok := err.(*serviceerrors.ShardOwnershipLost); ok {
				// TODO: consider emitting a metric for number of redirects
				ret, err := c.clients.GetClientForClientKey(s.OwnerHost)
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
