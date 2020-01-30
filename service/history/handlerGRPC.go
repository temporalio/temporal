// Copyright (c) 2019 Temporal Technologies, Inc.
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

	"github.com/temporalio/temporal/.gen/proto/healthservice"
	"github.com/temporalio/temporal/.gen/proto/historyservice"
	"github.com/temporalio/temporal/common/adapter"
	"github.com/temporalio/temporal/common/log"
)

var _ historyservice.HistoryServiceYARPCServer = (*HandlerGRPC)(nil)

type (
	// HandlerGRPC - gRPC handler interface for historyservice
	HandlerGRPC struct {
		handlerThrift *Handler
	}
)

// NewHandlerGRPC creates a gRPC handler for the cadence historyservice
func NewHandlerGRPC(
	handlerThrift *Handler,
) *HandlerGRPC {
	handler := &HandlerGRPC{
		handlerThrift: handlerThrift,
	}

	return handler
}

// RegisterHandler register this handler, must be called before Start()
func (h *HandlerGRPC) RegisterHandler() {
	h.handlerThrift.Resource.GetGRPCDispatcher().Register(historyservice.BuildHistoryServiceYARPCProcedures(h))
	h.handlerThrift.Resource.GetGRPCDispatcher().Register(healthservice.BuildMetaYARPCProcedures(h))
}

// Health is for health check
func (h *HandlerGRPC) Health(ctx context.Context, _ *healthservice.HealthRequest) (_ *healthservice.HealthStatus, retError error) {
	h.handlerThrift.startWG.Wait()
	h.handlerThrift.GetLogger().Debug("Matching service health check endpoint reached.")
	hs := &healthservice.HealthStatus{Ok: true, Msg: "matching good"}
	return hs, nil
}

func (h *HandlerGRPC) StartWorkflowExecution(ctx context.Context, request *historyservice.StartWorkflowExecutionRequest) (_ *historyservice.StartWorkflowExecutionResponse, retError error) {
	defer log.CapturePanicGRPC(h.handlerThrift.GetLogger(), &retError)

	resp, err := h.handlerThrift.StartWorkflowExecution(ctx, adapter.ToThriftHistoryStartWorkflowExecutionRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoHistoryStartWorkflowExecutionResponse(resp), nil
}

func (h *HandlerGRPC) GetMutableState(ctx context.Context, request *historyservice.GetMutableStateRequest) (_ *historyservice.GetMutableStateResponse, retError error) {
	panic("implement me")
}

func (h *HandlerGRPC) PollMutableState(ctx context.Context, request *historyservice.PollMutableStateRequest) (_ *historyservice.PollMutableStateResponse, retError error) {
	panic("implement me")
}

func (h *HandlerGRPC) ResetStickyTaskList(ctx context.Context, request *historyservice.ResetStickyTaskListRequest) (_ *historyservice.ResetStickyTaskListResponse, retError error) {
	panic("implement me")
}

func (h *HandlerGRPC) RecordDecisionTaskStarted(ctx context.Context, request *historyservice.RecordDecisionTaskStartedRequest) (_ *historyservice.RecordDecisionTaskStartedResponse, retError error) {
	panic("implement me")
}

func (h *HandlerGRPC) RecordActivityTaskStarted(ctx context.Context, request *historyservice.RecordActivityTaskStartedRequest) (_ *historyservice.RecordActivityTaskStartedResponse, retError error) {
	panic("implement me")
}

func (h *HandlerGRPC) RespondDecisionTaskCompleted(ctx context.Context, request *historyservice.RespondDecisionTaskCompletedRequest) (_ *historyservice.RespondDecisionTaskCompletedResponse, retError error) {
	panic("implement me")
}

func (h *HandlerGRPC) RespondDecisionTaskFailed(ctx context.Context, request *historyservice.RespondDecisionTaskFailedRequest) (_ *historyservice.RespondDecisionTaskFailedResponse, retError error) {
	panic("implement me")
}

func (h *HandlerGRPC) RecordActivityTaskHeartbeat(ctx context.Context, request *historyservice.RecordActivityTaskHeartbeatRequest) (_ *historyservice.RecordActivityTaskHeartbeatResponse, retError error) {
	panic("implement me")
}

func (h *HandlerGRPC) RespondActivityTaskCompleted(ctx context.Context, request *historyservice.RespondActivityTaskCompletedRequest) (_ *historyservice.RespondActivityTaskCompletedResponse, retError error) {
	panic("implement me")
}

func (h *HandlerGRPC) RespondActivityTaskFailed(ctx context.Context, request *historyservice.RespondActivityTaskFailedRequest) (_ *historyservice.RespondActivityTaskFailedResponse, retError error) {
	panic("implement me")
}

func (h *HandlerGRPC) RespondActivityTaskCanceled(ctx context.Context, request *historyservice.RespondActivityTaskCanceledRequest) (_ *historyservice.RespondActivityTaskCanceledResponse, retError error) {
	panic("implement me")
}

func (h *HandlerGRPC) SignalWorkflowExecution(ctx context.Context, request *historyservice.SignalWorkflowExecutionRequest) (_ *historyservice.SignalWorkflowExecutionResponse, retError error) {
	panic("implement me")
}

func (h *HandlerGRPC) SignalWithStartWorkflowExecution(ctx context.Context, request *historyservice.SignalWithStartWorkflowExecutionRequest) (_ *historyservice.StartWorkflowExecutionResponse, retError error) {
	panic("implement me")
}

func (h *HandlerGRPC) RemoveSignalMutableState(ctx context.Context, request *historyservice.RemoveSignalMutableStateRequest) (_ *historyservice.RemoveSignalMutableStateResponse, retError error) {
	panic("implement me")
}

func (h *HandlerGRPC) TerminateWorkflowExecution(ctx context.Context, request *historyservice.TerminateWorkflowExecutionRequest) (_ *historyservice.TerminateWorkflowExecutionResponse, retError error) {
	panic("implement me")
}

func (h *HandlerGRPC) ResetWorkflowExecution(ctx context.Context, request *historyservice.ResetWorkflowExecutionRequest) (_ *historyservice.ResetWorkflowExecutionResponse, retError error) {
	panic("implement me")
}

func (h *HandlerGRPC) RequestCancelWorkflowExecution(ctx context.Context, request *historyservice.RequestCancelWorkflowExecutionRequest) (_ *historyservice.RequestCancelWorkflowExecutionResponse, retError error) {
	panic("implement me")
}

func (h *HandlerGRPC) ScheduleDecisionTask(ctx context.Context, request *historyservice.ScheduleDecisionTaskRequest) (_ *historyservice.ScheduleDecisionTaskResponse, retError error) {
	panic("implement me")
}

func (h *HandlerGRPC) RecordChildExecutionCompleted(ctx context.Context, request *historyservice.RecordChildExecutionCompletedRequest) (_ *historyservice.RecordChildExecutionCompletedResponse, retError error) {
	panic("implement me")
}

func (h *HandlerGRPC) DescribeWorkflowExecution(ctx context.Context, request *historyservice.DescribeWorkflowExecutionRequest) (_ *historyservice.DescribeWorkflowExecutionResponse, retError error) {
	panic("implement me")
}

func (h *HandlerGRPC) ReplicateEvents(ctx context.Context, request *historyservice.ReplicateEventsRequest) (_ *historyservice.ReplicateEventsResponse, retError error) {
	panic("implement me")
}

func (h *HandlerGRPC) ReplicateRawEvents(ctx context.Context, request *historyservice.ReplicateRawEventsRequest) (_ *historyservice.ReplicateRawEventsResponse, retError error) {
	panic("implement me")
}

func (h *HandlerGRPC) ReplicateEventsV2(ctx context.Context, request *historyservice.ReplicateEventsV2Request) (_ *historyservice.ReplicateEventsV2Response, retError error) {
	panic("implement me")
}

func (h *HandlerGRPC) SyncShardStatus(ctx context.Context, request *historyservice.SyncShardStatusRequest) (_ *historyservice.SyncShardStatusResponse, retError error) {
	panic("implement me")
}

func (h *HandlerGRPC) SyncActivity(ctx context.Context, request *historyservice.SyncActivityRequest) (_ *historyservice.SyncActivityResponse, retError error) {
	panic("implement me")
}

func (h *HandlerGRPC) DescribeMutableState(ctx context.Context, request *historyservice.DescribeMutableStateRequest) (_ *historyservice.DescribeMutableStateResponse, retError error) {
	panic("implement me")
}

func (h *HandlerGRPC) DescribeHistoryHost(ctx context.Context, request *historyservice.DescribeHistoryHostRequest) (_ *historyservice.DescribeHistoryHostResponse, retError error) {
	panic("implement me")
}

func (h *HandlerGRPC) CloseShard(ctx context.Context, request *historyservice.CloseShardRequest) (_ *historyservice.CloseShardResponse, retError error) {
	panic("implement me")
}

func (h *HandlerGRPC) RemoveTask(ctx context.Context, request *historyservice.RemoveTaskRequest) (_ *historyservice.RemoveTaskResponse, retError error) {
	panic("implement me")
}

func (h *HandlerGRPC) GetReplicationMessages(ctx context.Context, request *historyservice.GetReplicationMessagesRequest) (_ *historyservice.GetReplicationMessagesResponse, retError error) {
	panic("implement me")
}

func (h *HandlerGRPC) GetDLQReplicationMessages(ctx context.Context, request *historyservice.GetDLQReplicationMessagesRequest) (_ *historyservice.GetDLQReplicationMessagesResponse, retError error) {
	panic("implement me")
}

func (h *HandlerGRPC) QueryWorkflow(ctx context.Context, request *historyservice.QueryWorkflowRequest) (_ *historyservice.QueryWorkflowResponse, retError error) {
	panic("implement me")
}

func (h *HandlerGRPC) ReapplyEvents(ctx context.Context, request *historyservice.ReapplyEventsRequest) (_ *historyservice.ReapplyEventsResponse, retError error) {
	panic("implement me")
}
