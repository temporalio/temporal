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

	"github.com/temporalio/temporal/.gen/proto/historyservice"
)

// Disable lint due to missing comments.
// Code generated by generate-adapter. DO NOT EDIT.

var _ historyservice.HistoryServiceServer = (*NilCheckHandler)(nil)

type (
	// NilCheckHandler - gRPC handler interface for historyservice
	NilCheckHandler struct {
		parentHandler historyservice.HistoryServiceServer
	}
)

// NewNilCheckHandler creates a gRPC handler for the historyservice
func NewNilCheckHandler(
	parentHandler historyservice.HistoryServiceServer,
) *NilCheckHandler {
	handler := &NilCheckHandler{
		parentHandler: parentHandler,
	}

	return handler
}

func (h *NilCheckHandler) StartWorkflowExecution(ctx context.Context, request *historyservice.StartWorkflowExecutionRequest) (_ *historyservice.StartWorkflowExecutionResponse, retError error) {
	resp, err := h.parentHandler.StartWorkflowExecution(ctx, request)
	if resp == nil && err == nil {
		return &historyservice.StartWorkflowExecutionResponse{}, err
	}
	return resp, err
}

func (h *NilCheckHandler) GetMutableState(ctx context.Context, request *historyservice.GetMutableStateRequest) (_ *historyservice.GetMutableStateResponse, retError error) {
	resp, err := h.parentHandler.GetMutableState(ctx, request)
	if resp == nil && err == nil {
		return &historyservice.GetMutableStateResponse{}, err
	}
	return resp, err
}

func (h *NilCheckHandler) PollMutableState(ctx context.Context, request *historyservice.PollMutableStateRequest) (_ *historyservice.PollMutableStateResponse, retError error) {
	resp, err := h.parentHandler.PollMutableState(ctx, request)
	if resp == nil && err == nil {
		return &historyservice.PollMutableStateResponse{}, err
	}
	return resp, err
}

func (h *NilCheckHandler) ResetStickyTaskList(ctx context.Context, request *historyservice.ResetStickyTaskListRequest) (_ *historyservice.ResetStickyTaskListResponse, retError error) {
	resp, err := h.parentHandler.ResetStickyTaskList(ctx, request)
	if resp == nil && err == nil {
		return &historyservice.ResetStickyTaskListResponse{}, err
	}
	return resp, err
}

func (h *NilCheckHandler) RecordDecisionTaskStarted(ctx context.Context, request *historyservice.RecordDecisionTaskStartedRequest) (_ *historyservice.RecordDecisionTaskStartedResponse, retError error) {
	resp, err := h.parentHandler.RecordDecisionTaskStarted(ctx, request)
	if resp == nil && err == nil {
		return &historyservice.RecordDecisionTaskStartedResponse{}, err
	}
	return resp, err
}

func (h *NilCheckHandler) RecordActivityTaskStarted(ctx context.Context, request *historyservice.RecordActivityTaskStartedRequest) (_ *historyservice.RecordActivityTaskStartedResponse, retError error) {
	resp, err := h.parentHandler.RecordActivityTaskStarted(ctx, request)
	if resp == nil && err == nil {
		return &historyservice.RecordActivityTaskStartedResponse{}, err
	}
	return resp, err
}

func (h *NilCheckHandler) RespondDecisionTaskCompleted(ctx context.Context, request *historyservice.RespondDecisionTaskCompletedRequest) (_ *historyservice.RespondDecisionTaskCompletedResponse, retError error) {
	resp, err := h.parentHandler.RespondDecisionTaskCompleted(ctx, request)
	if resp == nil && err == nil {
		return &historyservice.RespondDecisionTaskCompletedResponse{}, err
	}
	return resp, err
}

func (h *NilCheckHandler) RespondDecisionTaskFailed(ctx context.Context, request *historyservice.RespondDecisionTaskFailedRequest) (_ *historyservice.RespondDecisionTaskFailedResponse, retError error) {
	resp, err := h.parentHandler.RespondDecisionTaskFailed(ctx, request)
	if resp == nil && err == nil {
		return &historyservice.RespondDecisionTaskFailedResponse{}, err
	}
	return resp, err
}

func (h *NilCheckHandler) RecordActivityTaskHeartbeat(ctx context.Context, request *historyservice.RecordActivityTaskHeartbeatRequest) (_ *historyservice.RecordActivityTaskHeartbeatResponse, retError error) {
	resp, err := h.parentHandler.RecordActivityTaskHeartbeat(ctx, request)
	if resp == nil && err == nil {
		return &historyservice.RecordActivityTaskHeartbeatResponse{}, err
	}
	return resp, err
}

func (h *NilCheckHandler) RespondActivityTaskCompleted(ctx context.Context, request *historyservice.RespondActivityTaskCompletedRequest) (_ *historyservice.RespondActivityTaskCompletedResponse, retError error) {
	resp, err := h.parentHandler.RespondActivityTaskCompleted(ctx, request)
	if resp == nil && err == nil {
		return &historyservice.RespondActivityTaskCompletedResponse{}, err
	}
	return resp, err
}

func (h *NilCheckHandler) RespondActivityTaskFailed(ctx context.Context, request *historyservice.RespondActivityTaskFailedRequest) (_ *historyservice.RespondActivityTaskFailedResponse, retError error) {
	resp, err := h.parentHandler.RespondActivityTaskFailed(ctx, request)
	if resp == nil && err == nil {
		return &historyservice.RespondActivityTaskFailedResponse{}, err
	}
	return resp, err
}

func (h *NilCheckHandler) RespondActivityTaskCanceled(ctx context.Context, request *historyservice.RespondActivityTaskCanceledRequest) (_ *historyservice.RespondActivityTaskCanceledResponse, retError error) {
	resp, err := h.parentHandler.RespondActivityTaskCanceled(ctx, request)
	if resp == nil && err == nil {
		return &historyservice.RespondActivityTaskCanceledResponse{}, err
	}
	return resp, err
}

func (h *NilCheckHandler) SignalWorkflowExecution(ctx context.Context, request *historyservice.SignalWorkflowExecutionRequest) (_ *historyservice.SignalWorkflowExecutionResponse, retError error) {
	resp, err := h.parentHandler.SignalWorkflowExecution(ctx, request)
	if resp == nil && err == nil {
		return &historyservice.SignalWorkflowExecutionResponse{}, err
	}
	return resp, err
}

func (h *NilCheckHandler) SignalWithStartWorkflowExecution(ctx context.Context, request *historyservice.SignalWithStartWorkflowExecutionRequest) (_ *historyservice.SignalWithStartWorkflowExecutionResponse, retError error) {
	resp, err := h.parentHandler.SignalWithStartWorkflowExecution(ctx, request)
	if resp == nil && err == nil {
		return &historyservice.SignalWithStartWorkflowExecutionResponse{}, err
	}
	return resp, err
}

func (h *NilCheckHandler) RemoveSignalMutableState(ctx context.Context, request *historyservice.RemoveSignalMutableStateRequest) (_ *historyservice.RemoveSignalMutableStateResponse, retError error) {
	resp, err := h.parentHandler.RemoveSignalMutableState(ctx, request)
	if resp == nil && err == nil {
		return &historyservice.RemoveSignalMutableStateResponse{}, err
	}
	return resp, err
}

func (h *NilCheckHandler) TerminateWorkflowExecution(ctx context.Context, request *historyservice.TerminateWorkflowExecutionRequest) (_ *historyservice.TerminateWorkflowExecutionResponse, retError error) {
	resp, err := h.parentHandler.TerminateWorkflowExecution(ctx, request)
	if resp == nil && err == nil {
		return &historyservice.TerminateWorkflowExecutionResponse{}, err
	}
	return resp, err
}

func (h *NilCheckHandler) ResetWorkflowExecution(ctx context.Context, request *historyservice.ResetWorkflowExecutionRequest) (_ *historyservice.ResetWorkflowExecutionResponse, retError error) {
	resp, err := h.parentHandler.ResetWorkflowExecution(ctx, request)
	if resp == nil && err == nil {
		return &historyservice.ResetWorkflowExecutionResponse{}, err
	}
	return resp, err
}

func (h *NilCheckHandler) RequestCancelWorkflowExecution(ctx context.Context, request *historyservice.RequestCancelWorkflowExecutionRequest) (_ *historyservice.RequestCancelWorkflowExecutionResponse, retError error) {
	resp, err := h.parentHandler.RequestCancelWorkflowExecution(ctx, request)
	if resp == nil && err == nil {
		return &historyservice.RequestCancelWorkflowExecutionResponse{}, err
	}
	return resp, err
}

func (h *NilCheckHandler) ScheduleDecisionTask(ctx context.Context, request *historyservice.ScheduleDecisionTaskRequest) (_ *historyservice.ScheduleDecisionTaskResponse, retError error) {
	resp, err := h.parentHandler.ScheduleDecisionTask(ctx, request)
	if resp == nil && err == nil {
		return &historyservice.ScheduleDecisionTaskResponse{}, err
	}
	return resp, err
}

func (h *NilCheckHandler) RecordChildExecutionCompleted(ctx context.Context, request *historyservice.RecordChildExecutionCompletedRequest) (_ *historyservice.RecordChildExecutionCompletedResponse, retError error) {
	resp, err := h.parentHandler.RecordChildExecutionCompleted(ctx, request)
	if resp == nil && err == nil {
		return &historyservice.RecordChildExecutionCompletedResponse{}, err
	}
	return resp, err
}

func (h *NilCheckHandler) DescribeWorkflowExecution(ctx context.Context, request *historyservice.DescribeWorkflowExecutionRequest) (_ *historyservice.DescribeWorkflowExecutionResponse, retError error) {
	resp, err := h.parentHandler.DescribeWorkflowExecution(ctx, request)
	if resp == nil && err == nil {
		return &historyservice.DescribeWorkflowExecutionResponse{}, err
	}
	return resp, err
}

func (h *NilCheckHandler) ReplicateEvents(ctx context.Context, request *historyservice.ReplicateEventsRequest) (_ *historyservice.ReplicateEventsResponse, retError error) {
	resp, err := h.parentHandler.ReplicateEvents(ctx, request)
	if resp == nil && err == nil {
		return &historyservice.ReplicateEventsResponse{}, err
	}
	return resp, err
}

func (h *NilCheckHandler) ReplicateRawEvents(ctx context.Context, request *historyservice.ReplicateRawEventsRequest) (_ *historyservice.ReplicateRawEventsResponse, retError error) {
	resp, err := h.parentHandler.ReplicateRawEvents(ctx, request)
	if resp == nil && err == nil {
		return &historyservice.ReplicateRawEventsResponse{}, err
	}
	return resp, err
}

func (h *NilCheckHandler) ReplicateEventsV2(ctx context.Context, request *historyservice.ReplicateEventsV2Request) (_ *historyservice.ReplicateEventsV2Response, retError error) {
	resp, err := h.parentHandler.ReplicateEventsV2(ctx, request)
	if resp == nil && err == nil {
		return &historyservice.ReplicateEventsV2Response{}, err
	}
	return resp, err
}

func (h *NilCheckHandler) SyncShardStatus(ctx context.Context, request *historyservice.SyncShardStatusRequest) (_ *historyservice.SyncShardStatusResponse, retError error) {
	resp, err := h.parentHandler.SyncShardStatus(ctx, request)
	if resp == nil && err == nil {
		return &historyservice.SyncShardStatusResponse{}, err
	}
	return resp, err
}

func (h *NilCheckHandler) SyncActivity(ctx context.Context, request *historyservice.SyncActivityRequest) (_ *historyservice.SyncActivityResponse, retError error) {
	resp, err := h.parentHandler.SyncActivity(ctx, request)
	if resp == nil && err == nil {
		return &historyservice.SyncActivityResponse{}, err
	}
	return resp, err
}

func (h *NilCheckHandler) DescribeMutableState(ctx context.Context, request *historyservice.DescribeMutableStateRequest) (_ *historyservice.DescribeMutableStateResponse, retError error) {
	resp, err := h.parentHandler.DescribeMutableState(ctx, request)
	if resp == nil && err == nil {
		return &historyservice.DescribeMutableStateResponse{}, err
	}
	return resp, err
}

func (h *NilCheckHandler) DescribeHistoryHost(ctx context.Context, request *historyservice.DescribeHistoryHostRequest) (_ *historyservice.DescribeHistoryHostResponse, retError error) {
	resp, err := h.parentHandler.DescribeHistoryHost(ctx, request)
	if resp == nil && err == nil {
		return &historyservice.DescribeHistoryHostResponse{}, err
	}
	return resp, err
}

func (h *NilCheckHandler) CloseShard(ctx context.Context, request *historyservice.CloseShardRequest) (_ *historyservice.CloseShardResponse, retError error) {
	resp, err := h.parentHandler.CloseShard(ctx, request)
	if resp == nil && err == nil {
		return &historyservice.CloseShardResponse{}, err
	}
	return resp, err
}

func (h *NilCheckHandler) RemoveTask(ctx context.Context, request *historyservice.RemoveTaskRequest) (_ *historyservice.RemoveTaskResponse, retError error) {
	resp, err := h.parentHandler.RemoveTask(ctx, request)
	if resp == nil && err == nil {
		return &historyservice.RemoveTaskResponse{}, err
	}
	return resp, err
}

func (h *NilCheckHandler) GetReplicationMessages(ctx context.Context, request *historyservice.GetReplicationMessagesRequest) (_ *historyservice.GetReplicationMessagesResponse, retError error) {
	resp, err := h.parentHandler.GetReplicationMessages(ctx, request)
	if resp == nil && err == nil {
		return &historyservice.GetReplicationMessagesResponse{}, err
	}
	return resp, err
}

func (h *NilCheckHandler) GetDLQReplicationMessages(ctx context.Context, request *historyservice.GetDLQReplicationMessagesRequest) (_ *historyservice.GetDLQReplicationMessagesResponse, retError error) {
	resp, err := h.parentHandler.GetDLQReplicationMessages(ctx, request)
	if resp == nil && err == nil {
		return &historyservice.GetDLQReplicationMessagesResponse{}, err
	}
	return resp, err
}

func (h *NilCheckHandler) QueryWorkflow(ctx context.Context, request *historyservice.QueryWorkflowRequest) (_ *historyservice.QueryWorkflowResponse, retError error) {
	resp, err := h.parentHandler.QueryWorkflow(ctx, request)
	if resp == nil && err == nil {
		return &historyservice.QueryWorkflowResponse{}, err
	}
	return resp, err
}

func (h *NilCheckHandler) ReapplyEvents(ctx context.Context, request *historyservice.ReapplyEventsRequest) (_ *historyservice.ReapplyEventsResponse, retError error) {
	resp, err := h.parentHandler.ReapplyEvents(ctx, request)
	if resp == nil && err == nil {
		return &historyservice.ReapplyEventsResponse{}, err
	}
	return resp, err
}
