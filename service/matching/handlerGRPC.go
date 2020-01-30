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

package matching

import (
	"context"

	"github.com/temporalio/temporal/.gen/proto/healthservice"
	"github.com/temporalio/temporal/.gen/proto/matchingservice"
	"github.com/temporalio/temporal/common/adapter"
	"github.com/temporalio/temporal/common/log"
)

var _ matchingservice.MatchingServiceYARPCServer = (*HandlerGRPC)(nil)

type (
	// HandlerGRPC - gRPC handler interface for matchingservice
	HandlerGRPC struct {
		handlerThrift *Handler
	}
)

// NewHandlerGRPC creates a gRPC handler for the cadence matchingservice
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
	h.handlerThrift.Resource.GetGRPCDispatcher().Register(matchingservice.BuildMatchingServiceYARPCProcedures(h))
	h.handlerThrift.Resource.GetGRPCDispatcher().Register(healthservice.BuildMetaYARPCProcedures(h))
}

// Health is for health check
func (h *HandlerGRPC) Health(ctx context.Context, _ *healthservice.HealthRequest) (_ *healthservice.HealthStatus, retError error) {
	h.handlerThrift.startWG.Wait()
	h.handlerThrift.GetLogger().Debug("Matching service health check endpoint (gRPC) reached.")
	hs := &healthservice.HealthStatus{Ok: true, Msg: "matching good"}
	return hs, nil
}

func (h *HandlerGRPC) PollForDecisionTask(ctx context.Context, request *matchingservice.PollForDecisionTaskRequest) (_ *matchingservice.PollForDecisionTaskResponse, retError error) {
	defer log.CapturePanicGRPC(h.handlerThrift.GetLogger(), &retError)

	resp, err := h.handlerThrift.PollForDecisionTask(ctx, adapter.ToThriftMatchingPollForDecisionTaskRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoMatchingPollForDecisionTaskResponse(resp), nil
}

func (h *HandlerGRPC) PollForActivityTask(ctx context.Context, request *matchingservice.PollForActivityTaskRequest) (_ *matchingservice.PollForActivityTaskResponse, retError error) {
	defer log.CapturePanicGRPC(h.handlerThrift.GetLogger(), &retError)

	resp, err := h.handlerThrift.PollForActivityTask(ctx, adapter.ToThriftMatchingPollForActivityTaskRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoMatchingPollForActivityTaskResponse(resp), nil
}

func (h *HandlerGRPC) AddDecisionTask(ctx context.Context, request *matchingservice.AddDecisionTaskRequest) (_ *matchingservice.AddDecisionTaskResponse, retError error) {
	defer log.CapturePanicGRPC(h.handlerThrift.GetLogger(), &retError)

	err := h.handlerThrift.AddDecisionTask(ctx, adapter.ToThriftAddDecisionTaskRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return &matchingservice.AddDecisionTaskResponse{}, nil
}

func (h *HandlerGRPC) AddActivityTask(ctx context.Context, request *matchingservice.AddActivityTaskRequest) (_ *matchingservice.AddActivityTaskResponse, retError error) {
	defer log.CapturePanicGRPC(h.handlerThrift.GetLogger(), &retError)

	err := h.handlerThrift.AddActivityTask(ctx, adapter.ToThriftAddActivityTaskRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return &matchingservice.AddActivityTaskResponse{}, nil
}

func (h *HandlerGRPC) QueryWorkflow(ctx context.Context, request *matchingservice.QueryWorkflowRequest) (_ *matchingservice.QueryWorkflowResponse, retError error) {
	defer log.CapturePanicGRPC(h.handlerThrift.GetLogger(), &retError)

	resp, err := h.handlerThrift.QueryWorkflow(ctx, adapter.ToThriftMatchingQueryWorkflowRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoMatchingQueryWorkflowResponse(resp), nil
}

func (h *HandlerGRPC) RespondQueryTaskCompleted(ctx context.Context, request *matchingservice.RespondQueryTaskCompletedRequest) (_ *matchingservice.RespondQueryTaskCompletedResponse, retError error) {
	defer log.CapturePanicGRPC(h.handlerThrift.GetLogger(), &retError)

	err := h.handlerThrift.RespondQueryTaskCompleted(ctx, adapter.ToThriftMatchingRespondQueryTaskCompletedRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return &matchingservice.RespondQueryTaskCompletedResponse{}, nil
}

func (h *HandlerGRPC) CancelOutstandingPoll(ctx context.Context, request *matchingservice.CancelOutstandingPollRequest) (_ *matchingservice.CancelOutstandingPollResponse, retError error) {
	defer log.CapturePanicGRPC(h.handlerThrift.GetLogger(), &retError)

	err := h.handlerThrift.CancelOutstandingPoll(ctx, adapter.ToThriftCancelOutstandingPollRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return &matchingservice.CancelOutstandingPollResponse{}, nil
}

func (h *HandlerGRPC) DescribeTaskList(ctx context.Context, request *matchingservice.DescribeTaskListRequest) (_ *matchingservice.DescribeTaskListResponse, retError error) {
	defer log.CapturePanicGRPC(h.handlerThrift.GetLogger(), &retError)

	resp, err := h.handlerThrift.DescribeTaskList(ctx, adapter.ToThriftMatchingDescribeTaskListRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoMatchingDescribeTaskListResponse(resp), nil
}

func (h *HandlerGRPC) ListTaskListPartitions(ctx context.Context, request *matchingservice.ListTaskListPartitionsRequest) (_ *matchingservice.ListTaskListPartitionsResponse, retError error) {
	defer log.CapturePanicGRPC(h.handlerThrift.GetLogger(), &retError)

	resp, err := h.handlerThrift.ListTaskListPartitions(ctx, adapter.ToThriftMatchingListTaskListPartitionsRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoMatchingListTaskListPartitionsResponse(resp), nil
}
