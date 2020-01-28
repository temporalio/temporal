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

package frontend

import (
	"context"

	"github.com/temporalio/temporal/.gen/proto/adminservice"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/service/frontend/adapter"
)

var _ adminservice.AdminServiceYARPCServer = (*AdminHandlerGRPC)(nil)

type (
	// AdminHandlerGRPC - gRPC handler interface for workflow workflowservice
	AdminHandlerGRPC struct {
		adminHandlerThrift *AdminHandler
	}
)

// NewAdminHandlerGRPC creates a thrift handler for the cadence workflowservice
func NewAdminHandlerGRPC(
	adminHandlerThrift *AdminHandler,
) *AdminHandlerGRPC {
	handler := &AdminHandlerGRPC{
		adminHandlerThrift: adminHandlerThrift,
	}

	return handler
}

// RegisterHandler register this handler, must be called before Start()
// if DCRedirectionHandler is also used, use RegisterHandler in DCRedirectionHandler instead
func (adh *AdminHandlerGRPC) RegisterHandler() {
	adh.adminHandlerThrift.GetGRPCDispatcher().Register(adminservice.BuildAdminServiceYARPCProcedures(adh))
}

// DescribeWorkflowExecution ...
func (adh *AdminHandlerGRPC) DescribeWorkflowExecution(ctx context.Context, request *adminservice.DescribeWorkflowExecutionRequest) (_ *adminservice.DescribeWorkflowExecutionResponse, retError error) {
	defer log.CapturePanicGRPC(adh.adminHandlerThrift.GetLogger(), &retError)

	resp, err := adh.adminHandlerThrift.DescribeWorkflowExecution(ctx, adapter.ToThriftAdminDescribeWorkflowExecutionRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoAdminDescribeWorkflowExecutionResponse(resp), nil
}

// DescribeHistoryHost ...
func (adh *AdminHandlerGRPC) DescribeHistoryHost(ctx context.Context, request *adminservice.DescribeHistoryHostRequest) (_ *adminservice.DescribeHistoryHostResponse, retError error) {
	defer log.CapturePanicGRPC(adh.adminHandlerThrift.GetLogger(), &retError)

	resp, err := adh.adminHandlerThrift.DescribeHistoryHost(ctx, adapter.ToThriftDescribeHistoryHostRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoDescribeHistoryHostResponse(resp), nil
}

// CloseShard ...
func (adh *AdminHandlerGRPC) CloseShard(ctx context.Context, request *adminservice.CloseShardRequest) (_ *adminservice.CloseShardResponse, retError error) {
	defer log.CapturePanicGRPC(adh.adminHandlerThrift.GetLogger(), &retError)

	err := adh.adminHandlerThrift.CloseShard(ctx, adapter.ToThriftCloseShardRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return &adminservice.CloseShardResponse{}, nil
}

// RemoveTask ...
func (adh *AdminHandlerGRPC) RemoveTask(ctx context.Context, request *adminservice.RemoveTaskRequest) (_ *adminservice.RemoveTaskResponse, retError error) {
	defer log.CapturePanicGRPC(adh.adminHandlerThrift.GetLogger(), &retError)

	err := adh.adminHandlerThrift.RemoveTask(ctx, adapter.ToThriftRemoveTaskRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return &adminservice.RemoveTaskResponse{}, nil
}

// GetWorkflowExecutionRawHistory ...
func (adh *AdminHandlerGRPC) GetWorkflowExecutionRawHistory(ctx context.Context, request *adminservice.GetWorkflowExecutionRawHistoryRequest) (_ *adminservice.GetWorkflowExecutionRawHistoryResponse, retError error) {
	defer log.CapturePanicGRPC(adh.adminHandlerThrift.GetLogger(), &retError)

	resp, err := adh.adminHandlerThrift.GetWorkflowExecutionRawHistory(ctx, adapter.ToThriftAdminGetWorkflowExecutionRawHistoryRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoAdminGetWorkflowExecutionRawHistoryResponse(resp), nil
}

// GetWorkflowExecutionRawHistoryV2 ...
func (adh *AdminHandlerGRPC) GetWorkflowExecutionRawHistoryV2(ctx context.Context, request *adminservice.GetWorkflowExecutionRawHistoryV2Request) (_ *adminservice.GetWorkflowExecutionRawHistoryV2Response, retError error) {
	defer log.CapturePanicGRPC(adh.adminHandlerThrift.GetLogger(), &retError)

	resp, err := adh.adminHandlerThrift.GetWorkflowExecutionRawHistoryV2(ctx, adapter.ToThriftGetWorkflowExecutionRawHistoryV2Request(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoGetWorkflowExecutionRawHistoryV2Response(resp), nil
}

// AddSearchAttribute ...
func (adh *AdminHandlerGRPC) AddSearchAttribute(ctx context.Context, request *adminservice.AddSearchAttributeRequest) (_ *adminservice.AddSearchAttributeResponse, retError error) {
	defer log.CapturePanicGRPC(adh.adminHandlerThrift.GetLogger(), &retError)

	err := adh.adminHandlerThrift.AddSearchAttribute(ctx, adapter.ToThriftAddSearchAttributeRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return &adminservice.AddSearchAttributeResponse{}, nil
}

// DescribeCluster ...
func (adh *AdminHandlerGRPC) DescribeCluster(ctx context.Context, _ *adminservice.DescribeClusterRequest) (_ *adminservice.DescribeClusterResponse, retError error) {
	defer log.CapturePanicGRPC(adh.adminHandlerThrift.GetLogger(), &retError)

	resp, err := adh.adminHandlerThrift.DescribeCluster(ctx)
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoDescribeClusterResponse(resp), nil
}

func (adh *AdminHandlerGRPC) GetReplicationMessages(ctx context.Context, request *adminservice.GetReplicationMessagesRequest) (_ *adminservice.GetReplicationMessagesResponse, retError error) {
	defer log.CapturePanicGRPC(adh.adminHandlerThrift.GetLogger(), &retError)

	resp, err := adh.adminHandlerThrift.GetReplicationMessages(ctx, adapter.ToThriftGetReplicationMessagesRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoGetReplicationMessagesResponse(resp), nil
}

func (adh *AdminHandlerGRPC) GetDomainReplicationMessages(ctx context.Context, request *adminservice.GetDomainReplicationMessagesRequest) (_ *adminservice.GetDomainReplicationMessagesResponse, retError error) {
	defer log.CapturePanicGRPC(adh.adminHandlerThrift.GetLogger(), &retError)

	resp, err := adh.adminHandlerThrift.GetDomainReplicationMessages(ctx, adapter.ToThriftGetDomainReplicationMessagesRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoGetDomainReplicationMessagesResponse(resp), nil
}

func (adh *AdminHandlerGRPC) GetDLQReplicationMessages(ctx context.Context, request *adminservice.GetDLQReplicationMessagesRequest) (_ *adminservice.GetDLQReplicationMessagesResponse, retError error) {
	defer log.CapturePanicGRPC(adh.adminHandlerThrift.GetLogger(), &retError)

	resp, err := adh.adminHandlerThrift.GetDLQReplicationMessages(ctx, adapter.ToThriftGetDLQReplicationMessagesRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoGetDLQReplicationMessagesResponse(resp), nil
}

func (adh *AdminHandlerGRPC) ReapplyEvents(ctx context.Context, request *adminservice.ReapplyEventsRequest) (_ *adminservice.ReapplyEventsResponse, retError error) {
	defer log.CapturePanicGRPC(adh.adminHandlerThrift.GetLogger(), &retError)

	err := adh.adminHandlerThrift.ReapplyEvents(ctx, adapter.ToThriftReapplyEventsRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return &adminservice.ReapplyEventsResponse{}, nil
}
