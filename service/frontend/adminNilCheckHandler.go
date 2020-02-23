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
)

var _ adminservice.AdminServiceServer = (*AdminNilCheckHandler)(nil)

type (
	// AdminNilCheckHandler - gRPC handler interface for workflow workflowservice
	AdminNilCheckHandler struct {
		parentHandler adminservice.AdminServiceServer
	}
)

// Due to bug in gogo/protobuf https://github.com/gogo/protobuf/issues/651 response can't be nil when error is also nil.
// This handler makes sure response is always not nil, when error is nil.
// Can be removed from pipeline when bug is resolved.

// NewAdminNilCheckHandler creates handler that never returns nil response when error is nil
func NewAdminNilCheckHandler(
	parentHandler adminservice.AdminServiceServer,
) *AdminNilCheckHandler {
	handler := &AdminNilCheckHandler{
		parentHandler: parentHandler,
	}

	return handler
}

// DescribeWorkflowExecution ...
func (adh *AdminNilCheckHandler) DescribeWorkflowExecution(ctx context.Context, request *adminservice.DescribeWorkflowExecutionRequest) (_ *adminservice.DescribeWorkflowExecutionResponse, retError error) {
	resp, err := adh.parentHandler.DescribeWorkflowExecution(ctx, request)
	if resp == nil && err == nil {
		return &adminservice.DescribeWorkflowExecutionResponse{}, err
	}
	return resp, err
}

// DescribeHistoryHost ...
func (adh *AdminNilCheckHandler) DescribeHistoryHost(ctx context.Context, request *adminservice.DescribeHistoryHostRequest) (_ *adminservice.DescribeHistoryHostResponse, retError error) {
	resp, err := adh.parentHandler.DescribeHistoryHost(ctx, request)
	if resp == nil && err == nil {
		return &adminservice.DescribeHistoryHostResponse{}, err
	}
	return resp, err
}

// CloseShard ...
func (adh *AdminNilCheckHandler) CloseShard(ctx context.Context, request *adminservice.CloseShardRequest) (_ *adminservice.CloseShardResponse, retError error) {
	resp, err := adh.parentHandler.CloseShard(ctx, request)
	if resp == nil && err == nil {
		return &adminservice.CloseShardResponse{}, err
	}
	return resp, err
}

// RemoveTask ...
func (adh *AdminNilCheckHandler) RemoveTask(ctx context.Context, request *adminservice.RemoveTaskRequest) (_ *adminservice.RemoveTaskResponse, retError error) {
	resp, err := adh.parentHandler.RemoveTask(ctx, request)
	if resp == nil && err == nil {
		return &adminservice.RemoveTaskResponse{}, err
	}
	return resp, err
}

// GetWorkflowExecutionRawHistory ...
func (adh *AdminNilCheckHandler) GetWorkflowExecutionRawHistory(ctx context.Context, request *adminservice.GetWorkflowExecutionRawHistoryRequest) (_ *adminservice.GetWorkflowExecutionRawHistoryResponse, retError error) {
	resp, err := adh.parentHandler.GetWorkflowExecutionRawHistory(ctx, request)
	if resp == nil && err == nil {
		return &adminservice.GetWorkflowExecutionRawHistoryResponse{}, err
	}
	return resp, err
}

// GetWorkflowExecutionRawHistoryV2 ...
func (adh *AdminNilCheckHandler) GetWorkflowExecutionRawHistoryV2(ctx context.Context, request *adminservice.GetWorkflowExecutionRawHistoryV2Request) (_ *adminservice.GetWorkflowExecutionRawHistoryV2Response, retError error) {
	resp, err := adh.parentHandler.GetWorkflowExecutionRawHistoryV2(ctx, request)
	if resp == nil && err == nil {
		return &adminservice.GetWorkflowExecutionRawHistoryV2Response{}, err
	}
	return resp, err
}

// AddSearchAttribute ...
func (adh *AdminNilCheckHandler) AddSearchAttribute(ctx context.Context, request *adminservice.AddSearchAttributeRequest) (_ *adminservice.AddSearchAttributeResponse, retError error) {
	resp, err := adh.parentHandler.AddSearchAttribute(ctx, request)
	if resp == nil && err == nil {
		return &adminservice.AddSearchAttributeResponse{}, err
	}
	return resp, err
}

// DescribeCluster ...
func (adh *AdminNilCheckHandler) DescribeCluster(ctx context.Context, request *adminservice.DescribeClusterRequest) (_ *adminservice.DescribeClusterResponse, retError error) {
	resp, err := adh.parentHandler.DescribeCluster(ctx, request)
	if resp == nil && err == nil {
		return &adminservice.DescribeClusterResponse{}, err
	}
	return resp, err
}

// GetReplicationMessages ...
func (adh *AdminNilCheckHandler) GetReplicationMessages(ctx context.Context, request *adminservice.GetReplicationMessagesRequest) (_ *adminservice.GetReplicationMessagesResponse, retError error) {
	resp, err := adh.parentHandler.GetReplicationMessages(ctx, request)
	if resp == nil && err == nil {
		return &adminservice.GetReplicationMessagesResponse{}, err
	}
	return resp, err
}

// GetDomainReplicationMessages ...
func (adh *AdminNilCheckHandler) GetDomainReplicationMessages(ctx context.Context, request *adminservice.GetDomainReplicationMessagesRequest) (_ *adminservice.GetDomainReplicationMessagesResponse, retError error) {
	resp, err := adh.parentHandler.GetDomainReplicationMessages(ctx, request)
	if resp == nil && err == nil {
		return &adminservice.GetDomainReplicationMessagesResponse{}, err
	}
	return resp, err
}

// GetDLQReplicationMessages ...
func (adh *AdminNilCheckHandler) GetDLQReplicationMessages(ctx context.Context, request *adminservice.GetDLQReplicationMessagesRequest) (_ *adminservice.GetDLQReplicationMessagesResponse, retError error) {
	resp, err := adh.parentHandler.GetDLQReplicationMessages(ctx, request)
	if resp == nil && err == nil {
		return &adminservice.GetDLQReplicationMessagesResponse{}, err
	}
	return resp, err
}

// ReapplyEvents ...
func (adh *AdminNilCheckHandler) ReapplyEvents(ctx context.Context, request *adminservice.ReapplyEventsRequest) (_ *adminservice.ReapplyEventsResponse, retError error) {
	resp, err := adh.parentHandler.ReapplyEvents(ctx, request)
	if resp == nil && err == nil {
		return &adminservice.ReapplyEventsResponse{}, err
	}
	return resp, err
}

// ReadDLQMessages returns messages from DLQ
func (adh *AdminNilCheckHandler) ReadDLQMessages(ctx context.Context, request *adminservice.ReadDLQMessagesRequest) (*adminservice.ReadDLQMessagesResponse, error) {
	resp, err := adh.parentHandler.ReadDLQMessages(ctx, request)
	if resp == nil && err == nil {
		return &adminservice.ReadDLQMessagesResponse{}, err
	}
	return resp, err
}

// PurgeDLQMessages purges messages from DLQ
func (adh *AdminNilCheckHandler) PurgeDLQMessages(ctx context.Context, request *adminservice.PurgeDLQMessagesRequest) (*adminservice.PurgeDLQMessagesResponse, error) {
	resp, err := adh.parentHandler.PurgeDLQMessages(ctx, request)
	if resp == nil && err == nil {
		return &adminservice.PurgeDLQMessagesResponse{}, err
	}
	return resp, err
}

// MergeDLQMessages merges messages from DLQ
func (adh *AdminNilCheckHandler) MergeDLQMessages(ctx context.Context, request *adminservice.MergeDLQMessagesRequest) (*adminservice.MergeDLQMessagesResponse, error) {
	resp, err := adh.parentHandler.MergeDLQMessages(ctx, request)
	if resp == nil && err == nil {
		return &adminservice.MergeDLQMessagesResponse{}, err
	}
	return resp, err
}

// RefreshWorkflowTasks refreshes all tasks of a workflow
func (adh *AdminNilCheckHandler) RefreshWorkflowTasks(ctx context.Context, request *adminservice.RefreshWorkflowTasksRequest) (*adminservice.RefreshWorkflowTasksResponse, error) {
	resp, err := adh.parentHandler.RefreshWorkflowTasks(ctx, request)
	if resp == nil && err == nil {
		return &adminservice.RefreshWorkflowTasksResponse{}, err
	}
	return resp, err
}
