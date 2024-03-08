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

package getworkflowexecutionrawhistory

import (
	"context"

	"github.com/pborman/uuid"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/api/adminservice/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/shard"
)

func Invoke(
	ctx context.Context,
	shardContext shard.Context,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
	eventNotifier events.Notifier,
	request *historyservice.GetWorkflowExecutionRawHistoryRequest,
) (_ *historyservice.GetWorkflowExecutionRawHistoryResponse, retError error) {
	err := validateGetWorkflowExecutionRawHistoryRequest(request)
	if err != nil {
		return nil, err
	}
	ns, err := shardContext.GetNamespaceRegistry().GetNamespaceByID(namespace.ID(request.GetNamespaceId()))
	if err != nil {
		return nil, err
	}

	req := request.Request
	execution := req.Execution
	var pageToken *tokenspb.RawHistoryContinuation
	var targetVersionHistory *historyspb.VersionHistory
	if req.NextPageToken == nil {
		response, err := api.GetOrPollMutableState(
			ctx,
			shardContext,
			&historyservice.GetMutableStateRequest{
				NamespaceId: ns.ID().String(),
				Execution:   execution,
			},
			workflowConsistencyChecker,
			eventNotifier,
		)
		if err != nil {
			return nil, err
		}

		targetVersionHistory, err = setRequestDefaultValueAndGetTargetVersionHistory(
			request,
			response.GetVersionHistories(),
		)
		if err != nil {
			return nil, err
		}

		pageToken = api.GeneratePaginationToken(request, response.GetVersionHistories())
	} else {
		pageToken, err = api.DeserializeRawHistoryToken(req.NextPageToken)
		if err != nil {
			return nil, err
		}
		versionHistories := pageToken.GetVersionHistories()
		if versionHistories == nil {
			return nil, consts.ErrInvalidVersionHistories
		}
		targetVersionHistory, err = setRequestDefaultValueAndGetTargetVersionHistory(
			request,
			versionHistories,
		)
		if err != nil {
			return nil, err
		}
	}

	if err := api.ValidatePaginationToken(
		request,
		pageToken,
	); err != nil {
		return nil, err
	}

	pageSize := int(req.GetMaximumPageSize())
	shardID := common.WorkflowIDToHistoryShard(
		ns.ID().String(),
		execution.GetWorkflowId(),
		shardContext.GetConfig().NumberOfShards,
	)
	rawHistoryResponse, err := shardContext.GetExecutionManager().ReadRawHistoryBranch(ctx, &persistence.ReadHistoryBranchRequest{
		BranchToken: targetVersionHistory.GetBranchToken(),
		// GetWorkflowExecutionRawHistory is inclusive/inclusive.
		// ReadRawHistoryBranch is inclusive/exclusive.
		MinEventID:    pageToken.GetStartEventId(),
		MaxEventID:    pageToken.GetEndEventId() + 1,
		PageSize:      pageSize,
		NextPageToken: pageToken.PersistenceToken,
		ShardID:       shardID,
	})
	if err != nil {
		if common.IsNotFoundError(err) {
			// when no events can be returned from DB, DB layer will return
			// EntityNotExistsError, this API shall return empty response
			return &historyservice.GetWorkflowExecutionRawHistoryResponse{
				Response: &adminservice.GetWorkflowExecutionRawHistoryResponse{
					HistoryBatches: []*commonpb.DataBlob{},
					NextPageToken:  nil, // no further pagination
					VersionHistory: targetVersionHistory,
				},
			}, nil
		}
		return nil, err
	}

	pageToken.PersistenceToken = rawHistoryResponse.NextPageToken
	size := rawHistoryResponse.Size
	metricsHandler := interceptor.GetMetricsHandlerFromContext(ctx, shardContext.GetLogger()).WithTags(metrics.OperationTag(metrics.HistoryGetWorkflowExecutionRawHistoryScope))
	metrics.HistorySize.With(metricsHandler).Record(
		int64(size),
		metrics.NamespaceTag(ns.Name().String()),
		metrics.OperationTag(metrics.AdminGetWorkflowExecutionRawHistoryScope),
	)

	result :=
		&adminservice.GetWorkflowExecutionRawHistoryResponse{
			HistoryBatches: rawHistoryResponse.HistoryEventBlobs,
			VersionHistory: targetVersionHistory,
			HistoryNodeIds: rawHistoryResponse.NodeIDs,
		}
	if len(pageToken.PersistenceToken) == 0 {
		result.NextPageToken = nil
	} else {
		result.NextPageToken, err = api.SerializeRawHistoryToken(pageToken)
		if err != nil {
			return nil, err
		}
	}

	return &historyservice.GetWorkflowExecutionRawHistoryResponse{
		Response: result,
	}, nil
}

func validateGetWorkflowExecutionRawHistoryRequest(
	request *historyservice.GetWorkflowExecutionRawHistoryRequest,
) error {

	req := request.Request
	execution := req.Execution
	if execution.GetWorkflowId() == "" {
		return consts.ErrWorkflowIDNotSet
	}

	if execution.GetRunId() == "" || uuid.Parse(execution.GetRunId()) == nil {
		return consts.ErrInvalidRunID
	}

	pageSize := int(req.GetMaximumPageSize())
	if pageSize <= 0 {
		return consts.ErrInvalidPageSize
	}

	if req.GetStartEventId() == common.EmptyEventID &&
		req.GetStartEventVersion() == common.EmptyVersion &&
		req.GetEndEventId() == common.EmptyEventID &&
		req.GetEndEventVersion() == common.EmptyVersion {
		return consts.ErrInvalidEventQueryRange
	}

	return nil
}

func setRequestDefaultValueAndGetTargetVersionHistory(
	request *historyservice.GetWorkflowExecutionRawHistoryRequest,
	versionHistories *historyspb.VersionHistories,
) (*historyspb.VersionHistory, error) {

	req := request.Request

	targetBranch, err := versionhistory.GetCurrentVersionHistory(versionHistories)
	if err != nil {
		return nil, err
	}
	firstItem, err := versionhistory.GetFirstVersionHistoryItem(targetBranch)
	if err != nil {
		return nil, err
	}
	lastItem, err := versionhistory.GetLastVersionHistoryItem(targetBranch)
	if err != nil {
		return nil, err
	}

	setDefaultStartAndEndEvent(req, firstItem, lastItem)

	if req.GetStartEventId() < 0 {
		return nil, consts.ErrInvalidFirstNextEventCombination
	}

	// get branch based on the end event if end event is defined in the request
	if req.GetEndEventId() == lastItem.GetEventId() &&
		req.GetEndEventVersion() == lastItem.GetVersion() {
		// this is a special case, target branch remains the same
	} else {
		endItem := versionhistory.NewVersionHistoryItem(req.GetEndEventId(), req.GetEndEventVersion())
		idx, err := versionhistory.FindFirstVersionHistoryIndexByVersionHistoryItem(versionHistories, endItem)
		if err != nil {
			return nil, err
		}

		targetBranch, err = versionhistory.GetVersionHistory(versionHistories, idx)
		if err != nil {
			return nil, err
		}
	}

	startItem := versionhistory.NewVersionHistoryItem(req.GetStartEventId(), req.GetStartEventVersion())
	// If the request start event is defined. The start event may be on a different branch as current branch.
	// We need to find the LCA of the start event and the current branch.
	if req.GetStartEventId() == common.FirstEventID &&
		req.GetStartEventVersion() == firstItem.GetVersion() {
		// this is a special case, start event is on the same branch as target branch
	} else {
		if !versionhistory.ContainsVersionHistoryItem(targetBranch, startItem) {
			idx, err := versionhistory.FindFirstVersionHistoryIndexByVersionHistoryItem(versionHistories, startItem)
			if err != nil {
				return nil, err
			}
			startBranch, err := versionhistory.GetVersionHistory(versionHistories, idx)
			if err != nil {
				return nil, err
			}
			startItem, err = versionhistory.FindLCAVersionHistoryItem(targetBranch, startBranch)
			if err != nil {
				return nil, err
			}
			req.StartEventId = startItem.GetEventId()
			req.StartEventVersion = startItem.GetVersion()
		}
	}

	return targetBranch, nil
}

func setDefaultStartAndEndEvent(
	req *adminservice.GetWorkflowExecutionRawHistoryRequest,
	firstItem *historyspb.VersionHistoryItem,
	lastItem *historyspb.VersionHistoryItem,
) {
	if req.GetStartEventId() == common.EmptyEventID || req.GetStartEventVersion() == common.EmptyVersion {
		// If start event is not set, get the events from the first event
		req.StartEventId = common.FirstEventID
		req.StartEventVersion = firstItem.GetVersion()
	}
	if req.GetEndEventId() == common.EmptyEventID || req.GetEndEventVersion() == common.EmptyVersion {
		// If end event is not set, get the events until the end event
		req.EndEventId = lastItem.GetEventId()
		req.EndEventVersion = lastItem.GetVersion()
	}
}
