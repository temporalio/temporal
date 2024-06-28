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

package getworkflowexecutionrawhistoryv2

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"

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
	request *historyservice.GetWorkflowExecutionRawHistoryV2Request,
) (_ *historyservice.GetWorkflowExecutionRawHistoryV2Response, retError error) {
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

		targetVersionHistory, err = SetRequestDefaultValueAndGetTargetVersionHistory(
			request,
			response.GetVersionHistories(),
		)
		if err != nil {
			return nil, err
		}

		pageToken = api.GeneratePaginationTokenV2Request(request, response.GetVersionHistories())
	} else {
		pageToken, err = api.DeserializeRawHistoryToken(req.NextPageToken)
		if err != nil {
			return nil, err
		}
		versionHistories := pageToken.GetVersionHistories()
		if versionHistories == nil {
			return nil, consts.ErrInvalidVersionHistories
		}
		targetVersionHistory, err = SetRequestDefaultValueAndGetTargetVersionHistory(
			request,
			versionHistories,
		)
		if err != nil {
			return nil, err
		}
	}

	if err := api.ValidatePaginationTokenV2Request(
		request,
		pageToken,
	); err != nil {
		return nil, err
	}

	if pageToken.GetStartEventId()+1 == pageToken.GetEndEventId() {
		// API is exclusive-exclusive. Return empty response here.
		return &historyservice.GetWorkflowExecutionRawHistoryV2Response{
			Response: &adminservice.GetWorkflowExecutionRawHistoryV2Response{
				HistoryBatches: []*commonpb.DataBlob{},
				NextPageToken:  nil, // no further pagination
				VersionHistory: targetVersionHistory,
			},
		}, nil
	}
	pageSize := int(req.GetMaximumPageSize())
	shardID := common.WorkflowIDToHistoryShard(
		ns.ID().String(),
		execution.GetWorkflowId(),
		shardContext.GetConfig().NumberOfShards,
	)
	rawHistoryResponse, err := shardContext.GetExecutionManager().ReadRawHistoryBranch(ctx, &persistence.ReadHistoryBranchRequest{
		BranchToken: targetVersionHistory.GetBranchToken(),
		// GetWorkflowExecutionRawHistoryV2 is exclusive exclusive.
		// ReadRawHistoryBranch is inclusive exclusive.
		MinEventID:    pageToken.GetStartEventId() + 1,
		MaxEventID:    pageToken.GetEndEventId(),
		PageSize:      pageSize,
		NextPageToken: pageToken.PersistenceToken,
		ShardID:       shardID,
	})
	if err != nil {
		if _, isNotFound := err.(*serviceerror.NotFound); isNotFound {
			// when no events can be returned from DB, DB layer will return
			// EntityNotExistsError, this API shall return empty response
			return &historyservice.GetWorkflowExecutionRawHistoryV2Response{
				Response: &adminservice.GetWorkflowExecutionRawHistoryV2Response{
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
	metricsHandler := interceptor.GetMetricsHandlerFromContext(ctx, shardContext.GetLogger()).WithTags(metrics.OperationTag(metrics.HistoryGetWorkflowExecutionRawHistoryV2Scope))
	metrics.HistorySize.With(metricsHandler).Record(
		int64(size),
		metrics.NamespaceTag(ns.Name().String()),
		metrics.OperationTag(metrics.AdminGetWorkflowExecutionRawHistoryV2Scope),
	)

	result :=
		&adminservice.GetWorkflowExecutionRawHistoryV2Response{
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

	return &historyservice.GetWorkflowExecutionRawHistoryV2Response{
		Response: result,
	}, nil
}

func SetRequestDefaultValueAndGetTargetVersionHistory(
	request *historyservice.GetWorkflowExecutionRawHistoryV2Request,
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

	if req.GetStartEventId() == common.EmptyEventID || req.GetStartEventVersion() == common.EmptyVersion {
		// If start event is not set, get the events from the first event
		// As the API is exclusive-exclusive, use first event id - 1 here
		req.StartEventId = common.FirstEventID - 1
		req.StartEventVersion = firstItem.GetVersion()
	}
	if req.GetEndEventId() == common.EmptyEventID || req.GetEndEventVersion() == common.EmptyVersion {
		// If end event is not set, get the events until the end event
		// As the API is exclusive-exclusive, use end event id + 1 here
		req.EndEventId = lastItem.GetEventId() + 1
		req.EndEventVersion = lastItem.GetVersion()
	}

	if req.GetStartEventId() < 0 {
		return nil, consts.ErrInvalidFirstNextEventCombination
	}

	// get branch based on the end event if end event is defined in the request
	if req.GetEndEventId() == lastItem.GetEventId()+1 &&
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
	if req.GetStartEventId() == common.FirstEventID-1 &&
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
