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

package frontend

import (
	"context"
	"fmt"

	"github.com/pborman/uuid"

	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/api/adminservice/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
)

// DEPRECATED: DO NOT MODIFY UNLESS ALSO APPLIED TO ./service/history/api/getworkflowexecutionrawhistoryv2/api.go
func (adh *AdminHandler) getWorkflowExecutionRawHistoryV2(
	ctx context.Context,
	request *adminservice.GetWorkflowExecutionRawHistoryV2Request,
) (_ *adminservice.GetWorkflowExecutionRawHistoryV2Response, retError error) {
	ns, err := adh.namespaceRegistry.GetNamespaceByID(namespace.ID(request.GetNamespaceId()))
	if err != nil {
		return nil, err
	}

	execution := request.Execution
	var pageToken *tokenspb.RawHistoryContinuation
	var targetVersionHistory *historyspb.VersionHistory
	if request.NextPageToken == nil {
		response, err := adh.historyClient.GetMutableState(ctx, &historyservice.GetMutableStateRequest{
			NamespaceId: ns.ID().String(),
			Execution:   execution,
		})
		if err != nil {
			return nil, err
		}

		targetVersionHistory, err = adh.setRequestDefaultValueAndGetTargetVersionHistory(
			request,
			response.GetVersionHistories(),
		)
		if err != nil {
			return nil, err
		}

		pageToken = generatePaginationToken(request, response.GetVersionHistories())
	} else {
		pageToken, err = deserializeRawHistoryToken(request.NextPageToken)
		if err != nil {
			return nil, err
		}
		versionHistories := pageToken.GetVersionHistories()
		if versionHistories == nil {
			return nil, errInvalidVersionHistories
		}
		targetVersionHistory, err = adh.setRequestDefaultValueAndGetTargetVersionHistory(
			request,
			versionHistories,
		)
		if err != nil {
			return nil, err
		}
	}

	if err := validatePaginationToken(
		request,
		pageToken,
	); err != nil {
		return nil, err
	}

	if pageToken.GetStartEventId()+1 == pageToken.GetEndEventId() {
		// API is exclusive-exclusive. Return empty response here.
		return &adminservice.GetWorkflowExecutionRawHistoryV2Response{
			HistoryBatches: []*commonpb.DataBlob{},
			NextPageToken:  nil, // no further pagination
			VersionHistory: targetVersionHistory,
		}, nil
	}
	pageSize := int(request.GetMaximumPageSize())
	shardID := common.WorkflowIDToHistoryShard(
		ns.ID().String(),
		execution.GetWorkflowId(),
		adh.numberOfHistoryShards,
	)
	rawHistoryResponse, err := adh.persistenceExecutionManager.ReadRawHistoryBranch(ctx, &persistence.ReadHistoryBranchRequest{
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
			return &adminservice.GetWorkflowExecutionRawHistoryV2Response{
				HistoryBatches: []*commonpb.DataBlob{},
				NextPageToken:  nil, // no further pagination
				VersionHistory: targetVersionHistory,
			}, nil
		}
		return nil, err
	}

	pageToken.PersistenceToken = rawHistoryResponse.NextPageToken
	size := rawHistoryResponse.Size
	adh.metricsHandler.Histogram(metrics.HistorySize.Name(), metrics.HistorySize.Unit()).Record(
		int64(size),
		metrics.NamespaceTag(ns.Name().String()),
		metrics.OperationTag(metrics.AdminGetWorkflowExecutionRawHistoryV2Scope),
	)

	result := &adminservice.GetWorkflowExecutionRawHistoryV2Response{
		HistoryBatches: rawHistoryResponse.HistoryEventBlobs,
		VersionHistory: targetVersionHistory,
		HistoryNodeIds: rawHistoryResponse.NodeIDs,
	}
	if len(pageToken.PersistenceToken) == 0 {
		result.NextPageToken = nil
	} else {
		result.NextPageToken, err = serializeRawHistoryToken(pageToken)
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

// DEPRECATED: DO NOT MODIFY UNLESS ALSO APPLIED TO ./service/history/api/forcedeleteworkflowexecution/api.go
func (adh *AdminHandler) deleteWorkflowExecution(
	ctx context.Context,
	request *adminservice.DeleteWorkflowExecutionRequest,
) (_ *adminservice.DeleteWorkflowExecutionResponse, err error) {
	namespaceID, err := adh.namespaceRegistry.GetNamespaceID(namespace.Name(request.GetNamespace()))
	if err != nil {
		return nil, err
	}
	execution := request.Execution

	shardID := common.WorkflowIDToHistoryShard(
		namespaceID.String(),
		execution.GetWorkflowId(),
		adh.numberOfHistoryShards,
	)
	logger := log.With(adh.logger,
		tag.WorkflowNamespace(request.Namespace),
		tag.WorkflowID(execution.WorkflowId),
		tag.WorkflowRunID(execution.RunId),
	)

	if execution.RunId == "" {
		resp, err := adh.persistenceExecutionManager.GetCurrentExecution(ctx, &persistence.GetCurrentExecutionRequest{
			ShardID:     shardID,
			NamespaceID: namespaceID.String(),
			WorkflowID:  execution.WorkflowId,
		})
		if err != nil {
			return nil, err
		}
		execution.RunId = resp.RunID
	}

	var warnings []string
	var branchTokens [][]byte

	resp, err := adh.persistenceExecutionManager.GetWorkflowExecution(ctx, &persistence.GetWorkflowExecutionRequest{
		ShardID:     shardID,
		NamespaceID: namespaceID.String(),
		WorkflowID:  execution.WorkflowId,
		RunID:       execution.RunId,
	})
	if err != nil {
		if common.IsContextCanceledErr(err) || common.IsContextDeadlineExceededErr(err) {
			return nil, err
		}
		// continue to deletion
		warnMsg := "Unable to load mutable state when deleting workflow execution, " +
			"will skip deleting workflow history and visibility record"
		logger.Warn(warnMsg, tag.Error(err))
		warnings = append(warnings, fmt.Sprintf("%s. Error: %v", warnMsg, err.Error()))
	} else {
		// load necessary information from mutable state
		executionInfo := resp.State.GetExecutionInfo()
		histories := executionInfo.GetVersionHistories().GetHistories()
		branchTokens = make([][]byte, 0, len(histories))
		for _, historyItem := range histories {
			branchTokens = append(branchTokens, historyItem.GetBranchToken())
		}
	}

	// NOTE: the deletion is best effort, for sql visibility implementation,
	// we can't guarantee there's no update or record close request for this workflow since
	// visibility queue processing is async. Operator can call this api again to delete visibility
	// record again if this happens.
	if _, err := adh.historyClient.DeleteWorkflowVisibilityRecord(ctx, &historyservice.DeleteWorkflowVisibilityRecordRequest{
		NamespaceId: namespaceID.String(),
		Execution:   execution,
	}); err != nil {
		return nil, err
	}

	if err := adh.persistenceExecutionManager.DeleteCurrentWorkflowExecution(ctx, &persistence.DeleteCurrentWorkflowExecutionRequest{
		ShardID:     shardID,
		NamespaceID: namespaceID.String(),
		WorkflowID:  execution.WorkflowId,
		RunID:       execution.RunId,
	}); err != nil {
		return nil, err
	}

	if err := adh.persistenceExecutionManager.DeleteWorkflowExecution(ctx, &persistence.DeleteWorkflowExecutionRequest{
		ShardID:     shardID,
		NamespaceID: namespaceID.String(),
		WorkflowID:  execution.WorkflowId,
		RunID:       execution.RunId,
	}); err != nil {
		return nil, err
	}

	for _, branchToken := range branchTokens {
		if err := adh.persistenceExecutionManager.DeleteHistoryBranch(ctx, &persistence.DeleteHistoryBranchRequest{
			ShardID:     shardID,
			BranchToken: branchToken,
		}); err != nil {
			warnMsg := "Failed to delete history branch, skip"
			adh.logger.Warn(warnMsg, tag.WorkflowBranchID(string(branchToken)), tag.Error(err))
			warnings = append(warnings, fmt.Sprintf("%s. BranchToken: %v, Error: %v", warnMsg, branchToken, err.Error()))
		}
	}

	return &adminservice.DeleteWorkflowExecutionResponse{
		Warnings: warnings,
	}, nil
}

// DEPRECATED: DO NOT MODIFY UNLESS ALSO APPLIED TO ./service/history/api/getworkflowexecutionrawhistoryv2/api.go
func (adh *AdminHandler) validateGetWorkflowExecutionRawHistoryV2Request(
	request *adminservice.GetWorkflowExecutionRawHistoryV2Request,
) error {

	execution := request.Execution
	if execution.GetWorkflowId() == "" {
		return errWorkflowIDNotSet
	}
	// TODO currently, this API is only going to be used by re-send history events
	// to remote cluster if kafka is lossy again, in the future, this API can be used
	// by CLI and client, then empty runID (meaning the current workflow) should be allowed
	if execution.GetRunId() == "" || uuid.Parse(execution.GetRunId()) == nil {
		return errInvalidRunID
	}

	pageSize := int(request.GetMaximumPageSize())
	if pageSize <= 0 {
		return errInvalidPageSize
	}

	if request.GetStartEventId() == common.EmptyEventID &&
		request.GetStartEventVersion() == common.EmptyVersion &&
		request.GetEndEventId() == common.EmptyEventID &&
		request.GetEndEventVersion() == common.EmptyVersion {
		return errInvalidEventQueryRange
	}

	return nil
}

// DEPRECATED: DO NOT MODIFY UNLESS ALSO APPLIED TO ./service/history/api/getworkflowexecutionrawhistoryv2/api.go
func (adh *AdminHandler) setRequestDefaultValueAndGetTargetVersionHistory(
	request *adminservice.GetWorkflowExecutionRawHistoryV2Request,
	versionHistories *historyspb.VersionHistories,
) (*historyspb.VersionHistory, error) {

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

	if request.GetStartEventId() == common.EmptyEventID || request.GetStartEventVersion() == common.EmptyVersion {
		// If start event is not set, get the events from the first event
		// As the API is exclusive-exclusive, use first event id - 1 here
		request.StartEventId = common.FirstEventID - 1
		request.StartEventVersion = firstItem.GetVersion()
	}
	if request.GetEndEventId() == common.EmptyEventID || request.GetEndEventVersion() == common.EmptyVersion {
		// If end event is not set, get the events until the end event
		// As the API is exclusive-exclusive, use end event id + 1 here
		request.EndEventId = lastItem.GetEventId() + 1
		request.EndEventVersion = lastItem.GetVersion()
	}

	if request.GetStartEventId() < 0 {
		return nil, errInvalidFirstNextEventCombination
	}

	// get branch based on the end event if end event is defined in the request
	if request.GetEndEventId() == lastItem.GetEventId()+1 &&
		request.GetEndEventVersion() == lastItem.GetVersion() {
		// this is a special case, target branch remains the same
	} else {
		endItem := versionhistory.NewVersionHistoryItem(request.GetEndEventId(), request.GetEndEventVersion())
		idx, err := versionhistory.FindFirstVersionHistoryIndexByVersionHistoryItem(versionHistories, endItem)
		if err != nil {
			return nil, err
		}

		targetBranch, err = versionhistory.GetVersionHistory(versionHistories, idx)
		if err != nil {
			return nil, err
		}
	}

	startItem := versionhistory.NewVersionHistoryItem(request.GetStartEventId(), request.GetStartEventVersion())
	// If the request start event is defined. The start event may be on a different branch as current branch.
	// We need to find the LCA of the start event and the current branch.
	if request.GetStartEventId() == common.FirstEventID-1 &&
		request.GetStartEventVersion() == firstItem.GetVersion() {
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
			request.StartEventId = startItem.GetEventId()
			request.StartEventVersion = startItem.GetVersion()
		}
	}

	return targetBranch, nil
}

// DEPRECATED: DO NOT MODIFY UNLESS ALSO APPLIED TO ./service/history/api/forcedeleteworkflowexecution/api.go
func (adh *AdminHandler) getWorkflowCompletionEvent(
	ctx context.Context,
	shardID int32,
	mutableState *persistencespb.WorkflowMutableState,
) (*historypb.HistoryEvent, error) {
	executionInfo := mutableState.GetExecutionInfo()
	completionEventID := mutableState.GetNextEventId() - 1

	currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(executionInfo.VersionHistories)
	if err != nil {
		return nil, err
	}
	version, err := versionhistory.GetVersionHistoryEventVersion(currentVersionHistory, completionEventID)
	if err != nil {
		return nil, err
	}

	resp, err := adh.persistenceExecutionManager.ReadHistoryBranch(ctx, &persistence.ReadHistoryBranchRequest{
		ShardID:     shardID,
		BranchToken: currentVersionHistory.GetBranchToken(),
		MinEventID:  executionInfo.CompletionEventBatchId,
		MaxEventID:  completionEventID + 1,
		PageSize:    1,
	})
	if err != nil {
		return nil, err
	}

	// find history event from batch and return back single event to caller
	for _, e := range resp.HistoryEvents {
		if e.EventId == completionEventID && e.Version == version {
			return e, nil
		}
	}

	return nil, serviceerror.NewInternal("Unable to find closed event for workflow")
}
