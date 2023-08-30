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

package forcedeleteworkflowexecution

import (
	"context"
	"fmt"
	"math"
	"time"

	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store/standard/cassandra"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/shard"
)

func Invoke(
	ctx context.Context,
	shard shard.Context,
	request *historyservice.ForceDeleteWorkflowExecutionRequest,
) (_ *historyservice.ForceDeleteWorkflowExecutionResponse, retError error) {
	namespaceID := namespace.ID(request.GetNamespaceId())
	err := api.ValidateNamespaceUUID(namespaceID)
	if err != nil {
		return nil, err
	}
	execution := request.Execution

	shardID := common.WorkflowIDToHistoryShard(
		namespaceID.String(),
		execution.GetWorkflowId(),
		shard.GetConfig().NumberOfShards,
	)
	logger := log.With(shard.GetLogger(),
		tag.WorkflowNamespaceID(request.NamespaceId),
		tag.WorkflowID(execution.WorkflowId),
		tag.WorkflowRunID(execution.RunId),
	)

	persistenceExecutionManager := shard.GetExecutionManager()

	if execution.RunId == "" {
		resp, err := persistenceExecutionManager.GetCurrentExecution(ctx, &persistence.GetCurrentExecutionRequest{
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
	var startTime, closeTime *time.Time
	cassVisBackend := shard.GetVisibilityManager().HasStoreName(cassandra.CassandraPersistenceName)

	resp, err := persistenceExecutionManager.GetWorkflowExecution(ctx, &persistence.GetWorkflowExecutionRequest{
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
			"will skip deleting workflow history and cassandra visibility record"
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

		if cassVisBackend {
			if resp.State.ExecutionState.State != enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED {
				startTime = executionInfo.GetStartTime()
			} else if executionInfo.GetCloseTime() != nil {
				closeTime = executionInfo.GetCloseTime()
			} else {
				completionEvent, err := getWorkflowCompletionEvent(ctx, shardID, resp.State, persistenceExecutionManager)
				if err != nil {
					warnMsg := "Unable to load workflow completion event, will skip deleting visibility record"
					shard.GetLogger().Warn(warnMsg, tag.Error(err))
					warnings = append(warnings, fmt.Sprintf("%s. Error: %v", warnMsg, err.Error()))
				} else {
					closeTime = completionEvent.GetEventTime()
				}
			}
		}
	}

	if !cassVisBackend || (startTime != nil || closeTime != nil) {
		// if using cass visibility, then either start or close time should be non-nil
		// NOTE: the deletion is best effort, for sql and cassandra visibility implementation,
		// we can't guarantee there's no update or record close request for this workflow since
		// visibility queue processing is async. Operator can call this api again to delete visibility
		// record again if this happens.
		if err := shard.GetVisibilityManager().DeleteWorkflowExecution(ctx, &manager.VisibilityDeleteWorkflowExecutionRequest{
			NamespaceID: namespaceID,
			WorkflowID:  execution.GetWorkflowId(),
			RunID:       execution.GetRunId(),
			TaskID:      math.MaxInt64,
			StartTime:   startTime,
			CloseTime:   closeTime,
		}); err != nil {
			return nil, err
		}
	}

	if err := persistenceExecutionManager.DeleteCurrentWorkflowExecution(ctx, &persistence.DeleteCurrentWorkflowExecutionRequest{
		ShardID:     shardID,
		NamespaceID: namespaceID.String(),
		WorkflowID:  execution.WorkflowId,
		RunID:       execution.RunId,
	}); err != nil {
		return nil, err
	}

	if err := persistenceExecutionManager.DeleteWorkflowExecution(ctx, &persistence.DeleteWorkflowExecutionRequest{
		ShardID:     shardID,
		NamespaceID: namespaceID.String(),
		WorkflowID:  execution.WorkflowId,
		RunID:       execution.RunId,
	}); err != nil {
		return nil, err
	}

	for _, branchToken := range branchTokens {
		if err := persistenceExecutionManager.DeleteHistoryBranch(ctx, &persistence.DeleteHistoryBranchRequest{
			ShardID:     shardID,
			BranchToken: branchToken,
		}); err != nil {
			warnMsg := "Failed to delete history branch, skip"
			shard.GetLogger().Warn(warnMsg, tag.WorkflowBranchID(string(branchToken)), tag.Error(err))
			warnings = append(warnings, fmt.Sprintf("%s. BranchToken: %v, Error: %v", warnMsg, branchToken, err.Error()))
		}
	}

	return &historyservice.ForceDeleteWorkflowExecutionResponse{
		Warnings: warnings,
	}, nil
}

func getWorkflowCompletionEvent(
	ctx context.Context,
	shardID int32,
	mutableState *persistencespb.WorkflowMutableState,
	persistenceExecutionManager persistence.ExecutionManager,
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

	resp, err := persistenceExecutionManager.ReadHistoryBranch(ctx, &persistence.ReadHistoryBranchRequest{
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
