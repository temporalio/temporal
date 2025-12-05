package forcedeleteworkflowexecution

import (
	"context"
	"fmt"
	"math"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
)

func Invoke(
	ctx context.Context,
	request *historyservice.ForceDeleteWorkflowExecutionRequest,
	shardID int32,
	chasmRegistry *chasm.Registry,
	persistenceExecutionMgr persistence.ExecutionManager,
	persistenceVisibilityMgr manager.VisibilityManager,
	logger log.Logger,
) (_ *historyservice.ForceDeleteWorkflowExecutionResponse, retError error) {
	req := request.Request
	execution := req.Execution

	logger = log.With(logger,
		tag.WorkflowNamespaceID(request.NamespaceId),
		tag.WorkflowID(execution.WorkflowId),
		tag.WorkflowRunID(execution.RunId),
	)

	archetypeID := request.GetArchetypeId()
	if archetypeID == chasm.UnspecifiedArchetypeID {
		archetypeID = chasm.WorkflowArchetypeID
	}

	if execution.RunId == "" {
		resp, err := persistenceExecutionMgr.GetCurrentExecution(ctx, &persistence.GetCurrentExecutionRequest{
			ShardID:     shardID,
			NamespaceID: request.NamespaceId,
			WorkflowID:  execution.WorkflowId,
			ArchetypeID: archetypeID,
		})
		if err != nil {
			return nil, err
		}
		execution.RunId = resp.RunID
	}

	var warnings []string
	var branchTokens [][]byte

	resp, err := persistenceExecutionMgr.GetWorkflowExecution(ctx, &persistence.GetWorkflowExecutionRequest{
		ShardID:     shardID,
		NamespaceID: request.NamespaceId,
		WorkflowID:  execution.WorkflowId,
		RunID:       execution.RunId,
		ArchetypeID: archetypeID,
	})
	if err != nil {
		if common.IsContextCanceledErr(err) || common.IsContextDeadlineExceededErr(err) {
			return nil, err
		}
		// continue to deletion
		warnMsg := "Unable to load mutable state. Skipping workflow history deletion."
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
	if err := persistenceVisibilityMgr.DeleteWorkflowExecution(ctx, &manager.VisibilityDeleteWorkflowExecutionRequest{
		NamespaceID: namespace.ID(request.GetNamespaceId()),
		WorkflowID:  execution.GetWorkflowId(),
		RunID:       execution.GetRunId(),
		TaskID:      math.MaxInt64,
	}); err != nil {
		return nil, err
	}

	if err := persistenceExecutionMgr.DeleteCurrentWorkflowExecution(ctx, &persistence.DeleteCurrentWorkflowExecutionRequest{
		ShardID:     shardID,
		NamespaceID: request.NamespaceId,
		WorkflowID:  execution.WorkflowId,
		RunID:       execution.RunId,
		ArchetypeID: archetypeID,
	}); err != nil {
		return nil, err
	}

	if err := persistenceExecutionMgr.DeleteWorkflowExecution(ctx, &persistence.DeleteWorkflowExecutionRequest{
		ShardID:     shardID,
		NamespaceID: request.NamespaceId,
		WorkflowID:  execution.WorkflowId,
		RunID:       execution.RunId,
		ArchetypeID: archetypeID,
	}); err != nil {
		return nil, err
	}

	for _, branchToken := range branchTokens {
		if err := persistenceExecutionMgr.DeleteHistoryBranch(ctx, &persistence.DeleteHistoryBranchRequest{
			ShardID:     shardID,
			BranchToken: branchToken,
		}); err != nil {
			warnMsg := "Failed to delete history branch, skip"
			logger.Warn(warnMsg, tag.WorkflowBranchID(string(branchToken)), tag.Error(err))
			warnings = append(warnings, fmt.Sprintf("%s. BranchToken: %v, Error: %v", warnMsg, branchToken, err.Error()))
		}
	}

	return &historyservice.ForceDeleteWorkflowExecutionResponse{
		Response: &adminservice.DeleteWorkflowExecutionResponse{
			Warnings: warnings,
		},
	}, nil
}
