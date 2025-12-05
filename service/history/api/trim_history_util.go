package api

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/events"
	historyi "go.temporal.io/server/service/history/interfaces"
)

func TrimHistoryNode(
	ctx context.Context,
	shardContext historyi.ShardContext,
	workflowConsistencyChecker WorkflowConsistencyChecker,
	eventNotifier events.Notifier,
	namespaceID string,
	workflowID string,
	runID string,
) {
	response, err := GetOrPollWorkflowMutableState(
		ctx,
		shardContext,
		&historyservice.GetMutableStateRequest{
			NamespaceId: namespaceID,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
		},
		workflowConsistencyChecker,
		eventNotifier,
	)
	if err != nil {
		return // abort
	}

	_, err = shardContext.GetExecutionManager().TrimHistoryBranch(ctx, &persistence.TrimHistoryBranchRequest{
		ShardID:       common.WorkflowIDToHistoryShard(namespaceID, workflowID, shardContext.GetConfig().NumberOfShards),
		BranchToken:   response.CurrentBranchToken,
		NodeID:        response.GetLastFirstEventId(),
		TransactionID: response.GetLastFirstEventTxnId(),
	})
	if err != nil {
		// best effort
		shardContext.GetLogger().Error("unable to trim history branch",
			tag.WorkflowNamespaceID(namespaceID),
			tag.WorkflowID(workflowID),
			tag.WorkflowRunID(runID),
			tag.Error(err),
		)
	}
}
