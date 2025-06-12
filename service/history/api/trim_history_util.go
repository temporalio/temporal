package api

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/events"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.uber.org/fx"
)

type TrimHistoryNodeDeps struct {
	fx.In

	ShardContext               historyi.ShardContext
	Logger                     log.Logger
	WorkflowConsistencyChecker WorkflowConsistencyChecker
	EventNotifier              events.Notifier
	ExecutionManager           persistence.ExecutionManager
	Config                     *configs.Config

	GetOrPollMutableState GetOrPollMutableStateDeps
}

func (deps *TrimHistoryNodeDeps) Invoke(
	ctx context.Context,
	namespaceID string,
	workflowID string,
	runID string,
) {
	response, err := deps.GetOrPollMutableState.Invoke(
		ctx,
		&historyservice.GetMutableStateRequest{
			NamespaceId: namespaceID,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
		},
	)
	if err != nil {
		return // abort
	}

	_, err = deps.ExecutionManager.TrimHistoryBranch(ctx, &persistence.TrimHistoryBranchRequest{
		ShardID:       common.WorkflowIDToHistoryShard(namespaceID, workflowID, deps.Config.NumberOfShards),
		BranchToken:   response.CurrentBranchToken,
		NodeID:        response.GetLastFirstEventId(),
		TransactionID: response.GetLastFirstEventTxnId(),
	})
	if err != nil {
		// best effort
		deps.Logger.Error("unable to trim history branch",
			tag.WorkflowNamespaceID(namespaceID),
			tag.WorkflowID(workflowID),
			tag.WorkflowRunID(runID),
			tag.Error(err),
		)
	}
}
